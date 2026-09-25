//! Scanner orchestration: [`ProcessServer`] owns the generation pointer,
//! the scan/refresh/watcher threads and every channel.
//!
//! Reads scale lock-free: the bundle behind [`ArcSwap`], exclusions and
//! Steam roots behind `RwLock`. Only genuinely mutable per-tick state
//! (caches, pid sets, wake handles) sits behind short `Mutex` sections.

use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

use arc_swap::ArcSwap;
use rsrpc_steam::SteamLibraries;
#[cfg(not(target_os = "linux"))]
use sysinfo::System;

use crate::bundle::{DetectablesBundle, initial_bundle};
use crate::cache::AppIdMemo;
use crate::db::{DetectableActivity, Exclusions};
use crate::refresh::RefreshConfig;
use crate::types::{ProcessDetectedEvent, ProcessEventListeners};

// 0.36.1 paths kept working after the module split: these items now
// live in `runtime`, re-exported here so existing imports keep building.
pub use super::runtime::{ScanGuard, idle_wait};

pub struct ProcessServer {
  /// Current detection generation (see [`DetectablesBundle`]): lock-free
  /// reads via [`ArcSwap`], whole-generation swaps by writers. Custom
  /// overrides live in the same bundle, so user appends can never tear
  /// against the main patterns either. The `Arc` wrapper makes every
  /// clone share the same generation pointer, so hourly refreshes reach
  /// the scan loop's clone too.
  pub(crate) detectables: Arc<ArcSwap<DetectablesBundle>>,
  /// Serializes bundle writers (hourly refresh vs override rebuilds):
  /// both read the current generation, build, then store, so without
  /// this a refresh landing inside an append's window silently discards
  /// the override (or the refreshed main list). Readers are unaffected:
  /// they keep loading the lock-free pointer.
  pub(crate) writer_lock: Arc<Mutex<()>>,
  pub(crate) scanning: Arc<AtomicBool>,
  /// Double-`start` guard: a second scan generation would orphan the
  /// first loop's wake handle and double-emit EXEC hits.
  pub(crate) started: Arc<AtomicBool>,
  /// Directed shutdown, set by `shutdown`: every thread spawned by
  /// `start` observes it and exits, so `join` returns promptly. Plain
  /// flag (no lock needed: set once, read in loops).
  pub(crate) shutdown: Arc<AtomicBool>,
  /// Set by the EXEC fast path whenever it publishes a hit, consumed by
  /// the scan loop: an EXEC-published game that dies before any poll
  /// observes it would otherwise never emit its clear (the delta would
  /// see two identical empty snapshots). Forcing one full emission per
  /// EXEC publication closes that hole; repeats dedup downstream.
  pub(crate) scan_dirty: Arc<AtomicBool>,
  /// Threads registered for shutdown unpark (refresh, watcher retry):
  /// each registers once at startup; `shutdown` unparks all so no
  /// `park_timeout` sleeps out its full duration. The scan loop keeps
  /// its own single-slot `scan_wake` (EXIT wakes share it), which
  /// `shutdown` unparks too.
  pub(crate) shutdown_wake: Arc<Mutex<Vec<std::thread::Thread>>>,
  /// Join handles of every thread `start` spawned, taken by `join`.
  /// Empty before `start` and after `join` (a second `join` is safe).
  pub(crate) worker_handles: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,

  pub event_sender: rsrpc_telemetry::GaugeSender<ProcessDetectedEvent>,

  pub(crate) event_listeners: Arc<Mutex<ProcessEventListeners>>,

  /// Hourly database-refresh inputs (source URL, toggle, startup seeds).
  /// Read once when the refresh thread spawns; immutable afterwards.
  pub(crate) refresh: RefreshConfig,
  /// Pids of the currently detected games (refreshed every scan tick,
  /// plus event-driven EXEC hits). Lets the proc-events watcher wake the
  /// scan loop the moment a TRACKED game exits — untracked exits never
  /// cause a scan.
  pub detected_pids: Arc<Mutex<FxHashSet<u64>>>,
  /// Memoized SteamAppId per pid: environ never changes after exec, so
  /// one read per process lifetime suffices (environ is kilobytes — the
  /// biggest per-process cost in the profiler). Invalidated by EXEC (the
  /// watcher drops the entry before reclassifying) and by death (swept
  /// every tick against the live pid set). Bounded by live process count.
  /// Each entry carries a sequence number: a `drop_appid` racing an
  /// in-flight read bumps it, and the stale read is discarded instead of
  /// pinning a pre-exec environ for the pid lifetime.
  pub(crate) appid_cache: Arc<Mutex<FxHashMap<u64, AppIdMemo>>>,
  /// Scan thread handle for early wakeups (proc-events EXIT of a tracked
  /// game). Registered by the scan thread itself on startup.
  pub(crate) scan_wake: Arc<Mutex<Option<std::thread::Thread>>>,
  /// Last scan-loop iteration start. EXIT wakes are debounced against it:
  /// Proton games spawn/die short-lived helpers constantly, and every one
  /// of them matches the game — without this, tracked exits unpark the
  /// loop several times per second (measured 0.76s effective cadence
  /// instead of 5s during NFS). Minimum 1s between early scans.
  pub last_scan: Arc<Mutex<std::time::Instant>>,
  /// Discord detection exclusions (installer/crash-reporter basenames +
  /// regexes): excluded processes are dropped before any matching.
  /// Empty until [`ProcessServer::set_exclusions`] (startup fetch) or the
  /// hourly refresh fills it; empty behaves exactly like no exclusions.
  /// Reads dominate (one per process per tick), hence `RwLock`.
  pub(crate) exclusions: Arc<RwLock<Exclusions>>,
  /// Steam install-dir -> AppId (VDF provider): refreshed once per scan
  /// tick when a `libraryfolders.vdf` changed, consulted on path misses.
  pub(crate) steam_libraries: Arc<RwLock<SteamLibraries>>,
  /// Application IDs never published by the scan thread (coexistence with
  /// a richer publisher elsewhere). Filtered right after the scan, so an
  /// ignored-only result behaves exactly like no game: null event, clear.
  /// Hash set (built once): consulted per detected game per tick.
  pub(crate) ignored_ids: HashSet<String>,
  /// Event-driven proc-events watcher (netlink `cn_proc` fast path)
  /// on/off. Plain bool (no lock needed: written once via
  /// [`ProcessServer::set_proc_events`] before [`ProcessServer::start`],
  /// read once there). `true` unless `--no-proc-events` opted out.
  /// Linux-only read (other platforms have no watcher); the allow keeps
  /// cross-platform builds warning-free.
  #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
  pub enable_proc_events: bool,
  /// Whether the watcher socket is currently believed up. Set around the
  /// blocking `watch()` call (Linux only); read every scan tick to gate
  /// idle backoff. Starts `false` (pure polling) so macOS, Windows,
  /// disabled watchers and failure windows never back off. `Relaxed` would
  /// suffice for a cadence hint, but `Acquire`/`Release` is free here and
  /// keeps the watcher-state reasoning uniform.
  pub(crate) watcher_live: Arc<AtomicBool>,

  #[cfg(not(target_os = "linux"))]
  pub(crate) sysinfo: Arc<Mutex<System>>,
}

/// Return one-time parse arenas (fetch body, JSON DOM, trimmed copy,
/// automaton build scratch) to the OS. Steady state is the lean
/// structures only; without this, the allocator holds the startup spike
/// as RSS indefinitely. Called after the initial build and every hourly
/// rebuild (refresh cadence itself is unchanged). Shared with the
/// database module, which rebuilds generations on the same path.
pub(crate) fn release_parse_arenas() {
  release_platform_arenas();
}

/// Live vs free heap bytes from the allocator (glibc `mallinfo2`):
/// distinguishes live objects from retained arenas (`live` growing vs
/// `free` growing with a flat RSS floor).
/// `None` off glibc-Linux (musl, macOS, Windows), where the post-retrim
/// line simply carries no suffix. Permanent telemetry, not DIAG: one
/// line per installed rebuild, zero retained state.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) fn allocator_numbers() -> Option<(u64, u64)> {
  // SAFETY: `mallinfo2` only reads allocator statistics; it neither
  // allocates nor mutates anything.
  let info = unsafe { libc::mallinfo2() };
  #[allow(clippy::cast_possible_truncation)]
  Some((info.uordblks as u64, info.fordblks as u64))
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub(crate) fn allocator_numbers() -> Option<(u64, u64)> {
  None
}

/// Linux (glibc/musl).
#[cfg(target_os = "linux")]
fn release_platform_arenas() {
  // SAFETY: malloc_trim only advises the allocator to release free pages;
  // it cannot invalidate live allocations, so it is always safe to call.
  unsafe {
    libc::malloc_trim(0);
  }
}

/// macOS: drain purgeable memory in all zones.
#[cfg(target_os = "macos")]
fn release_platform_arenas() {
  // Declared locally: libc 0.2 exposes only the zone-struct field, not
  // this stable Darwin function (malloc/malloc.h, present since 10.6).
  // `core::ffi` types keep this branch dependency-free (`libc` is only a
  // Linux dependency of this crate).
  unsafe extern "C" {
    /// Advisory purge of malloc zones (NULL zone = all zones, goal 0 = no target).
    fn malloc_zone_pressure_relief(zone: *mut core::ffi::c_void, goal: usize) -> usize;
  }
  // SAFETY: (NULL, 0) means "all zones, no goal" and is advisory-only;
  // it cannot invalidate live allocations.
  unsafe {
    malloc_zone_pressure_relief(std::ptr::null_mut(), 0);
  }
}

/// Windows: compact our own process heap.
#[cfg(target_os = "windows")]
fn release_platform_arenas() {
  // SAFETY: HeapCompact with flags=0 only coalesces free blocks of the
  // given heap; it cannot invalidate live allocations.
  unsafe {
    let heap = winapi::um::heapapi::GetProcessHeap();
    if !heap.is_null() {
      winapi::um::heapapi::HeapCompact(heap, 0);
    }
  }
}

/// Other platforms: nothing to release through a stable API.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn release_platform_arenas() {}

impl Clone for ProcessServer {
  /// Share every handle (generation pointer, channels, caches, locks).
  fn clone(&self) -> Self {
    Self {
      // Shared generation pointer: clones (scan loop, refresh thread)
      // must observe each other's swaps.
      detectables: Arc::clone(&self.detectables),
      writer_lock: Arc::clone(&self.writer_lock),
      scanning: Arc::clone(&self.scanning),
      started: Arc::clone(&self.started),
      shutdown: Arc::clone(&self.shutdown),
      scan_dirty: Arc::clone(&self.scan_dirty),
      shutdown_wake: Arc::clone(&self.shutdown_wake),
      worker_handles: Arc::clone(&self.worker_handles),
      event_sender: self.event_sender.clone(),
      event_listeners: Arc::clone(&self.event_listeners),
      refresh: self.refresh.clone(),
      detected_pids: Arc::clone(&self.detected_pids),
      appid_cache: Arc::clone(&self.appid_cache),
      scan_wake: Arc::clone(&self.scan_wake),
      last_scan: Arc::clone(&self.last_scan),
      exclusions: Arc::clone(&self.exclusions),
      steam_libraries: Arc::clone(&self.steam_libraries),
      ignored_ids: self.ignored_ids.clone(),
      enable_proc_events: self.enable_proc_events,
      watcher_live: Arc::clone(&self.watcher_live),
      #[cfg(not(target_os = "linux"))]
      sysinfo: Arc::clone(&self.sysinfo),
    }
  }
}

impl ProcessServer {
  /// Folds `custom` overrides into the initial build: one automaton
  /// construction instead of build-then-rebuild. Startup (and one-shot
  /// diagnostics) use this so staged overrides never cost a second full
  /// build per boot.
  pub fn new_with_custom(
    detectable: Vec<Arc<DetectableActivity>>,
    custom: Vec<DetectableActivity>,
    event_sender: rsrpc_telemetry::GaugeSender<ProcessDetectedEvent>,
    event_listeners: ProcessEventListeners,
    refresh: RefreshConfig,
    ignored_ids: Vec<String>,
  ) -> Self {
    let bundle = initial_bundle(detectable, custom);

    let server = ProcessServer {
      scanning: Arc::new(AtomicBool::new(false)),
      started: Arc::new(AtomicBool::new(false)),
      shutdown: Arc::new(AtomicBool::new(false)),
      scan_dirty: Arc::new(AtomicBool::new(false)),
      shutdown_wake: Arc::new(Mutex::new(Vec::new())),
      worker_handles: Arc::new(Mutex::new(Vec::new())),
      detectables: Arc::new(ArcSwap::new(bundle)),
      writer_lock: Arc::new(Mutex::new(())),
      event_sender,

      // Event listeners
      event_listeners: Arc::new(Mutex::new(event_listeners)),

      // Detectable database auto-refresh
      refresh,
      ignored_ids: ignored_ids.into_iter().collect(),
      enable_proc_events: true,
      watcher_live: Arc::new(AtomicBool::new(false)),
      exclusions: Arc::new(RwLock::new(Exclusions::default())),
      steam_libraries: Arc::new(RwLock::new(SteamLibraries::discover())),
      detected_pids: Arc::new(Mutex::new(FxHashSet::default())),
      appid_cache: Arc::new(Mutex::new(FxHashMap::default())),
      scan_wake: Arc::new(Mutex::new(None)),
      last_scan: Arc::new(Mutex::new(std::time::Instant::now())),

      // sysinfo System
      #[cfg(not(target_os = "linux"))]
      sysinfo: Arc::new(Mutex::new(System::new())),
    };

    // One-time parse arenas are now garbage: steady state is the lean
    // structures just built.
    release_parse_arenas();

    server
  }

  /// Opt out of the event-driven proc-events watcher (called once,
  /// before [`ProcessServer::start`]). Polling continues either way.
  pub fn set_proc_events(&mut self, enable: bool) {
    self.enable_proc_events = enable;
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::time::Duration;

  use crate::runtime::{
    exec_hit_ignored, removed_slots, scan_cadence, should_exit_on_send_error, unpark_scan_wake,
  };
  use crate::types::{ProcessDetectedEvent, ScannedEntry, ScannedHit};

  /// Empty-database server with a live gauge sender for unit tests.
  fn fixture_server() -> ProcessServer {
    let (tx, _rx) = rsrpc_telemetry::QueueGauge::pair();
    ProcessServer::new_with_custom(
      vec![],
      vec![],
      tx,
      ProcessEventListeners::default(),
      RefreshConfig::default(),
      vec![],
    )
  }

  /// Single shared-generation custom entry (id 777).
  fn custom_entry() -> DetectableActivity {
    custom_entry_named("777")
  }

  /// Custom entry with a caller-chosen id for concurrency tests.
  fn custom_entry_named(id: &str) -> DetectableActivity {
    serde_json::from_value(serde_json::json!({
      "id": id,
      "name": format!("Shared Generation {id}"),
      "hook": true,
      "executables": [{"name": format!("shared-{id}.exe"), "is_launcher": false, "os": "win32"}],
    }))
    .expect("fixture parses")
  }

  /// A wide main database: widens the refresh's load->store window so a
  /// concurrent append is actually likely to land inside it.
  fn main_entries(count: usize) -> Vec<DetectableActivity> {
    (0..count)
      .map(|i| {
        serde_json::from_value(serde_json::json!({
          "id": format!("main-{i}"),
          "name": format!("Main {i}"),
          "hook": true,
          "executables": [{"name": format!("main-{i}.exe"), "is_launcher": false, "os": "win32"}],
        }))
        .expect("fixture parses")
      })
      .collect()
  }

  /// Vanished slots (including pid rotations) report once; survivors never do.
  #[test]
  fn removed_slots_reports_only_vanished_ids() {
    let previous = vec![("a".to_string(), 1), ("b".to_string(), 2)];
    let current = vec![("b".to_string(), 2)];
    assert_eq!(
      removed_slots(&previous, &current),
      vec![("a".to_string(), 1)]
    );
    // Identical tables: nothing to clear (downstream dedups repeats).
    assert!(removed_slots(&current, &current).is_empty());
    // Empty current: full-table clear path handles it, not per-slot.
    // `removed_slots` still reports all previous as vanished; the caller
    // only uses it when `detected` is non-empty (see emission block).
    assert_eq!(removed_slots(&previous, &[]).len(), 2);
    // Pid replacement rotates the card: same id, new pid reports the old
    // pair as removed so the bridge clears before republishing.
    let restarted = vec![("a".to_string(), 9), ("b".to_string(), 2)];
    assert_eq!(
      removed_slots(&previous, &restarted),
      vec![("a".to_string(), 1)]
    );
  }

  /// A dropped receiver fails sends, and the policy says exit (no retry).
  #[test]
  fn closed_queue_exits_scan_instead_of_retrying() {
    // `GaugeSender` wraps an unbounded `std::mpsc`: `Err` means the
    // receiver is gone (shutdown), never transient backpressure.
    let (tx, rx) = rsrpc_telemetry::QueueGauge::pair();
    drop(rx);
    let failed: Result<(), std::sync::mpsc::SendError<ProcessDetectedEvent>> =
      tx.send(ProcessDetectedEvent::cleared());
    assert!(failed.is_err());
    assert!(should_exit_on_send_error(&failed));
  }

  /// Ignored app ids never publish on the EXEC fast path either.
  #[test]
  fn exec_path_skips_ignored_ids() {
    use std::collections::HashSet;
    let hit = ScannedHit::stamp(
      std::sync::Arc::new(ScannedEntry::from_activity(&custom_entry())),
      4242,
    );
    let ignored: HashSet<String> = ["777".to_string()].into_iter().collect();
    assert!(exec_hit_ignored(&ignored, &hit));
    let empty: HashSet<String> = HashSet::new();
    assert!(!exec_hit_ignored(&empty, &hit));
  }

  /// Clones share one generation pointer in both directions.
  #[test]
  fn clones_share_one_detection_generation() {
    // The scan loop runs on a clone made in `start()`, while the hourly
    // refresh swaps through its own clone: both must observe the same
    // generation pointer, or refreshed databases never reach the scan.
    let server = fixture_server();
    let scan_clone = server.clone();
    server.append_detectables(vec![custom_entry()]);
    assert_eq!(
      scan_clone.bundle().custom.len(),
      server.bundle().custom.len(),
      "clone must observe overrides appended after cloning"
    );
    assert_eq!(scan_clone.bundle().custom.len(), 1);

    // The reverse direction holds too: a swap through the clone is
    // visible to the original.
    let reverse = scan_clone.clone();
    scan_clone.append_detectables(vec![custom_entry()]);
    assert_eq!(reverse.bundle().custom.len(), server.bundle().custom.len());
    assert_eq!(reverse.bundle().custom.len(), 2);
  }

  /// Concurrent appends and refreshes lose neither side (writer lock).
  #[test]
  fn watcher_failure_unparks_stale_scan() {
    // A parked thread woken through the helper must observe the permit:
    // park/unpark pairing is the whole contract (no scan logic here).
    let woken = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = woken.clone();
    let waker = std::thread::spawn(move || {
      std::thread::park();
      flag.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let slot: Arc<Mutex<Option<std::thread::Thread>>> =
      Arc::new(Mutex::new(Some(waker.thread().clone())));
    unpark_scan_wake(&slot);
    waker.join().expect("helper must unpark the waiter");
    assert!(woken.load(std::sync::atomic::Ordering::SeqCst));
    // Missing handle or lock: silent no-ops, never panics.
    unpark_scan_wake(&Arc::new(Mutex::new(None)));
  }

  /// Backoff stretches idle ticks only while the watcher is confirmed live.
  #[test]
  fn backoff_applies_only_with_live_watcher() {
    use std::time::Duration;
    let base = Duration::from_secs(5);
    // Polling-only paths (other OSes, disabled/failed watcher) never back
    // off: a short configured interval stays short.
    assert_eq!(scan_cadence(base, 4, false), base);
    assert_eq!(scan_cadence(base, 0, false), base);
    // Live watcher: idle ticks stretch up to the 30s cap.
    assert_eq!(scan_cadence(base, 0, true), base);
    assert_eq!(scan_cadence(base, 4, true), Duration::from_secs(30));
  }

  /// Refused bundle swaps report failure so validators are never committed.
  #[test]
  fn refused_bundle_swap_reports_failure() {
    // Empty input and failing builds must report failure so the refresh
    // thread keeps its old validators (next hour retries the data instead
    // of trusting tags for a bundle that was never installed).
    let server = fixture_server();
    assert!(!server.update_main_detectables(vec![]));
    assert!(server.update_main_detectables(vec![custom_entry()]));
  }

  /// Concurrent appends and refreshes lose neither side (writer lock).
  #[test]
  fn concurrent_writers_preserve_both_sides() {
    // Refresh (`update_main_detectables`) and override rebuilds
    // (`rebuild_custom`) both load -> build -> store: with a shared
    // generation and no serialization, one store can silently discard
    // the other's input. Every append must survive the concurrent
    // refreshes (the writer lock serializes them).
    const THREADS: usize = 4;
    const PER_THREAD: usize = 8;
    const REFRESHES: usize = 20;
    let server = fixture_server();
    std::thread::scope(|scope| {
      for t in 0..THREADS {
        let writer = server.clone();
        scope.spawn(move || {
          for i in 0..PER_THREAD {
            writer.append_detectables(vec![custom_entry_named(&format!("{t}-{i}"))]);
            // Spread appends over the refresh window so the writers
            // actually overlap (a stress test, not a timing benchmark).
            std::thread::sleep(std::time::Duration::from_micros(250));
          }
        });
      }
      let refresher = server.clone();
      scope.spawn(move || {
        for _ in 0..REFRESHES {
          refresher.update_main_detectables(main_entries(300));
        }
      });
    });
    assert_eq!(
      server.bundle().custom.len(),
      THREADS * PER_THREAD,
      "no override may be lost to a concurrent refresh"
    );
    assert_eq!(server.bundle().list.len(), 300, "refresh result must stick");
  }

  /// `shutdown()` before `start()` is safe; `join()` with no threads
  /// returns at once.
  #[test]
  fn shutdown_before_start_is_safe() {
    let server = fixture_server();
    server.shutdown();
    server.join();
    server.shutdown();
    server.join();
  }

  /// Join a live scan thread through a timeout channel: `shutdown()` must
  /// unpark it and `join()` must return promptly (pre-shutdown-lifecycle
  /// this hung: the parked loop had no directed exit).
  #[test]
  fn shutdown_joins_scan_thread_promptly() {
    let mut server = fixture_server();
    server.set_proc_events(false);
    let watch_slot = Arc::new(Mutex::new(rsrpc_telemetry::QueueGauge::new()));
    server.start(Duration::from_millis(50), &watch_slot);
    // A few ticks so the thread is parked in its cadence wait, not still
    // starting up, when shutdown lands.
    std::thread::sleep(Duration::from_millis(200));
    server.shutdown();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
      server.join();
      // Structural proof alongside the prompt return below: `join`
      // drains the handle registry, so no worker is merely detached.
      let drained = server
        .worker_handles
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_empty();
      let _ = done_tx.send(drained);
    });
    assert!(
      done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("join must return promptly after shutdown"),
      "handle registry must drain on join"
    );
  }

  /// The hourly refresh sleep is interruptible: an enabled refresh whose
  /// URL refuses fast must still join promptly instead of sleeping out
  /// the hour. `127.0.0.1:9` is the discard port (refused, loopback-only,
  /// no external network); exclusions stay unset so only the DB fetch
  /// runs before the sleep.
  #[test]
  fn refresh_sleep_is_interrupted_by_shutdown() {
    let mut server = fixture_server();
    server.set_proc_events(false);
    server.refresh.enable = true;
    server.refresh.db_url = Some("http://127.0.0.1:9/unreachable".to_string());
    let watch_slot = Arc::new(Mutex::new(rsrpc_telemetry::QueueGauge::new()));
    server.start(Duration::from_millis(50), &watch_slot);
    // Let the refresh thread fail its first fetch and enter the hourly
    // sleep; the scan thread ticks alongside, both must stop below.
    std::thread::sleep(Duration::from_millis(500));
    server.shutdown();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
      server.join();
      let drained = server
        .worker_handles
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_empty();
      let _ = done_tx.send(drained);
    });
    assert!(
      done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("refresh sleep must not outlive shutdown"),
      "handle registry must drain on join"
    );
  }

  /// Spawned workers carry `rsrpc-*` thread names (Linux comm): join
  /// accounting and teardown tests observe threads by name. Sibling
  /// lifecycle tests may add their own `rsrpc-*` threads, so this
  /// asserts presence only — absence is asserted per-server via the
  /// drained handle registry (join tests above) and per-daemon via
  /// task enumeration (core daemon tests).
  #[cfg(target_os = "linux")]
  #[test]
  fn worker_threads_carry_names() {
    fn has_prefix(prefix: &str) -> bool {
      let mut found = false;
      if let Ok(tasks) = std::fs::read_dir("/proc/self/task") {
        for task in tasks.flatten() {
          if let Ok(comm) = std::fs::read_to_string(task.path().join("comm"))
            && comm.trim().starts_with(prefix)
          {
            found = true;
            break;
          }
        }
      }
      found
    }
    let mut server = fixture_server();
    server.set_proc_events(false);
    let watch_slot = Arc::new(Mutex::new(rsrpc_telemetry::QueueGauge::new()));
    server.start(Duration::from_millis(50), &watch_slot);
    std::thread::sleep(Duration::from_millis(200));
    assert!(has_prefix("rsrpc-scan"), "scan thread must be named");
    server.shutdown();
    server.join();
  }
}
