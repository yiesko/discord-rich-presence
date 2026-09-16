//! Scanner orchestration: [`ProcessServer`] owns the generation pointer,
//! the scan/refresh/watcher threads and every channel.
//!
//! Reads scale lock-free: the bundle behind [`ArcSwap`], exclusions and
//! Steam roots behind `RwLock`. Only genuinely mutable per-tick state
//! (caches, pid sets, wake handles) sits behind short `Mutex` sections.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

#[cfg(target_os = "linux")]
use crate::scan::read_exec;
use arc_swap::ArcSwap;
use rsrpc_steam::SteamLibraries;
#[cfg(not(target_os = "linux"))]
use sysinfo::System;

use crate::bundle::{DetectablesBundle, build_bundle, initial_bundle};
use crate::cache::AppIdMemo;
use crate::db::{DetectableActivity, Exclusions};
use crate::refresh::RefreshConfig;
use crate::refresh::{FetchOutcome, fetch_detectable_etag};
use crate::scan::apply_ignore_list;
use crate::scan::first_sightings;
use crate::types::{
  Exec, ProcessDetectedEvent, ProcessEventListeners, ProcessScanState, ScannedEntry, ScannedHit,
};

pub struct ProcessServer {
  /// Current detection generation (see [`DetectablesBundle`]): lock-free
  /// reads via [`ArcSwap`], whole-generation swaps by writers. Custom
  /// overrides live in the same bundle, so user appends can never tear
  /// against the main patterns either.
  pub(crate) detectables: ArcSwap<DetectablesBundle>,
  pub(crate) scanning: Arc<AtomicBool>,
  /// Double-`start` guard: a second scan generation would orphan the
  /// first loop's wake handle and double-emit EXEC hits.
  pub(crate) started: Arc<AtomicBool>,
  /// Set by the EXEC fast path whenever it publishes a hit, consumed by
  /// the scan loop: an EXEC-published game that dies before any poll
  /// observes it would otherwise never emit its clear (the delta would
  /// see two identical empty snapshots). Forcing one full emission per
  /// EXEC publication closes that hole; repeats dedup downstream.
  pub(crate) scan_dirty: Arc<AtomicBool>,

  pub event_sender: rsrpc_telemetry::GaugeSender<ProcessDetectedEvent>,

  pub(crate) event_listeners: Arc<Mutex<ProcessEventListeners>>,

  /// Hourly database-refresh inputs (source URL, toggle, startup seeds).
  /// Read once when the refresh thread spawns; immutable afterwards.
  pub(crate) refresh: RefreshConfig,
  /// Pids of the currently detected games (refreshed every scan tick,
  /// plus event-driven EXEC hits). Lets the proc-events watcher wake the
  /// scan loop the moment a TRACKED game exits — untracked exits never
  /// cause a scan.
  pub detected_pids: Arc<Mutex<HashSet<u64>>>,
  /// Memoized SteamAppId per pid: environ never changes after exec, so
  /// one read per process lifetime suffices (environ is kilobytes — the
  /// biggest per-process cost in the profiler). Invalidated by EXEC (the
  /// watcher drops the entry before reclassifying) and by death (swept
  /// every tick against the live pid set). Bounded by live process count.
  /// Each entry carries a sequence number: a `drop_appid` racing an
  /// in-flight read bumps it, and the stale read is discarded instead of
  /// pinning a pre-exec environ for the pid lifetime.
  pub(crate) appid_cache: Arc<Mutex<HashMap<u64, AppIdMemo>>>,
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

  #[cfg(not(target_os = "linux"))]
  pub(crate) sysinfo: Arc<Mutex<System>>,
}

/// Re-entrancy guard for [`ProcessServer::scan_for_processes`]: acquired
/// atomically, released on drop (all exit paths, panics included).
pub struct ScanGuard {
  flag: Arc<AtomicBool>,
}

impl ScanGuard {
  pub fn try_acquire(flag: &Arc<AtomicBool>) -> Option<Self> {
    flag
      .compare_exchange(
        false,
        true,
        std::sync::atomic::Ordering::Acquire,
        std::sync::atomic::Ordering::Relaxed,
      )
      .ok()?;
    Some(Self {
      flag: Arc::clone(flag),
    })
  }
}

impl Drop for ScanGuard {
  fn drop(&mut self) {
    self.flag.store(false, std::sync::atomic::Ordering::Release);
  }
}

/// Sleep the scan cadence. `park_timeout` (not `sleep`) so the proc-events
/// watcher can wake the loop early when a tracked game exits; a permit
/// stored by an unpark during scan work just causes one early rescan,
/// which downstream dedups harmlessly.
fn wait_scan(wait_time: Duration) {
  std::thread::park_timeout(wait_time);
}

/// Idle-stretched cadence: base × 2^idle_ticks, capped at 30s
/// (5s → 10s → 20s → 30s at the default base). Overflow-safe.
pub fn idle_wait(base: Duration, idle_ticks: u32) -> Duration {
  const MAX_BACKOFF: Duration = Duration::from_secs(30);
  let stretched = base
    .checked_mul(1 << idle_ticks.min(4))
    .unwrap_or(MAX_BACKOFF);
  stretched.min(MAX_BACKOFF)
}

/// Spawn the netlink dispatch (Linux): EXEC classifies one process and
/// emits hits at once; EXIT of a tracked game unparks the scan loop for
/// an immediate natural clear. Setup failure (or a dead receiver on
/// shutdown) ends the thread quietly — polling carries on.
/// Spawn the netlink watcher + dispatch threads. Returns the shared watch
/// backlog gauge so the resource census can read it (the pair itself stays
/// inside: `start` hands the gauge up to the daemon).
#[cfg(target_os = "linux")]
fn spawn_proc_watcher(server: &ProcessServer) -> rsrpc_telemetry::QueueGauge {
  use rsrpc_proc_events::{ProcEvent, watch};
  use rsrpc_telemetry::QueueGauge;

  let (tx, rx) = QueueGauge::pair();
  let gauge = tx.gauge();
  let dispatch = server.clone();
  std::thread::spawn(move || {
    let mut variant_bufs: [String; 5] = Default::default();
    let mut reversed_path = String::with_capacity(256);
    while let Ok(event) = rx.recv() {
      match event {
        ProcEvent::Exec(pid) => {
          let Some(exec) = read_exec(pid) else {
            tracing::debug!("[Process Scanner] exec event: pid {pid} unreadable, skipping");
            continue;
          };
          // Same pid, new image: the memoized AppId may be stale.
          dispatch.drop_appid(pid);
          // One generation for the whole classification: a refresh
          // landing mid-probe can only swap in the next bundle, which
          // this event simply won't see.
          let bundle = dispatch.bundle();
          let mut obs_open = false;
          if let Some(hit) = dispatch.match_process(
            &exec,
            &bundle,
            &mut variant_bufs,
            &mut reversed_path,
            &mut obs_open,
          ) {
            let game_pid = hit.pid;
            tracing::debug!(
              "[Process Scanner] exec event: pid {pid} matched {}",
              hit.entry.name
            );
            dispatch
              .detected_pids
              .lock()
              .unwrap_or_else(|e| e.into_inner())
              .insert(game_pid);
            // Mark the scan dirty: this publication bypasses the polling
            // snapshot, so the next tick must emit its full table even if
            // unchanged — otherwise a game that dies before any poll
            // observes it would never emit its clear (see `scan_dirty`).
            dispatch
              .scan_dirty
              .store(true, std::sync::atomic::Ordering::Release);
            // Receiver gone means shutdown: end the thread, polling dies
            // with the daemon anyway.
            if dispatch
              .event_sender
              .send(ProcessDetectedEvent { hit: Some(hit) })
              .is_err()
            {
              break;
            }
          }
        }
        ProcEvent::Exit(pid) => {
          // Only tracked games MAY wake the scan — decided in one place
          // so the debounce is unit-testable (see below).
          if dispatch.should_wake_on_exit(pid)
            && let Some(thread) = dispatch
              .scan_wake
              .lock()
              .unwrap_or_else(|e| e.into_inner())
              .as_ref()
          {
            thread.unpark();
          }
        }
      }
    }
  });
  // Best-effort watcher with periodic resubscribe: a failed self-test
  // (or a mid-run socket death) falls back to polling, but a transient
  // kernel stall must not pin polling until the next daemon restart —
  // delivery has been observed to resume on its own. First failure warns
  // (the documented sandbox diagnosis), later ones stay in debug so
  // genuinely unsupported systems do not log every 5 minutes forever.
  // A sleeping retry never delays shutdown: process exit does not wait
  // for this thread.
  std::thread::spawn(move || {
    let mut attempts = 0u32;
    loop {
      match watch(&tx) {
        // Receiver gone: daemon shutting down.
        Ok(()) => break,
        Err(err) => {
          attempts = attempts.saturating_add(1);
          if attempts == 1 {
            tracing::warn!(
              "[Process Scanner] proc-events unavailable ({err}), polling only; retrying"
            );
          } else {
            tracing::debug!(
              "[Process Scanner] proc-events still unavailable ({err}), polling only"
            );
          }
          std::thread::sleep(std::time::Duration::from_secs(5 * 60));
        }
      }
    }
  });
  gauge
}

/// Return one-time parse arenas (fetch body, JSON DOM, trimmed copy,
/// automaton build scratch) to the OS. Steady state is the lean
/// structures only; without this, the allocator holds the startup spike
/// as RSS indefinitely. Called after the initial build and every hourly
/// rebuild (refresh cadence itself is unchanged).
fn release_parse_arenas() {
  release_platform_arenas();
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
  unsafe extern "C" {
    fn malloc_zone_pressure_relief(zone: *mut libc::c_void, goal: libc::size_t) -> libc::size_t;
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
  fn clone(&self) -> Self {
    Self {
      // Fresh ArcSwap generation pointer: the clone classifies against
      // the same generation until the next swap lands in both.
      detectables: ArcSwap::new(self.detectables.load_full()),
      scanning: Arc::clone(&self.scanning),
      started: Arc::clone(&self.started),
      scan_dirty: Arc::clone(&self.scan_dirty),
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
      scan_dirty: Arc::new(AtomicBool::new(false)),
      detectables: ArcSwap::new(bundle),
      event_sender,

      // Event listeners
      event_listeners: Arc::new(Mutex::new(event_listeners)),

      // Detectable database auto-refresh
      refresh,
      ignored_ids: ignored_ids.into_iter().collect(),
      enable_proc_events: true,
      exclusions: Arc::new(RwLock::new(Exclusions::default())),
      steam_libraries: Arc::new(RwLock::new(SteamLibraries::discover())),
      detected_pids: Arc::new(Mutex::new(HashSet::new())),
      appid_cache: Arc::new(Mutex::new(HashMap::new())),
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

  /// Rebuild the bundle with a new custom list, swapping the pointer in
  /// one write: no scan can ever observe a half-rebuilt custom automaton.
  fn rebuild_custom(&self, custom: Vec<Arc<ScannedEntry>>) {
    tracing::info!(
      "[Process Scanner] Updating Aho-Corasick patterns for custom detectable activities..."
    );
    let current = self.detectables.load_full();
    let next = match build_bundle(current.list.clone(), custom) {
      Ok(next) => Arc::new(next),
      Err(e) => {
        tracing::warn!(
          "[Process Scanner] Refusing custom rebuild ({}), keeping current",
          e
        );
        return;
      }
    };
    self.detectables.store(next);
    tracing::info!("[Process Scanner] Done!");
    // Rebuilt automaton state is lean; the transient scratch is garbage.
    release_parse_arenas();
  }

  /// Replace the main detectable games database at runtime (used by the
  /// periodic refresh), rebuilding the whole bundle and swapping it in
  /// one pointer write.
  fn update_main_detectables(&self, detectable: Vec<DetectableActivity>) {
    // Never swap in an empty database (outage returning `[]`, corrupt
    // fetch): it would build a failing automaton and blind detection.
    // Keep serving the current data instead.
    if detectable.is_empty() {
      tracing::warn!(
        "[Process Scanner] Refusing empty detectable database update, keeping current"
      );
      return;
    }
    tracing::info!(
      "[Process Scanner] Rebuilding Aho-Corasick patterns for main detectable activities..."
    );
    // Move (not clone) into the slim form, then drop the input before
    // the automata build below (~tens of MB of scratch): the fat structs
    // must not ride along to the end of this function and double the
    // rebuild peak.
    let slim: Vec<Arc<ScannedEntry>> = detectable
      .into_iter()
      .map(|entry| Arc::new(ScannedEntry::from_owned(entry)))
      .collect();
    let detectable = slim;
    let custom = self.detectables.load().custom.clone();
    let next = match build_bundle(detectable, custom) {
      Ok(next) => Arc::new(next),
      Err(e) => {
        tracing::warn!(
          "[Process Scanner] Refusing detectable database update ({}), keeping current",
          e
        );
        return;
      }
    };
    // load_full bumps the count by one transiently; subtract it back so
    // the diagnostic reports pins held by real readers.
    let old_count = Arc::strong_count(&self.detectables.load_full()).saturating_sub(1);
    let rss_before = rsrpc_telemetry::rss_bytes();
    self.detectables.store(next);
    let rss_after = rsrpc_telemetry::rss_bytes();
    tracing::info!(
      "[Process Scanner] Done! (bundle swap old_refs={} rss_before={} rss_after={})",
      old_count,
      rss_before
        .map(|b| format!("{:.1}MB", b as f64 / 1_048_576.0))
        .unwrap_or_else(|| "n/a".to_string()),
      rss_after
        .map(|b| format!("{:.1}MB", b as f64 / 1_048_576.0))
        .unwrap_or_else(|| "n/a".to_string())
    );
    // Fetch string, JSON DOM and trimmed copy are now garbage: hand the
    // hourly spike back (refresh cadence itself is unchanged).
    release_parse_arenas();
    if let Some(rss) = rsrpc_telemetry::rss_bytes() {
      tracing::info!(
        "[Process Scanner] post-trim rss={:.1}MB",
        rss as f64 / 1_048_576.0
      );
    }
  }

  pub fn append_detectables(&self, detectable: Vec<DetectableActivity>) {
    // Append to the custom list, since that's what is actually scanned.
    // Full public entries convert once to the slim scanner form here,
    // moving (not cloning) their strings.
    let mut custom = self.detectables.load().custom.clone();
    custom.extend(
      detectable
        .into_iter()
        .map(|entry| Arc::new(ScannedEntry::from_owned(entry))),
    );
    self.rebuild_custom(custom);
  }

  pub fn remove_detectable_by_name(&self, name: &str) {
    let mut custom = self.detectables.load().custom.clone();
    custom.retain(|x| {
      let current: &str = &x.name;
      current != name
    });
    self.rebuild_custom(custom);
  }

  /// Replace the exclusions set (startup fetch, tests). The hourly refresh
  /// thread overwrites it on the same cadence when `exclusions_url` is set.
  pub fn set_exclusions(&self, exclusions: Exclusions) {
    *self.exclusions.write().unwrap_or_else(|e| e.into_inner()) = exclusions;
  }

  /// Replace the Steam libraries map (hermetic construction seam for
  /// tests and tooling; production discovers at construction and refreshes
  /// per tick).
  pub fn set_steam_libraries(&self, libraries: SteamLibraries) {
    *self
      .steam_libraries
      .write()
      .unwrap_or_else(|e| e.into_inner()) = libraries;
  }

  /// AppId whose Steam install dir prefixes `normalized_path` (already
  /// lowercased `/`-separated). Cloned out of the lock; tiny strings.
  pub fn steam_prefix_app_id(&self, normalized_path: &str) -> Option<String> {
    self
      .steam_libraries
      .read()
      .unwrap_or_else(|e| e.into_inner())
      .match_prefix(normalized_path)
      .map(str::to_string)
  }

  /// Whether a tracked game's EXIT should wake the scan loop early.
  /// Untracked exits never wake (one HashSet lookup, consumed either
  /// way — dead is dead). Tracked ones wake at most once per second:
  /// a recent EXIT stays tracked so a follow-up EXIT can still wake —
  /// only an actual wake consumes the pid. Proton helpers die
  /// constantly, and every one of them matches the game, so unwedged
  /// wakes would unpark the loop several times per second (measured
  /// 0.76s effective cadence instead of 5s during NFS).
  #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
  pub fn should_wake_on_exit(&self, pid: u64) -> bool {
    if !self
      .detected_pids
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .contains(&pid)
    {
      return false;
    }
    let due = self
      .last_scan
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .elapsed()
      .as_secs()
      >= 1;
    if due {
      self
        .detected_pids
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&pid);
    }
    due
  }

  /// Revalidate the Steam libraries (stats only unless something changed).
  /// Called once per scan tick; the scan loop goes through here so tests
  /// can drive the same path.
  pub fn refresh_steam_libraries(&self) {
    self
      .steam_libraries
      .write()
      .unwrap_or_else(|e| e.into_inner())
      .refresh_if_stale();
  }

  /// Start scan/refresh/watch threads. `watch_slot` receives the watch
  /// backlog gauge once the watcher spawns (stays detached when the
  /// watcher is disabled/unsupported, or on duplicate start).
  pub fn start(
    &self,
    scan_interval: Duration,
    watch_slot: &Arc<Mutex<rsrpc_telemetry::QueueGauge>>,
  ) {
    // Double-start is a caller bug: ignore fail-safe instead of leaking
    // a second scan/dispatch/watch generation.
    if self.started.swap(true, std::sync::atomic::Ordering::AcqRel) {
      tracing::warn!("[Process Scanner] Already started, ignoring duplicate start");
      return;
    }
    let wait_time = scan_interval;
    let clone = self.clone();

    // No custom rebuild needed here: new() already built the bundle with
    // its (empty) custom side, and appends rebuild on their own path.

    // Periodically refresh the detectable games database (like pog5-rsrpc).
    // Sleep first: startup already fetched synchronously, so an immediate
    // refetch would parse the whole DB twice for the same data (double
    // transient memory + startup time for zero new information). Refreshes
    // are conditional (ETag): an unchanged database costs one header round
    // trip and zero parsing, so steady-state RSS never ratchets.
    if clone.refresh.enable && clone.refresh.db_url.is_some() {
      let db_clone = clone.clone();
      // Hoisted once: `db_url` is immutable after `new()`, so the hourly
      // thread never unwraps an `Option` per iteration.
      let db_url = db_clone
        .refresh
        .db_url
        .clone()
        .expect("[bug] db_url checked above");
      std::thread::spawn(move || {
        // Seeded from the startup fetch when available: the first check
        // is conditional like every other, instead of one guaranteed
        // redundant full rebuild per daemon lifetime.
        let mut etag = db_clone.refresh.etag.clone();
        // Content hashes of the last built database: the raw hash guards
        // against byte-identical bodies (no parse at all), the trimmed
        // hash against volatile CDN bytes around identical games (parse,
        // but no rebuild). Either way an unchanged hour costs ~nothing.
        // Seeded from the startup fetch when available (see
        // `initial_db_content_hash`): without a seed the first check
        // always rebuilds once.
        let mut content_hash: Option<u64> = db_clone.refresh.content_hash.map(|(raw, _)| raw);
        let mut trimmed_hash: Option<u64> =
          db_clone.refresh.content_hash.map(|(_, trimmed)| trimmed);
        // Unlike the DB, exclusions are NOT fetched synchronously at
        // startup (tiny payload, empty = current behavior), so prime them
        // here instead of waiting an hour for the first set.
        db_clone.refresh_exclusions();
        loop {
          std::thread::sleep(Duration::from_secs(3600));
          db_clone.refresh_exclusions();
          match fetch_detectable_etag(&db_url, etag.as_deref(), content_hash, trimmed_hash) {
            Ok(FetchOutcome::Unchanged) => {
              tracing::info!(
                "[Process Scanner] DB check: unchanged (etag {})",
                etag.as_deref().unwrap_or("none")
              );
            }
            Ok(FetchOutcome::SameContent {
              etag: new_tag,
              content_hash: new_hash,
              trimmed_hash: new_trimmed,
            }) => {
              tracing::info!(
                "[Process Scanner] DB check: same content, new tag (etag {} -> {})",
                etag.as_deref().unwrap_or("none"),
                new_tag.as_deref().unwrap_or("none")
              );
              etag = new_tag;
              content_hash = Some(new_hash);
              trimmed_hash = Some(new_trimmed);
            }
            Ok(FetchOutcome::Updated {
              etag: new_tag,
              content_hash: new_hash,
              trimmed_hash: new_trimmed,
              detectable,
            }) => {
              tracing::info!(
                "[Process Scanner] DB updated: {} entries (etag {} -> {})",
                detectable.len(),
                etag.as_deref().unwrap_or("none"),
                new_tag.as_deref().unwrap_or("none")
              );
              etag = new_tag;
              content_hash = Some(new_hash);
              trimmed_hash = Some(new_trimmed);
              db_clone.update_main_detectables(detectable);
            }
            Err(err) => {
              tracing::warn!(
                "[Process Scanner] Error updating detectable database, retrying in 1h: {}",
                err
              );
            }
          }
        }
      });
    }

    std::thread::spawn(move || {
      // Register for early wakeups: the proc-events watcher unparks us
      // the moment a tracked game exits (Linux only; elsewhere None and
      // the cadence below is a plain sleep).
      *clone.scan_wake.lock().unwrap_or_else(|e| e.into_inner()) = Some(std::thread::current());
      // Idle backoff state: consecutive ticks with no games detected.
      let mut idle_ticks: u32 = 0;
      // Game ids already announced this boot (first-sighting INFO below).
      let mut seen_ids: HashSet<String> = HashSet::new();
      // Last detection snapshot forwarded to the bridge, as sorted
      // (app id, pid) pairs. The bridge dedups repeats internally, so
      // re-sending an identical table every tick only costs wakeups,
      // channel traffic and locks for zero effect — forward deltas only.
      // Transitions (including pid changes on restart) still send the
      // full current table, exactly like before.
      let mut last_emitted: Vec<(String, u64)> = Vec::new();
      // First-tick liveness proof (INFO, once): a scan thread that never
      // completes tick one is otherwise indistinguishable from an idle
      // one without a debug build.
      let mut first_tick = true;
      // Run the process scan repeatedly (base cadence, stretched while idle)
      loop {
        *clone.last_scan.lock().unwrap_or_else(|e| e.into_inner()) = std::time::Instant::now();
        let mut detected = match clone.scan_for_processes() {
          Ok(detected) => detected,
          Err(err) => {
            tracing::warn!(
              "[Process Scanner] Error while scanning processes, retrying: {}",
              err
            );
            wait_scan(wait_time);
            continue;
          }
        };
        // Coexistence filter: ignored app IDs behave as absent, so a richer
        // publisher elsewhere owns the slot (clears flow normally).
        let before = detected.len();
        detected = apply_ignore_list(detected, &clone.ignored_ids);
        if detected.len() != before {
          tracing::debug!(
            "[Process Scanner] Ignored {} detected game(s)",
            before - detected.len()
          );
        }
        // First-tick liveness proof (INFO, once per boot): proves the
        // loop enumerated and classified, whatever it found. A boot
        // with games running that reports 0 here is a wedged scan,
        // not an idle one — distinguishable without debug builds.
        if first_tick {
          first_tick = false;
          tracing::info!(
            "[Process Scanner] First tick complete: {} game(s)",
            detected.len()
          );
        }
        // First sightings this boot, at INFO: without this, a daemon
        // whose bridge path goes quiet is indistinguishable from a
        // blind scanner except with a debug build. Bounded: one line
        // per game id per boot, same cadence as bridge publishes.
        for game in first_sightings(&mut seen_ids, &detected) {
          tracing::info!(
            "[Process Scanner] Detected: {} ({})",
            game.entry.name,
            game.entry.id
          );
        }
        // Track live game pids for the proc-events watcher: only THEIR
        // exits wake us early (a build storm's exits never cause a scan).
        // Wholesale replace (not merge): a watcher insert racing this
        // write can be dropped, but the next tick re-derives it from the
        // live table while the game still runs — and a lost EXIT entry
        // heals the same way. Bounded staleness (≤1 tick), no growth,
        // no liveness probe per entry.
        *clone
          .detected_pids
          .lock()
          .unwrap_or_else(|e| e.into_inner()) = detected.iter().map(|game| game.pid).collect();
        // Forward on change only (see `last_emitted`): identical tables
        // are already fully represented downstream (`note_scan` entries
        // persist, `last_process` dedups), so skipping them changes
        // nothing observable — it just stops waking the bridge thread.
        // The EXEC fast path can publish a game no poll ever observes
        // (sub-tick lifetime + missed EXIT): `scan_dirty` forces one full
        // emission after any EXEC publication, so its clear still flows.
        let mut snapshot: Vec<(String, u64)> = detected
          .iter()
          .map(|game| (game.entry.id.to_string(), game.pid))
          .collect();
        snapshot.sort();
        let changed = snapshot != last_emitted;
        let forced = clone
          .scan_dirty
          .swap(false, std::sync::atomic::Ordering::AcqRel);
        let emit = changed || forced;
        if emit {
          last_emitted = snapshot;
        }
        // Forward the changed table, one event per slot. Downstream
        // publishes per app id and dedups repeats, so co-running games
        // each own their card instead of only the first.
        if emit && !detected.is_empty() {
          for game in &detected {
            if clone
              .event_sender
              .send(ProcessDetectedEvent {
                hit: Some(game.clone()),
              })
              .is_err()
            {
              tracing::warn!("[Process Scanner] Event receiver gone, retrying scan");
              wait_scan(wait_time);
              continue;
            }
          }
        }

        // If there are no detected processes, send an empty message —
        // but only on the transition into emptiness (the bridge clears
        // once and then ignores further nulls the same way). A forced
        // emission of the empty table carries the EXEC-hole clear.
        if emit && detected.is_empty() {
          let cleared = clone.event_sender.send(ProcessDetectedEvent { hit: None });
          if cleared.is_err() {
            tracing::warn!("[Process Scanner] Event receiver gone, retrying scan");
            wait_scan(wait_time);
            continue;
          }
        }

        // Idle backoff: consecutive empty ticks stretch the cadence
        // (base → 30s cap). Safe because game START arrives via EXEC
        // events instantly and EXITs of tracked games unpark us early —
        // polling only backstops what the watcher cannot (untracked
        // exits, DB refreshes). Any detection or early wake resets.
        if detected.is_empty() {
          idle_ticks = idle_ticks.saturating_add(1);
        } else {
          idle_ticks = 0;
        }
        let cadence = idle_wait(wait_time, idle_ticks);
        let wait_start = std::time::Instant::now();
        wait_scan(cadence);
        if wait_start.elapsed() < cadence.mul_f32(0.9) {
          idle_ticks = 0;
        }
      }
    });

    // Event-driven fast path (Linux): EXEC classifies one process at once,
    // EXIT of a tracked game wakes the scan above. Best-effort — setup
    // failure keeps pure polling, silently.
    #[cfg(target_os = "linux")]
    if self.enable_proc_events {
      let gauge = spawn_proc_watcher(self);
      *watch_slot
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = gauge;
    } else {
      tracing::info!(
        "[Process Scanner] proc-events watcher disabled by configuration, polling only"
      );
    }
  }

  /// Opt out of the event-driven proc-events watcher (called once,
  /// before [`ProcessServer::start`]). Polling continues either way.
  pub fn set_proc_events(&mut self, enable: bool) {
    self.enable_proc_events = enable;
  }

  #[cfg(not(target_os = "linux"))]
  fn process_list(&self) -> rsrpc_protocol::error::Result<Vec<Exec>> {
    use std::path::Path;
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, UpdateKind};

    let mut processes = Vec::new();
    let mut sys = self.sysinfo.lock().unwrap_or_else(|e| e.into_inner());
    sys.refresh_processes_specifics(
      ProcessesToUpdate::All,
      true,
      ProcessRefreshKind::nothing()
        .with_exe(UpdateKind::OnlyIfNotSet)
        .with_cmd(UpdateKind::OnlyIfNotSet),
    );

    for proc in sys.processes() {
      let mut cmd = proc.1.cmd().iter();
      processes.push(Exec {
        pid: u64::from(proc.0.as_u32()),
        path: proc.1.exe().unwrap_or(Path::new("")).display().to_string(),
        arguments: cmd.next().map(|_| {
          cmd
            .map(|x| x.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
        }),
      });
    }

    Ok(processes)
  }

  #[cfg(target_os = "linux")]
  fn process_list() -> rsrpc_protocol::error::Result<Vec<Exec>> {
    use std::fs;

    let proc_list = fs::read_dir("/proc")?.filter(|e| {
      if let Ok(entry) = e {
        // Only if we can parse this as a number (lossy: /proc names are
        // always ASCII pids; anything else is skipped, never fatal).
        return entry.file_name().to_string_lossy().parse::<u64>().is_ok();
      }

      false
    });
    let mut processes = Vec::new();

    for entry in proc_list {
      let entry = entry?;
      let path = entry.path();

      let Ok(pid) = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .parse::<u64>()
      else {
        continue;
      };
      // Same single-pid reader as the EXEC fast path: unreadable pids
      // (kernel threads, zombies, races) are skipped, never fatal.
      if let Some(exec) = read_exec(pid) {
        processes.push(exec);
      }
    }

    Ok(processes)
  }

  /// Current detection generation, shared lock-free after the clone.
  /// Scan ticks and EXEC events each hold one Arc for their whole
  /// classification, so a concurrent refresh can only swap in the NEXT
  /// fully-built generation — never a torn mix.
  #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
  pub fn bundle(&self) -> Arc<DetectablesBundle> {
    self.detectables.load_full()
  }

  #[hotpath::measure]
  pub fn scan_for_processes(&self) -> rsrpc_protocol::error::Result<Vec<ScannedHit>> {
    #[cfg(not(target_os = "linux"))]
    let processes = self.process_list()?;
    #[cfg(target_os = "linux")]
    let processes = ProcessServer::process_list()?;

    tracing::debug!("[Process Scanner] Process scan triggered");

    // Re-entrancy guard: a manual `scan_for_processes` racing the scan
    // thread (or two manual triggers) must not interleave. RAII so every
    // exit path — including `?` and panics — releases it.
    let _scan_guard = ScanGuard::try_acquire(&self.scanning).ok_or_else(|| {
      tracing::debug!("[Process Scanner] Scanning already in progress");
      rsrpc_protocol::error::RsrpcError::ScanInProgress
    })?;

    let mut obs_open = false;

    // One generation for the whole tick: clone the Arc once, classify
    // every process against it. A refresh landing mid-tick only swaps in
    // the next bundle, which this tick simply won't see — no torn reads.
    let bundle = self.detectables.load_full();

    // Steam generation marker: one stat per watched libraryfolders.vdf;
    // the parse itself runs only when something actually changed.
    self.refresh_steam_libraries();

    // Drop memoized AppIds of dead pids (pid reuse must never serve a
    // stale id): one set build + retain per tick, replacing hundreds of
    // kilobyte environ re-reads.
    self.sweep_dead_appids(&processes)?;

    let mut reversed_path = String::with_capacity(256);
    // Variant scratch space, reused for every process: the scan allocates
    // nothing per process at steady state (see path_variants_into).
    let mut variant_bufs: [String; 5] = Default::default();

    let mut detected_list: Vec<ScannedHit> = processes
      .iter()
      .filter_map(|process| {
        self.match_process(
          process,
          &bundle,
          &mut variant_bufs,
          &mut reversed_path,
          &mut obs_open,
        )
      })
      .collect();

    let callback = self
      .event_listeners
      .lock()
      .map_err(|e| rsrpc_protocol::error::RsrpcError::Poisoned("event_listeners", e.to_string()))?
      .on_process_scan_complete
      .clone();

    if let Some(callback) = callback.as_ref() {
      callback.lock().map_err(|e| {
        rsrpc_protocol::error::RsrpcError::Poisoned("process callback", e.to_string())
      })?(ProcessScanState { obs_open });
    }

    detected_list.shrink_to_fit();

    tracing::debug!("[Process Scanner] Process scan complete");

    Ok(detected_list)
  }
}
