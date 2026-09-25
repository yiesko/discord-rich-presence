//! Scanner thread lifecycle: supervised workers with directed shutdown.
//!
//! Every thread `start` spawns (scan loop, database refresh, proc-events
//! dispatch and watcher retry) registers here and observes the shared
//! shutdown flag, so `ProcessServer::shutdown` plus `join` ends them all
//! promptly instead of waiting out hourly sleeps. Workers carry `rsrpc-*`
//! names so teardown accounting (and `top`/`ps`) can observe them.

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::refresh::{FetchOutcome, fetch_detectable_etag};
use super::scan::{ExecScratch, MatchScratch, apply_ignore_list, first_sightings, read_exec_into};
use super::server::{ProcessServer, allocator_numbers, release_parse_arenas};
use super::types::{Exec, ProcessDetectedEvent, ScannedHit};
use rsrpc_telemetry::QueueGauge;

/// Re-entrancy guard for [`ProcessServer::scan_for_processes`]: acquired
/// atomically, released on drop (all exit paths, panics included).
pub struct ScanGuard {
  flag: Arc<AtomicBool>,
}

impl ScanGuard {
  /// Acquire the guard, or `None` when a scan is already in progress.
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
  /// Release the re-entrancy flag (all exit paths, panics included).
  fn drop(&mut self) {
    self.flag.store(false, std::sync::atomic::Ordering::Release);
  }
}

/// Sleep the scan cadence. `park_timeout` (not `sleep`) so the proc-events
/// watcher can wake the loop early when a tracked game exits; a permit
/// stored by an unpark during scan work just causes one early rescan,
/// which downstream dedups harmlessly.
pub(crate) fn wait_scan(wait_time: Duration) {
  std::thread::park_timeout(wait_time);
}

/// Whether directed shutdown was requested (checked on every loop
/// iteration and after every blocking wait).
pub(crate) fn is_shutting_down(shutdown: &Arc<AtomicBool>) -> bool {
  shutdown.load(std::sync::atomic::Ordering::Acquire)
}

/// Register the calling thread for shutdown unpark. Called once per
/// spawned thread at startup (not per wait: the registry stays one
/// entry per thread for the server lifetime).
pub(crate) fn track_shutdown_thread(wake: &Arc<Mutex<Vec<std::thread::Thread>>>) {
  wake
    .lock()
    .unwrap_or_else(|e| e.into_inner())
    .push(std::thread::current());
}

/// Spawn a supervised worker: the handle joins `worker_handles` for
/// `join`, so no thread outlives a joined server. Takes the registry by
/// value (callers pass `Arc::clone`) so the moved body closure never
/// fights a borrow of its owner. Threads carry `rsrpc-*` names so
/// teardown accounting (and `top`/`ps`) can observe them.
pub(crate) fn spawn_worker(
  handles: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
  name: &str,
  body: impl FnOnce() + Send + 'static,
) {
  let handle = std::thread::Builder::new()
    .name(name.to_string())
    // Same failure semantics as `thread::spawn` (panics when the OS
    // refuses a thread): startup cannot proceed half-supervised.
    .spawn(body)
    .expect("scanner worker spawn failed");
  handles
    .lock()
    .unwrap_or_else(|e| e.into_inner())
    .push(handle);
}

/// Whether a failed `event_sender.send` must end the calling thread.
///
/// The sender wraps an unbounded `std::mpsc`: `Err` means the receiver is
/// gone (shutdown), never transient backpressure — retrying would spin
/// `scan_for_processes` forever after shutdown. The EXEC fast path already
/// breaks; the polling loop must do the same.
pub(crate) fn should_exit_on_send_error<T>(
  result: &Result<(), std::sync::mpsc::SendError<T>>,
) -> bool {
  result.is_err()
}

/// `(app id, pid)` pairs present in the previous snapshot but absent now.
/// Exact-tuple matching (not id-only): a pid replacement reports the old
/// pair as removed before the new detection is published, so the bridge
/// rotates the card instead of keeping a stale pid. Tables are tiny
/// (co-running games), so the quadratic scan allocates nothing.
pub(crate) fn removed_slots(
  previous: &[(String, u64)],
  current: &[(String, u64)],
) -> Vec<(String, u64)> {
  previous
    .iter()
    .filter(|pair| !current.contains(pair))
    .cloned()
    .collect()
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

/// Effective scan cadence: idle backoff applies only while the EXEC
/// watcher is confirmed live (socket up). Polling-only paths — other
/// OSes, disabled proc-events, failed watcher setups and retry windows —
/// always use the configured base interval, or a short interval would
/// silently grow to 30s with nothing to wake the scan early.
pub(crate) fn scan_cadence(base: Duration, idle_ticks: u32, watcher_live: bool) -> Duration {
  if watcher_live {
    idle_wait(base, idle_ticks)
  } else {
    base
  }
}

/// Shared shutdown signaling without ownership: drop-guards and other
/// non-owners stop the threads; joining stays with `join`. Holds no
/// channel sender, so dropping it never keeps the event pump alive.
#[derive(Clone)]
pub struct ShutdownHandle {
  shutdown: Arc<AtomicBool>,
  wake: Arc<Mutex<Vec<std::thread::Thread>>>,
  scan_wake: Arc<Mutex<Option<std::thread::Thread>>>,
}

impl ShutdownHandle {
  /// Signal directed shutdown: parked threads are unparked; every loop
  /// observes the flag on its next iteration. Idempotent.
  pub fn signal(&self) {
    self
      .shutdown
      .store(true, std::sync::atomic::Ordering::Release);
    for thread in self.wake.lock().unwrap_or_else(|e| e.into_inner()).iter() {
      thread.unpark();
    }
    unpark_scan_wake(&self.scan_wake);
  }
}

/// Wake the scan thread if registered (best-effort): used when the
/// watcher fails so one fresh poll compensates immediately instead of
/// sleeping into the failure with possibly stale state. Missing handle:
/// silent no-op. Linux-only caller (watcher retry thread); allowed dead
/// elsewhere like [`ProcessServer::should_wake_on_exit`].
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn unpark_scan_wake(scan_wake: &Arc<Mutex<Option<std::thread::Thread>>>) {
  if let Some(thread) = scan_wake.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
    thread.unpark();
  }
}

impl ProcessServer {
  /// A signaling-only view of this server's shutdown state, for owners
  /// that must stop threads without joining (async drop guards): holds
  /// no channel sender, so it can never keep the event pump alive.
  pub fn shutdown_handle(&self) -> ShutdownHandle {
    ShutdownHandle {
      shutdown: Arc::clone(&self.shutdown),
      wake: Arc::clone(&self.shutdown_wake),
      scan_wake: Arc::clone(&self.scan_wake),
    }
  }

  /// Signal directed shutdown: every thread spawned by `start`
  /// observes the flag on its next iteration and exits; parked threads
  /// are unparked so no sleep runs its full duration. Idempotent:
  /// repeated calls only re-unpark. Pair with `join` to wait out the
  /// exits. (The CLI relies on process exit instead and never calls
  /// either: unchanged behavior there.)
  ///
  /// # Startup race
  ///
  /// Every worker registers and checks the flag first thing (refresh
  /// exits before any fetch; scan and retry exit at their loop-top
  /// checks; dispatch exits at its first receive timeout), so even a
  /// shutdown that lands mid-startup parks nothing: the unpark permit
  /// only shortens waits already in flight.
  pub fn shutdown(&self) {
    self.shutdown_handle().signal();
  }

  /// Block until every thread spawned by `start` has exited. Call after
  /// `shutdown`: joining without it waits out sleeps (refresh hours,
  /// watcher retry minutes). Takes the handle registry, so a second
  /// `join` returns at once. A panicked worker logs and continues
  /// joining the rest instead of abandoning them.
  pub fn join(&self) {
    let handles: Vec<std::thread::JoinHandle<()>> = std::mem::take(
      &mut *self
        .worker_handles
        .lock()
        .unwrap_or_else(|e| e.into_inner()),
    );
    for handle in handles {
      if handle.join().is_err() {
        tracing::warn!("[Process Scanner] worker thread panicked during join");
      }
    }
  }
}

/// Cadence between dispatch receives: the only blocking wait in the
/// dispatch loop doubles as the shutdown poll. Traffic returns at once;
/// silence costs one cheap timeout per interval.
const DISPATCH_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Whether an EXEC fast-path hit must be skipped: ignored app IDs behave
/// as absent everywhere (parity with the polling path's
/// [`apply_ignore_list`]). One `HashSet` lookup; empty set early-outs via
/// the caller's check below (`contains` on empty is already cheap, but the
/// intent reads explicitly at the call site).
/// Linux-only caller (`spawn_proc_watcher`): allow dead code elsewhere,
/// same as [`ProcessServer::should_wake_on_exit`].
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn exec_hit_ignored(ignored_ids: &HashSet<String>, hit: &ScannedHit) -> bool {
  ignored_ids.contains(hit.entry.id.as_ref())
}

/// Spawn the netlink dispatch (Linux): EXEC classifies one process and
/// emits hits at once; EXIT of a tracked game unparks the scan loop for
/// an immediate natural clear. Setup failure (or a dead receiver on
/// shutdown) ends the thread quietly — polling carries on.
/// Spawn the netlink watcher + dispatch threads. Returns the shared watch
/// backlog gauge so the resource census can read it (the pair itself stays
/// inside: `start` hands the gauge up to the daemon).
#[cfg(target_os = "linux")]
fn spawn_proc_watcher(server: &ProcessServer) -> QueueGauge {
  use rsrpc_proc_events::{ProcEvent, watch};
  use rsrpc_telemetry::QueueGauge;

  let (tx, rx) = QueueGauge::pair();
  let gauge = tx.gauge();
  let dispatch = server.clone();
  spawn_worker(
    Arc::clone(&server.worker_handles),
    "rsrpc-dispatch",
    move || {
      let mut variant_bufs: [String; 5] = Default::default();
      let mut reversed_path = String::with_capacity(256);
      // EXEC reuse buffers, owned by this loop: one `Exec` slot plus the
      // read scratch, refilled per event instead of allocated per event.
      // `match_scratch` same idea for the classification tail.
      let mut exec_slot = Exec::default();
      let mut exec_scratch = ExecScratch::default();
      let mut match_scratch = MatchScratch::default();
      loop {
        // Exit at once when shutdown landed during the previous message
        // handling: the bounded wait below would otherwise park up to a
        // second needlessly.
        if is_shutting_down(&dispatch.shutdown) {
          break;
        }
        // Bounded wait: every expiry re-checks directed shutdown so the
        // thread never blocks past it; a gone sender still ends the loop
        // at once, exactly like the `while let` before.
        let event = match rx.recv_timeout(DISPATCH_POLL_INTERVAL) {
          Ok(event) => event,
          Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            if is_shutting_down(&dispatch.shutdown) {
              break;
            }
            continue;
          }
          Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };
        match event {
          ProcEvent::Exec(pid) => {
            if !read_exec_into(pid, &mut exec_slot, &mut exec_scratch) {
              tracing::debug!("[Process Scanner] exec event: pid {pid} unreadable, skipping");
              continue;
            }
            let exec = &exec_slot;
            // Same pid, new image: the memoized AppId may be stale.
            dispatch.drop_appid(pid);
            // One generation for the whole classification: a refresh
            // landing mid-probe can only swap in the next bundle, which
            // this event simply won't see.
            let bundle = dispatch.bundle();
            let mut obs_open = false;
            if let Some(hit) = dispatch.match_process(
              exec,
              &bundle,
              &mut variant_bufs,
              &mut reversed_path,
              &mut obs_open,
              &mut match_scratch,
            ) {
              // Coexistence parity with the polling path: ignored IDs never
              // publish, even on the event-driven fast path.
              if exec_hit_ignored(&dispatch.ignored_ids, &hit) {
                tracing::debug!(
                  "[Process Scanner] exec event: pid {pid} ignored ({}), skipping",
                  hit.entry.id.as_ref() as &str
                );
                continue;
              }
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
              if should_exit_on_send_error(
                &dispatch
                  .event_sender
                  .send(ProcessDetectedEvent::detected(hit)),
              ) {
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
    },
  );
  // Best-effort watcher with periodic resubscribe: a failed self-test
  // (or a mid-run socket death) falls back to polling, but a transient
  // kernel stall must not pin polling until the next daemon restart —
  // delivery has been observed to resume on its own. First failure warns
  // (the documented sandbox diagnosis), later ones stay in debug so
  // genuinely unsupported systems do not log every 5 minutes forever.
  let watcher_state = server.clone();
  spawn_worker(
    Arc::clone(&server.worker_handles),
    "rsrpc-watch",
    move || {
      // Register once: `shutdown` unparks this thread out of the retry
      // park below, so teardown never waits out the five minutes.
      track_shutdown_thread(&watcher_state.shutdown_wake);
      let mut attempts = 0u32;
      loop {
        // Directed shutdown before blocking in `watch()`: with the flag
        // set the call below returns at once (preset-stop fast path).
        if is_shutting_down(&watcher_state.shutdown) {
          break;
        }
        // Believed up while the blocking watch runs; a fast failure flips
        // back before the retry sleep, so polling-only windows never back
        // off (see `scan_cadence`).
        watcher_state
          .watcher_live
          .store(true, std::sync::atomic::Ordering::Release);
        match watch(&tx, &watcher_state.shutdown) {
          // Receiver gone: daemon shutting down.
          Ok(()) => {
            watcher_state
              .watcher_live
              .store(false, std::sync::atomic::Ordering::Release);
            break;
          }
          Err(err) => {
            watcher_state
              .watcher_live
              .store(false, std::sync::atomic::Ordering::Release);
            // Poll once now: the watcher just died, so the scan loop must
            // not sit out its whole backoff on possibly stale state.
            unpark_scan_wake(&watcher_state.scan_wake);
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
            // Interruptible park (not `sleep`): the pre-park check exits
            // at once when shutdown is already requested, otherwise the
            // unpark bounds the five minutes and the loop-top check exits.
            if is_shutting_down(&watcher_state.shutdown) {
              break;
            }
            std::thread::park_timeout(std::time::Duration::from_secs(5 * 60));
          }
        }
      }
    },
  );
  gauge
}

impl ProcessServer {
  /// Start scan/refresh/watch threads. `watch_slot` receives the watch
  /// backlog gauge once the watcher spawns (stays detached when the
  /// watcher is disabled/unsupported, or on duplicate start).
  pub fn start(
    &self,
    scan_interval: Duration,
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] watch_slot: &Arc<
      Mutex<rsrpc_telemetry::QueueGauge>,
    >,
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
      spawn_worker(
        Arc::clone(&db_clone.worker_handles),
        "rsrpc-refresh",
        move || {
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
          //
          // Register before any blocking work: a shutdown landing during
          // the prime fetch leaves an unpark permit, so the hourly park
          // below returns at once instead of sleeping out the hour. A
          // shutdown that lands before this thread even starts exits at
          // the flag check without touching the network at all.
          track_shutdown_thread(&db_clone.shutdown_wake);
          if is_shutting_down(&db_clone.shutdown) {
            return;
          }
          db_clone.refresh_exclusions();
          loop {
            // Pre-park flag check plus the unpark permit: either one exits
            // without waiting when shutdown is already requested.
            if is_shutting_down(&db_clone.shutdown) {
              break;
            }
            // Hourly cadence as an interruptible park (not `sleep`): the
            // shutdown unpark bounds the wait instead of the hour.
            std::thread::park_timeout(Duration::from_secs(3600));
            if is_shutting_down(&db_clone.shutdown) {
              break;
            }
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
                // Commit validators only after the bundle is validated and
                // installed: a refused swap must not teach the next request
                // that never-installed data is current.
                if db_clone.update_main_detectables(detectable) {
                  etag = new_tag;
                  content_hash = Some(new_hash);
                  trimmed_hash = Some(new_trimmed);
                  // The pre-swap generation is typically still pinned by an
                  // in-flight tick at swap time, so the trim inside
                  // `update_main_detectables` cannot release it. Trim once
                  // more past any tick: production measurement showed the
                  // second trim recovering ~30MB the first could not.
                  // Delays the next refresh by one tick length; negligible
                  // on the hourly cadence.
                  if is_shutting_down(&db_clone.shutdown) {
                    break;
                  }
                  std::thread::park_timeout(Duration::from_secs(60));
                  if is_shutting_down(&db_clone.shutdown) {
                    break;
                  }
                  release_parse_arenas();
                  match allocator_numbers() {
                    Some((live, free)) => tracing::info!(
                      "[Process Scanner] post-retrim rss={:.1}MB (alloc live={:.1}MB free={:.1}MB)",
                      rsrpc_telemetry::rss_bytes().unwrap_or(0) as f64 / 1_048_576.0,
                      live as f64 / 1_048_576.0,
                      free as f64 / 1_048_576.0
                    ),
                    None => {
                      if let Some(rss) = rsrpc_telemetry::rss_bytes() {
                        tracing::info!(
                          "[Process Scanner] post-retrim rss={:.1}MB",
                          rss as f64 / 1_048_576.0
                        );
                      }
                    }
                  }
                }
              }
              Err(err) => {
                tracing::warn!(
                  "[Process Scanner] Error updating detectable database, retrying in 1h: {}",
                  err
                );
              }
            }
          }
        },
      );
    }

    spawn_worker(Arc::clone(&clone.worker_handles), "rsrpc-scan", move || {
      // Register for early wakeups: the proc-events watcher unparks us
      // the moment a tracked game exits (Linux only; elsewhere None and
      // the cadence below is a plain sleep). `shutdown` unparks this
      // same slot, so teardown never waits out the cadence either.
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
      // Tick reuse buffers, owned by this loop: the process list backing,
      // every `Exec` slot and the read scratch survive across ticks, so a
      // warm tick allocates nothing per process (see `read_exec_into`).
      // `match_scratch` follows the same pattern one level down: the lowercased path and
      // the normalized folder refill per classification.
      let mut tick_processes: Vec<Exec> = Vec::new();
      let mut tick_scratch = ExecScratch::default();
      let mut match_scratch = MatchScratch::default();
      // Run the process scan repeatedly (base cadence, stretched while idle)
      loop {
        // Directed shutdown: the unpark below only shortens the cadence
        // wait; the flag is the actual exit (checked here and after the
        // wait, since EXIT wakes share the same unpark).
        if is_shutting_down(&clone.shutdown) {
          break;
        }
        *clone.last_scan.lock().unwrap_or_else(|e| e.into_inner()) = std::time::Instant::now();
        let mut detected = match clone.scan_for_processes(
          &mut tick_processes,
          &mut tick_scratch,
          &mut match_scratch,
        ) {
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
        // Slots vanished while others remain (A+B -> B): clear exactly
        // those before republishing the rest. The empty-table clear below
        // only fires when nothing remains, so without this the removed
        // game would ghost while another continues.
        let removed = if emit {
          removed_slots(&last_emitted, &snapshot)
        } else {
          Vec::new()
        };
        if emit {
          last_emitted = snapshot;
        }
        // A failed send means the receiver is gone (shutdown): the channel
        // is unbounded, so there is no transient backpressure to retry.
        // End the thread instead of spinning `scan_for_processes` forever.
        if emit && !removed.is_empty() {
          for (id, pid) in removed {
            if should_exit_on_send_error(
              &clone
                .event_sender
                .send(ProcessDetectedEvent::removed(id.into_boxed_str(), pid)),
            ) {
              tracing::warn!("[Process Scanner] Event receiver gone, shutting down scan");
              return;
            }
          }
        }
        // Forward the changed table, one event per slot. Downstream
        // publishes per app id and dedups repeats, so co-running games
        // each own their card instead of only the first.
        if emit && !detected.is_empty() {
          for game in &detected {
            if should_exit_on_send_error(
              &clone
                .event_sender
                .send(ProcessDetectedEvent::detected(game.clone())),
            ) {
              tracing::warn!("[Process Scanner] Event receiver gone, shutting down scan");
              return;
            }
          }
        }

        // If there are no detected processes, send an empty message —
        // but only on the transition into emptiness (the bridge clears
        // once and then ignores further nulls the same way). A forced
        // emission of the empty table carries the EXEC-hole clear.
        if emit
          && detected.is_empty()
          && should_exit_on_send_error(&clone.event_sender.send(ProcessDetectedEvent::cleared()))
        {
          tracing::warn!("[Process Scanner] Event receiver gone, shutting down scan");
          return;
        }

        // Idle backoff: consecutive empty ticks stretch the cadence
        // (base → 30s cap), but only while the EXEC watcher is live to
        // wake us early. Game START arrives via EXEC instantly and EXITs
        // of tracked games unpark us — polling only backstops what the
        // watcher cannot (untracked exits, DB refreshes). Polling-only
        // paths (other OSes, disabled/failed watcher) keep the base
        // cadence so a short interval never silently grows to 30s.
        // Any detection or early wake resets.
        if detected.is_empty() {
          idle_ticks = idle_ticks.saturating_add(1);
        } else {
          idle_ticks = 0;
        }
        let cadence = scan_cadence(
          wait_time,
          idle_ticks,
          clone
            .watcher_live
            .load(std::sync::atomic::Ordering::Acquire),
        );
        let wait_start = std::time::Instant::now();
        if is_shutting_down(&clone.shutdown) {
          break;
        }
        wait_scan(cadence);
        if is_shutting_down(&clone.shutdown) {
          break;
        }
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
}
