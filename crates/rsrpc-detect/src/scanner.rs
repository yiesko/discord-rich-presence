//! Process scanning: enumerate processes, classify each against the
//! current bundle, wake early on tracked exits.
//!
//! Enumeration is platform-specific (`/proc` on Linux, `sysinfo`
//! elsewhere); classification funnels through the shared bundle in
//! [`database`](super::database). Ticks stay allocation-free via
//! caller-owned reuse buffers.

use super::runtime::ScanGuard;
use super::scan::{ExecScratch, MatchScratch, read_exec_into};
use super::server::ProcessServer;
use super::types::{Exec, ProcessScanState, ScannedHit};

impl ProcessServer {
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

  /// Enumerate processes via `sysinfo` (non-Linux: exe + cmdline snapshot).
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

  /// Enumerate processes via `/proc` into caller-owned buffers (Linux):
  /// the `Vec` backing and every `Exec` slot survive across ticks
  /// (in-place refill, never rebuilt), so a warm tick
  /// allocates nothing per process. Unreadable pids (kernel threads,
  /// zombies, races) are skipped without leaving holes: only successful
  /// reads advance. Returns the filled prefix length; surplus high-water
  /// slots past it are kept (not truncated) so their string buffers
  /// survive count dips. Callers must use only `&processes[..filled]`.
  #[cfg(target_os = "linux")]
  pub(crate) fn process_list_into(
    processes: &mut Vec<Exec>,
    scratch: &mut ExecScratch,
  ) -> rsrpc_protocol::error::Result<usize> {
    use std::fs;

    let proc_list = fs::read_dir("/proc")?.filter(|e| {
      if let Ok(entry) = e {
        // Only if we can parse this as a number (lossy: /proc names are
        // always ASCII pids; anything else is skipped, never fatal).
        return entry.file_name().to_string_lossy().parse::<u64>().is_ok();
      }

      false
    });
    // No `clear` and no `truncate`: the Vec length is the high-water
    // slot count. Slots refill in place below, so a warm tick reuses
    // every string buffer; `push` only grows past the high-water mark.
    // Tail slots past `filled` keep their buffers for the next rise.
    let mut filled = 0usize;

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
      // Same single-pid reader as the EXEC fast path, refilling a reused
      // slot: unreadable pids are skipped, never fatal, never a hole.
      if filled == processes.len() {
        processes.push(Exec::default());
      }
      // `filled <= len` always: slots refill in place, `push` only grows
      // past the high-water mark.
      if read_exec_into(pid, &mut processes[filled], scratch) {
        filled += 1;
      }
    }
    Ok(filled)
  }

  #[hotpath::measure]
  /// One full process sweep, classifying every process against the current
  /// generation bundle. `processes` + `exec_scratch` are caller-owned
  /// reuse buffers (the scan loop keeps them across ticks; one-shot
  /// callers pass throwaways): a warm tick allocates nothing per process.
  /// On non-Linux the list still comes from `sysinfo` (scratch unused).
  pub fn scan_for_processes(
    &self,
    // Linux needs the Vec (in-place refill); elsewhere the list is built
    // fresh and this param is only carried for a uniform signature.
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables, clippy::ptr_arg))]
    processes: &mut Vec<Exec>,
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] exec_scratch: &mut ExecScratch,
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] match_scratch: &mut MatchScratch,
  ) -> rsrpc_protocol::error::Result<Vec<ScannedHit>> {
    #[cfg(not(target_os = "linux"))]
    let processes = self.process_list()?;
    #[cfg(not(target_os = "linux"))]
    let processes = processes.as_slice();
    #[cfg(target_os = "linux")]
    let processes = {
      let filled = ProcessServer::process_list_into(processes, exec_scratch)?;
      &processes[..filled]
    };

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
    self.sweep_dead_appids(processes);

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
          &mut *match_scratch,
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
