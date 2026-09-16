//! Memoized per-pid Steam AppIds with invalidation sequences.
//!
//! Environ reads are kilobytes and never change after exec: one read per
//! process lifetime, discarded (never stored) when an EXEC races it.

use std::collections::HashSet;

use crate::scan::read_steam_app_id;
use crate::server::ProcessServer;
use crate::types::Exec;

/// Memoized SteamAppId plus its invalidation sequence (see
/// [`ProcessServer::cached_app_id`]). Verified outcomes (`Present` /
/// `Absent`) cost zero I/O until EXEC invalidation or pid death; only
/// `Stale` reads environ. Distinguishing `Absent` from `Stale` is the
/// whole point: nearly every desktop process has no SteamAppId, and
/// re-reading kilobytes of environ for all of them every tick was the
/// biggest per-process I/O cost.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AppIdMemo {
  /// Verified: this pid has no SteamAppId. Valid until invalidated.
  Absent(u64),
  /// Verified SteamAppId, valid under the same rules.
  Present(u64, String),
  /// Never read, or invalidated by EXEC: read environ, then store the
  /// outcome — unless the sequence moved under us (racing EXEC), in
  /// which case the stale read is discarded, never stored.
  /// (Constructed only by the Linux-only `drop_appid`; other platforms
  /// would flag the variant as dead without the allow.)
  #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
  Stale(u64),
}

impl ProcessServer {
  /// SteamAppId for one pid, memoized: environ is kilobytes and never
  /// changes after exec, so re-reading it every 5s per process was pure
  /// waste (profiler: ~5KB of the ~5KB per-process cost). IO happens
  /// outside the lock; EXEC invalidates via [`ProcessServer::drop_appid`],
  /// whose sequence bump discards a racing stale read below.
  /// (`pub(crate)` for the memo-semantics regression test.)
  pub fn cached_app_id(&self, pid: u64) -> Option<String> {
    // Fast paths: verified memos, cloned once straight into the return.
    // `Absent` is the common case (non-Steam processes) and performs no
    // I/O at all; only `Stale` falls through to the environ read below.
    let memo = self
      .appid_cache
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .get(&pid)
      .cloned();
    let seq0 = match memo {
      Some(AppIdMemo::Present(_, id)) => return Some(id),
      Some(AppIdMemo::Absent(_)) => return None,
      Some(AppIdMemo::Stale(seq)) => seq,
      None => 0,
    };
    let id = read_steam_app_id(pid);
    let mut cache = self.appid_cache.lock().unwrap_or_else(|e| e.into_inner());
    // A racing EXEC invalidated this pid mid-read (sequence bumped) or
    // the tick swept it: discard the stale environ instead of pinning
    // it for the pid lifetime.
    let fresh = match cache.get(&pid) {
      Some(AppIdMemo::Stale(seq)) if *seq == seq0 => true,
      None if seq0 == 0 => true,
      _ => false,
    };
    if fresh {
      let memo = match id.clone() {
        Some(id) => AppIdMemo::Present(seq0, id),
        None => AppIdMemo::Absent(seq0),
      };
      cache.insert(pid, memo);
    }
    id
  }

  /// Drop memoized AppIds of pids that died since the last tick: pid
  /// reuse must never serve a stale id. One set build + retain per tick.
  pub(crate) fn sweep_dead_appids(&self, processes: &[Exec]) -> rsrpc_protocol::error::Result<()> {
    let mut live = HashSet::with_capacity(processes.len());
    live.extend(processes.iter().map(|process| process.pid));
    self
      .appid_cache
      .lock()
      .map_err(|e| rsrpc_protocol::error::RsrpcError::Poisoned("appid_cache", e.to_string()))?
      .retain(|pid, _| live.contains(pid));
    Ok(())
  }

  /// Drop one pid's memoized AppId (EXEC: same pid, new image, possibly
  /// new environ). Called by the proc-events watcher before reclassifying.
  /// Bumps the sequence so an in-flight [`ProcessServer::cached_app_id`]
  /// read for the old image is discarded, never stored.
  #[cfg(target_os = "linux")]
  pub fn drop_appid(&self, pid: u64) {
    self
      .appid_cache
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .entry(pid)
      .and_modify(|entry| {
        let seq = match entry {
          AppIdMemo::Absent(seq) | AppIdMemo::Present(seq, _) | AppIdMemo::Stale(seq) => *seq,
        };
        *entry = AppIdMemo::Stale(seq.wrapping_add(1));
      })
      .or_insert(AppIdMemo::Stale(1));
  }
}
