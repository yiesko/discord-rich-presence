//! IPC-wins handoff: generic process detection yields its slot to a live
//! game-SDK presence and reclaims it when that source clears.
//!
//! Retro-compat rule (several companions may target one app): **last
//! publisher wins**. Each app maps to the pid of its current owner; a
//! publish replaces the owner, and a clear only releases the slot when it
//! comes from that same owner.

use std::collections::HashMap;

use rsrpc_types::AppId;

/// One process-detected game, remembered so a clear can hand the slot back
/// to generic detection (the scanner only emits on *changes*).
#[derive(Clone, Debug, PartialEq)]
pub struct ScannedGame {
  /// Discord application id of the detected game.
  pub id: AppId,
  /// Human-readable game name for logs and generic cards.
  pub name: String,
  /// OS pid of the detected process.
  pub pid: u64,
  /// Process start time (Unix seconds) for card timestamps.
  pub start: u64,
}

/// Scanner input to the bridge: one game appeared, or the table is empty.
#[derive(Clone, Debug)]
pub enum ProcInput {
  /// A game was detected (re-emits are deduped downstream).
  Detected(ScannedGame),
  /// No games detected: clear every outstanding generic publication.
  Cleared,
}

/// Cap for the handoff tables: distinct live app-ids are tiny in practice
/// (co-running games plus their companions). The cap only bites a client
/// publishing hundreds of ids without clearing (malicious or buggy) —
/// without it, memory grows forever on untrusted input. Enforcement
/// purges dead owners first (the actual garbage), so live slots are only
/// evicted in pathological cases, and even then the next scan or publish
/// re-arms them (self-healing).
pub const MAX_HANDOFF_ENTRIES: usize = 64;

/// Best-effort liveness probe so a clear for an already-dead game doesn't
/// flash the generic card on the way out (the scanner's null event clears
/// the slot anyway).
#[must_use]
pub fn is_process_alive(pid: u64) -> bool {
  if pid == 0 {
    return false;
  }
  if cfg!(target_os = "linux") {
    return std::path::Path::new(&format!("/proc/{pid}")).exists();
  }
  #[cfg(all(unix, not(target_os = "linux")))]
  {
    // SAFETY: signal 0 performs no action; only error reporting. A zero
    // return (or EPERM: exists but unowned) means alive; ESRCH means dead.
    let alive = unsafe { libc::kill(pid as libc::pid_t, 0) } == 0
      || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH);
    return alive;
  }
  #[cfg(windows)]
  {
    use windows_sys::Win32::Foundation::{CloseHandle, FALSE};
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};
    // Pids beyond u32 cannot exist: narrowing is safe by construction.
    let Ok(pid_u32) = u32::try_from(pid) else {
      return false;
    };
    // SAFETY: a query-only handle has no side effects on the target and
    // is always closed before returning; null means no such process.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid_u32) };
    if handle.is_null() {
      return false;
    }
    unsafe { CloseHandle(handle) };
    return true;
  }
  {
    // Platforms without a probe: assume alive (ghost reaping stays off
    // rather than risking live cards).
    true
  }
}

/// IPC-wins handoff state. One lock for the whole state (short critical
/// sections, no I/O under it), shared by the event and process pumps.
#[derive(Clone, Debug, Default)]
pub struct HandoffState {
  /// App id → pid of its current IPC/WS owner (`SET_ACTIVITY` with activity).
  live_ipc: HashMap<AppId, u64>,
  /// Last games the scanner reported, by app id (a null/clear event wipes
  /// the table).
  last_scans: HashMap<AppId, ScannedGame>,
}

impl HandoffState {
  /// Record a live SDK publication for `app_id` from `pid`.
  pub fn note_publish(&mut self, app_id: &str, pid: u64) {
    if self.live_ipc.len() >= MAX_HANDOFF_ENTRIES {
      // Purge dead owners first (the actual garbage: crashed companions
      // that never cleared). Whatever remains is live.
      self.live_ipc.retain(|_, owner| is_process_alive(*owner));
    }
    self.live_ipc.insert(AppId::from(app_id), pid);
    // Hard bound: even all-live flooding (one pid, infinite ids) stops
    // here. Evicting a live slot only desuppresses its generic until the
    // next publish re-arms it — unreachable in legitimate use (<5 ids).
    while self.live_ipc.len() > MAX_HANDOFF_ENTRIES {
      let Some(victim) = self.live_ipc.keys().next().cloned() else {
        break;
      };
      self.live_ipc.remove(&victim);
    }
  }

  /// Record a clear; returns true when the slot was actually released
  /// (the clear came from the owning pid).
  pub fn note_clear(&mut self, app_id: &str, pid: u64) -> bool {
    if self.live_ipc.get(app_id).is_some_and(|owner| *owner == pid) {
      self.live_ipc.remove(app_id);
      true
    } else {
      false
    }
  }

  /// Record a scanner report (`None` = table empty, forget every game).
  pub fn note_scan(&mut self, game: Option<ScannedGame>) {
    match game {
      Some(game) => {
        if self.last_scans.len() >= MAX_HANDOFF_ENTRIES {
          self
            .last_scans
            .retain(|_, known| is_process_alive(known.pid));
        }
        self.last_scans.insert(game.id.clone(), game);
        while self.last_scans.len() > MAX_HANDOFF_ENTRIES {
          let Some(victim) = self.last_scans.keys().next().cloned() else {
            break;
          };
          self.last_scans.remove(&victim);
        }
      }
      None => self.last_scans.clear(),
    }
  }

  /// Release every slot owned by `pid` (abrupt close without CLEAR) and
  /// return the released app ids. Without this, a dead owner suppresses
  /// its slots' generics forever.
  pub fn note_clear_pid(&mut self, pid: u64) -> Vec<AppId> {
    self
      .live_ipc
      .extract_if(|_, owner| *owner == pid)
      .map(|(app, _)| app)
      .collect()
  }

  /// Whether generic detection must stay out of this slot right now.
  #[must_use]
  pub fn is_suppressed(&self, app_id: &str) -> bool {
    self.live_ipc.contains_key(app_id)
  }

  /// The game to re-assert when `app_id`'s IPC source cleared, if the
  /// scanner still reports that same game.
  #[must_use]
  pub fn resume_for(&self, app_id: &str) -> Option<ScannedGame> {
    self.last_scans.get(app_id).cloned()
  }
}

/// Track a generic publication for its later clear, bounded like the
/// handoff tables above (purge dead pids first, then evict arbitrarily).
/// Evicting a live entry only drops its future clear — the next scan
/// re-arms it (self-healing).
pub fn track_process_publication(map: &mut HashMap<AppId, u64>, app_id: AppId, pid: u64) {
  if map.len() >= MAX_HANDOFF_ENTRIES {
    map.retain(|_, known| is_process_alive(*known));
  }
  map.insert(app_id, pid);
  while map.len() > MAX_HANDOFF_ENTRIES {
    let Some(victim) = map.keys().next().cloned() else {
      break;
    };
    map.remove(&victim);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn liveness_spots_own_pid_and_rejects_absurd_ones() {
    // Contract on every platform: our own pid is alive, pid 0 never is,
    // and u32::MAX is not a real pid anywhere.
    assert!(is_process_alive(u64::from(std::process::id())));
    assert!(!is_process_alive(0));
    assert!(!is_process_alive(u64::from(u32::MAX)));
  }

  fn game(id: &str) -> ScannedGame {
    ScannedGame {
      id: AppId::from(id),
      name: "Game".to_string(),
      pid: 1234,
      start: 0,
    }
  }

  #[test]
  fn suppresses_while_ipc_live_and_resumes_on_owner_clear() {
    let game = game("111111111111111111");
    let mut handoff = HandoffState::default();
    assert!(!handoff.is_suppressed(game.id.as_ref()));

    handoff.note_publish(game.id.as_ref(), 77);
    assert!(handoff.is_suppressed(game.id.as_ref()));

    // A clear from a *different* pid (superseded companion) is ignored.
    handoff.note_scan(Some(game.clone()));
    assert!(!handoff.note_clear(game.id.as_ref(), 78));
    assert!(handoff.is_suppressed(game.id.as_ref()));

    // The owner's clear releases it, and the scan still reports the game.
    assert!(handoff.note_clear(game.id.as_ref(), 77));
    assert!(!handoff.is_suppressed(game.id.as_ref()));
    assert_eq!(handoff.resume_for(game.id.as_ref()), Some(game));
  }

  #[test]
  fn takeover_last_publisher_wins() {
    let mut handoff = HandoffState::default();
    handoff.note_publish("1", 10);
    // Companion B takes over: A's pid is forgotten, no leak.
    handoff.note_publish("1", 20);
    // A's late close must not resume the generic card under B.
    assert!(!handoff.note_clear("1", 10));
    assert!(handoff.is_suppressed("1"));
    // B's close releases.
    assert!(handoff.note_clear("1", 20));
    assert!(!handoff.is_suppressed("1"));
  }

  #[test]
  fn resume_only_matches_scanned_game() {
    let mut handoff = HandoffState::default();
    handoff.note_publish("1", 10);
    handoff.note_scan(None);
    assert!(handoff.note_clear("1", 10));
    // Scanner reports nothing: nothing to resume.
    assert_eq!(handoff.resume_for("1"), None);

    handoff.note_scan(Some(ScannedGame {
      id: AppId::from("2"),
      name: "Other".to_string(),
      pid: 9,
      start: 0,
    }));
    // A different game on screen: not ours to resume.
    assert_eq!(handoff.resume_for("1"), None);
  }

  #[test]
  fn abrupt_close_releases_every_slot_of_dead_pid() {
    let mut handoff = HandoffState::default();
    handoff.note_publish("1", 10);
    handoff.note_publish("2", 10);
    handoff.note_publish("3", 99);

    let mut released = handoff.note_clear_pid(10);
    released.sort();
    assert_eq!(released, vec![AppId::from("1"), AppId::from("2")]);
    // Other pids untouched; release is idempotent.
    assert!(handoff.is_suppressed("3"));
    assert!(!handoff.is_suppressed("1"));
    assert!(handoff.note_clear_pid(10).is_empty());
  }

  #[test]
  fn tables_stay_bounded() {
    let mut handoff = HandoffState::default();
    // pid 0 is never alive: every entry is purgeable garbage, so the
    // tables cannot grow past the cap even under hostile input.
    for index in 0..(MAX_HANDOFF_ENTRIES + 50) {
      handoff.note_publish(&format!("app-{index}"), 0);
    }
    assert!(handoff.live_ipc.len() <= MAX_HANDOFF_ENTRIES);
  }

  #[test]
  fn process_alive_rejects_zero_and_dead_pids() {
    assert!(!is_process_alive(0));
    assert!(!is_process_alive(u32::MAX as u64));
    assert!(is_process_alive(std::process::id() as u64));
  }
}
