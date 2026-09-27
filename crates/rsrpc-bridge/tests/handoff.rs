//! Handoff ownership tests: suppression, resume, release and bounds.
//!
//! Public-API behavior only (the one private-helper test stays inline in
//! `handoff.rs` by design).

use rsrpc_bridge::handoff::{HandoffState, ScannedGame, is_process_alive};
use rsrpc_types::AppId;

/// Own pid is alive everywhere; 0 and `u32::MAX` never are.
#[test]
fn liveness_spots_own_pid_and_rejects_absurd_ones() {
  // Contract on every platform: our own pid is alive, pid 0 never is,
  // and u32::MAX is not a real pid anywhere.
  assert!(is_process_alive(u64::from(std::process::id())));
  assert!(!is_process_alive(0));
  assert!(!is_process_alive(u64::from(u32::MAX)));
}

/// Fixture game (fixed pid 1234) for handoff unit tests.
fn game(id: &str) -> ScannedGame {
  ScannedGame {
    id: AppId::from(id),
    name: "Game".to_string(),
    pid: 1234,
    start: 0,
    source: "automaton".to_string(),
    process_start_ms: None,
  }
}

/// Only the owning pid's clear releases suppression; others are ignored.
#[test]
fn suppresses_while_ipc_live_and_resumes_on_owner_clear() {
  let game = game("111111111111111111");
  let mut handoff = HandoffState::default();
  assert!(!handoff.is_suppressed(game.id.as_ref()));

  handoff.note_publish(game.id.as_ref(), 77, &[]);
  assert!(handoff.is_suppressed(game.id.as_ref()));

  // A clear from a *different* pid (superseded companion) is ignored.
  handoff.note_scan(Some(game.clone()), &[]);
  assert!(!handoff.note_clear(game.id.as_ref(), 78));
  assert!(handoff.is_suppressed(game.id.as_ref()));

  // The owner's clear releases it, and the scan still reports the game.
  assert!(handoff.note_clear(game.id.as_ref(), 77));
  assert!(!handoff.is_suppressed(game.id.as_ref()));
  assert_eq!(handoff.resume_for(game.id.as_ref()), Some(game));
}

/// `note_remove` drops one slot (pid-gated) and leaves the rest alone.
#[test]
fn per_slot_remove_forgets_only_that_slot() {
  let mut handoff = HandoffState::default();
  handoff.note_scan(Some(game("1")), &[]);
  handoff.note_scan(Some(game("2")), &[]);
  assert!(handoff.resume_for("1").is_some());
  // Stale pid never drops a newer scan.
  assert!(handoff.note_remove("1", 9999).is_none());
  assert!(handoff.resume_for("1").is_some());
  assert!(handoff.note_remove("1", 1234).is_some());
  assert_eq!(handoff.resume_for("1"), None);
  assert!(handoff.resume_for("2").is_some());
  assert!(handoff.note_remove("missing", 1).is_none());
}

/// Takeover forgets the old pid: its late clear must not resume generics.
#[test]
fn takeover_last_publisher_wins() {
  let mut handoff = HandoffState::default();
  handoff.note_publish("1", 10, &[]);
  // Companion B takes over: A's pid is forgotten, no leak.
  handoff.note_publish("1", 20, &[]);
  // A's late close must not resume the generic card under B.
  assert!(!handoff.note_clear("1", 10));
  assert!(handoff.is_suppressed("1"));
  // B's close releases.
  assert!(handoff.note_clear("1", 20));
  assert!(!handoff.is_suppressed("1"));
}

/// `owner_of` tracks the current owner through publishes, takeovers
/// and releases: the takeover log reads this to name both sides.
#[test]
fn owner_of_follows_publishes_and_releases() {
  let mut handoff = HandoffState::default();
  assert_eq!(handoff.owner_of("1"), None);
  handoff.note_publish("1", 10, &[]);
  assert_eq!(handoff.owner_of("1"), Some(10));
  // Companion takeover replaces the owner.
  handoff.note_publish("1", 20, &[]);
  assert_eq!(handoff.owner_of("1"), Some(20));
  // Owner's clear releases; a stranger's does not.
  assert!(!handoff.note_clear("1", 10));
  assert_eq!(handoff.owner_of("1"), Some(20));
  assert!(handoff.note_clear("1", 20));
  assert_eq!(handoff.owner_of("1"), None);
}

/// Resume fires only for the game the scanner still reports.
#[test]
fn resume_only_matches_scanned_game() {
  let mut handoff = HandoffState::default();
  handoff.note_publish("1", 10, &[]);
  handoff.note_scan(None, &[]);
  assert!(handoff.note_clear("1", 10));
  // Scanner reports nothing: nothing to resume.
  assert_eq!(handoff.resume_for("1"), None);

  handoff.note_scan(
    Some(ScannedGame {
      id: AppId::from("2"),
      name: "Other".to_string(),
      pid: 9,
      start: 0,
      source: "automaton".to_string(),
      process_start_ms: None,
    }),
    &[],
  );
  // A different game on screen: not ours to resume.
  assert_eq!(handoff.resume_for("1"), None);
}

/// Abrupt close releases every slot of the dead pid, idempotently.
#[test]
fn abrupt_close_releases_every_slot_of_dead_pid() {
  let mut handoff = HandoffState::default();
  handoff.note_publish("1", 10, &[]);
  handoff.note_publish("2", 10, &[]);
  handoff.note_publish("3", 99, &[]);

  let mut released = handoff.note_clear_pid(10);
  released.sort();
  assert_eq!(released, vec![AppId::from("1"), AppId::from("2")]);
  // Other pids untouched; release is idempotent.
  assert!(handoff.is_suppressed("3"));
  assert!(!handoff.is_suppressed("1"));
  assert!(handoff.note_clear_pid(10).is_empty());
}

/// Pid 0 and dead pids read dead; our own pid reads alive.
#[test]
fn process_alive_rejects_zero_and_dead_pids() {
  assert!(!is_process_alive(0));
  assert!(!is_process_alive(u32::MAX as u64));
  assert!(is_process_alive(std::process::id() as u64));
}
