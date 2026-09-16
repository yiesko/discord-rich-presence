use crate::cmd::{ActivityCmd, ActivityCmdArgs};
use crate::server::client_connector::{
  ClientConnector, MAX_CACHED_ACTIVITIES, MAX_HANDOFF_ENTRIES, handle_bridge_control,
  is_genuine_clear, prune_cache, state_activities, take_process_clear,
};
use crate::server::utils::QueueGauge;
use crate::user::RpcUser;

fn test_user() -> std::sync::Arc<std::sync::Mutex<RpcUser>> {
  std::sync::Arc::new(std::sync::Mutex::new(RpcUser::default()))
}

fn cmd_with(pid: Option<u64>, activity: Option<crate::cmd::Activity>) -> ActivityCmd {
  ActivityCmd {
    args: Some(ActivityCmdArgs {
      pid,
      activity,
      code: None,
      user_id: None,
    }),
    ..ActivityCmd::empty()
  }
}

#[test]
fn genuine_clear_needs_nonzero_pid_and_null_activity() {
  // Sober/Roblox disconnect: null activity, real pid -> genuine
  assert!(is_genuine_clear(&cmd_with(Some(3), None)));
  // SUBSCRIBE before any SET_ACTIVITY: pid 0 -> spurious, ignore
  assert!(!is_genuine_clear(&cmd_with(Some(0), None)));
  // Missing pid entirely -> ignore
  assert!(!is_genuine_clear(&cmd_with(None, None)));
  // No args at all -> ignore
  assert!(!is_genuine_clear(&ActivityCmd::empty()));
  // Real Sober/Roblox payload shape (truncated): must not reset detection
  let cmd: ActivityCmd = serde_json::from_str(
    r#"{"nonce":"4","cmd":"SET_ACTIVITY","args":{"pid":3,"activity":{"details":"In the Roblox app"}}}"#,
  )
  .unwrap();
  assert!(!is_genuine_clear(&cmd));
}

#[test]
fn clones_share_detection_state() {
  // ClientConnector::new binds bridge ports; use uncommon ones for the test.
  let (_ipc_tx, ipc_rx) = std::sync::mpsc::channel();
  let (_proc_tx, proc_rx) = QueueGauge::pair();
  let (_ws_tx, ws_rx) = QueueGauge::pair();
  let a = ClientConnector::new(45971, 45981, 45972, test_user(), ipc_rx, proc_rx, ws_rx)
    .expect("test setup");
  let b = a.clone();

  // Shared detection state must be visible across clones (event_loop on
  // one, process_loop on the other): arming a process publication via b
  // shows up when a drains it.
  b.last_process
    .lock()
    .unwrap()
    .insert(crate::AppId::from("123456789012345678".to_string()), 42);
  assert_eq!(
    a.last_process.lock().unwrap().clone(),
    [(crate::AppId::from("123456789012345678".to_string()), 42)]
      .into_iter()
      .collect::<std::collections::HashMap<_, _>>()
  );
  assert_eq!(
    take_process_clear(&a),
    vec![(42, crate::AppId::from("123456789012345678".to_string()))]
  );
  assert!(b.last_process.lock().unwrap().is_empty());
}

#[test]
fn process_clear_consumes_outstanding_publication_once() {
  // ClientConnector::new binds bridge ports; use uncommon ones for the test.
  let (_ipc_tx, ipc_rx) = std::sync::mpsc::channel();
  let (_proc_tx, proc_rx) = QueueGauge::pair();
  let (_ws_tx, ws_rx) = QueueGauge::pair();
  let connector = ClientConnector::new(45973, 45983, 45974, test_user(), ipc_rx, proc_rx, ws_rx)
    .expect("test setup");

  // Nothing published: null scans skip.
  assert_eq!(take_process_clear(&connector), Vec::new());

  // A process publication arms exactly one clear, carrying its own pid.
  connector
    .last_process
    .lock()
    .unwrap()
    .insert(crate::AppId::from("111111111111111111".to_string()), 1234);
  assert_eq!(
    take_process_clear(&connector),
    vec![(1234, crate::AppId::from("111111111111111111".to_string()))]
  );
  // Consumed: further null scans skip (no clear spam, no re-clear).
  assert_eq!(take_process_clear(&connector), Vec::new());
}

#[test]
fn replay_cache_evicts_oldest_beyond_cap() {
  use crate::commands::empty_cached;

  let mut cache = std::collections::HashMap::new();
  for i in 0..=(MAX_CACHED_ACTIVITIES as u64) {
    cache.insert(
      crate::SocketId::from(format!("pid-{i}")),
      (
        empty_cached(1, crate::SocketId::from(format!("pid-{i}"))),
        i,
      ),
    );
  }
  assert_eq!(cache.len(), MAX_CACHED_ACTIVITIES + 1);

  prune_cache(&mut cache);

  assert_eq!(cache.len(), MAX_CACHED_ACTIVITIES);
  // Oldest (seq 0) evicted, newest kept.
  assert!(!cache.contains_key("pid-0"));
  assert!(cache.contains_key(format!("pid-{}", MAX_CACHED_ACTIVITIES).as_str()));

  // Within cap: a lone entry is untouched (no-shrink control case).
  let mut small = std::collections::HashMap::new();
  small.insert(
    crate::SocketId::from("a".to_string()),
    (empty_cached(1, crate::SocketId::from("a".to_string())), 7),
  );
  prune_cache(&mut small);
  assert!(small.contains_key("a"));
}

#[test]
fn set_user_patches_identity_and_acks() {
  let user = test_user();
  let (ack_text, changed) = handle_bridge_control(
    &user,
    r#"{"type":"SET_USER","nonce":3,"patch":{"username":"web"}}"#,
  )
  .expect("ack");
  let ack: serde_json::Value = serde_json::from_str(&ack_text).expect("valid json");

  assert_eq!(ack["type"], "SET_USER_ACK");
  assert_eq!(ack["nonce"], 3);
  assert_eq!(ack["data"]["success"], true);
  assert_eq!(ack["data"]["user"]["username"], "web");
  assert_eq!(user.lock().unwrap().username, "web");
  // The changed identity is reported for CURRENT_USER_UPDATE fan-out.
  assert_eq!(changed.expect("changed").username, "web");
}

#[test]
fn set_user_without_changes_reports_no_fanout() {
  let user = test_user();
  let (ack_text, changed) =
    handle_bridge_control(&user, r#"{"type":"SET_USER","patch":{"username":"arRPC"}}"#)
      .expect("ack");

  let ack: serde_json::Value = serde_json::from_str(&ack_text).expect("valid json");
  assert_eq!(ack["type"], "SET_USER_ACK");
  // Same identity: ACKed, but no DISPATCH needed.
  assert!(changed.is_none());
}

#[test]
fn reset_user_restores_startup_identity() {
  let user = test_user();
  user.lock().unwrap().username = "changed".to_string();

  let (ack_text, changed) = handle_bridge_control(&user, r#"{"type":"RESET_USER"}"#).expect("ack");
  let ack: serde_json::Value = serde_json::from_str(&ack_text).expect("valid json");

  assert_eq!(ack["type"], "RESET_USER_ACK");
  assert_eq!(user.lock().unwrap().username, "arRPC");
  assert!(changed.is_some());
}

#[test]
fn non_control_messages_pass_through_untouched() {
  let user = test_user();
  // Activity payloads and unknown types are not control: no ACK.
  assert!(handle_bridge_control(&user, r#"{"activity":null,"pid":1}"#).is_none());
  assert!(handle_bridge_control(&user, r#"{"type":"GET_IGNORED_GAMES"}"#).is_none());
  assert!(handle_bridge_control(&user, "not json").is_none());
  // Identity untouched by pass-through.
  assert_eq!(user.lock().unwrap().username, "arRPC");
}

#[test]
fn state_activities_flatten_replay_cache() {
  use crate::cmd::ActivityCmd;
  use crate::commands::cached_activity;

  let mut cmd: ActivityCmd = serde_json::from_str(
    r#"{"cmd":"SET_ACTIVITY","nonce":"n","application_id":"111","args":{"pid":9,"activity":{"name":"Game","timestamps":{"start":5}}}}"#,
  )
  .expect("parse");
  let payload = cached_activity(&mut cmd).expect("encodes");
  let mut cache = std::collections::HashMap::new();
  cache.insert(crate::SocketId::from("9".to_string()), (payload, 1));

  let activities = state_activities(&cache);
  assert_eq!(activities.len(), 1);
  assert_eq!(activities[0].socket_id, "9");
  assert_eq!(activities[0].name.as_deref(), Some("Game"));
  assert_eq!(activities[0].application_id.as_deref(), Some("111"));
  assert_eq!(activities[0].pid, Some(9));
}

#[test]
fn handoff_suppresses_while_ipc_live_and_resumes_on_owner_clear() {
  use crate::server::client_connector::{HandoffState, ScannedGame};

  let game = ScannedGame {
    id: crate::AppId::from("111111111111111111".to_string()),
    name: "Game".to_string(),
    pid: 1234,
    start: 0,
  };
  let mut handoff = HandoffState::default();
  assert!(!handoff.is_suppressed(game.id.as_ref()));

  // A live SDK presence takes the slot.
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
fn handoff_takeover_last_publisher_wins() {
  use crate::server::client_connector::HandoffState;

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
fn handoff_resume_only_matches_scanned_game() {
  use crate::server::client_connector::{HandoffState, ScannedGame};

  let mut handoff = HandoffState::default();
  handoff.note_publish("1", 10);
  handoff.note_scan(None);
  assert!(handoff.note_clear("1", 10));
  // Scanner reports nothing: nothing to resume.
  assert_eq!(handoff.resume_for("1"), None);

  handoff.note_scan(Some(ScannedGame {
    id: crate::AppId::from("2".to_string()),
    name: "Other".to_string(),
    pid: 9,
    start: 0,
  }));
  // A different game on screen: not ours to resume.
  assert_eq!(handoff.resume_for("1"), None);
}

#[test]
fn abrupt_close_releases_every_slot_of_dead_pid() {
  // Socket died without CLEAR (send_empty carries no app id): every slot
  // the dead pid owned must release, or their generics stay suppressed
  // by a ghost owner forever.
  use crate::server::client_connector::HandoffState;

  let mut handoff = HandoffState::default();
  handoff.note_publish("1", 10);
  handoff.note_publish("2", 10);
  handoff.note_publish("3", 99);

  let mut released = handoff.note_clear_pid(10);
  released.sort();
  assert_eq!(
    released,
    vec![
      crate::AppId::from("1".to_string()),
      crate::AppId::from("2".to_string())
    ]
  );
  // Other pids untouched; release is idempotent.
  assert!(handoff.is_suppressed("3"));
  assert!(!handoff.is_suppressed("1"));
  assert!(handoff.note_clear_pid(10).is_empty());
}

#[test]
fn process_clear_drains_every_armed_slot_sorted() {
  // Multi-game scans publish per slot: one null event must clear all of
  // them, deterministically ordered, then go quiet.
  use crate::server::client_connector::take_process_clear;

  let (_ipc_tx, ipc_rx) = std::sync::mpsc::channel();
  let (_proc_tx, proc_rx) = QueueGauge::pair();
  let (_ws_tx, ws_rx) = QueueGauge::pair();
  let connector = ClientConnector::new(45977, 45987, 45978, test_user(), ipc_rx, proc_rx, ws_rx)
    .expect("test setup");

  {
    let mut armed = connector.last_process.lock().unwrap();
    armed.insert(crate::AppId::from("222222222222222222".to_string()), 22);
    armed.insert(crate::AppId::from("111111111111111111".to_string()), 11);
  }
  assert_eq!(
    take_process_clear(&connector),
    vec![
      (11, crate::AppId::from("111111111111111111".to_string())),
      (22, crate::AppId::from("222222222222222222".to_string())),
    ]
  );
  assert_eq!(take_process_clear(&connector), Vec::new());
}

#[test]
fn scan_memory_is_per_slot() {
  // Every scan event is remembered by app id (not just the last one),
  // so any slot's IPC clear can resume its own generic; null wipes all.
  use crate::server::client_connector::{HandoffState, ScannedGame};

  let game = |id: &str| ScannedGame {
    id: crate::AppId::from(id),
    name: "Game".to_string(),
    pid: 1,
    start: 0,
  };
  let mut handoff = HandoffState::default();
  handoff.note_scan(Some(game("1")));
  handoff.note_scan(Some(game("2")));
  assert_eq!(handoff.resume_for("1"), Some(game("1")));
  assert_eq!(handoff.resume_for("2"), Some(game("2")));
  handoff.note_scan(None);
  assert_eq!(handoff.resume_for("1"), None);
  assert_eq!(handoff.resume_for("2"), None);
}

#[test]
fn process_alive_rejects_zero_and_dead_pids() {
  use crate::server::client_connector::is_process_alive;

  assert!(!is_process_alive(0));
  assert!(is_process_alive(std::process::id() as u64));
  assert!(!is_process_alive(u64::MAX));
}

#[test]
fn generic_payload_carries_scanned_identity() {
  use crate::server::client_connector::{ScannedGame, generic_payload};

  let payload = generic_payload(&ScannedGame {
    id: crate::AppId::from("111111111111111111".to_string()),
    name: "Game".to_string(),
    pid: 1234,
    start: 7,
  });
  let body: serde_json::Value = serde_json::from_str(&payload.json).expect("valid json");
  assert_eq!(body["socketId"], "111111111111111111");
  assert_eq!(body["activity"]["application_id"], "111111111111111111");
  assert_eq!(body["activity"]["name"], "Game");
  assert_eq!(body["activity"]["timestamps"]["start"], 7);
  assert_eq!(body["pid"], 1234);
}

#[test]
fn generic_payload_start_is_numeric_millis() {
  // Strict clients silently drop generic frames whose start timestamp is a
  // string: Discord's schema wants integer millis (see stamp_activity).
  use crate::server::client_connector::{ScannedGame, generic_payload};

  let payload = generic_payload(&ScannedGame {
    id: crate::AppId::from("111111111111111111".to_string()),
    name: "Game".to_string(),
    pid: 1234,
    start: 1_700_000_000_000,
  });
  let body: serde_json::Value = serde_json::from_str(&payload.json).expect("valid json");
  let start = &body["activity"]["timestamps"]["start"];
  assert!(
    start.is_u64(),
    "start must serialize as a number, got {start}"
  );
  assert_eq!(start.as_u64(), Some(1_700_000_000_000));
}

#[test]
fn handoff_tables_are_bounded() {
  use crate::server::client_connector::{HandoffState, ScannedGame};

  let mut handoff = HandoffState::default();
  // Flooding with dead owners (crashed companions that never cleared):
  // purged, never accumulated.
  for i in 0..(MAX_HANDOFF_ENTRIES + 10) {
    handoff.note_publish(&format!("dead-app-{i}"), u64::MAX);
    handoff.note_scan(Some(ScannedGame {
      id: crate::AppId::from(format!("dead-scan-{i}")),
      name: "Dead".to_string(),
      pid: u64::MAX,
      start: 1,
    }));
  }
  assert!(handoff.live_ipc_len() <= MAX_HANDOFF_ENTRIES);
  assert!(handoff.last_scans_len() <= MAX_HANDOFF_ENTRIES);

  // Flooding with one LIVE pid (malicious/buggy client, infinite ids):
  // hard-capped, legitimate slots (<5) never near it.
  let live = std::process::id() as u64;
  for i in 0..(MAX_HANDOFF_ENTRIES + 10) {
    handoff.note_publish(&format!("live-app-{i}"), live);
  }
  assert_eq!(handoff.live_ipc_len(), MAX_HANDOFF_ENTRIES);
}

#[test]
fn broadcast_raw_prunes_dead_bridge_client() {
  // Real server-side Responder, kill -9 simulated: deterministic after the
  // helper's gate (send already fails, Disconnect already drained).
  let responder = super::WsTestClient::connect().kill();

  // `new` binds but spawns no threads: deterministic, no background drain.
  let (_ipc_tx, ipc_rx) = std::sync::mpsc::channel();
  let (_proc_tx, proc_rx) = QueueGauge::pair();
  let (_ws_tx, ws_rx) = QueueGauge::pair();
  let connector = ClientConnector::new(46171, 46181, 46172, test_user(), ipc_rx, proc_rx, ws_rx)
    .expect("test setup");
  connector.json_clients.lock().unwrap().insert(7, responder);
  let cmd = ActivityCmd {
    cmd: "INVITE_BROWSER".to_string(),
    ..ActivityCmd::empty()
  };
  connector.broadcast_raw(&cmd);
  assert!(
    !connector.json_clients.lock().unwrap().contains_key(&7),
    "dead bridge client must be pruned by broadcast_raw, like send_to_all does"
  );
}
