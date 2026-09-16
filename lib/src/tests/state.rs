use crate::state::{STATE_FILE_PREFIX, StateServers, StateSnapshot, select_slot, write_snapshot};

#[test]
fn missing_slot_is_reusable() {
  let dir = std::env::temp_dir().join("rsrpc-state-test-missing");
  let _ = std::fs::create_dir_all(&dir);

  let slot = select_slot(&dir, 1_700_000_000).expect("a slot");
  assert!(slot.starts_with(&dir));
  assert!(
    slot
      .file_name()
      .expect("name")
      .to_str()
      .expect("utf8")
      .starts_with(STATE_FILE_PREFIX)
  );
  let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fresh_slot_is_skipped_stale_slot_is_reused() {
  let dir = std::env::temp_dir().join("rsrpc-state-test-fresh");
  let _ = std::fs::create_dir_all(&dir);
  let now_secs = 1_700_000_000_u64;

  // Fresh snapshot in slot 0 (timestamp == now).
  let snapshot = StateSnapshot::new(env!("CARGO_PKG_VERSION"), StateServers::default(), vec![]);
  let slot0 = dir.join(format!("{STATE_FILE_PREFIX}0"));
  // Rewrite with a controlled fresh timestamp.
  let mut body = serde_json::to_value(&snapshot).expect("json");
  body["timestamp"] = serde_json::json!(now_secs as i64 * 1000);
  std::fs::write(&slot0, serde_json::to_vec(&body).expect("bytes")).expect("write");

  // Slot 0 is fresh: selection must skip to slot 1.
  let slot = select_slot(&dir, now_secs).expect("a slot");
  assert_eq!(slot, dir.join(format!("{STATE_FILE_PREFIX}1")));

  // Age slot 0 past staleness: it becomes reusable again.
  body["timestamp"] = serde_json::json!((now_secs - 60) as i64 * 1000);
  std::fs::write(&slot0, serde_json::to_vec(&body).expect("bytes")).expect("write");
  let slot = select_slot(&dir, now_secs).expect("a slot");
  assert_eq!(slot, slot0);

  // Corrupt slot: reusable too.
  std::fs::write(&slot0, b"{not json").expect("write");
  let slot = select_slot(&dir, now_secs).expect("a slot");
  assert_eq!(slot, slot0);

  let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_round_trips_through_atomic_write() {
  let dir = std::env::temp_dir().join("rsrpc-state-test-write");
  let _ = std::fs::create_dir_all(&dir);
  let path = dir.join("rsrpc-state-0");

  let snapshot = StateSnapshot::new(env!("CARGO_PKG_VERSION"), StateServers::default(), vec![]);
  write_snapshot(&path, &snapshot).expect("writes");
  assert!(path.exists());
  // No temp file leaks beside it.
  assert!(!dir.join("rsrpc-state-0.tmp").exists());

  let back: serde_json::Value =
    serde_json::from_str(&std::fs::read_to_string(&path).expect("reads")).expect("json");
  assert_eq!(back["appVersion"], env!("CARGO_PKG_VERSION"));
  assert!(back.get("servers").is_some());
  assert!(back.get("activities").is_some());

  let _ = std::fs::remove_dir_all(&dir);
}
