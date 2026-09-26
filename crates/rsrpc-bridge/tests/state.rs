use rsrpc_bridge::state::{
  STATE_FILE_PREFIX, StateServers, StateSnapshot, now_secs, select_slot, write_snapshot,
};

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
  let snapshot = StateSnapshot::new("0.35.0", StateServers::default(), vec![]);
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

  let snapshot = StateSnapshot::new("0.35.0", StateServers::default(), vec![]);
  write_snapshot(&path, &snapshot).expect("writes");
  assert!(path.exists());
  // No temp file leaks beside it (unique per-write names).
  let leftover_tmp = std::fs::read_dir(&dir)
    .expect("entries")
    .flatten()
    .any(|entry| {
      entry
        .path()
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.starts_with("tmp-"))
    });
  assert!(!leftover_tmp);

  let back: serde_json::Value =
    serde_json::from_str(&std::fs::read_to_string(&path).expect("reads")).expect("json");
  assert_eq!(back["appVersion"], "0.35.0");
  assert!(back.get("servers").is_some());
  assert!(back.get("activities").is_some());

  let _ = std::fs::remove_dir_all(&dir);
}

/// Snapshots are owner-only on Unix: presence metadata (app ids, pids)
/// must not leak to other local users under a permissive umask.
#[cfg(unix)]
#[test]
fn snapshot_file_is_owner_only() {
  use std::os::unix::fs::PermissionsExt;

  let dir = std::env::temp_dir().join("rsrpc-state-test-perms");
  let _ = std::fs::create_dir_all(&dir);
  let path = dir.join("rsrpc-state-0");

  let snapshot = StateSnapshot::new("0.35.0", StateServers::default(), vec![]);
  write_snapshot(&path, &snapshot).expect("writes");
  let mode = std::fs::metadata(&path)
    .expect("metadata")
    .permissions()
    .mode()
    & 0o777;
  assert_eq!(mode, 0o600, "snapshot must be owner-only, got {mode:o}");

  let _ = std::fs::remove_dir_all(&dir);
}

/// Concurrent writers sharing one slot all succeed and the published
/// file is whole: unique temps plus atomic rename, no torn publish.
#[test]
fn concurrent_writes_to_one_slot_stay_whole() {
  let dir = std::env::temp_dir().join(format!("rsrpc-state-test-races-{}", std::process::id()));
  let _ = std::fs::create_dir_all(&dir);
  let path = dir.join("rsrpc-state-0");

  std::thread::scope(|scope| {
    for _ in 0..8 {
      scope.spawn(|| {
        let snapshot = StateSnapshot::new("0.35.0", StateServers::default(), vec![]);
        write_snapshot(&path, &snapshot).expect("writes");
      });
    }
  });
  let back: serde_json::Value =
    serde_json::from_str(&std::fs::read_to_string(&path).expect("reads")).expect("whole json");
  assert_eq!(back["appVersion"], "0.35.0");

  let _ = std::fs::remove_dir_all(&dir);
}

/// A symlink planted at the slot is replaced, never followed: the
/// target keeps its content and the slot becomes a regular file.
#[cfg(unix)]
#[test]
fn slot_symlink_is_replaced_not_followed() {
  use std::os::unix::fs::symlink;

  let dir = std::env::temp_dir().join(format!("rsrpc-state-test-link-{}", std::process::id()));
  let _ = std::fs::create_dir_all(&dir);
  let target = dir.join("canary");
  std::fs::write(&target, b"untouched").expect("canary");
  let slot = dir.join("rsrpc-state-0");
  symlink(&target, &slot).expect("plant link");

  let snapshot = StateSnapshot::new("0.35.0", StateServers::default(), vec![]);
  write_snapshot(&slot, &snapshot).expect("writes");
  assert_eq!(std::fs::read(&target).expect("canary reads"), b"untouched");
  assert!(
    std::fs::symlink_metadata(&slot)
      .expect("slot meta")
      .is_file(),
    "slot must be a regular file after the write"
  );

  let _ = std::fs::remove_dir_all(&dir);
}

/// Slot selection sweeps stale crash temps and keeps fresh ones, under
/// the same staleness rule as slots (faked clock: a future `now` ages
/// everything past staleness, the real `now` keeps young files).
#[test]
fn select_slot_sweeps_only_stale_tmps() {
  let dir = std::env::temp_dir().join(format!("rsrpc-state-test-sweep-{}", std::process::id()));
  let _ = std::fs::create_dir_all(&dir);
  let real_now = now_secs();

  // Aged past staleness by a future clock: swept.
  let stale_tmp = dir.join("rsrpc-state-0.tmp-1-1");
  std::fs::write(&stale_tmp, b"crash").expect("stale tmp");
  select_slot(&dir, real_now + 3600).expect("a slot");
  assert!(!stale_tmp.exists(), "stale crash temp must go");

  // Young under the real clock: kept.
  let fresh_tmp = dir.join("rsrpc-state-1.tmp-1-2");
  std::fs::write(&fresh_tmp, b"racing").expect("fresh tmp");
  select_slot(&dir, real_now).expect("a slot");
  assert!(fresh_tmp.exists(), "fresh temp must stay");

  // Another program's temp in the shared dir: never ours, never
  // touched, however stale.
  let foreign_tmp = dir.join("other-program.tmp-9-9");
  std::fs::write(&foreign_tmp, b"not ours").expect("foreign tmp");
  select_slot(&dir, real_now + 3600).expect("a slot");
  assert!(foreign_tmp.exists(), "foreign temps must survive the sweep");

  let _ = std::fs::remove_dir_all(&dir);
}

/// Snapshots stamp the caller daemon version, never the defining crate's
/// version (this held when the code lived in its own crate, and holds
/// now that it lives in the bridge).
#[test]
fn snapshot_carries_caller_version_not_crate_version() {
  // Regression net: `env!("CARGO_PKG_VERSION")` inside the defining
  // module would freeze the version, so the daemon version travels as a
  // parameter instead.
  let snapshot = StateSnapshot::new("9.9.9-test", StateServers::default(), vec![]);
  assert_eq!(snapshot.app_version, "9.9.9-test");
}
