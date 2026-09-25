//! Hostile timestamps must saturate, never panic: debug builds
//! overflow-check arithmetic, so `i64::MIN` seconds times 1000 would
//! abort the daemon without saturation.

use rsrpc_types::cmd::ActivityCmd;

/// `i64::MIN` seconds would overflow `* 1000`: saturation keeps the
/// floor instead of panicking (debug) or wrapping (release).
#[test]
fn hostile_timestamps_saturate_instead_of_overflowing() {
  let mut cmd: ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app",
    "args": {"pid": 7, "activity": {"name": "G", "type": 0,
      "timestamps": {"start": i64::MIN, "end": i64::MIN}}},
    "nonce": "n",
  }))
  .unwrap();
  // Must return, not panic.
  cmd.fix_timestamps();
  let timestamps = cmd
    .args
    .as_ref()
    .unwrap()
    .activity
    .as_ref()
    .unwrap()
    .timestamps
    .as_ref()
    .unwrap();
  // `TimeoutValue.0` is crate-private: read through `Debug` instead.
  let rendered = format!(
    "{:?}",
    (
      timestamps.start.as_ref().unwrap(),
      timestamps.end.as_ref().unwrap()
    )
  );
  assert!(
    rendered.contains("-9223372036854775808"),
    "must saturate at the floor, got {rendered}"
  );
}
