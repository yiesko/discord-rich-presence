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

/// `fix_buttons` splits label/url objects into the official shape: labels
/// into `buttons`, urls into `metadata.button_urls`. A mixed array keeps
/// the two collections independent (arRPC `map(x => x.label)` /
/// `map(x => x.url)` parity): a url-only object contributes no label and
/// a label-only object no url — they must never align per index.
#[test]
fn fix_buttons_maps_mixed_label_and_url_objects() {
  let mut cmd: ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app",
    "args": {"pid": 7, "activity": {"name": "G", "type": 0,
      "buttons": [
        {"label": "Play", "url": "https://example.com/play"},
        {"label": "Watch"},
        {"url": "https://example.com/only-url"},
        "plain-string"
      ]}},
    "nonce": "n",
  }))
  .unwrap();
  cmd.fix_buttons();
  let activity = cmd.args.as_ref().unwrap().activity.as_ref().unwrap();
  assert_eq!(
    activity.buttons,
    Some(vec![
      serde_json::json!("Play"),
      serde_json::json!("Watch"),
      serde_json::json!("plain-string"),
    ]),
    "labels and urls collect independently — url-only objects add no label"
  );
  let metadata = activity.metadata.as_ref().expect("urls attach metadata");
  assert_eq!(
    metadata.button_urls,
    Some(vec![
      "https://example.com/play".to_string(),
      "https://example.com/only-url".to_string()
    ])
  );
}

/// Buttons with no urls at all attach no metadata; plain strings and
/// non-string labels pass through verbatim (JS `map` parity).
#[test]
fn fix_buttons_without_urls_attaches_no_metadata() {
  let mut cmd: ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app",
    "args": {"pid": 7, "activity": {"name": "G", "type": 0,
      "buttons": [{"label": "Play"}, "Watch", {"label": 42}]}},
    "nonce": "n",
  }))
  .unwrap();
  cmd.fix_buttons();
  let activity = cmd.args.as_ref().unwrap().activity.as_ref().unwrap();
  assert_eq!(
    activity.buttons,
    Some(vec![
      serde_json::json!("Play"),
      serde_json::json!("Watch"),
      serde_json::json!(42),
    ]),
    "non-string labels are pushed verbatim"
  );
  assert!(activity.metadata.is_none());
}

/// `fix_flags` derives `flags: 1` from `instance: true` only — an
/// explicit flags value (even 0) and `instance: false` stay untouched.
#[test]
fn fix_flags_derives_one_from_instance_only() {
  let mut derived: ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app",
    "args": {"pid": 7, "activity": {"name": "G", "type": 0, "instance": true}},
    "nonce": "n",
  }))
  .unwrap();
  derived.fix_flags();
  let activity = derived.args.as_ref().unwrap().activity.as_ref().unwrap();
  assert_eq!(activity.flags, Some(1));

  let mut explicit: ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app",
    "args": {"pid": 7, "activity": {"name": "G", "type": 0, "instance": true, "flags": 0}},
    "nonce": "n",
  }))
  .unwrap();
  explicit.fix_flags();
  let activity = explicit.args.as_ref().unwrap().activity.as_ref().unwrap();
  assert_eq!(
    activity.flags,
    Some(0),
    "explicit flags must not be overwritten"
  );

  let mut off: ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app",
    "args": {"pid": 7, "activity": {"name": "G", "type": 0, "instance": false}},
    "nonce": "n",
  }))
  .unwrap();
  off.fix_flags();
  let activity = off.args.as_ref().unwrap().activity.as_ref().unwrap();
  assert_eq!(activity.flags, None);
}

/// `fix_timestamps` normalizes every client precision to milliseconds:
/// seconds ×1000, millis pass through, µs ÷1e3, ns ÷1e6 (arRPC
/// precision-sniffing parity).
#[test]
fn fix_timestamps_normalizes_every_unit_branch() {
  let mut cmd: ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app",
    "args": {"pid": 7, "activity": {"name": "G", "type": 0,
      "timestamps": {
        "start": 1789148307i64,
        "end": 1789148307000i64
      }}},
    "nonce": "n",
  }))
  .unwrap();
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
  assert_eq!(
    timestamps.start.as_ref().unwrap().value(),
    1789148307000,
    "seconds multiply by 1000"
  );
  assert_eq!(
    timestamps.end.as_ref().unwrap().value(),
    1789148307000,
    "milliseconds pass through"
  );
}

/// Microsecond and nanosecond timestamps divide down to milliseconds.
#[test]
fn fix_timestamps_normalizes_micros_and_nanos() {
  let mut cmd: ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app",
    "args": {"pid": 7, "activity": {"name": "G", "type": 0,
      "timestamps": {
        "start": 178914830700000i64,
        "end": 178914830700000000i64
      }}},
    "nonce": "n",
  }))
  .unwrap();
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
  assert_eq!(
    timestamps.start.as_ref().unwrap().value(),
    178914830700,
    "microseconds divide by 1000"
  );
  assert_eq!(
    timestamps.end.as_ref().unwrap().value(),
    178914830700,
    "nanoseconds divide by 1000000"
  );
}
