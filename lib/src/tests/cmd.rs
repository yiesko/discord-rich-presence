use serde_json::json;

use crate::cmd::ActivityCmd;

fn parse_cmd(json: &str) -> ActivityCmd {
  serde_json::from_str(json).expect("failed to parse test command")
}

#[test]
fn fix_buttons_maps_objects_to_labels_and_urls() {
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 42,
          "activity": {
            "name": "Test",
            "buttons": [
              { "label": "Join", "url": "https://example.com/join" },
              { "label": "Watch", "url": "https://example.com/watch" }
            ]
          }
        },
        "nonce": "n"
      }"#,
  );

  cmd.fix();

  let activity = cmd.args.unwrap().activity.unwrap();
  assert_eq!(
    activity.buttons.unwrap(),
    vec![json!("Join"), json!("Watch")]
  );
  assert_eq!(
    activity.metadata.unwrap().button_urls.unwrap(),
    vec![
      "https://example.com/join".to_string(),
      "https://example.com/watch".to_string()
    ]
  );
}

#[test]
fn fix_buttons_keeps_plain_labels() {
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 42,
          "activity": {
            "name": "Test",
            "buttons": ["Play", "Details"]
          }
        },
        "nonce": "n"
      }"#,
  );

  cmd.fix();

  let activity = cmd.args.unwrap().activity.unwrap();
  assert_eq!(
    activity.buttons.unwrap(),
    vec![json!("Play"), json!("Details")]
  );
  assert!(activity.metadata.is_none());
}

#[test]
fn fix_buttons_no_urls_leaves_metadata_untouched() {
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 42,
          "activity": {
            "name": "Test",
            "buttons": [{ "label": "NoUrl" }]
          }
        },
        "nonce": "n"
      }"#,
  );

  cmd.fix();

  let activity = cmd.args.unwrap().activity.unwrap();
  assert_eq!(activity.buttons.unwrap(), vec![json!("NoUrl")]);
  assert!(activity.metadata.is_none());
}

#[test]
fn fix_buttons_no_buttons_is_noop() {
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 42,
          "activity": { "name": "Test" }
        },
        "nonce": "n"
      }"#,
  );

  cmd.fix();

  let activity = cmd.args.unwrap().activity.unwrap();
  assert!(activity.buttons.is_none());
  assert!(activity.metadata.is_none());
}

#[test]
fn fix_timestamps_converts_seconds_to_millis() {
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 42,
          "activity": {
            "name": "Test",
            "timestamps": { "start": 1000000000, "end": 2000000000 }
          }
        },
        "nonce": "n"
      }"#,
  );

  cmd.fix();

  let activity = cmd.args.unwrap().activity.unwrap();
  let timestamps = activity.timestamps.unwrap();
  assert_eq!(timestamps.start.unwrap().value(), 1_000_000_000_000);
  assert_eq!(timestamps.end.unwrap().value(), 2_000_000_000_000);
}

#[test]
fn fix_timestamps_converts_micros_and_nanos_to_millis() {
  // Real-world shapes: some SDKs send µs (~1e15) or ns (~1e18).
  let micros = 1_780_000_000_000_000_i64;
  let nanos = 1_780_000_000_000_000_000_i64;
  let json = format!(
    r#"{{
        "cmd": "SET_ACTIVITY",
        "args": {{
          "pid": 42,
          "activity": {{
            "name": "Test",
            "timestamps": {{ "start": {micros}, "end": {nanos} }}
          }}
        }},
        "nonce": "n"
      }}"#
  );
  let mut cmd = parse_cmd(&json);

  cmd.fix();

  let activity = cmd.args.unwrap().activity.unwrap();
  let timestamps = activity.timestamps.unwrap();
  assert_eq!(timestamps.start.unwrap().value(), micros / 1_000);
  assert_eq!(timestamps.end.unwrap().value(), nanos / 1_000_000);
}

#[test]
fn fix_timestamps_keeps_millis_untouched() {
  let future_ms = chrono::Utc::now().timestamp() + (100 * 365 * 24 * 3600) + 10_000;
  let json = format!(
    r#"{{
        "cmd": "SET_ACTIVITY",
        "args": {{
          "pid": 42,
          "activity": {{
            "name": "Test",
            "timestamps": {{ "start": {} }}
          }}
        }},
        "nonce": "n"
      }}"#,
    future_ms
  );
  let mut cmd = parse_cmd(&json);

  cmd.fix();

  let activity = cmd.args.unwrap().activity.unwrap();
  let timestamps = activity.timestamps.unwrap();
  assert_eq!(timestamps.start.unwrap().value(), future_ms);
}

#[test]
fn fix_flags_sets_instance_flag() {
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 42,
          "activity": {
            "name": "Test",
            "instance": true
          }
        },
        "nonce": "n"
      }"#,
  );

  cmd.fix();

  let activity = cmd.args.unwrap().activity.unwrap();
  assert_eq!(activity.flags, Some(1));
}

#[test]
fn fix_flags_keeps_existing_flags() {
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 42,
          "activity": {
            "name": "Test",
            "instance": true,
            "flags": 8
          }
        },
        "nonce": "n"
      }"#,
  );

  cmd.fix();

  let activity = cmd.args.unwrap().activity.unwrap();
  assert_eq!(activity.flags, Some(8));
}

#[test]
fn bridge_payload_preserves_buttons_and_metadata() {
  // End-to-end through the bridge encoder (arrpc #141): what a game
  // sends must reach bridge clients untouched.
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 42,
          "activity": {
            "name": "Test",
            "details": "Racing",
            "buttons": [
              { "label": "Join", "url": "https://example.com/join" },
              { "label": "Watch", "url": "https://example.com/watch" }
            ]
          }
        },
        "nonce": "n"
      }"#,
  );
  let cached = crate::commands::cached_activity(&mut cmd).expect("encodes");
  let payload: serde_json::Value = serde_json::from_str(&cached.json).expect("valid json");
  let activity = &payload["activity"];
  assert_eq!(activity["details"], "Racing");
  assert_eq!(activity["buttons"], json!(["Join", "Watch"]));
  assert_eq!(
    activity["metadata"]["button_urls"],
    json!(["https://example.com/join", "https://example.com/watch"])
  );
  // MessagePack twin carries the same activity.
  let decoded: serde_json::Value = rmp_serde::from_slice(&cached.msgpack).expect("valid msgpack");
  assert_eq!(decoded["activity"]["buttons"], json!(["Join", "Watch"]));
}

#[test]
fn bridge_payload_preserves_all_secret_kinds() {
  // Sibling of upstream #30 (payload fidelity): join/spectate/match must
  // all survive the bridge. `match` is a Rust keyword, hence match_secret.
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 7,
          "activity": {
            "name": "SecretGame",
            "secrets": { "join": "j", "spectate": "s", "match": "m" }
          }
        },
        "nonce": "n"
      }"#,
  );
  let cached = crate::commands::cached_activity(&mut cmd).expect("encodes");
  let payload: serde_json::Value = serde_json::from_str(&cached.json).expect("valid json");
  assert_eq!(
    payload["activity"]["secrets"],
    json!({ "join": "j", "spectate": "s", "match": "m" })
  );
}

#[test]
fn display_name_falls_back_to_details_then_state() {
  fn activity_of(json: &str) -> crate::cmd::Activity {
    parse_cmd(json).args.unwrap().activity.unwrap()
  }

  let named = activity_of(
    r#"{"cmd":"SET_ACTIVITY","nonce":"n","args":{"pid":1,"activity":{"name":"Game","details":"Song","state":"Artist"}}}"#,
  );
  assert_eq!(named.display_name(), "Game");

  // Music apps often send no name: song/artist still identify the publisher.
  let nameless = activity_of(
    r#"{"cmd":"SET_ACTIVITY","nonce":"n","args":{"pid":1,"activity":{"details":"Song","state":"Artist"}}}"#,
  );
  assert_eq!(nameless.display_name(), "Song");

  let stateless = activity_of(
    r#"{"cmd":"SET_ACTIVITY","nonce":"n","args":{"pid":1,"activity":{"state":"Artist"}}}"#,
  );
  assert_eq!(stateless.display_name(), "Artist");

  let blank =
    activity_of(r#"{"cmd":"SET_ACTIVITY","nonce":"n","args":{"pid":1,"activity":{"name":"   "}}}"#);
  assert_eq!(blank.display_name(), "?");
}

#[test]
fn activity_urls_survive_fix_and_bridge_encoding() {
  // Official clickable-asset fields: passthrough, never validated here.
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 42,
          "activity": {
            "name": "Test",
            "details": "Level 1",
            "details_url": "https://example.com/level/1",
            "state": "Hub",
            "state_url": "https://example.com/hub",
            "assets": {
              "large_image": "map",
              "large_url": "https://example.wiki/maps/Numbani",
              "small_image": "hero",
              "small_url": "https://example.wiki/heroes/Pharah"
            }
          }
        },
        "nonce": "n"
      }"#,
  );
  let cached = crate::commands::cached_activity(&mut cmd).expect("encodes");
  let payload: serde_json::Value = serde_json::from_str(&cached.json).expect("valid json");
  let activity = &payload["activity"];
  assert_eq!(activity["details_url"], "https://example.com/level/1");
  assert_eq!(activity["state_url"], "https://example.com/hub");
  assert_eq!(
    activity["assets"]["large_url"],
    "https://example.wiki/maps/Numbani"
  );
  assert_eq!(
    activity["assets"]["small_url"],
    "https://example.wiki/heroes/Pharah"
  );
}

#[test]
fn party_privacy_and_status_display_survive_bridge() {
  // Previously dropped silently (serde ignores unknown fields): a full
  // party + display-type payload must cross untouched.
  let mut cmd = parse_cmd(
    r#"{
        "cmd": "SET_ACTIVITY",
        "args": {
          "pid": 42,
          "activity": {
            "name": "Test",
            "party": {"id": "p1", "size": [3, 6], "privacy": 1},
            "status_display_type": 2
          }
        },
        "nonce": "n"
      }"#,
  );
  let cached = crate::commands::cached_activity(&mut cmd).expect("encodes");
  let payload: serde_json::Value = serde_json::from_str(&cached.json).expect("valid json");
  assert_eq!(
    payload["activity"]["party"],
    json!({"id": "p1", "size": [3, 6], "privacy": 1})
  );
  assert_eq!(payload["activity"]["status_display_type"], 2);
}
