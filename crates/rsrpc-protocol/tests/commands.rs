use std::time::{Duration, Instant};

use serde_json::Value;

use rsrpc_protocol::commands::{
  RecentActivities, activity_fingerprint, cached_activity, current_user_update, generic_ack,
  rpc_error, set_activity_response, subscribe_ack, unsupported_command, user_response,
};
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;

fn parse_cmd(json: &str) -> ActivityCmd {
  serde_json::from_str(json).expect("failed to parse test command")
}

#[test]
fn subscribe_ack_echoes_cmd_evt_and_nonce() {
  let cmd =
    parse_cmd(r#"{"cmd":"SUBSCRIBE","evt":"VOICE_STATE_UPDATE","nonce":"abc","args":null}"#);

  let reply: Value = serde_json::from_str(&subscribe_ack(&cmd)).expect("valid json");

  assert_eq!(reply["cmd"], "SUBSCRIBE");
  assert_eq!(reply["data"]["evt"], "VOICE_STATE_UPDATE");
  assert!(reply["evt"].is_null());
  assert_eq!(reply["nonce"], "abc");
}

#[test]
fn rpc_error_carries_code_and_message() {
  let reply: Value = serde_json::from_str(&rpc_error(
    "INVITE_BROWSER",
    &Value::String("n".to_string()),
    4011,
    "Invalid invite id",
  ))
  .expect("valid json");

  assert_eq!(reply["cmd"], "INVITE_BROWSER");
  assert_eq!(reply["data"]["code"], 4011);
  assert_eq!(reply["data"]["message"], "Invalid invite id");
  assert_eq!(reply["evt"], "ERROR");
  assert_eq!(reply["nonce"], "n");
}

#[test]
fn generic_ack_confirms_receipt_without_claiming_success() {
  let cmd = parse_cmd(r#"{"cmd":"DEEP_LINK","nonce":"7","args":null}"#);

  let reply: Value = serde_json::from_str(&generic_ack(&cmd)).expect("valid json");

  assert_eq!(reply["cmd"], "DEEP_LINK");
  assert!(reply["data"].is_null());
  assert!(reply["evt"].is_null());
  assert_eq!(reply["nonce"], "7");
}

#[test]
fn user_response_returns_identity_or_null() {
  let cmd = parse_cmd(r#"{"cmd":"GET_USER","nonce":"g1","args":null}"#);
  let user = RpcUser::default();

  let hit: Value = serde_json::from_str(&user_response(&cmd, Some(&user))).expect("valid json");
  assert_eq!(hit["cmd"], "GET_USER");
  assert_eq!(hit["data"]["id"], "1045800378228281345");
  assert!(hit["evt"].is_null());
  assert_eq!(hit["nonce"], "g1");

  // Unknown id: official "user object or null".
  let miss: Value = serde_json::from_str(&user_response(&cmd, None)).expect("valid json");
  assert!(miss["data"].is_null());
}

#[test]
fn current_user_update_is_a_dispatch_with_the_user_inside() {
  let frame: Value =
    serde_json::from_str(&current_user_update(&RpcUser::default())).expect("valid json");

  assert_eq!(frame["cmd"], "DISPATCH");
  assert_eq!(frame["evt"], "CURRENT_USER_UPDATE");
  assert_eq!(frame["data"]["username"], "arRPC");
  assert!(frame["nonce"].is_null());
}

#[test]
fn unsupported_commands_map_to_official_codes() {
  // OAuth needs the in-client flow + app secret.
  assert_eq!(
    unsupported_command("AUTHORIZE"),
    Some((5000, "Authorization requires the real Discord client"))
  );
  assert_eq!(
    unsupported_command("AUTHENTICATE"),
    Some((5000, "Authorization requires the real Discord client"))
  );
  // Invites target live sessions on the real client.
  assert_eq!(
    unsupported_command("SEND_ACTIVITY_JOIN_INVITE"),
    Some((
      5006,
      "No eligible activity: invites require the real Discord client"
    ))
  );
  // Voice/guilds/overlay/store: real-client state only.
  let (code, _) = unsupported_command("SELECT_VOICE_CHANNEL").expect("known");
  assert_eq!(code, 1000);
  let (code, _) = unsupported_command("GET_GUILDS").expect("known");
  assert_eq!(code, 1000);
  // Handled elsewhere or genuinely unknown: no entry.
  assert!(unsupported_command("SET_ACTIVITY").is_none());
  assert!(unsupported_command("GET_USER").is_none());
  assert!(unsupported_command("FROBNICATE").is_none());
}

#[test]
fn rich_activity_roundtrips_every_field_through_cached_activity() {
  // Full-rich SET_ACTIVITY as a real game SDK sends it (details, state,
  // timestamps, assets, party, secrets, buttons, flags, emoji, display
  // type, plus one unknown future key): the bridge payload must carry
  // every one of them to consumers, byte-equivalent modulo key order.
  let mut cmd = parse_cmd(
    r#"{"cmd":"SET_ACTIVITY","nonce":"rich-1","args":{"pid":424242,"activity":{
      "name":"Rook R.E.P.O. Detail","type":0,"details":"Battle Creek",
      "state":"In Competitive Match","timestamps":{"start":1789148307000},
      "assets":{"large_image":"canary-large","large_text":"Canary","small_image":"ptb-small","small_text":"PTB"},
      "party":{"id":"party_aac0ffee","size":[2,4],"privacy":1},
      "secrets":{"join":"L33tJoin","spectate":"L33tSpec","match":"L33tMatch"},
      "buttons":[{"label":"Play","url":"https://example.com/play"}],
      "instance":true,"flags":1,"status_display_type":1,
      "emoji":{"name":" 잠수","id":"123","animated":false},
      "mystery_field_xyz":"must-survive"}}}"#,
  );
  let fp = activity_fingerprint(&mut cmd);
  let payload = cached_activity(&mut cmd, fp).expect("rich activity encodes");
  let body: Value = serde_json::from_str(&payload.json).expect("valid json");
  let activity = &body["activity"];
  assert_eq!(activity["details"], "Battle Creek");
  assert_eq!(activity["state"], "In Competitive Match");
  assert_eq!(activity["timestamps"]["start"], 1789148307000i64);
  assert_eq!(activity["assets"]["large_image"], "canary-large");
  assert_eq!(activity["party"]["size"], serde_json::json!([2, 4]));
  assert_eq!(activity["party"]["privacy"], 1);
  assert_eq!(activity["secrets"]["join"], "L33tJoin");
  assert_eq!(activity["secrets"]["spectate"], "L33tSpec");
  assert_eq!(activity["secrets"]["match"], "L33tMatch");
  assert_eq!(activity["buttons"], serde_json::json!(["Play"]));
  assert_eq!(activity["instance"], true);
  assert_eq!(activity["status_display_type"], 1);
  assert_eq!(activity["mystery_field_xyz"], "must-survive");
  // Same bytes on the MessagePack leg.
  let decoded: Value = rmp_serde::from_slice(&payload.msgpack).expect("valid msgpack");
  assert_eq!(
    decoded["activity"]["party"]["size"],
    serde_json::json!([2, 4])
  );
  assert_eq!(decoded["activity"]["mystery_field_xyz"], "must-survive");
}

#[test]
fn set_activity_response_echoes_activity_intact() {
  // A non-Playing activity: the echo must preserve name/type/details
  // (official echo semantics) instead of rewriting them.
  let mut cmd = parse_cmd(
    r#"{"cmd":"SET_ACTIVITY","nonce":"echo-1","args":{"pid":777,"activity":{
      "name":"My Game","type":2,"details":"AFK"}}}"#,
  );
  cmd.application_id = Some("123".to_string());

  let reply: Value =
    serde_json::from_str(&set_activity_response(&cmd).expect("response")).expect("valid json");

  assert_eq!(reply["cmd"], "SET_ACTIVITY");
  assert_eq!(reply["data"]["name"], "My Game");
  assert_eq!(reply["data"]["type"], 2);
  assert_eq!(reply["data"]["details"], "AFK");
  assert_eq!(reply["data"]["application_id"], "123");
  assert_eq!(reply["nonce"], "echo-1");
}

#[test]
fn set_activity_response_keeps_extra_and_derived_metadata() {
  // arRPC #141 parity: the echo carries unknown future keys (extra)
  // and derived metadata (button_urls from buttons), not just the
  // known activity fields.
  let mut cmd = parse_cmd(
    r#"{"cmd":"SET_ACTIVITY","nonce":"echo-2","args":{"pid":777,"activity":{
      "name":"My Game","type":0,
      "buttons":[{"label":"Play","url":"https://example.com/play"}],
      "mystery_field_xyz":"must-survive"}}}"#,
  );
  cmd.application_id = Some("123".to_string());
  cmd.fix();

  let reply: Value =
    serde_json::from_str(&set_activity_response(&cmd).expect("response")).expect("valid json");

  assert_eq!(
    reply["data"]["metadata"]["button_urls"],
    serde_json::json!(["https://example.com/play"])
  );
  assert_eq!(reply["data"]["mystery_field_xyz"], "must-survive");
}

#[test]
fn set_activity_response_clear_carries_null_data() {
  let mut cmd =
    parse_cmd(r#"{"cmd":"SET_ACTIVITY","nonce":"clear-1","args":{"pid":777,"activity":null}}"#);
  cmd.application_id = Some("123".to_string());

  let reply: Value =
    serde_json::from_str(&set_activity_response(&cmd).expect("response")).expect("valid json");

  assert_eq!(reply["cmd"], "SET_ACTIVITY");
  assert!(reply["data"].is_null());
  assert_eq!(reply["nonce"], "clear-1");
}

#[test]
fn dedup_drops_exact_duplicate_inside_window() {
  let mut recent = RecentActivities::new(Duration::from_secs(5), 8);
  let t0 = Instant::now();

  assert!(!recent.should_drop("app", 1, Some(b"{}"), t0));
  assert!(recent.should_drop("app", 1, Some(b"{}"), t0 + Duration::from_secs(1)));
  // ...but the same bytes from another pid/app are a different slot.
  assert!(!recent.should_drop("app", 2, Some(b"{}"), t0 + Duration::from_secs(1)));
  assert!(!recent.should_drop("other", 1, Some(b"{}"), t0 + Duration::from_secs(1)));
}

#[test]
fn dedup_passes_changes_clears_and_expiry() {
  let mut recent = RecentActivities::new(Duration::from_secs(5), 8);
  let t0 = Instant::now();

  assert!(!recent.should_drop("app", 1, Some(b"a"), t0));
  // Any changed byte passes and becomes the new reference.
  assert!(!recent.should_drop("app", 1, Some(b"b"), t0 + Duration::from_secs(1)));
  assert!(recent.should_drop("app", 1, Some(b"b"), t0 + Duration::from_secs(2)));
  // A clear always passes and re-arms the slot: the pre-clear payload
  // passes again right after it.
  assert!(!recent.should_drop("app", 1, None, t0 + Duration::from_secs(3)));
  assert!(!recent.should_drop("app", 1, Some(b"b"), t0 + Duration::from_secs(3)));
  // Past the window, even identical bytes pass.
  assert!(!recent.should_drop("app", 1, Some(b"b"), t0 + Duration::from_secs(9)));
}

#[test]
fn dedup_table_stays_bounded() {
  let mut recent = RecentActivities::new(Duration::from_secs(5), 4);
  let t0 = Instant::now();

  for pid in 0..20u64 {
    recent.should_drop("app", pid, Some(b"x"), t0);
  }

  assert!(recent.len() <= 4);
  assert!(!recent.is_empty());
}

/// Cloned payloads share one allocation (`Arc` + refcounted encodings).
#[test]
fn cached_builds_share_one_allocation() {
  use rsrpc_protocol::commands::empty_cached;
  use rsrpc_types::SocketId;

  let payload = empty_cached(42, SocketId::from("sock"));
  let shared = payload.clone();
  assert!(std::sync::Arc::ptr_eq(&payload, &shared));
  // Deref reaches both encodings without unwrapping.
  assert!(payload.json.contains("\"pid\": 42"));
  assert!(!payload.msgpack.is_empty());
  // Cloning shares the refcounted backing, not the bytes.
  assert!(std::ptr::eq(
    payload.json.as_str().as_ptr(),
    shared.json.as_str().as_ptr()
  ));
  assert_eq!(payload.msgpack.as_ptr(), shared.msgpack.as_ptr());
}

/// `is_clear` is set at construction for all three build shapes.
#[test]
fn cached_payload_knows_whether_it_clears() {
  use rsrpc_protocol::commands::{cached_activity, empty_cached};
  use rsrpc_types::SocketId;
  use rsrpc_types::cmd::ActivityCmd;

  // Clear builds always clear, without parsing to find out.
  assert!(empty_cached(1, SocketId::from("s")).is_clear);

  // Activity builds never clear.
  let mut cmd: ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app",
    "args": {"pid": 7, "activity": {"name": "G", "type": 0}},
    "nonce": "n",
  }))
  .unwrap();
  let fp = activity_fingerprint(&mut cmd);
  let payload = cached_activity(&mut cmd, fp).expect("builds");
  assert!(!payload.is_clear);

  // Null-activity commands build clears.
  let mut clear: ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app",
    "args": {"pid": 7, "activity": null},
    "nonce": "n",
  }))
  .unwrap();
  let payload = cached_activity(&mut clear, None).expect("builds");
  assert!(payload.is_clear);
}

/// Canonical `SET_ACTIVITY` command fixture (app 123, pid 42).
fn set_activity_cmd() -> ActivityCmd {
  parse_cmd(
    r#"{
      "cmd": "SET_ACTIVITY",
      "application_id": "123",
      "args": {"pid": 42, "activity": {"name": "Game", "type": 0}},
      "nonce": "n1"
    }"#,
  )
}

/// Fingerprints equal the standalone serialization of the fixed activity.
#[test]
fn activity_fingerprint_matches_standalone_serialization() {
  use rsrpc_protocol::commands::activity_fingerprint;

  let mut cmd = set_activity_cmd();
  let fp = activity_fingerprint(&mut cmd).expect("fingerprint");
  // fix() runs inside; the stored activity must serialize identically.
  let activity = cmd
    .args
    .as_ref()
    .and_then(|args| args.activity.as_ref())
    .expect("activity");
  assert_eq!(fp, serde_json::to_vec(activity).expect("serializable"));
}

/// Activity-less commands (clears) fingerprint to `None`.
#[test]
fn activity_fingerprint_is_none_for_clears() {
  use rsrpc_protocol::commands::activity_fingerprint;

  let mut cmd = parse_cmd(r#"{"cmd":"SET_ACTIVITY","args":{"pid":42},"nonce":"n1"}"#);
  assert!(activity_fingerprint(&mut cmd).is_none());
}

/// Built payloads store the exact fingerprint bytes for change detection.
#[test]
fn cached_activity_carries_fingerprint_bytes() {
  use rsrpc_protocol::commands::activity_fingerprint;

  let mut cmd = set_activity_cmd();
  let fp = activity_fingerprint(&mut cmd).expect("fingerprint");
  let cached = cached_activity(&mut cmd, Some(fp.clone())).expect("payload");
  assert_eq!(cached.activity_json.as_ref(), fp.as_slice());
  assert!(!cached.is_clear);
}

/// Outer application ids feed the fingerprint (no cross-app aliasing).
#[test]
fn fingerprint_includes_outer_application_id() {
  // The stamp (outer application_id -> activity.application_id) must
  // happen before fingerprinting: two commands differing only in the
  // outer id must not share a fingerprint, or the second publish would
  // be misclassified as unchanged and keep the old application.
  let mut a = set_activity_cmd();
  let mut b = set_activity_cmd();
  b.application_id = Some("999".to_string());
  let fa = activity_fingerprint(&mut a).expect("fingerprint");
  let fb = activity_fingerprint(&mut b).expect("fingerprint");
  assert_ne!(fa, fb);
}
