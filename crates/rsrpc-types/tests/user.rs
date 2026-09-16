use std::collections::HashMap;

use serde_json::{Value, json};

use rsrpc_types::user::RpcUser;

#[test]
fn default_user_matches_arrpc_identity() {
  let user = RpcUser::default();
  assert_eq!(user.id, "1045800378228281345");
  assert_eq!(user.username, "arRPC");
  assert_eq!(user.discriminator, "0000");
  assert!(!user.bot);
}

#[test]
fn env_map_overrides_identity_and_ignores_blanks() {
  let mut user = RpcUser::default();
  let env: HashMap<String, String> = [
    ("RSRPC_USER_ID", "123"),
    ("RSRPC_USER_USERNAME", "  "),
    ("RSRPC_USER_GLOBAL_NAME", "Pretty"),
    ("RSRPC_USER_AVATAR", "abc"),
  ]
  .into_iter()
  .map(|(key, value)| (key.to_string(), value.to_string()))
  .collect();

  user.apply_env_map(&env);

  assert_eq!(user.id, "123");
  // Blank values never wipe the default.
  assert_eq!(user.username, "arRPC");
  assert_eq!(user.global_name.as_deref(), Some("Pretty"));
  assert_eq!(user.avatar.as_deref(), Some("abc"));
}

#[test]
fn patch_applies_whitelist_only() {
  let mut user = RpcUser::default();
  user.patch(&json!({
    "username": "patched",
    "avatar": null,
    "bot": true,
    "flags": 64,
    "premium_type": 2,
    "injected": "must not stick",
  }));

  assert_eq!(user.username, "patched");
  assert!(user.avatar.is_none());
  assert!(user.bot);
  assert_eq!(user.flags, 64);
  assert_eq!(user.premium_type, 2);
  // Unknown keys never land on the struct (and cannot reach READY).
  let serialized = serde_json::to_value(&user).expect("serializes");
  assert!(serialized.get("injected").is_none());
}

#[test]
fn patch_ignores_non_objects_and_wrong_types() {
  let mut user = RpcUser::default();
  user.patch(&Value::String("nope".to_string()));
  user.patch(&json!({ "flags": "not-a-number", "bot": "yes" }));

  assert_eq!(user.flags, 0);
  assert!(!user.bot);
}

#[test]
fn ready_payload_is_a_dispatch_ready_frame() {
  let payload: Value =
    serde_json::from_str(&RpcUser::default().ready_payload()).expect("valid json");

  assert_eq!(payload["cmd"], "DISPATCH");
  assert_eq!(payload["evt"], "READY");
  assert_eq!(payload["data"]["v"], 1);
  assert_eq!(payload["data"]["user"]["id"], "1045800378228281345");
  assert_eq!(payload["data"]["config"]["environment"], "production");
  assert!(payload["nonce"].is_null());
}

#[test]
fn patch_ignores_blank_strings_like_env() {
  let mut user = RpcUser::default();
  user.patch(&json!({
    "username": "   ",
    "global_name": "",
    "id": "",
    "discriminator": "  ",
  }));

  assert_eq!(user.username, "arRPC");
  assert_eq!(user.global_name.as_deref(), Some("arRPC"));
  assert_eq!(user.id, "1045800378228281345");
  assert_eq!(user.discriminator, "0000");
}

#[test]
fn patch_clamps_huge_integers_instead_of_wrapping() {
  let mut user = RpcUser::default();
  user.patch(&json!({ "flags": 1_u64 << 40, "premium_type": 7 }));

  assert_eq!(user.flags, u32::MAX);
  assert_eq!(user.premium_type, 7);
}
