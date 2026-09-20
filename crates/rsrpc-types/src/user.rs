use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Identity presented in the `DISPATCH`/`READY` frame on every transport
/// (IPC, RPC websocket, bridge), mirroring arRPC's fake user.
///
/// Some game SDKs display this user before any activity arrives; it is
/// cosmetic (no authentication happens here). Override at startup with
/// `RSRPC_USER_ID` / `RSRPC_USER_USERNAME` / `RSRPC_USER_GLOBAL_NAME` /
/// `RSRPC_USER_DISCRIMINATOR` / `RSRPC_USER_AVATAR`, or at runtime from a
/// bridge client with `SET_USER` (reset with `RESET_USER`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RpcUser {
  pub id: String,
  pub username: String,
  pub discriminator: String,
  pub global_name: Option<String>,
  pub avatar: Option<String>,
  pub avatar_decoration_data: Option<Value>,
  pub bot: bool,
  pub flags: u32,
  pub premium_type: u32,
}

impl Default for RpcUser {
  fn default() -> Self {
    Self {
      id: "1045800378228281345".to_string(),
      username: "arRPC".to_string(),
      discriminator: "0000".to_string(),
      global_name: Some("arRPC".to_string()),
      avatar: Some("cfefa4d9839fb4bdf030f91c2a13e95c".to_string()),
      avatar_decoration_data: None,
      bot: false,
      flags: 0,
      premium_type: 0,
    }
  }
}

impl RpcUser {
  /// Startup identity: defaults with `RSRPC_USER_*` overrides applied.
  /// Reads the process environment (thin wrapper over `apply_env_map`).
  #[must_use]
  pub fn from_env() -> Self {
    let mut user = Self::default();
    let env: HashMap<String, String> = std::env::vars().collect();
    user.apply_env_map(&env);
    user
  }

  /// Apply `RSRPC_USER_*` overrides from a pre-collected map (the testable
  /// core of `from_env`: blank values are ignored, unknown keys unseen).
  pub fn apply_env_map(&mut self, env: &HashMap<String, String>) {
    if let Some(value) = non_blank(env.get("RSRPC_USER_ID")) {
      self.id = value;
    }
    if let Some(value) = non_blank(env.get("RSRPC_USER_USERNAME")) {
      self.username = value;
    }
    if let Some(value) = non_blank(env.get("RSRPC_USER_GLOBAL_NAME")) {
      self.global_name = Some(value);
    }
    if let Some(value) = non_blank(env.get("RSRPC_USER_DISCRIMINATOR")) {
      self.discriminator = value;
    }
    if let Some(value) = non_blank(env.get("RSRPC_USER_AVATAR")) {
      self.avatar = Some(value);
    }
  }

  /// Merge a bridge `SET_USER` patch. Only the arRPC-whitelisted keys
  /// apply (`id`, `username`, `global_name`, `discriminator`,
  /// `avatar` as string-or-null, `bot`, `flags`, `premium_type`);
  /// anything else is ignored so a hostile web client cannot smuggle
  /// fields into the READY frame.
  pub fn patch(&mut self, patch: &Value) {
    let Some(obj) = patch.as_object() else {
      return;
    };
    // Blank strings are ignored, mirroring `apply_env_map`: a patch can
    // never wipe the identity with empty values.
    let text = |key: &str| {
      obj
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
    };
    if let Some(value) = text("id") {
      self.id = value.to_string();
    }
    if let Some(value) = text("username") {
      self.username = value.to_string();
    }
    if let Some(value) = text("global_name") {
      self.global_name = Some(value.to_string());
    }
    if let Some(value) = text("discriminator") {
      self.discriminator = value.to_string();
    }
    match obj.get("avatar") {
      Some(Value::String(value)) => self.avatar = Some(value.clone()),
      Some(Value::Null) => self.avatar = None,
      _ => {}
    }
    if let Some(value) = obj.get("bot").and_then(Value::as_bool) {
      self.bot = value;
    }
    // Clamp instead of wrapping: absurd values saturate at u32::MAX.
    if let Some(value) = obj.get("flags").and_then(Value::as_u64) {
      self.flags = u32::try_from(value).unwrap_or(u32::MAX);
    }
    if let Some(value) = obj.get("premium_type").and_then(Value::as_u64) {
      self.premium_type = u32::try_from(value).unwrap_or(u32::MAX);
    }
  }

  /// The `DISPATCH`/`READY` frame sent on every new connection.
  #[must_use]
  pub fn ready_payload(&self) -> String {
    serde_json::json!({
      "cmd": "DISPATCH",
      "evt": "READY",
      "data": {
        "v": 1,
        "user": self,
        "config": {
          "api_endpoint": "//discord.com/api",
          "cdn_host": "cdn.discordapp.com",
          "environment": "production"
        }
      },
      "nonce": null
    })
    .to_string()
  }
}

fn non_blank(value: Option<&String>) -> Option<String> {
  value
    .map(String::as_str)
    .map(str::trim)
    .filter(|text| !text.is_empty())
    .map(str::to_string)
}
