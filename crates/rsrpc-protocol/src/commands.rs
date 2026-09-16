use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use serde_json::Value;
use serde_with::skip_serializing_none;
use tungstenite::Utf8Bytes;

use rsrpc_types::cmd::{ActivityCmd, ActivityPayload};
use rsrpc_types::user::RpcUser;
use rsrpc_types::{AppId, SocketId};

#[skip_serializing_none]
#[derive(Serialize)]
pub struct ProcessActivity {
  pub application_id: AppId,
  pub name: String,
  pub timestamps: ProcessTimestamps,
  pub r#type: u32,
  pub metadata: HashMap<String, String>,
  pub flags: u32,
}

#[derive(Serialize)]
pub struct ProcessTimestamps {
  pub start: u64,
}

#[derive(Serialize)]
pub struct ProcessPayload {
  pub activity: ProcessActivity,
  pub pid: u64,
  #[serde(rename = "socketId")]
  pub socket_id: SocketId,
}

#[must_use]
fn empty_activity(pid: u64, socket_id: SocketId) -> String {
  format!(
    r#"
    {{
      "activity": null,
      "pid": {pid},
      "socketId": "{socket_id}"
    }}
  "#
  )
}

/// An activity payload serialized once for both bridge protocols (JSON text
/// frames for the 1337 port, MessagePack binary frames for the 1338 port).
/// Builders return `Arc<CachedActivity>` so broadcast fan-out shares the
/// single build instead of cloning per client (`mem-zero-copy`).
///
/// Both encodings are refcounted (`Utf8Bytes`/`Bytes`): cloning a payload
/// bumps counters, and builders adopt their serialization buffers with no
/// copy (`String`/`Vec<u8>` move straight in).
#[derive(Clone, Debug)]
pub struct CachedActivity {
  pub json: Utf8Bytes,
  pub msgpack: Bytes,
  /// Whether this payload clears presence (`activity: null`). Recorded at
  /// construction so consumers never re-parse the JSON to find out.
  pub is_clear: bool,
}

/// Build the empty (clear) payload in both protocols.
#[must_use]
pub fn empty_cached(pid: u64, socket_id: SocketId) -> Arc<CachedActivity> {
  let payload = ActivityPayload {
    activity: None,
    pid: Some(pid),
    socket_id: Some(socket_id.to_string()),
  };

  // Fixed-shape struct (String/int/bool/Option only, no floats or
  // non-string map keys): serialization cannot fail by construction.
  // The silent fallback below exists so a future field addition that
  // breaks that invariant degrades to an empty frame (observed downstream
  // as a clear) instead of panicking the broadcast path. Protocol has no
  // logger by design (`obs-library-facade`); callers that need the signal
  // should validate before building.
  Arc::new(CachedActivity {
    json: empty_activity(pid, socket_id).into(),
    msgpack: rmp_serde::to_vec_named(&payload)
      .map(Bytes::from)
      .unwrap_or_default(),
    is_clear: true,
  })
}

/// Turn a `SET_ACTIVITY` command into the bridge payload in both protocols.
///
/// Returns `None` when the command cannot be converted into a valid payload
/// (e.g. it is missing its arguments entirely).
#[must_use]
pub fn cached_activity(cmd: &mut ActivityCmd) -> Option<Arc<CachedActivity>> {
  cmd.fix();

  let args = cmd.args.as_mut()?;

  if args.activity.is_none() {
    let pid = args.pid.unwrap_or_default();
    return Some(empty_cached(pid, SocketId::from(pid.to_string())));
  }

  let activity = args.activity.as_mut()?;
  activity.application_id = cmd.application_id.clone();

  let payload = ActivityPayload {
    activity: Some(activity.clone()),
    pid: args.pid,
    socket_id: Some(args.pid.unwrap_or(0).to_string()),
  };

  Some(Arc::new(CachedActivity {
    json: serde_json::to_string(&payload).ok()?.into(),
    msgpack: rmp_serde::to_vec_named(&payload).ok().map(Bytes::from)?,
    // Reached only when `args.activity` is `Some`: the serialized body
    // carries a real activity object, never null.
    is_clear: false,
  }))
}

/// Build the acknowledgement for a `SUBSCRIBE`/`UNSUBSCRIBE` command:
/// echoes `cmd`/`nonce`, reports the subscribed event name in `data.evt`
/// (the official shape; arRPC blind-ACKs the same way).
///
/// # Blind-ACK scope
///
/// The ACK confirms receipt only — there is no backend behind most event
/// families, so a subscription that is ACKed here will simply never fire.
/// Events this server can actually dispatch: `READY` (on connect),
/// `ERROR` (command failures) and `CURRENT_USER_UPDATE` (bridge identity
/// changes via `SET_USER`/`RESET_USER`). Everything else in the official
/// table is accepted and then silent, because it needs the real Discord
/// client: voice (`VOICE_*`, `SPEAKING_*`), guilds/channels
/// (`GUILD_*`, `CHANNEL_CREATE`), messages/notifications
/// (`MESSAGE_*`, `NOTIFICATION_CREATE`), activity invites
/// (`ACTIVITY_JOIN`, `ACTIVITY_SPECTATE`, `ACTIVITY_JOIN_REQUEST`,
/// `ACTIVITY_INVITE`), relationships (`RELATIONSHIP_UPDATE`) and store
/// (`ENTITLEMENT_CREATE`, `ENTITLEMENT_DELETE`).
#[must_use]
pub fn subscribe_ack(cmd: &ActivityCmd) -> String {
  serde_json::to_string(&serde_json::json!({
    "cmd": cmd.cmd,
    "data": { "evt": cmd.evt },
    "evt": null,
    "nonce": cmd.nonce,
  }))
  .unwrap_or_else(|_| format!(r#"{{"cmd":"{}","evt":"ERROR"}}"#, cmd.cmd))
}

/// Build the reply for a `GET_USER` command: the current identity, or
/// `null` when the requested id names somebody else (the official
/// response is "an RPC user object or null").
#[must_use]
pub fn user_response(cmd: &ActivityCmd, user: Option<&RpcUser>) -> String {
  let data = user
    .and_then(|user| serde_json::to_value(user).ok())
    .unwrap_or(Value::Null);
  serde_json::to_string(&serde_json::json!({
    "cmd": cmd.cmd,
    "data": data,
    "evt": null,
    "nonce": cmd.nonce,
  }))
  .unwrap_or_else(|_| format!(r#"{{"cmd":"{}","evt":"ERROR"}}"#, cmd.cmd))
}

/// Build the `CURRENT_USER_UPDATE` dispatch emitted when the local
/// identity changes (`SET_USER`/`RESET_USER`). The inner payload is the
/// user object itself, per the official event shape.
#[must_use]
pub fn current_user_update(user: &RpcUser) -> String {
  let data = serde_json::to_value(user).unwrap_or(Value::Null);
  serde_json::json!({
    "cmd": "DISPATCH",
    "evt": "CURRENT_USER_UPDATE",
    "data": data,
    "nonce": null,
  })
  .to_string()
}

/// Official error for a known command that has no backend here (OAuth,
/// voice, guilds, overlay, store...). Returns `(code, message)` so the IPC
/// and WebSocket dispatches share one table instead of drifting apart;
/// `None` means "not a known-unbacked command" (handled elsewhere, or
/// genuinely unknown).
#[must_use]
pub fn unsupported_command(cmd: &str) -> Option<(u16, &'static str)> {
  const NEEDS_CLIENT: &str = "requires the real Discord client";
  match cmd {
    // OAuth needs the in-client modal plus token exchange with the app's
    // secret: 5000 is the official OAuth2 error bucket.
    "AUTHORIZE" | "AUTHENTICATE" => Some((5000, "Authorization requires the real Discord client")),
    // Activity invites target live sessions on the real client.
    "SEND_ACTIVITY_JOIN_INVITE"
    | "CLOSE_ACTIVITY_REQUEST"
    | "ACCEPT_ACTIVITY_INVITE"
    | "ACTIVITY_INVITE_USER" => Some((
      5006,
      "No eligible activity: invites require the real Discord client",
    )),
    // Voice, guilds, channels, overlay, store, capture, certs: real-client
    // state only. Grouped under the generic code with an honest message.
    "GET_GUILD"
    | "GET_GUILDS"
    | "GET_CHANNEL"
    | "GET_CHANNELS"
    | "CREATE_CHANNEL_INVITE"
    | "GET_RELATIONSHIPS"
    | "SET_USER_VOICE_SETTINGS"
    | "SET_USER_VOICE_SETTINGS_2"
    | "PUSH_TO_TALK"
    | "SELECT_VOICE_CHANNEL"
    | "GET_SELECTED_VOICE_CHANNEL"
    | "SELECT_TEXT_CHANNEL"
    | "GET_VOICE_SETTINGS"
    | "SET_VOICE_SETTINGS"
    | "SET_VOICE_SETTINGS_2"
    | "SET_CERTIFIED_DEVICES"
    | "CAPTURE_SHORTCUT"
    | "GET_IMAGE"
    | "OVERLAY"
    | "SET_OVERLAY_LOCKED"
    | "OPEN_OVERLAY_ACTIVITY_INVITE"
    | "OPEN_OVERLAY_GUILD_INVITE"
    | "OPEN_OVERLAY_VOICE_SETTINGS"
    | "GET_SKUS"
    | "GET_ENTITLEMENTS"
    | "START_PURCHASE"
    | "VALIDATE_APPLICATION" => Some((1000, NEEDS_CLIENT)),
    _ => None,
  }
}

/// Build an `evt: "ERROR"` reply for a command the server refuses
/// (unknown command, invalid invite code, unsupported callback, ...),
/// mirroring arRPC's `{cmd, data: {code, message}, evt: "ERROR", nonce}`.
#[must_use]
pub fn rpc_error(cmd: &str, nonce: &Value, code: u16, message: &str) -> String {
  serde_json::to_string(&serde_json::json!({
    "cmd": cmd,
    "data": { "code": code, "message": message },
    "evt": "ERROR",
    "nonce": nonce,
  }))
  .unwrap_or_else(|_| format!(r#"{{"cmd":"{cmd}","evt":"ERROR"}}"#))
}

/// Build a neutral acknowledgement for known secondary commands
/// (`INVITE_BROWSER`, `DEEP_LINK`, ...) that are forwarded to bridge
/// clients: the outcome lives downstream, so the reply only confirms
/// receipt (arRPC answers these from the bridge round-trip; without
/// bridge clients there is nothing more to report).
#[must_use]
pub fn generic_ack(cmd: &ActivityCmd) -> String {
  serde_json::to_string(&serde_json::json!({
    "cmd": cmd.cmd,
    "data": null,
    "evt": null,
    "nonce": cmd.nonce,
  }))
  .unwrap_or_else(|_| format!(r#"{{"cmd":"{}","evt":null}}"#, cmd.cmd))
}
/// Build the official-shaped confirmation reply for a `SET_ACTIVITY` command.
///
/// The reply echoes `cmd`/`nonce` and carries `data` with the (fixed)
/// activity **exactly as the game sent it** — `name`, `type` (Playing /
/// Listening / Watching / Competing) and every other field are preserved,
/// matching the official echo semantics. The lock-step guarantee RPC
/// libraries rely on (e.g. pypresence must receive *some* reply or it
/// hangs) comes from always answering, never from rewriting the body, so
/// strict clients validating the echo see their own activity back.
/// `application_id` (known from the handshake, absent from what the game
/// sent) is attached as a routing enrichment; unknown keys tolerate it.
/// Returns `None` when the command has no arguments.
#[must_use]
pub fn set_activity_response(cmd: &ActivityCmd) -> Option<String> {
  let args = cmd.args.as_ref()?;

  let data = match args.activity.as_ref() {
    Some(activity) => {
      let mut data = serde_json::to_value(activity).ok()?;
      if let Some(obj) = data.as_object_mut() {
        obj.insert(
          "application_id".to_string(),
          cmd
            .application_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
        );
      }
      data
    }
    None => Value::Null,
  };

  serde_json::to_string(&serde_json::json!({
    "cmd": cmd.cmd,
    "data": data,
    "evt": null,
    "nonce": cmd.nonce,
  }))
  .ok()
}

/// Flood guard for `SET_ACTIVITY`: collapses byte-identical republishes
/// from the same `(application_id, pid)` arriving inside a short window.
///
/// The official client throttles presence updates; without a guard here a
/// spinning or buggy SDK resending the same bytes at Hz rates would fan
/// out to every bridge consumer at full rate. Only *exact duplicates* are
/// dropped — any changed byte passes — and clears always pass and re-arm
/// the slot, so a wrong drop costs at most one stale frame for `window`,
/// self-healed by the next distinct publish (or the periodic bridge
/// refresh). The game already received its echo upstream, so lock-step
/// clients that require a reply never hang on a dropped duplicate.
#[derive(Clone, Debug)]
pub struct RecentActivities {
  window: std::time::Duration,
  cap: usize,
  entries: HashMap<(String, u64), (Vec<u8>, std::time::Instant)>,
}

impl RecentActivities {
  /// Canonical guard: 5s window (conservative — healthy SDK heartbeats
  /// re-send every 15s+, so only floods collapse).
  pub const DEFAULT_WINDOW: std::time::Duration = std::time::Duration::from_secs(5);
  /// Canonical table bound (far above the handful of co-running games;
  /// stops untrusted input from growing the map forever).
  pub const DEFAULT_CAP: usize = 128;

  /// Track duplicates inside `window`, keeping at most `cap` slots.
  #[must_use]
  pub fn new(window: std::time::Duration, cap: usize) -> Self {
    Self {
      window,
      cap,
      entries: HashMap::new(),
    }
  }

  /// Number of slots currently remembered (for tests and diagnostics).
  #[must_use]
  pub fn len(&self) -> usize {
    self.entries.len()
  }

  /// Whether `len` reports no remembered slot.
  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.entries.is_empty()
  }

  /// `true` when this exact payload was already seen from `(app_id, pid)`
  /// inside the window — the caller should drop it without broadcasting.
  /// Records the payload otherwise. A `None` payload (a clear) always
  /// returns `false` and forgets the slot, so the next publish — even
  /// byte-identical to a pre-clear one — is forwarded.
  pub fn should_drop(
    &mut self,
    app_id: &str,
    pid: u64,
    payload: Option<&[u8]>,
    now: std::time::Instant,
  ) -> bool {
    let Some(bytes) = payload else {
      self.entries.remove(&(app_id.to_string(), pid));
      return false;
    };
    let key = (app_id.to_string(), pid);
    if let Some((last, at)) = self.entries.get(&key)
      && last.as_slice() == bytes
      && now.duration_since(*at) < self.window
    {
      return true;
    }
    // Evict-then-insert, bounded: purge expired slots first (the actual
    // garbage), then fall back to one arbitrary eviction so hostile input
    // cannot grow the map without bound.
    if self.entries.len() >= self.cap && !self.entries.contains_key(&key) {
      self
        .entries
        .retain(|_, (_, at)| now.duration_since(*at) < self.window);
      if self.entries.len() >= self.cap
        && let Some(victim) = self.entries.keys().next().cloned()
      {
        self.entries.remove(&victim);
      }
    }
    self.entries.insert(key, (bytes.to_vec(), now));
    false
  }
}

impl Default for RecentActivities {
  fn default() -> Self {
    Self::new(Self::DEFAULT_WINDOW, Self::DEFAULT_CAP)
  }
}
