//! Per-command handlers: protocol replies plus sink forwarding.
//!
//! Every handler returns whether the client is still alive; `false` prunes
//! the slot immediately instead of waiting for a `Disconnect` that may lag
//! behind under flood. Bodies are ported from the legacy dispatch with the
//! sync `Responder::send` replaced by `send_async`.

use std::collections::HashMap;

use rsrpc_protocol::commands;
use rsrpc_types::cmd::{ActivityCmd, ActivityCmdArgs};
use rsrpc_types::user::RpcUser;
use rsrpc_ws::{Message, Responder};
use serde_json::Value;

use crate::transport::Sink;

/// Serialize command args to a string map, preserving value types.
/// Borrows: `to_value` already produces an owned `Value`, so cloning the
/// args first would copy the whole subtree twice.
fn event_args_as_hashmap(args: Option<&ActivityCmdArgs>) -> HashMap<String, Value> {
  let args = match args {
    Some(args) => serde_json::to_value(args).unwrap_or(Value::Null),
    None => Value::Null,
  };
  match args {
    Value::Object(map) => map.into_iter().collect(),
    _ => HashMap::new(),
  }
}

/// Forward an invite/template/gift command after validating its code.
pub(crate) async fn handle_browser_command(
  event: &ActivityCmd,
  responder: &Responder,
  sink: &Sink,
) -> bool {
  // Discord error codes for unusable invite/template/gift ids.
  let (code, message) = if event.cmd == "GUILD_TEMPLATE_BROWSER" {
    (4017_u16, "Invalid guild template id")
  } else if event.cmd == "GIFT_CODE_BROWSER" {
    (4016_u16, "Invalid gift code")
  } else {
    (4011_u16, "Invalid invite id")
  };
  let has_code = event
    .args
    .as_ref()
    .and_then(|args| args.code.as_ref())
    .is_some_and(|code| !code.trim().is_empty());
  if !has_code {
    tracing::warn!("[transport-ws] {} without code", event.cmd);
    return responder
      .send_async(Message::Text(
        commands::rpc_error(&event.cmd, &event.nonce, code, message).into(),
      ))
      .await
      .is_ok();
  }

  // Optimistic forward; the outcome lives downstream.
  let response = ActivityCmd {
    application_id: event.application_id.clone(),
    cmd: event.cmd.clone(),
    args: None,
    data: Some(event_args_as_hashmap(event.args.as_ref())),
    evt: None,
    nonce: event.nonce.clone(),
  };
  sink.emit(event.clone()).await;

  // Client-supplied `data` may hold non-finite floats, which JSON cannot
  // encode: drop loudly instead of ending the pump. Nothing was sent, so
  // the client is still considered alive.
  let Ok(response) = serde_json::to_string(&response) else {
    tracing::warn!(
      "[transport-ws] Dropping unserializable response for {}",
      event.cmd
    );
    return true;
  };
  responder
    .send_async(Message::Text(response.into()))
    .await
    .is_ok()
}

/// Acknowledge a deep link (forwarded downstream).
pub(crate) async fn handle_deep_link(event: &ActivityCmd, responder: &Responder) -> bool {
  let response = ActivityCmd {
    application_id: event.application_id.clone(),
    cmd: event.cmd.clone(),
    args: None,
    data: None,
    evt: None,
    nonce: event.nonce.clone(),
  };
  let Ok(response) = serde_json::to_string(&response) else {
    tracing::warn!("[transport-ws] Dropping unserializable deep-link response");
    return true;
  };
  responder
    .send_async(Message::Text(response.into()))
    .await
    .is_ok()
}

/// Answer `CONNECTIONS_CALLBACK` with the official-shaped error.
pub(crate) async fn handle_connections_callback(
  event: &ActivityCmd,
  responder: &Responder,
) -> bool {
  let mut data = HashMap::new();
  data.insert("code".to_string(), Value::Number(1000.into()));
  let response = ActivityCmd {
    application_id: event.application_id.clone(),
    cmd: event.cmd.clone(),
    args: None,
    data: Some(data),
    evt: Some("ERROR".to_string()),
    nonce: event.nonce.clone(),
  };
  // `data` is locally built (no client floats): encode failure would be a
  // coding bug, but the pump must not die on it.
  let Ok(response) = serde_json::to_string(&response) else {
    tracing::warn!("[transport-ws] Dropping unserializable connections response");
    return true;
  };
  responder
    .send_async(Message::Text(response.into()))
    .await
    .is_ok()
}

/// Blind-ACK a subscription (no voice/guild backend exists; clients wait
/// for the lock-step reply).
pub(crate) async fn handle_subscribe(event: &ActivityCmd, responder: &Responder) -> bool {
  responder
    .send_async(Message::Text(commands::subscribe_ack(event).into()))
    .await
    .is_ok()
}

/// Answer `GET_USER` with the current identity, or null for strangers.
pub(crate) async fn handle_get_user(
  event: &ActivityCmd,
  user: &RpcUser,
  responder: &Responder,
) -> bool {
  let wanted = event.args.as_ref().and_then(|args| args.user_id.as_ref());
  let matched = wanted.is_none_or(|id| *id == user.id);
  responder
    .send_async(Message::Text(
      commands::user_response(event, matched.then_some(user)).into(),
    ))
    .await
    .is_ok()
}

/// Answer unknown commands from the shared unbacked-command table.
pub(crate) async fn handle_unknown(cmd: &str, event: &ActivityCmd, responder: &Responder) -> bool {
  let unsupported = commands::unsupported_command(cmd);
  if unsupported.is_none() {
    tracing::warn!("[transport-ws] Unknown command: {cmd}");
  }
  let (code, message) = unsupported.unwrap_or((1000, "Unknown command"));
  responder
    .send_async(Message::Text(
      commands::rpc_error(&event.cmd, &event.nonce, code, message).into(),
    ))
    .await
    .is_ok()
}

/// Forward `SET_ACTIVITY` and confirm with the arRPC-shaped reply.
///
/// Returns whether the client is still alive; `false` prunes the slot
/// immediately. The fixed command (with the connect-query `client_id`
/// fallback applied) goes to the sink; the caller records the slim
/// [`PublishedSlot`] itself, so no second deep clone happens here.
pub(crate) async fn handle_set_activity(
  event: &ActivityCmd,
  query_client_id: Option<&str>,
  responder: &Responder,
  sink: &Sink,
) -> bool {
  // Fall back to the client_id provided on connect (query param) when the
  // command itself does not carry an application_id.
  let mut event = event.clone();
  if event.application_id.is_none() {
    event.application_id = query_client_id.map(str::to_string);
  }

  // Apply field fixes so the confirmation reply carries labels/urls (fix is
  // idempotent, so a downstream fix pass is harmless).
  event.fix();
  sink.emit(event.clone()).await;

  // Confirm to the game client; some RPC libraries wait for this before
  // considering the presence set. No confirm to send means the client is
  // still considered alive.
  match commands::set_activity_response(&event) {
    Some(response) => responder
      .send_async(Message::Text(response.into()))
      .await
      .is_ok(),
    None => true,
  }
}

/// Slim per-pid publication record: everything a disconnect clear needs,
/// without retaining the full command. A connection normally publishes
/// one pid; companions multiplexing several stay covered up to
/// [`MAX_TRACKED_PIDS`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishedSlot {
  pub app_id: Option<String>,
  pub pid: u64,
  pub nonce: Value,
}

/// Cap on tracked pids per connection: bounds memory against pathological
/// publishers while covering every realistic multiplexer. Beyond the cap
/// the oldest entry drops (same as today's single-slot behavior for it).
pub(crate) const MAX_TRACKED_PIDS: usize = 16;

/// Build the clear command emitted when a published pid dies.
pub(crate) fn clear_for_slot(published: &PublishedSlot) -> ActivityCmd {
  ActivityCmd {
    application_id: published.app_id.clone(),
    cmd: "SET_ACTIVITY".to_string(),
    data: None,
    evt: None,
    args: Some(ActivityCmdArgs {
      // pid 0 is never a genuine clear, so it is safely ignored
      // downstream — same convention as the old whole-command clear.
      pid: Some(published.pid),
      activity: None,
      code: None,
      user_id: None,
    }),
    nonce: published.nonce.clone(),
  }
}
