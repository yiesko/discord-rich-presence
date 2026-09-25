//! Bridge control messages: `SET_USER` / `RESET_USER` (arRPC parity).
//!
//! Any localhost consumer may send them; the handler answers with an ACK
//! and reports the new identity when it changed. Callers echo anything
//! else back to the sender untouched.

use std::sync::{Arc, Mutex};

use rsrpc_types::user::RpcUser;

/// Handle a bridge control message (`SET_USER`/`RESET_USER`, arRPC parity).
/// Returns the ACK text plus the new identity when it changed, `None` for
/// anything else (the caller echoes those to the sender untouched).
pub(crate) fn handle_bridge_control(
  user: &Arc<Mutex<RpcUser>>,
  text: &str,
) -> Option<(String, Option<RpcUser>)> {
  let body: serde_json::Value = serde_json::from_str(text).ok()?;
  let msg_type = body.get("type")?.as_str()?;
  if !matches!(msg_type, "SET_USER" | "RESET_USER") {
    return None;
  }
  let nonce = body
    .get("nonce")
    .cloned()
    .unwrap_or(serde_json::Value::Null);
  // One critical section for the whole read-modify-read: patch/reset
  // and both snapshots share a single guard.
  let (before, after) = {
    let mut guard = user.lock().unwrap_or_else(|e| e.into_inner());
    let before = guard.clone();
    if msg_type == "SET_USER" {
      // `patch` (arRPC shape) or `data` (defensive alias) carry the patch.
      if let Some(patch) = body.get("patch").or_else(|| body.get("data")) {
        guard.patch(patch);
      }
    } else {
      // Reset to the startup identity (defaults + `RSRPC_USER_*`).
      *guard = RpcUser::from_env();
    }
    let after = guard.clone();
    (before, after)
  };
  let changed = (before != after).then_some(after.clone());
  let user_value = serde_json::to_value(after).unwrap_or(serde_json::Value::Null);
  let ack = serde_json::json!({
    "type": format!("{msg_type}_ACK"),
    "nonce": nonce,
    "data": { "success": true, "user": user_value },
  })
  .to_string();
  Some((ack, changed))
}
