//! Snapshot persistence: flatten the replay cache into state files.
//!
//! Snapshots persist dirty-gated on a cadence (never per publish) into
//! the configured state dir, so external tooling can read presence
//! without joining the bridge.

use super::bridge::Shared;
use super::replay::ReplayCache;
use rsrpc_state::{StateActivity, StateServer, StateServers, StateSnapshot};

impl Shared {
  /// Write the snapshot when dirty (spawn_blocking: sync temp+rename+fsync
  /// must not sit on an async worker). Best-effort: failures stay in
  /// diagnostics so a full tmpfs never breaks presence.
  pub(crate) async fn persist_now(&self) {
    let Some(path) = self.state_path.clone() else {
      return;
    };
    let servers = StateServers {
      bridge: Some(StateServer {
        host: "127.0.0.1".to_string(),
        port: self.json_port,
      }),
      msgpack: Some(StateServer {
        host: "127.0.0.1".to_string(),
        port: self.msgpack_port,
      }),
      websocket: self.ws_port.map(|port| StateServer {
        host: "127.0.0.1".to_string(),
        port,
      }),
      ipc: self.ipc_path.clone(),
    };
    let activities = state_activities(&self.cache.lock().unwrap_or_else(|e| e.into_inner()));
    let snapshot = StateSnapshot::new(&self.app_version, servers, activities);
    let result =
      tokio::task::spawn_blocking(move || rsrpc_state::write_snapshot(&path, &snapshot)).await;
    match result {
      Ok(Ok(())) => {}
      Ok(Err(err)) => tracing::debug!("[bridge] State snapshot failed: {err}"),
      Err(err) => tracing::debug!("[bridge] Snapshot task failed: {err}"),
    }
  }
}

/// Flatten the replay cache into state-snapshot activities (best-effort:
/// unparseable entries contribute their socket id only).
pub(crate) fn state_activities(cache: &ReplayCache) -> Vec<StateActivity> {
  let mut out = Vec::with_capacity(cache.len());
  out.extend(cache.iter().map(|(socket_id, (payload, _))| {
    let body: serde_json::Value =
      serde_json::from_str(&payload.json).unwrap_or(serde_json::Value::Null);
    let activity = body.get("activity");
    StateActivity {
      socket_id: socket_id.to_string(),
      name: activity
        .and_then(|item| item.get("name"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string),
      application_id: activity
        .and_then(|item| item.get("application_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string),
      pid: body.get("pid").and_then(serde_json::Value::as_u64),
      start_time: activity
        .and_then(|item| item.get("timestamps"))
        .and_then(|item| item.get("start"))
        .map(|value| match value {
          serde_json::Value::String(text) => text.clone(),
          other => other.to_string(),
        }),
    }
  }));
  out
}
