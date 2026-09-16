use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// Optional presence snapshot for external tooling, enabled by setting
/// `RSRPC_STATE_FILE` (any value, like arRPC). Slots live beside arRPC's
/// own files but use an `rsrpc-` prefix so both daemons coexist:
/// `<tmpdir>/rsrpc-state-{0..9}`.
pub const STATE_FILE_PREFIX: &str = "rsrpc-state-";
/// How many slots to scan before giving up (arRPC uses 10).
pub const MAX_STATE_SLOTS: u8 = 10;
/// A slot older than this (by mtime) is stale and reusable.
pub const STATE_STALE_SECS: u64 = 10;

#[derive(Serialize, Clone, Debug)]
pub struct StateServer {
  pub host: String,
  pub port: u16,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct StateServers {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub bridge: Option<StateServer>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub msgpack: Option<StateServer>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub websocket: Option<StateServer>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub ipc: Option<String>,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct StateActivity {
  #[serde(rename = "socketId")]
  pub socket_id: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub name: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub application_id: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub pid: Option<u64>,
  #[serde(rename = "startTime", skip_serializing_if = "Option::is_none")]
  pub start_time: Option<String>,
}

#[derive(Serialize, Clone, Debug)]
pub struct StateSnapshot {
  #[serde(rename = "appVersion")]
  pub app_version: String,
  pub timestamp: i64,
  pub servers: StateServers,
  pub activities: Vec<StateActivity>,
}

impl StateSnapshot {
  /// Build a snapshot stamped with the *caller* version: `env!` here would
  /// freeze this crate's version, not the daemon's.
  pub fn new(app_version: &str, servers: StateServers, activities: Vec<StateActivity>) -> Self {
    Self {
      app_version: app_version.to_string(),
      timestamp: chrono::Utc::now().timestamp_millis(),
      servers,
      activities,
    }
  }
}

/// Pick a snapshot slot in `dir`: the first missing, stale (`mtime` older
/// than [`STATE_STALE_SECS`]), or corrupt (unparseable / missing fresh
/// timestamp) slot. Returns `None` when every slot holds a fresh snapshot
/// (another live daemon owns them all).
pub fn select_slot(dir: &Path, now_secs: u64) -> Option<PathBuf> {
  for index in 0..MAX_STATE_SLOTS {
    let path = dir.join(format!("{STATE_FILE_PREFIX}{index}"));
    if slot_reusable(&path, now_secs) {
      return Some(path);
    }
  }
  None
}

fn slot_reusable(path: &Path, now_secs: u64) -> bool {
  let content = match std::fs::read_to_string(path) {
    Ok(content) => content,
    // Missing (or unreadable): reusable.
    Err(_) => return true,
  };
  let snapshot: serde_json::Value = match serde_json::from_str(&content) {
    Ok(snapshot) => snapshot,
    // Corrupt: reusable.
    Err(_) => return true,
  };
  let timestamp_ms = snapshot.get("timestamp").and_then(|value| {
    value.as_i64().or_else(|| {
      // Millis since epoch exceed u32/i32 range: serde may decode large
      // positives as u64.
      value.as_u64().and_then(|millis| i64::try_from(millis).ok())
    })
  });
  match timestamp_ms {
    // Fresh snapshot: owned by a live daemon.
    Some(timestamp_ms) => {
      // Non-negative by construction (`.max(0)` above): the `try_from`
      // documents the narrowing instead of a silent `as` cast.
      let age_secs =
        now_secs.saturating_sub(u64::try_from((timestamp_ms / 1000).max(0)).unwrap_or(0));
      age_secs > STATE_STALE_SECS
    }
    // No timestamp: not ours, reusable.
    None => true,
  }
}

/// Atomically persist a snapshot (write temp + rename), so readers never
/// see a torn file. Best-effort by design: callers log and continue.
pub fn write_snapshot(path: &Path, snapshot: &StateSnapshot) -> std::io::Result<()> {
  let body = serde_json::to_vec(snapshot)
    .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
  let tmp = path.with_extension("tmp");
  let mut file = std::fs::File::create(&tmp)?;
  file.write_all(&body)?;
  file.sync_all()?;
  drop(file);
  std::fs::rename(&tmp, path)
}

/// Current time as seconds since the epoch (for slot-freshness checks).
#[must_use]
pub fn now_secs() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|elapsed| elapsed.as_secs())
    .unwrap_or(0)
}
