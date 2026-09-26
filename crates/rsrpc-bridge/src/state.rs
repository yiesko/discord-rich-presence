//! Presence snapshot slots for external tooling, enabled with
//! `--state-file` / `RSRPC_STATE_FILE`.
//!
//! Slots live beside arRPC's own files but use an `rsrpc-` prefix so
//! both daemons coexist: `<tmpdir>/rsrpc-state-{0..9}`. Writes are
//! atomic (temp + rename) so readers never see a torn file, and
//! owner-only (`0600` where the platform supports it) so presence
//! metadata is not world-readable under a permissive umask. Every write
//! uses its own temp (`<slot>.tmp-<pid>-<serial>`), created exclusively
//! without following symlinks, so concurrent daemons sharing a slot
//! still write apart and a planted link is refused, never followed.
//! Slot selection is best-effort: concurrent daemons may pick the same
//! slot and overwrite each other — last writer wins, same as arRPC.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

/// Temp-file serial: with the pid, makes every snapshot write's temp
/// unique, so concurrent daemons sharing a slot never share a temp.
static SNAPSHOT_TMP_SERIAL: AtomicU64 = AtomicU64::new(0);

/// Optional presence snapshot for external tooling, enabled with
/// `--state-file` / `RSRPC_STATE_FILE` (any boolish value, e.g. `1`).
/// Slots live beside arRPC's own files but use an `rsrpc-` prefix so
/// both daemons coexist: `<tmpdir>/rsrpc-state-{0..9}`.
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
  /// freeze the bridge crate's version, not the daemon's.
  pub fn new(app_version: &str, servers: StateServers, activities: Vec<StateActivity>) -> Self {
    Self {
      app_version: app_version.to_string(),
      timestamp: std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|age| i64::try_from(age.as_millis()).ok())
        .unwrap_or(0),
      servers,
      activities,
    }
  }
}

/// Pick a snapshot slot in `dir`: the first missing, stale (`mtime` older
/// than [`STATE_STALE_SECS`]), or corrupt (unparseable / missing fresh
/// timestamp) slot. Returns `None` when every slot holds a fresh snapshot
/// (another live daemon owns them all). Stale temp leftovers from crashed
/// writers are swept first, under the same staleness rule.
pub fn select_slot(dir: &Path, now_secs: u64) -> Option<PathBuf> {
  sweep_stale_tmps(dir, now_secs);
  for index in 0..MAX_STATE_SLOTS {
    let path = dir.join(format!("{STATE_FILE_PREFIX}{index}"));
    if slot_reusable(&path, now_secs) {
      return Some(path);
    }
  }
  None
}

/// Remove temp files from crashed snapshot writers: same directory and
/// prefix as slots, `tmp-` extension, `mtime` older than staleness.
/// Best-effort (a crowded `read_dir` simply skips); symlinks are removed
/// as links, never followed.
fn sweep_stale_tmps(dir: &Path, now_secs: u64) {
  let Ok(entries) = std::fs::read_dir(dir) else {
    return;
  };
  for entry in entries.flatten() {
    let path = entry.path();
    let is_tmp = path
      .extension()
      .and_then(|ext| ext.to_str())
      .is_some_and(|ext| ext.starts_with("tmp-"));
    if !is_tmp {
      continue;
    }
    let stale = entry
      .metadata()
      .and_then(|meta| meta.modified())
      .ok()
      .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
      .is_some_and(|age| now_secs.saturating_sub(age.as_secs()) > STATE_STALE_SECS);
    if stale {
      let _ = std::fs::remove_file(&path);
    }
  }
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
/// see a torn file. The temp is unique per write and created exclusively
/// without following symlinks (owner-only `0600` on Unix; the rename
/// carries the mode to the final path), so presence metadata stays
/// private under a permissive umask even with a hostile temp dir.
/// Best-effort by design: callers log and continue.
pub fn write_snapshot(path: &Path, snapshot: &StateSnapshot) -> std::io::Result<()> {
  let body = serde_json::to_vec(snapshot)
    .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
  let tmp = path.with_extension(format!(
    "tmp-{}-{}",
    std::process::id(),
    SNAPSHOT_TMP_SERIAL.fetch_add(1, Ordering::Relaxed)
  ));
  let mut file = open_snapshot_tmp(&tmp)?;
  file.write_all(&body)?;
  file.sync_all()?;
  drop(file);
  std::fs::rename(&tmp, path)
}

/// Exclusive owner-only temp: `create_new` refuses a pre-existing path —
/// a planted symlink counts as existing and is never followed — and
/// `O_NOFOLLOW` on Unix refuses symlinks even without exclusivity. One
/// remove-and-retry covers a stale temp from a crashed holder of a
/// recycled pid; a rival that keeps replanting loses with an error
/// instead of a followed write.
fn open_snapshot_tmp(tmp: &Path) -> std::io::Result<std::fs::File> {
  match open_exclusive_tmp(tmp) {
    Ok(file) => Ok(file),
    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
      let _ = std::fs::remove_file(tmp);
      open_exclusive_tmp(tmp)
    }
    Err(err) => Err(err),
  }
}

fn open_exclusive_tmp(tmp: &Path) -> std::io::Result<std::fs::File> {
  #[cfg(unix)]
  {
    std::fs::OpenOptions::new()
      .write(true)
      .create_new(true)
      .mode(0o600)
      .custom_flags(libc::O_NOFOLLOW)
      .open(tmp)
  }
  #[cfg(not(unix))]
  {
    std::fs::OpenOptions::new()
      .write(true)
      .create_new(true)
      .open(tmp)
  }
}

/// Current time as seconds since the epoch (for slot-freshness checks).
#[must_use]
pub fn now_secs() -> u64 {
  SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|elapsed| elapsed.as_secs())
    .unwrap_or(0)
}

#[cfg(test)]
mod tests {
  use super::*;

  /// A planted symlink at the temp path is never followed: the target
  /// keeps its content. Same-uid plants are reclaimed (link removed,
  /// fresh file created); other-uid plants cannot even be removed under
  /// the sticky bit, so the write fails closed instead.
  #[cfg(unix)]
  #[test]
  fn tmp_symlink_is_never_followed() {
    use std::os::unix::fs::symlink;

    let dir = std::env::temp_dir().join(format!("rsrpc-state-test-tmplink-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let target = dir.join("canary");
    std::fs::write(&target, b"untouched").expect("canary");
    let tmp = dir.join("rsrpc-state-0.tmp-1-1");
    symlink(&target, &tmp).expect("plant link");

    let _ = open_snapshot_tmp(&tmp);
    assert_eq!(std::fs::read(&target).expect("canary reads"), b"untouched");
    assert!(
      !std::fs::symlink_metadata(&tmp)
        .expect("tmp meta")
        .is_symlink(),
      "link must be gone or never created through"
    );

    let _ = std::fs::remove_dir_all(&dir);
  }

  /// A stale regular temp (crashed holder, recycled pid) is replaced:
  /// remove-and-retry reclaims the name with a fresh owner-only file.
  #[test]
  fn stale_regular_tmp_is_reclaimed() {
    let dir = std::env::temp_dir().join(format!(
      "rsrpc-state-test-tmpreclaim-{}",
      std::process::id()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let tmp = dir.join("rsrpc-state-0.tmp-1-1");
    std::fs::write(&tmp, b"stale").expect("stale tmp");

    let mut file = open_snapshot_tmp(&tmp).expect("reclaims stale tmp");
    file.write_all(b"fresh").expect("write");
    drop(file);
    assert_eq!(std::fs::read(&tmp).expect("reads"), b"fresh");
    #[cfg(unix)]
    {
      use std::os::unix::fs::PermissionsExt;
      let mode = std::fs::metadata(&tmp).expect("meta").permissions().mode() & 0o777;
      assert_eq!(
        mode, 0o600,
        "reclaimed temp must be owner-only, got {mode:o}"
      );
    }

    let _ = std::fs::remove_dir_all(&dir);
  }
}
