//! Bridge tuning: port ranges, snapshot extras, cadences.

use std::path::PathBuf;
use std::time::Duration;

/// Default JSON bridge range (arRPC-compatible).
pub const DEFAULT_JSON_PORT_START: u16 = 1337;
/// Default JSON bridge range end.
pub const DEFAULT_JSON_PORT_END: u16 = 1347;
/// Default MessagePack bridge port.
pub const DEFAULT_MSGPACK_PORT_START: u16 = 1338;
/// Default MessagePack bridge range end.
pub const DEFAULT_MSGPACK_PORT_END: u16 = 1348;

/// Default snapshot cadence: presence writes land at most this often no
/// matter the publish rate (the legacy wrote on *every* publish).
pub const DEFAULT_PERSIST_INTERVAL: Duration = Duration::from_secs(5);

/// Default replay rebroadcast cadence for clients that missed a frame.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Cap on replayed activities (arRPC keeps 50): bounds memory when many
/// distinct pids publish without clearing.
pub const MAX_CACHED_ACTIVITIES: usize = 50;

/// arRPC-compatible activity bridge configuration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BridgeConfig {
  /// First JSON port to try (inclusive).
  pub json_port_start: u16,
  /// Last JSON port to try (inclusive).
  pub json_port_end: u16,
  /// First MessagePack port to try (inclusive).
  pub msgpack_port_start: u16,
  /// Last MessagePack port to try (inclusive).
  pub msgpack_port_end: u16,
  /// Version stamped into state snapshots (the daemon version; defaults
  /// to this crate's version when the wiring does not override it).
  pub app_version: String,
  /// Directory holding the snapshot slot (`None` disables snapshots).
  pub state_dir: Option<PathBuf>,
  /// Game WebSocket port, recorded in the snapshot for tooling.
  pub ws_port: Option<u16>,
  /// IPC socket path, recorded in the snapshot for tooling.
  pub ipc_path: Option<String>,
  /// Extra browser origins allowed to drive bridge commands, beyond
  /// Discord's own pages (`RSRPC_BRIDGE_ALLOWED_ORIGINS`, comma-separated).
  /// Absent `Origin` (native clients) always passes.
  pub allowed_origins: Vec<String>,
  /// Snapshot write cadence (dirty-gated).
  pub persist_interval: Duration,
  /// Replay rebroadcast cadence.
  pub refresh_interval: Duration,
}

impl BridgeConfig {
  /// Port ranges with arRPC-compatible defaults otherwise.
  /// `(0, 0, 0, 0)` binds ephemeral ports (tests).
  #[must_use]
  pub fn new(
    json_port_start: u16,
    json_port_end: u16,
    msgpack_port_start: u16,
    msgpack_port_end: u16,
  ) -> Self {
    Self {
      json_port_start,
      json_port_end,
      msgpack_port_start,
      msgpack_port_end,
      app_version: env!("CARGO_PKG_VERSION").to_string(),
      state_dir: None,
      ws_port: None,
      ipc_path: None,
      allowed_origins: Vec::new(),
      persist_interval: DEFAULT_PERSIST_INTERVAL,
      refresh_interval: DEFAULT_REFRESH_INTERVAL,
    }
  }

  /// Version stamped into state snapshots.
  #[must_use]
  pub fn app_version(mut self, version: impl Into<String>) -> Self {
    self.app_version = version.into();
    self
  }

  /// Enable snapshots in `dir` (slot selected at bind).
  #[must_use]
  pub fn state_dir(mut self, dir: PathBuf) -> Self {
    self.state_dir = Some(dir);
    self
  }

  /// Game WebSocket port recorded in the snapshot.
  #[must_use]
  pub fn ws_port(mut self, port: Option<u16>) -> Self {
    self.ws_port = port;
    self
  }

  /// IPC socket path recorded in the snapshot.
  #[must_use]
  pub fn ipc_path(mut self, path: Option<String>) -> Self {
    self.ipc_path = path;
    self
  }

  /// Extra browser origins allowed to drive bridge commands.
  #[must_use]
  pub fn allowed_origins(mut self, origins: Vec<String>) -> Self {
    self.allowed_origins = origins;
    self
  }

  /// Snapshot write cadence. Must be non-zero.
  #[must_use]
  pub fn persist_interval(mut self, interval: Duration) -> Self {
    self.persist_interval = interval;
    self
  }

  /// Replay rebroadcast cadence. Must be non-zero.
  #[must_use]
  pub fn refresh_interval(mut self, interval: Duration) -> Self {
    self.refresh_interval = interval;
    self
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Default port ranges stay on the arRPC-known values.
  #[test]
  fn defaults_match_arrpc_ports() {
    let config = BridgeConfig::new(
      DEFAULT_JSON_PORT_START,
      DEFAULT_JSON_PORT_END,
      DEFAULT_MSGPACK_PORT_START,
      DEFAULT_MSGPACK_PORT_END,
    );
    assert_eq!(config.json_port_start, 1337);
    assert_eq!(config.msgpack_port_start, 1338);
    assert_eq!(config.persist_interval, DEFAULT_PERSIST_INTERVAL);
    assert!(config.state_dir.is_none());
  }
}
