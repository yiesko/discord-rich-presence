//! Daemon configuration: ports, scan cadence, database sources.
//!
//! [`RPCConfig`] is a plain struct (all fields `pub`, `Default`
//! implemented); [`RPCConfigBuilder`] covers programmatic construction
//! without a 19-field literal.

/// Daemon configuration. Field docs carry the CLI flag / env mapping.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RPCConfig {
  /// Run the process scanner (`--no-process-scan` / `RSRPC_NO_PROCESS_SCAN`).
  pub enable_process_scanner: bool,
  /// Event-driven proc-events watcher, Linux only (`--no-proc-events`).
  pub enable_proc_events: bool,
  /// Serve game clients over IPC (`discord-ipc-0..9`).
  pub enable_ipc_connector: bool,
  /// Serve game clients over WebSocket (ports below).
  pub enable_websocket_connector: bool,
  /// Forward secondary game commands (browser/deep-link/callbacks).
  pub enable_secondary_events: bool,
  /// First JSON bridge port (`RSRPC_BRIDGE_PORT`, default 1337).
  pub port: u16,
  /// Last JSON bridge port (`RSRPC_BRIDGE_PORT_END`, default 1347).
  pub bridge_port_end: u16,
  /// MessagePack bridge port (`RSRPC_MSGPACK_PORT`, default 1338).
  pub msgpack_port: u16,
  /// First game WebSocket port (`RSRPC_WS_PORT_START`, default 6463).
  pub ws_port_start: u16,
  /// Last game WebSocket port (`RSRPC_WS_PORT_END`, default 6472).
  pub ws_port_end: u16,
  /// Base scan cadence in seconds (`RSRPC_SCAN_INTERVAL`, default 5).
  pub scan_interval_secs: u64,
  /// Database fetch URL (`--db-url` / `RSRPC_DB_URL`).
  pub db_url: Option<String>,
  /// Hourly database refresh (`--enable-db-update`).
  pub enable_db_update: bool,
  /// ETag seeding the first conditional refresh.
  pub initial_db_etag: Option<String>,
  /// Content-hash seeding the first conditional refresh.
  pub initial_db_content_hash: Option<(u64, u64)>,
  /// App ids the scanner never publishes (`RSRPC_IGNORE_IDS`).
  pub ignored_ids: Vec<String>,
  /// Exclusions feed URL (`RSRPC_EXCLUSIONS_URL`).
  pub exclusions_url: Option<String>,
  /// Version stamped into state snapshots (the daemon version: the CLI
  /// sets this to its own; the default is this crate's version).
  pub app_version: String,
  /// Write presence snapshots to `<tmpdir>/rsrpc-state-{0..9}` for
  /// external tooling (`--state-file` / `RSRPC_STATE_FILE`).
  pub state_file: bool,
}

impl Default for RPCConfig {
  /// All connectors on, legacy ports (1337/1338), db updates disabled.
  fn default() -> Self {
    Self {
      enable_process_scanner: true,
      enable_proc_events: true,
      enable_ipc_connector: true,
      enable_websocket_connector: true,
      enable_secondary_events: true,
      port: 1337,
      bridge_port_end: 1347,
      msgpack_port: 1338,
      ws_port_start: 6463,
      ws_port_end: 6472,
      scan_interval_secs: 5,
      db_url: None,
      enable_db_update: false,
      initial_db_etag: None,
      initial_db_content_hash: None,
      ignored_ids: Vec::new(),
      exclusions_url: None,
      app_version: env!("CARGO_PKG_VERSION").to_string(),
      state_file: false,
    }
  }
}

impl RPCConfig {
  /// Start building programmatically.
  #[must_use]
  pub fn builder() -> RPCConfigBuilder {
    RPCConfigBuilder {
      inner: RPCConfig::default(),
    }
  }
}

/// Builder for [`RPCConfig`] (`api-builder-pattern`).
#[derive(Clone, Debug)]
pub struct RPCConfigBuilder {
  inner: RPCConfig,
}

/// Generate `#[must_use]` bool/u64/Option setters for the builder.
macro_rules! builder_setters {
  ($(($field:ident, $ty:ty)),*) => {
    $(
      #[must_use]
      pub fn $field(mut self, value: $ty) -> Self {
        self.inner.$field = value;
        self
      }
    )*
  };
}

impl RPCConfigBuilder {
  builder_setters!(
    (enable_process_scanner, bool),
    (enable_proc_events, bool),
    (enable_ipc_connector, bool),
    (enable_websocket_connector, bool),
    (enable_secondary_events, bool),
    (port, u16),
    (bridge_port_end, u16),
    (msgpack_port, u16),
    (ws_port_start, u16),
    (ws_port_end, u16),
    (scan_interval_secs, u64),
    (db_url, Option<String>),
    (enable_db_update, bool),
    (initial_db_etag, Option<String>),
    (initial_db_content_hash, Option<(u64, u64)>),
    (ignored_ids, Vec<String>),
    (exclusions_url, Option<String>),
    (app_version, String),
    (state_file, bool)
  );

  /// Finish building.
  #[must_use]
  pub fn build(self) -> RPCConfig {
    self.inner
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Defaults keep the legacy ports and every connector enabled.
  #[test]
  fn defaults_match_legacy_ports_and_flags() {
    let config = RPCConfig::default();
    assert!(config.enable_process_scanner);
    assert!(config.enable_proc_events);
    assert!(config.enable_ipc_connector);
    assert!(config.enable_websocket_connector);
    assert!(config.enable_secondary_events);
    assert_eq!(config.port, 1337);
    assert_eq!(config.bridge_port_end, 1347);
    assert_eq!(config.msgpack_port, 1338);
    assert_eq!(config.ws_port_start, 6463);
    assert_eq!(config.ws_port_end, 6472);
    assert_eq!(config.scan_interval_secs, 5);
    assert!(!config.enable_db_update);
    assert!(config.ignored_ids.is_empty());
    assert!(!config.state_file);
  }

  /// The builder reaches every field (no silent fallback).
  #[test]
  fn builder_covers_every_field() {
    let config = RPCConfig::builder()
      .enable_process_scanner(false)
      .enable_proc_events(false)
      .enable_ipc_connector(false)
      .enable_websocket_connector(false)
      .enable_secondary_events(false)
      .port(1)
      .bridge_port_end(2)
      .msgpack_port(3)
      .ws_port_start(4)
      .ws_port_end(5)
      .scan_interval_secs(6)
      .db_url(Some("https://example.invalid/db".to_string()))
      .enable_db_update(true)
      .initial_db_etag(Some("tag".to_string()))
      .initial_db_content_hash(Some((1, 2)))
      .ignored_ids(vec!["7".to_string()])
      .exclusions_url(Some("https://example.invalid/ex".to_string()))
      .app_version("test".to_string())
      .state_file(true)
      .build();
    assert!(!config.enable_process_scanner);
    assert_eq!(config.port, 1);
    assert_eq!(config.msgpack_port, 3);
    assert_eq!(config.initial_db_content_hash, Some((1, 2)));
    assert_eq!(config.ignored_ids, vec!["7".to_string()]);
    assert_eq!(config.app_version, "test");
    assert!(config.state_file);
  }
}
