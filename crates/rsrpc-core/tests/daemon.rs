//! Daemon integration: ephemeral bind, one-shot diagnostics, shutdown.

use std::time::Duration;

use rsrpc_core::{Daemon, RPCConfig};

/// Zero-port config: every listener binds ephemeral for hermetic tests.
fn ephemeral_config() -> RPCConfig {
  RPCConfig::builder()
    .port(0)
    .bridge_port_end(0)
    .msgpack_port(0)
    .ws_port_start(0)
    .ws_port_end(0)
    .build()
}

/// Shutdown-test config: scanner, proc-events and IPC off (nothing to
/// observe there), ephemeral TCP listener only — fast and hermetic.
fn shutdown_config() -> RPCConfig {
  RPCConfig::builder()
    .port(0)
    .bridge_port_end(0)
    .msgpack_port(0)
    .ws_port_start(0)
    .ws_port_end(0)
    .enable_process_scanner(false)
    .enable_proc_events(false)
    .enable_ipc_connector(false)
    .build()
}

/// Empty databases detect nothing but stay fully operational.
/// Zero scan intervals are rejected before the automaton build (a zero
/// cadence would hot-spin the scan loop).
#[test]
fn zero_scan_interval_is_rejected() {
  let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap();
  let mut config = ephemeral_config();
  config.scan_interval_secs = 0;
  let daemon = Daemon::from_json_str("[]", config).expect("empty db parses");
  let err = rt
    .block_on(daemon.run_until(async {}))
    .expect_err("zero interval must fail");
  assert!(
    err.to_string().contains("scan_interval_secs"),
    "unexpected error: {err}"
  );
}

/// Empty databases detect nothing but stay fully operational.
#[test]
fn empty_database_detects_nothing_but_stays_ok() {
  let daemon = Daemon::from_json_str("[]", ephemeral_config()).expect("empty db parses");
  let found = daemon.detect_once().expect("scan works");
  assert!(found.is_empty());
  assert!(daemon.database_summary().is_empty());
}

/// The bundled snapshot parses and summarizes non-empty.
#[test]
fn bundled_database_summarizes() {
  let daemon = Daemon::from_bundled(RPCConfig::default()).expect("bundled parses");
  assert!(!daemon.database_summary().is_empty());
}

/// Full boot on ephemeral ports shuts down cleanly inside the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_binds_and_shuts_down_cleanly() {
  let daemon = Daemon::from_json_str("[]", shutdown_config()).expect("empty db parses");
  tokio::time::timeout(Duration::from_secs(30), daemon.run_until(async {}))
    .await
    .expect("run returns after immediate shutdown")
    .expect("clean shutdown");
}
