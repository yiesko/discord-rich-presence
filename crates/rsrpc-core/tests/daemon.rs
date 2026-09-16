//! Daemon integration: ephemeral bind, one-shot diagnostics, shutdown.

use std::time::Duration;

use rsrpc_core::{Daemon, RPCConfig};

fn ephemeral_config() -> RPCConfig {
  RPCConfig::builder()
    .port(0)
    .bridge_port_end(0)
    .msgpack_port(0)
    .ws_port_start(0)
    .ws_port_end(0)
    .build()
}

#[test]
fn empty_database_detects_nothing_but_stays_ok() {
  let daemon = Daemon::from_json_str("[]", ephemeral_config()).expect("empty db parses");
  let found = daemon.detect_once().expect("scan works");
  assert!(found.is_empty());
  assert!(daemon.database_summary().is_empty());
}

#[test]
fn bundled_database_summarizes() {
  let daemon = Daemon::from_bundled(RPCConfig::default()).expect("bundled parses");
  assert!(!daemon.database_summary().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_binds_and_shuts_down_cleanly() {
  let daemon = Daemon::from_json_str("[]", ephemeral_config()).expect("empty db parses");
  tokio::time::timeout(Duration::from_secs(30), daemon.run_until(async {}))
    .await
    .expect("run returns after immediate shutdown")
    .expect("clean shutdown");
}
