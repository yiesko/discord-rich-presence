//! Census tests that need the connector-wired `StatsCtx` stay here; gauge,
//! RSS and rendering coverage moved to `rsrpc-telemetry/tests/telemetry.rs`.

use crate::server::utils::{QueueGauge, StatsCtx};

#[test]
fn stats_ctx_snapshot_reports_zeroed_state() {
  use std::collections::HashMap;
  use std::sync::{Arc, Mutex};

  let stats = StatsCtx {
    watch: Arc::new(Mutex::new(QueueGauge::new())),
    proc_events: QueueGauge::new(),
    ws_events: QueueGauge::new(),
    bridge_json: Arc::new(Mutex::new(HashMap::new())),
    bridge_msgpack: Arc::new(Mutex::new(HashMap::new())),
    ws_clients: Arc::new(Mutex::new(HashMap::new())),
  };
  let line = stats.snapshot("boot");
  assert!(line.contains("boot"), "reason missing: {line}");
  assert!(line.contains("watch:0"), "watch depth missing: {line}");
}
