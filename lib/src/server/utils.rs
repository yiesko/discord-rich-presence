//! Daemon resource census. Gauges and RSS live in `rsrpc-telemetry`; this
//! module keeps the connector-wiring snapshot until the core owns it
//! (Phase 6): it names concrete client maps the telemetry crate must not
//! depend on.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Re-exported telemetry primitives so existing `super::utils::` paths keep
/// working (this `pub use` also names them inside this module); new code
/// imports `rsrpc_telemetry` directly.
pub(crate) use rsrpc_telemetry::{
  GaugeReceiver, GaugeSender, QueueGauge, RecvQueue, StatsSnapshot, format_resource_stats,
  rss_bytes,
};

/// Everything a census line needs, as shared handles: readable from the
/// hourly sampler thread and from the game-session transition points
/// without moving any channel or map.
#[derive(Clone)]
pub(crate) struct StatsCtx {
  /// Late-bound: the watch pair is created inside `ProcessServer::start`,
  /// so the daemon hands a slot that the watcher fills once spawned. Reads
  /// zero until then — which is the truth (no watcher, no backlog).
  pub watch: Arc<Mutex<QueueGauge>>,
  pub proc_events: QueueGauge,
  pub ws_events: QueueGauge,
  pub bridge_json: Arc<Mutex<HashMap<u64, simple_websockets::Responder>>>,
  pub bridge_msgpack: Arc<Mutex<HashMap<u64, simple_websockets::Responder>>>,
  pub ws_clients: Arc<Mutex<HashMap<u64, super::websocket::ActivityResponder>>>,
}

impl StatsCtx {
  pub(crate) fn snapshot(&self, reason: &str) -> String {
    // Read RSS before touching any lock: no I/O under a held lock.
    let rss_bytes = rss_bytes();
    let snapshot = StatsSnapshot {
      rss_bytes,
      bridge_json: self
        .bridge_json
        .lock()
        .map(|clients| clients.len())
        .unwrap_or_else(|poisoned| poisoned.into_inner().len()),
      bridge_msgpack: self
        .bridge_msgpack
        .lock()
        .map(|clients| clients.len())
        .unwrap_or_else(|poisoned| poisoned.into_inner().len()),
      ws: self
        .ws_clients
        .lock()
        .map(|clients| clients.len())
        .unwrap_or_else(|poisoned| poisoned.into_inner().len()),
      watch_depth: self
        .watch
        .lock()
        .map(|gauge| gauge.depth())
        .unwrap_or_else(|poisoned| poisoned.into_inner().depth()),
      proc_depth: self.proc_events.depth(),
      ws_depth: self.ws_events.depth(),
    };
    format_resource_stats(reason, &snapshot)
  }
}

/// Cadence of the background census (plus one line at boot as baseline and
/// one per game-session transition for churn correlation).
pub(crate) const STATS_INTERVAL_SECS: u64 = 3600;
