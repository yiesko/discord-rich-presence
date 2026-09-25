//! `watch()` shutdown-stop: a preset stop flag returns promptly on every
//! platform instead of subscribing or blocking on netlink (join-safe
//! shutdown: the retry thread must never wait out a blocked `watch()`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Preset stop skips subscribe, self-test and the blocking receive: the
/// call below must not touch netlink at all, on any platform.
#[test]
fn watch_returns_promptly_when_stop_preset() {
  let (tx, _rx) = rsrpc_telemetry::QueueGauge::pair();
  let stop = AtomicBool::new(true);
  let start = Instant::now();
  let result = rsrpc_proc_events::watch(&tx, &stop);
  assert!(
    result.is_ok(),
    "preset stop must exit quietly, got {result:?}"
  );
  assert!(
    start.elapsed() < Duration::from_secs(5),
    "preset stop must not touch netlink"
  );
  // The flag is read with Acquire parity to the watcher loop.
  assert!(stop.load(Ordering::Acquire));
}
