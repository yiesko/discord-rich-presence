#[cfg(target_os = "linux")]
use rsrpc_telemetry::rss_bytes;
use rsrpc_telemetry::{QueueGauge, StatsSnapshot, format_resource_stats};

#[test]
fn queue_gauge_tracks_depth() {
  let (tx, rx) = QueueGauge::pair::<u64>();
  let gauge = tx.gauge();
  assert_eq!(gauge.depth(), 0);
  tx.send(1).expect("send");
  assert_eq!(gauge.depth(), 1);
  tx.send(2).expect("send");
  assert_eq!(gauge.depth(), 2);
  assert_eq!(rx.recv().expect("recv"), 1);
  assert_eq!(gauge.depth(), 1);
  assert_eq!(rx.recv().expect("recv"), 2);
  assert_eq!(gauge.depth(), 0);
}

#[test]
fn queue_gauge_clone_shares_depth() {
  // Production clones senders across threads: the gauge must follow.
  let (tx, _rx) = QueueGauge::pair::<u64>();
  let gauge = tx.gauge();
  let tx2 = tx.clone();
  tx2.send(1).expect("send");
  assert_eq!(gauge.depth(), 1);
}

#[test]
fn queue_gauge_ignores_failed_send() {
  // Receiver gone (shutdown): no phantom backlog may stick.
  let (tx, rx) = QueueGauge::pair::<u64>();
  let gauge = tx.gauge();
  drop(rx);
  assert!(tx.send(1).is_err());
  assert_eq!(gauge.depth(), 0);
}

#[test]
fn queue_gauge_settles_at_zero_after_concurrent_use() {
  // Hammering senders racing a drainer must never wrap the depth: at
  // quiescence (all senders joined, all values received) it reads zero.
  const SENDERS: usize = 4;
  const PER_SENDER: u64 = 500;
  let (tx, rx) = QueueGauge::pair::<u64>();
  let gauge = tx.gauge();
  std::thread::scope(|scope| {
    for _ in 0..SENDERS {
      let tx = tx.clone();
      scope.spawn(move || {
        for value in 0..PER_SENDER {
          tx.send(value).expect("receiver lives for the whole test");
        }
      });
    }
    for _ in 0..(SENDERS as u64 * PER_SENDER) {
      rx.recv().expect("senders outlive the drain");
    }
  });
  assert_eq!(gauge.depth(), 0);
}

#[test]
#[cfg(target_os = "linux")]
fn rss_bytes_reports_live_process() {
  let rss = rss_bytes().expect("rss readable on linux");
  assert!(rss > 0, "a live test process has resident memory");
}

#[test]
fn format_resource_stats_mentions_reason_and_fields() {
  // 40 MiB exactly: deterministic rendering check.
  let snapshot = StatsSnapshot {
    rss_bytes: Some(41_943_040),
    bridge_json: 1,
    bridge_msgpack: 0,
    ws: 2,
    watch_depth: 3,
    proc_depth: 4,
    ws_depth: 5,
  };
  let line = format_resource_stats("hourly", &snapshot);
  assert!(line.contains("hourly"), "reason missing: {line}");
  assert!(line.contains("40.0MB"), "rss missing: {line}");
  assert!(line.contains("json:1"), "bridge json count missing: {line}");
  assert!(
    line.contains("msgpack:0"),
    "bridge msgpack count missing: {line}"
  );
  assert!(line.contains("ws=2"), "ws client count missing: {line}");
  assert!(line.contains("watch:3"), "watch depth missing: {line}");
  assert!(line.contains("proc:4"), "proc depth missing: {line}");
  assert!(line.contains("ws:5"), "ws depth missing: {line}");
}
