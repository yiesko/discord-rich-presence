#[cfg(target_os = "linux")]
use rsrpc_telemetry::rss_bytes;
use rsrpc_telemetry::{QueueGauge, StatsSnapshot, format_resource_stats};

/// Depth follows sends and receives one by one.
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

/// Cloned senders report into the same shared depth.
#[test]
fn queue_gauge_clone_shares_depth() {
  // Production clones senders across threads: the gauge must follow.
  let (tx, _rx) = QueueGauge::pair::<u64>();
  let gauge = tx.gauge();
  let tx2 = tx.clone();
  tx2.send(1).expect("send");
  assert_eq!(gauge.depth(), 1);
}

/// Failed sends roll back, leaving no phantom backlog.
#[test]
fn queue_gauge_ignores_failed_send() {
  // Receiver gone (shutdown): no phantom backlog may stick.
  let (tx, rx) = QueueGauge::pair::<u64>();
  let gauge = tx.gauge();
  drop(rx);
  assert!(tx.send(1).is_err());
  assert_eq!(gauge.depth(), 0);
}

/// Concurrent hammering never wraps the depth; quiescence reads zero.
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

/// Self-RSS reads positive on a live Linux test process.
#[test]
#[cfg(target_os = "linux")]
fn rss_bytes_reports_live_process() {
  let rss = rss_bytes().expect("rss readable on linux");
  assert!(rss > 0, "a live test process has resident memory");
}

/// The census line carries reason, RSS and every count field.
/// Capacity-checked sends shed (counted) past the cap instead of growing.
#[test]
fn checked_send_sheds_past_cap_and_counts() {
  use rsrpc_telemetry::SendChecked;
  const CAP: usize = 8;
  let (tx, _rx) = QueueGauge::pair::<u64>();
  for _ in 0..CAP {
    assert_eq!(tx.send_checked(1, CAP), SendChecked::Sent);
  }
  assert_eq!(tx.send_checked(1, CAP), SendChecked::Shed);
  assert_eq!(tx.gauge().dropped_total(), 1);
}

/// A full queue with a dead receiver reports `Closed`, not `Shed`:
/// otherwise producers would spin forever instead of exiting.
#[test]
fn checked_send_prefers_closed_over_shed() {
  use rsrpc_telemetry::SendChecked;
  const CAP: usize = 4;
  let (tx, rx) = QueueGauge::pair::<u64>();
  for _ in 0..CAP {
    assert_eq!(tx.send_checked(1, CAP), SendChecked::Sent);
  }
  drop(rx);
  assert_eq!(tx.send_checked(1, CAP), SendChecked::Closed);
}

/// Concurrent cloned senders never jointly exceed cap: exactly `CAP`
/// sends land, the rest shed counted — regardless of scheduling.
#[test]
fn checked_send_never_exceeds_cap_with_cloned_senders() {
  use rsrpc_telemetry::SendChecked;
  use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  };
  const CAP: usize = 16;
  const THREADS: usize = 32;
  const PER_THREAD: usize = 500;
  let (tx, _rx) = QueueGauge::pair::<u64>();
  let sent = Arc::new(AtomicUsize::new(0));
  let barrier = Arc::new(std::sync::Barrier::new(THREADS));
  std::thread::scope(|scope| {
    for _ in 0..THREADS {
      let tx = tx.clone();
      let sent = Arc::clone(&sent);
      let barrier = Arc::clone(&barrier);
      scope.spawn(move || {
        // Aligned waves maximize check/send interleaving across threads.
        for _ in 0..PER_THREAD {
          barrier.wait();
          if tx.send_checked(1, CAP) == SendChecked::Sent {
            sent.fetch_add(1, Ordering::Relaxed);
          }
        }
      });
    }
  });
  assert_eq!(sent.load(Ordering::Relaxed), CAP);
  assert_eq!(tx.gauge().dropped_total(), THREADS * PER_THREAD - CAP);
}

/// A gone receiver reports `Closed` (callers exit), never `Shed`.
#[test]
fn checked_send_reports_closed_receiver() {
  use rsrpc_telemetry::SendChecked;
  let (tx, rx) = QueueGauge::pair::<u64>();
  drop(rx);
  assert_eq!(tx.send_checked(1, 8), SendChecked::Closed);
  assert_eq!(tx.gauge().dropped_total(), 0);
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
