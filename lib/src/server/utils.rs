//! Shared helpers for the server connectors: queue depth gauges, RSS
//! readings and the hourly/session resource census line.
//!
//! The gauges exist to discriminate two failure modes in long sessions
//! without guessing: an ever-growing queue depth points at producer
//! outpacing consumer (H1), while a climbing RSS with flat depths points
//! at allocator retention/fragmentation (H6). They never change runtime
//! behavior — sends and receives behave exactly like `std::mpsc`.

use std::collections::HashMap;
use std::sync::{
  Arc, Mutex,
  atomic::{AtomicUsize, Ordering},
  mpsc,
};

/// Depth gauge shared by one channel's sender(s) and receiver: incremented
/// on every successful send, decremented on every successful receive.
/// `Relaxed` is the weakest correct ordering here (diagnostic counter only:
/// no data is synchronized through it).
#[derive(Clone, Default)]
pub(crate) struct QueueGauge {
  depth: Arc<AtomicUsize>,
}

impl QueueGauge {
  pub(crate) fn new() -> Self {
    Self::default()
  }

  /// Current backlog. It can only drift from reality if a `send`/`recv`
  /// bypasses the wrappers below — there is no such bypass: the wrapped
  /// channel ends are moved, never the raw ones.
  pub(crate) fn depth(&self) -> usize {
    self.depth.load(Ordering::Relaxed)
  }

  fn inc(&self) {
    self.depth.fetch_add(1, Ordering::Relaxed);
  }

  fn dec(&self) {
    self.depth.fetch_sub(1, Ordering::Relaxed);
  }

  /// A fresh unbounded channel with both ends sharing one gauge.
  pub(crate) fn pair<T>() -> (GaugeSender<T>, GaugeReceiver<T>) {
    let (tx, rx) = mpsc::channel();
    let gauge = QueueGauge::new();
    (
      GaugeSender {
        inner: tx,
        gauge: gauge.clone(),
      },
      GaugeReceiver { inner: rx, gauge },
    )
  }
}

/// `mpsc::Sender` that counts its backlog. `Clone` shares the gauge, so
/// producers cloned across threads still report into the same depth.
pub(crate) struct GaugeSender<T> {
  inner: mpsc::Sender<T>,
  gauge: QueueGauge,
}

impl<T> Clone for GaugeSender<T> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      gauge: self.gauge.clone(),
    }
  }
}

impl<T> GaugeSender<T> {
  pub(crate) fn send(&self, value: T) -> Result<(), mpsc::SendError<T>> {
    match self.inner.send(value) {
      Ok(()) => {
        self.gauge.inc();
        Ok(())
      }
      // Receiver gone (shutdown): no phantom backlog may stick.
      Err(err) => Err(err),
    }
  }

  pub(crate) fn gauge(&self) -> QueueGauge {
    self.gauge.clone()
  }
}

/// Anything the bridge `event_loop` can drain: the raw bounded receiver
/// of the IPC leg, or a gauged receiver everywhere else. Both spellings
/// expose the same blocking `recv`, so one loop serves both legs without
/// duplicating its ~70 lines.
pub(crate) trait RecvQueue<T> {
  fn recv_q(&self) -> Result<T, mpsc::RecvError>;
}

impl<T> RecvQueue<T> for mpsc::Receiver<T> {
  fn recv_q(&self) -> Result<T, mpsc::RecvError> {
    self.recv()
  }
}

impl<T> RecvQueue<T> for GaugeReceiver<T> {
  fn recv_q(&self) -> Result<T, mpsc::RecvError> {
    self.recv()
  }
}

/// `mpsc::Receiver` that releases its backlog count on every receive.
pub(crate) struct GaugeReceiver<T> {
  inner: mpsc::Receiver<T>,
  gauge: QueueGauge,
}

impl<T> GaugeReceiver<T> {
  pub(crate) fn recv(&self) -> Result<T, mpsc::RecvError> {
    self.inner.recv().inspect(|_| {
      self.gauge.dec();
    })
  }
}

/// Resident set size in bytes, Linux only (`/proc/self/statm` resident
/// field × page size). `None` elsewhere or when the kernel won't tell.
#[cfg(target_os = "linux")]
pub(crate) fn rss_bytes() -> Option<u64> {
  let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
  let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
  // SAFETY: `sysconf` with `_SC_PAGESIZE` takes no pointer and has no
  // failure mode beyond returning -1.
  let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
  if page <= 0 {
    return None;
  }
  resident_pages.checked_mul(page as u64)
}

/// Non-Linux targets have no cheap self-RSS source: report absence and let
/// the census line render `n/a` instead of guessing.
#[cfg(not(target_os = "linux"))]
pub(crate) fn rss_bytes() -> Option<u64> {
  None
}

/// One resource census: client counts plus the three unbounded queue
/// depths (watch, proc-events, websocket legs).
pub(crate) struct StatsSnapshot {
  pub rss_bytes: Option<u64>,
  pub bridge_json: usize,
  pub bridge_msgpack: usize,
  pub ws: usize,
  pub watch_depth: usize,
  pub proc_depth: usize,
  pub ws_depth: usize,
}

impl StatsSnapshot {
  fn rss_mb(&self) -> String {
    self
      .rss_bytes
      .map(|bytes| format!("{:.1}MB", bytes as f64 / 1_048_576.0))
      .unwrap_or_else(|| "n/a".to_string())
  }
}

/// Render the census line. Pure function so tests pin the shape.
pub(crate) fn format_resource_stats(reason: &str, snapshot: &StatsSnapshot) -> String {
  format!(
    "[rsrpc] stats ({reason}): rss={} bridge=json:{}+msgpack:{} ws={} queues=watch:{}+proc:{}+ws:{}",
    snapshot.rss_mb(),
    snapshot.bridge_json,
    snapshot.bridge_msgpack,
    snapshot.ws,
    snapshot.watch_depth,
    snapshot.proc_depth,
    snapshot.ws_depth,
  )
}

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
