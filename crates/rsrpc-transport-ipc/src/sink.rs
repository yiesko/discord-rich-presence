//! Bounded downstream sink with shed accounting.
//!
//! The queue absorbs legitimate bursts; when the bridge stops draining, new
//! commands shed (counted, warned) instead of parking connection threads
//! without bound.

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use rsrpc_types::cmd::ActivityCmd;
use tokio::sync::mpsc;

/// Bound for the transport→bridge queue (matches the legacy 64: legitimate
/// bursts fit, a wedged bridge sheds counted instead of growing memory).
pub const DEFAULT_IPC_QUEUE: usize = 64;

/// Cloneable handle feeding validated commands downstream.
#[derive(Debug, Clone)]
pub struct EventSink {
  tx: mpsc::Sender<ActivityCmd>,
  dropped: Arc<AtomicU64>,
}

impl EventSink {
  /// Create a sink with `capacity` slots plus its receiver.
  ///
  /// # Panics
  ///
  /// Panics when `capacity` is zero (tokio channel contract).
  #[must_use]
  pub fn bounded(capacity: usize) -> (Self, mpsc::Receiver<ActivityCmd>) {
    let (tx, rx) = mpsc::channel(capacity);
    (
      Self {
        tx,
        dropped: Arc::new(AtomicU64::new(0)),
      },
      rx,
    )
  }

  /// Queue one command; shed (counted) when full or closed.
  ///
  /// Never blocks: connection threads must not stall on a wedged bridge.
  pub fn emit(&self, cmd: ActivityCmd) {
    if self.tx.try_send(cmd).is_err() {
      self.dropped.fetch_add(1, Ordering::Relaxed);
      tracing::warn!("[ipc] Event sink full/closed, dropping command");
    }
  }

  /// Commands shed since creation.
  #[must_use]
  pub fn dropped_total(&self) -> u64 {
    self.dropped.load(Ordering::Relaxed)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn emit_never_blocks_and_counts() {
    let (sink, mut rx) = EventSink::bounded(1);
    sink.emit(ActivityCmd::empty());
    sink.emit(ActivityCmd::empty());
    assert_eq!(sink.dropped_total(), 1);
    assert!(rx.try_recv().is_ok());
  }
}
