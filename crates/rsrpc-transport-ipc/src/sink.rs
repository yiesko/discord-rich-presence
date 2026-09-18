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

/// Upper bound a presence-clear waits for queue space before shedding
/// (counted, warned) like an ordinary command. Clears are final — no
/// later event repairs a dropped one — so a momentarily full queue must
/// delay them briefly, never drop them outright; the bound keeps a
/// wedged bridge from parking a connection thread without limit.
const CLEAR_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

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

  /// Queue a presence-clear command, retrying until the clear send
  /// timeout when the queue is momentarily full. Falls back to
  /// shed-and-count past the bound or when closed, exactly like
  /// [`emit`](Self::emit).
  pub fn emit_clear(&self, cmd: ActivityCmd) {
    let mut pending = Some(cmd);
    let deadline = std::time::Instant::now() + CLEAR_SEND_TIMEOUT;
    while let Some(cmd) = pending.take() {
      match self.tx.try_send(cmd) {
        Ok(()) => return,
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
          self.dropped.fetch_add(1, Ordering::Relaxed);
          tracing::warn!("[ipc] Event sink closed, dropping clear");
          return;
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(cmd)) => {
          if std::time::Instant::now() >= deadline {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("[ipc] Event sink still full, dropping clear");
            return;
          }
          pending = Some(cmd);
          std::thread::sleep(std::time::Duration::from_millis(1));
        }
      }
    }
  }

  /// Commands shed since creation.
  #[must_use]
  pub fn dropped_total(&self) -> u64 {
    self.dropped.load(Ordering::Relaxed)
  }

  /// Shared sender for census queue-depth sampling
  /// (`max_capacity - capacity` = queued).
  pub fn sender(&self) -> mpsc::Sender<ActivityCmd> {
    self.tx.clone()
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

  #[test]
  fn emit_clear_survives_a_full_queue() {
    // Clears are final (no later event repairs a dropped one): a full
    // queue must delay them briefly, never shed them like ordinary
    // commands.
    let (sink, mut rx) = EventSink::bounded(1);
    sink.emit(ActivityCmd::empty()); // queue full
    let clearer = sink.clone();
    let handle = std::thread::spawn(move || clearer.emit_clear(ActivityCmd::empty()));
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(rx.try_recv().is_ok(), "filler arrives");
    // The retry loop lands within milliseconds of the freed slot: poll
    // with a generous deadline instead of asserting instantly.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
      if rx.try_recv().is_ok() {
        break;
      }
      assert!(
        std::time::Instant::now() < deadline,
        "clear follows once space frees"
      );
      std::thread::sleep(std::time::Duration::from_millis(1));
    }
    handle.join().expect("clear delivered");
    assert_eq!(sink.dropped_total(), 0);
  }
}
