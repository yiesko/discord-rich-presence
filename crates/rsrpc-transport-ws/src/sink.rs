use std::{
  collections::VecDeque,
  sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use rsrpc_types::cmd::ActivityCmd;
use rsrpc_ws::Responder;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::handlers;

/// How long the pump waits for sink capacity before shedding (counted).
pub(crate) const SINK_SEND_TIMEOUT: Duration = Duration::from_millis(250);

/// Shared budget for one disconnect cleanup pass: clears retry until it
/// lapses instead of shedding on the first full queue, so a transiently
/// wedged bridge still gets its clears. Bounds the worst pump stall per
/// disconnect no matter how many pids were tracked.
pub(crate) const DISCONNECT_CLEAR_BUDGET: Duration = Duration::from_secs(1);

/// Cap on staged disconnect clears transport-wide: ~16 fully-wedged
/// disconnects. Past it clears shed counted (visible via `dropped_total`).
/// Unbounded retention would let connection churn grow memory forever while
/// the bridge is wedged; every handoff/publish table in this workspace
/// sheds oldest the same way.
pub(crate) const MAX_PENDING_CLEARS: usize = 256;

/// A staged disconnect clear with its own expiry: an older retry task
/// lapsing must never shed a newer entry, so each clear carries the
/// deadline it was staged with (see `stage_clear`).
#[derive(Debug, Clone)]
pub(crate) struct PendingClear {
  /// The clear command, oldest staged first in the queue.
  pub(crate) cmd: ActivityCmd,
  /// Lapses independently per entry (see `expire_pending`).
  pub(crate) deadline: std::time::Instant,
}

/// Per-client slot: responder plus the publication history needed for
/// clear-on-disconnect.
///
/// No `Clone` derive (see `SlotSnapshot`): the history must only ever be
/// read under a short guard, never duplicated wholesale.
#[derive(Debug)]
pub(crate) struct ClientSlot {
  pub(crate) responder: Responder,
  /// Every pid published on this connection (slim records), oldest first.
  /// A single-pid client — the norm — keeps exactly one entry. Capped at
  /// [`handlers::MAX_TRACKED_PIDS`]: extras are refused before forwarding,
  /// so this history is always complete and disconnect cleanup clears
  /// every forwarded card.
  pub(crate) published: Vec<handlers::PublishedSlot>,
  pub(crate) query_client_id: Option<String>,
}

/// Whether a publication was admitted to the bounded per-connection history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Admission {
  /// Tracked (re-publishes refresh as most recent). Carries the displaced
  /// prior slot, if any, so a shed delivery can roll back to it instead of
  /// dropping tracking for a previously shown card.
  Admitted {
    /// Previous record for this pid (`None` for first-time publishes).
    displaced: Option<handlers::PublishedSlot>,
  },
  /// Unknown pid past the bound: refused before forwarding.
  RefusedFull,
}

impl ClientSlot {
  /// Record a publication, refreshing a re-published pid as most recent.
  /// Unknown pids past [`handlers::MAX_TRACKED_PIDS`] are refused: the
  /// caller must not forward them, keeping every forwarded pid tracked
  /// (and therefore covered by disconnect cleanup).
  pub(crate) fn note_published(
    &mut self,
    app_id: Option<String>,
    pid: u64,
    nonce: Value,
  ) -> Admission {
    if let Some(pos) = self.published.iter().position(|entry| entry.pid == pid) {
      let displaced = self.published.remove(pos);
      self
        .published
        .push(handlers::PublishedSlot { app_id, pid, nonce });
      Admission::Admitted {
        displaced: Some(displaced),
      }
    } else if self.published.len() >= handlers::MAX_TRACKED_PIDS {
      Admission::RefusedFull
    } else {
      self
        .published
        .push(handlers::PublishedSlot { app_id, pid, nonce });
      Admission::Admitted { displaced: None }
    }
  }

  /// Forget one tracked pid. Used when a genuine client clear lands
  /// (frees the slot for new pids). No-op when absent.
  pub(crate) fn forget_published(&mut self, pid: u64) {
    if let Some(pos) = self.published.iter().position(|entry| entry.pid == pid) {
      self.published.remove(pos);
    }
  }

  /// Roll back a shed admission: drop the undelivered record, restoring the
  /// displaced slot of a refresh so a previously shown card stays tracked
  /// (and clearable on disconnect). First-time publishes leave nothing.
  pub(crate) fn rollback_publish(&mut self, pid: u64, displaced: Option<handlers::PublishedSlot>) {
    self.forget_published(pid);
    if let Some(old) = displaced {
      self.published.push(old);
    }
  }
}

/// Outcome of a non-blocking sink enqueue: the refused command rides
/// along on `Full` so callers can stage it instead of dropping it.
#[derive(Debug)]
pub(crate) enum TryEmit {
  /// Accepted by the sink.
  Sent,
  /// Queue full; command returned (boxed: the 1KB body would dwarf the
  /// enum) for staging.
  Full(Box<ActivityCmd>),
  /// Receiver gone; no receiver will ever return.
  Closed,
}

/// Bounded event sink with a shed counter.
///
/// `send_timeout` waits briefly for genuine bursts, then drops (counted)
/// instead of stalling the pump — one slow bridge must not freeze every
/// game client. A closed sink (dead bridge) also counts and continues.
#[derive(Debug, Clone)]
pub(crate) struct Sink {
  pub(crate) tx: mpsc::Sender<ActivityCmd>,
  pub(crate) dropped: Arc<AtomicU64>,
}

impl Sink {
  /// Queue one command downstream, shedding (counted) instead of stalling
  /// the pump when the bridge stops draining. Returns whether the command
  /// was accepted: callers track a publication only on `true`, so a shed
  /// publish leaves no disconnect clear behind for a card that never showed.
  pub(crate) async fn emit(&self, cmd: ActivityCmd) -> bool {
    match self.tx.send_timeout(cmd, SINK_SEND_TIMEOUT).await {
      Ok(()) => true,
      Err(_) => {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        tracing::warn!("[transport-ws] Event sink full/closed, dropping command");
        false
      }
    }
  }

  /// Count `n` shed clears (shared by staging overflow and drains).
  pub(crate) fn count_shed(&self, n: u64) {
    self.dropped.fetch_add(n, Ordering::Relaxed);
  }

  /// Non-blocking enqueue for the staged-clear flush below: reports the
  /// outcome instead of waiting, so the pump never stalls on a wedge.
  /// A dedicated enum (not `Result`): returning the refused command back
  /// for re-staging would trip `result_large_err` on the 1KB command.
  pub(crate) fn try_emit(&self, cmd: ActivityCmd) -> TryEmit {
    match self.tx.try_send(cmd) {
      Ok(()) => TryEmit::Sent,
      Err(tokio::sync::mpsc::error::TrySendError::Full(cmd)) => TryEmit::Full(Box::new(cmd)),
      Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => TryEmit::Closed,
    }
  }

  /// Stage a disconnect clear for opportunistic flush, stamped with its
  /// own expiry. Bounded: past `MAX_PENDING_CLEARS` the oldest sheds
  /// counted instead of growing memory without limit.
  pub(crate) fn stage_clear(&self, pending: &Mutex<VecDeque<PendingClear>>, cmd: ActivityCmd) {
    let mut pending = pending.lock().unwrap_or_else(|e| e.into_inner());
    if pending.len() >= MAX_PENDING_CLEARS {
      self.count_shed(1);
      tracing::warn!("[transport-ws] Pending clears full, shedding oldest");
      pending.pop_front();
    }
    pending.push_back(PendingClear {
      cmd,
      deadline: std::time::Instant::now() + DISCONNECT_CLEAR_BUDGET,
    });
  }

  /// Flush staged clears oldest-first, stopping at the first full queue
  /// (order preserved for the next flush). Delivery never expires: an
  /// unexpired clear always deserves its chance, whenever capacity
  /// returns. A closed sink drops the whole queue counted — no receiver
  /// will ever return. Never waits: safe on every pump path.
  pub(crate) fn flush_pending(&self, pending: &Mutex<VecDeque<PendingClear>>) {
    let mut pending = pending.lock().unwrap_or_else(|e| e.into_inner());
    while let Some(first) = pending.front() {
      match self.try_emit(first.cmd.clone()) {
        TryEmit::Sent => {
          pending.pop_front();
        }
        TryEmit::Full(_) => break,
        TryEmit::Closed => {
          self
            .dropped
            .fetch_add(pending.len() as u64, Ordering::Relaxed);
          tracing::warn!(
            "[transport-ws] Event sink closed, dropping {} staged clear(s)",
            pending.len()
          );
          pending.clear();
          break;
        }
      }
    }
  }

  /// Shed only entries whose own deadline elapsed, counting each. An older
  /// retry task lapsing must never discard a newer entry that still owns
  /// a retry window.
  pub(crate) fn expire_pending(&self, pending: &Mutex<VecDeque<PendingClear>>) {
    let mut pending = pending.lock().unwrap_or_else(|e| e.into_inner());
    let now = std::time::Instant::now();
    let mut expired = 0u64;
    pending.retain(|entry| {
      if now >= entry.deadline {
        expired += 1;
        false
      } else {
        true
      }
    });
    if expired > 0 {
      self.count_shed(expired);
      tracing::warn!(
        "[transport-ws] Clear-retry budget lapsed, dropping {expired} staged clear(s)"
      );
    }
  }

  /// Queue one disconnect clear, retrying while `deadline` holds.
  ///
  /// Survives a transiently full sink where a single attempt would shed.
  /// Used by the shutdown drain, where waiting (bounded) is acceptable;
  /// the live pump stages instead and never waits.
  pub(crate) async fn emit_clear_retry(
    &self,
    cmd: ActivityCmd,
    deadline: std::time::Instant,
  ) -> bool {
    let mut pending = Some(cmd);
    loop {
      let Some(cmd) = pending.take() else {
        return false;
      };
      match self.tx.try_send(cmd) {
        Ok(()) => return true,
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
          self.dropped.fetch_add(1, Ordering::Relaxed);
          tracing::warn!("[transport-ws] Event sink closed, dropping clear");
          return false;
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(cmd)) => {
          if std::time::Instant::now() >= deadline {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("[transport-ws] Event sink still full, dropping clear");
            return false;
          }
          pending = Some(cmd);
          tokio::time::sleep(Duration::from_millis(1)).await;
        }
      }
    }
  }
}
