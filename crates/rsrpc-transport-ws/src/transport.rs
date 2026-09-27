//! Game WebSocket transport: bind loop, event pump, graceful shutdown.
//!
//! The P1 fix, structurally: the client map behind
//! `Arc<RwLock<HashMap<..>>>` is only ever touched in short clone / insert /
//! remove sections. Every `.await` (socket I/O, sink send, responder send)
//! happens with **no guard alive** — a concurrent `client_count()` snapshot
//! can never stall behind message flow (see
//! `snapshots_under_flood_never_stall`).

#[path = "pump.rs"]
mod pump;
#[path = "server.rs"]
mod server;
#[path = "sink.rs"]
mod sink;

pub use server::{TransportHandle, WsTransport};
// Sibling modules (and the pre-split import path) resolve through here.
pub(crate) use sink::{PendingClear, Sink};

#[cfg(test)]
mod tests {
  use std::collections::VecDeque;
  use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
  };
  use std::time::Duration;

  use rsrpc_protocol::query::query_params;
  use rsrpc_types::cmd::ActivityCmd;
  use serde_json::Value;
  use tokio::sync::mpsc;

  use super::server::origin_allowed;
  use super::sink::{
    DISCONNECT_CLEAR_BUDGET, MAX_PENDING_CLEARS, PendingClear, SINK_SEND_TIMEOUT, Sink,
  };
  use crate::handlers;

  /// A briefly-full sink still delivers via retry (no shed on transients).
  #[tokio::test]
  async fn clear_retry_survives_a_briefly_full_sink() {
    // Cap-1 sink held full: the retry lands once space frees inside the
    // budget (a single 250ms attempt would shed it).
    let (tx, mut rx) = mpsc::channel(1);
    tx.send_timeout(
      handlers::clear_for_slot(&handlers::PublishedSlot {
        app_id: None,
        pid: 1,
        nonce: Value::Null,
      }),
      SINK_SEND_TIMEOUT,
    )
    .await
    .expect("filler");
    let sink = Sink {
      tx,
      dropped: Arc::new(AtomicU64::new(0)),
    };
    let cmd = handlers::clear_for_slot(&handlers::PublishedSlot {
      app_id: None,
      pid: 7,
      nonce: Value::Null,
    });
    // Drain once after 400ms, then hold the receiver open (dropping it
    // would close the channel and turn the retry into a `Closed` shed).
    let drain = tokio::spawn(async move {
      tokio::time::sleep(Duration::from_millis(400)).await;
      rx.recv().await.expect("filler");
      std::future::pending::<()>().await;
    });
    let deadline = std::time::Instant::now() + DISCONNECT_CLEAR_BUDGET;
    assert!(sink.emit_clear_retry(cmd, deadline).await);
    assert_eq!(sink.dropped.load(Ordering::Relaxed), 0);
    drain.abort();
  }

  /// A closed sink fails fast-ish and counts the shed clear.
  #[tokio::test]
  async fn clear_retry_gives_up_past_the_budget() {
    // Closed sink: no capacity will ever return — fail fast-ish, counted.
    let (tx, rx) = mpsc::channel::<ActivityCmd>(1);
    drop(rx);
    let sink = Sink {
      tx,
      dropped: Arc::new(AtomicU64::new(0)),
    };
    let cmd = handlers::clear_for_slot(&handlers::PublishedSlot {
      app_id: None,
      pid: 7,
      nonce: Value::Null,
    });
    let deadline = std::time::Instant::now() + DISCONNECT_CLEAR_BUDGET;
    assert!(!sink.emit_clear_retry(cmd, deadline).await);
    assert_eq!(sink.dropped.load(Ordering::Relaxed), 1);
  }

  /// Staged clears never exceed the bound; every shed is counted.
  #[test]
  fn staged_clears_stay_bounded_and_counted() {
    let (tx, _rx) = mpsc::channel::<ActivityCmd>(1);
    let sink = Sink {
      tx,
      dropped: Arc::new(AtomicU64::new(0)),
    };
    let pending = Mutex::new(VecDeque::new());
    for pid in 0..(MAX_PENDING_CLEARS + 5) as u64 {
      sink.stage_clear(
        &pending,
        handlers::clear_for_slot(&handlers::PublishedSlot {
          app_id: None,
          pid,
          nonce: Value::Null,
        }),
      );
    }
    // Bounded history, and every shed clear counted (never silent).
    assert!(pending.lock().unwrap().len() <= MAX_PENDING_CLEARS);
    assert_eq!(sink.dropped.load(Ordering::Relaxed), 5);
  }

  /// Flush delivers oldest-first and stops at the first full queue.
  #[tokio::test]
  async fn flush_preserves_order_and_stops_when_full() {
    // Cap-1 sink held full by a filler: flush delivers nothing, staged intact.
    let (tx, mut rx) = mpsc::channel(1);
    tx.send_timeout(
      handlers::clear_for_slot(&handlers::PublishedSlot {
        app_id: None,
        pid: 1,
        nonce: Value::Null,
      }),
      SINK_SEND_TIMEOUT,
    )
    .await
    .expect("filler");
    let sink = Sink {
      tx,
      dropped: Arc::new(AtomicU64::new(0)),
    };
    let pending = Mutex::new(VecDeque::new());
    for pid in [7u64, 8, 9] {
      sink.stage_clear(
        &pending,
        handlers::clear_for_slot(&handlers::PublishedSlot {
          app_id: None,
          pid,
          nonce: Value::Null,
        }),
      );
    }
    sink.flush_pending(&pending);
    assert_eq!(pending.lock().unwrap().len(), 3);
    // Free slots one by one: staged clears land oldest-first.
    for pid in [1u64, 7, 8] {
      let cmd = rx.try_recv().expect("slot frees in order");
      assert_eq!(cmd.args.as_ref().and_then(|a| a.pid), Some(pid));
      sink.flush_pending(&pending);
    }
    assert!(pending.lock().unwrap().is_empty());
    let last = rx.try_recv().expect("final staged clear");
    assert_eq!(last.args.as_ref().and_then(|a| a.pid), Some(9));
    assert_eq!(sink.dropped.load(Ordering::Relaxed), 0);
  }

  /// Expiry removes only lapsed entries, keeping fresh retry windows.
  #[test]
  fn expiry_sheds_only_lapsed_entries() {
    let (tx, _rx) = mpsc::channel::<ActivityCmd>(1);
    let sink = Sink {
      tx,
      dropped: Arc::new(AtomicU64::new(0)),
    };
    let pending = Mutex::new(VecDeque::new());
    // One long-lapsed entry and one fresh entry, built directly (staging
    // always stamps `now + budget`, which no deterministic test can outwait).
    let past = std::time::Instant::now() - Duration::from_secs(1);
    let future = std::time::Instant::now() + DISCONNECT_CLEAR_BUDGET;
    for (pid, deadline) in [(1u64, past), (2u64, future)] {
      pending.lock().unwrap().push_back(PendingClear {
        cmd: handlers::clear_for_slot(&handlers::PublishedSlot {
          app_id: None,
          pid,
          nonce: Value::Null,
        }),
        deadline,
      });
    }
    sink.expire_pending(&pending);
    // Only the lapsed entry shed (counted); the fresh one survives with
    // its own retry window intact.
    let pending = pending.lock().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].cmd.args.as_ref().and_then(|a| a.pid), Some(2));
    assert_eq!(sink.dropped.load(Ordering::Relaxed), 1);
  }

  /// Discord origins pass, missing origin passes, anything else is refused.
  #[test]
  fn origin_policy() {
    assert!(origin_allowed(None));
    assert!(origin_allowed(Some("https://discord.com")));
    assert!(origin_allowed(Some("https://canary.discord.com")));
    assert!(origin_allowed(Some("https://ptb.discord.com")));
    assert!(!origin_allowed(Some("https://evil.example")));
    assert!(!origin_allowed(Some("")));
  }

  /// The re-exported query parser keeps its legacy key/value behavior.
  #[test]
  fn query_params_match_legacy_semantics() {
    // Canonical parser lives in rsrpc-protocol (tested there); this pins
    // the import still resolves to the same behavior here.
    let params = query_params("/?v=1&client_id=abc");
    assert_eq!(params.get("v").map(String::as_str), Some("1"));
    assert_eq!(params.get("client_id").map(String::as_str), Some("abc"));
  }
}
