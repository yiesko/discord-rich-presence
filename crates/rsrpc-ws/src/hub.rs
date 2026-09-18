//! Event hub: the [`Event`] stream plus the cheap-clone [`Responder`].
//!
//! The central rule this module enforces: **no lock is held across `.await`**.
//! [`Responder`] owns only an `mpsc::Sender` and an `Arc<ConnectionDetails>`,
//! so cloning it per connection — or per task — never duplicates headers or
//! blocks. The server-side client map (owned by the consumer) is only ever
//! borrowed for the duration of a `HashMap` lookup, never across a receive.

use std::{
  net::SocketAddr,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
  time::Duration,
};

use futures_util::Stream;
use tokio::sync::mpsc;
use tungstenite::http::HeaderMap;

use crate::{ClientId, Message, SendError, TrySendError};

/// Why a client disconnected.
///
/// The reason is informational for telemetry; the connection is gone in all
/// cases and its id will not be reused within the process lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DisconnectReason {
  /// Close handshake completed, or the TCP stream ended cleanly.
  Clean,
  /// I/O or protocol error (including oversize message/frame rejections).
  Error,
  /// Silent past `idle_timeout` despite server pings (half-open reaped).
  IdleTimeout,
  /// Pruned by policy: outbound queue stayed full (slow consumer).
  SlowConsumer,
  /// Server is shutting down.
  ServerShutdown,
}

/// Close code for server-initiated closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseCode {
  /// Normal closure (1000).
  Normal,
  /// Overloaded / over connection limit: retry later (1013).
  TryAgainLater,
  /// Unexpected server-side condition (1011).
  InternalError,
}

/// Snapshot of the HTTP upgrade that opened a connection.
///
/// `headers` and `uri` sit behind [`Arc`] so [`Responder::clone`] stays cheap
/// even with large cookie headers (`closure-disjoint-capture` friendly).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConnectionDetails {
  /// Peer socket address; never discarded (unlike the old crate).
  pub peer: SocketAddr,
  /// Request headers from the upgrade.
  pub headers: Arc<HeaderMap>,
  /// Request-target as sent by the client (path + query, e.g. `/?v=1`).
  pub uri: Arc<str>,
}

/// Command sent to a connection task.
#[derive(Debug)]
pub(crate) enum Cmd {
  Message(Message),
  Close(CloseCode),
}

/// Handle to send messages to one connected client.
///
/// `Clone + Send + Sync`: share across tasks freely.
///
/// Dropping a responder does **not** disconnect the client — the connection
/// lives until the peer leaves, [`close`](Self::close) is called, or the
/// server shuts down. (The old crate closed on last-drop, which raced with
/// in-flight messages whenever a consumer discarded `Event::Connect`.) Use
/// [`is_closed`](Self::is_closed) to probe liveness before pruning map entries.
#[derive(Debug, Clone)]
pub struct Responder {
  tx: mpsc::Sender<Cmd>,
  id: ClientId,
  details: Arc<ConnectionDetails>,
}

impl Responder {
  pub(crate) fn new(tx: mpsc::Sender<Cmd>, id: ClientId, details: Arc<ConnectionDetails>) -> Self {
    Self { tx, id, details }
  }

  /// Id of the client this responder talks to.
  #[must_use]
  pub fn client_id(&self) -> ClientId {
    self.id
  }

  /// Upgrade snapshot for this client.
  #[must_use]
  pub fn details(&self) -> &ConnectionDetails {
    &self.details
  }

  /// Enqueue without waiting.
  ///
  /// Returns [`TrySendError::Full`] when the client's bounded outbox is full
  /// instead of growing memory: the caller decides to retry, coalesce-drop,
  /// or prune the client. Never blocks, never allocates.
  pub fn try_send(&self, message: Message) -> Result<(), TrySendError> {
    self
      .tx
      .try_send(Cmd::Message(message))
      .map_err(|err| match err {
        mpsc::error::TrySendError::Full(_) => TrySendError::Full,
        mpsc::error::TrySendError::Closed(_) => TrySendError::Closed,
      })
  }

  /// Enqueue, waiting for outbox capacity.
  ///
  /// Applies backpressure to the sender; prefer [`try_send`](Self::try_send)
  /// in poll loops that must never stall. Fails only when the client is gone.
  ///
  /// # Errors
  ///
  /// Returns [`SendError`] when the connection task has exited.
  pub async fn send_async(&self, message: Message) -> Result<(), SendError> {
    self
      .tx
      .send(Cmd::Message(message))
      .await
      .map_err(|_| SendError)
  }

  /// Enqueue, waiting at most `timeout` for outbox capacity.
  ///
  /// Bounded variant of [`send_async`](Self::send_async): a shared pump
  /// replying to many clients must never park behind one reader that
  /// stopped draining its outbox.
  ///
  /// # Errors
  ///
  /// Returns [`SendError`] when the connection task has exited or the
  /// timeout elapsed first.
  pub async fn send_timeout(&self, message: Message, timeout: Duration) -> Result<(), SendError> {
    match tokio::time::timeout(timeout, self.tx.send(Cmd::Message(message))).await {
      Ok(Ok(())) => Ok(()),
      Ok(Err(_)) | Err(_) => Err(SendError),
    }
  }

  /// Ask the connection task to close with `code`.
  ///
  /// Best-effort and idempotent: a dead client simply makes this a no-op.
  pub async fn close(&self, code: CloseCode) {
    let _ = self.tx.send(Cmd::Close(code)).await;
  }

  /// True when the connection task has exited (sends will fail).
  ///
  /// Useful when pruning a responder map without sending a probe message.
  #[must_use]
  pub fn is_closed(&self) -> bool {
    self.tx.is_closed()
  }
}

/// An incoming event from a client.
#[derive(Debug)]
#[non_exhaustive]
pub enum Event {
  /// A new client connected; store the responder to talk back.
  Connect(
    /// Id of the client that connected.
    ClientId,
    /// Handle used to send messages back to this client.
    Responder,
  ),
  /// A client disconnected; its id will not be reused.
  Disconnect(
    /// Id of the client that disconnected.
    ClientId,
    /// Why the connection ended.
    DisconnectReason,
  ),
  /// An incoming message from a client.
  Message(
    /// Id of the client that sent the message.
    ClientId,
    /// The message.
    Message,
  ),
}

/// Queue of incoming events; the centerpiece of the crate.
///
/// `None` from [`next_event`](Self::next_event) means every connection task
/// has exited (typically after [`Server::shutdown`](crate::Server::shutdown));
/// the hub itself holds no threads and needs no shutdown call.
#[derive(Debug)]
pub struct EventHub {
  rx: mpsc::Receiver<Event>,
}

impl EventHub {
  pub(crate) fn new(rx: mpsc::Receiver<Event>) -> Self {
    Self { rx }
  }

  /// Next event, waiting while the queue is empty.
  ///
  /// Returns `None` once all connection tasks have exited and the queue is
  /// drained — the natural loop-exit condition for graceful shutdown.
  pub async fn next_event(&mut self) -> Option<Event> {
    self.rx.recv().await
  }

  /// Next event without waiting; `None` when the queue is empty.
  ///
  /// Note: also returns `None` after shutdown drain; use [`next_event`](Self::next_event)
  /// in loops where the distinction matters, or check closure via shutdown handles.
  pub fn try_next_event(&mut self) -> Option<Event> {
    self.rx.try_recv().ok()
  }
}

impl Stream for EventHub {
  type Item = Event;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
    self.rx.poll_recv(cx)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn responder_with(channel: usize) -> (Responder, mpsc::Receiver<Cmd>) {
    let (tx, rx) = mpsc::channel(channel);
    let responder = Responder::new(
      tx,
      7,
      Arc::new(ConnectionDetails {
        peer: "127.0.0.1:1".parse().unwrap(),
        headers: Arc::new(HeaderMap::new()),
        uri: Arc::from("/"),
      }),
    );
    (responder, rx)
  }

  #[test]
  fn try_send_full_does_not_block() {
    let (r, _rx) = responder_with(1);
    r.try_send(Message::from("one")).unwrap();
    assert_eq!(r.try_send(Message::from("two")), Err(TrySendError::Full));
  }

  #[test]
  fn try_send_closed_after_task_exit() {
    let (r, rx) = responder_with(1);
    drop(rx);
    assert_eq!(r.try_send(Message::from("x")), Err(TrySendError::Closed));
  }

  #[test]
  fn closed_probe_tracks_task_exit() {
    let (r, rx) = responder_with(1);
    assert!(!r.is_closed());
    drop(rx);
    assert!(r.is_closed());
  }

  #[tokio::test]
  async fn send_async_and_close_are_best_effort() {
    let (r, _rx) = responder_with(8);
    r.send_async(Message::from("hi")).await.unwrap();
    r.close(CloseCode::Normal).await;
  }

  #[tokio::test]
  async fn send_timeout_gives_up_on_a_full_outbox() {
    let (r, mut rx) = responder_with(1);
    r.try_send(Message::from("filler")).unwrap();
    let started = std::time::Instant::now();
    let result = r
      .send_timeout(Message::from("reply"), Duration::from_millis(50))
      .await;
    assert!(result.is_err(), "full outbox must fail the bounded wait");
    assert!(
      started.elapsed() < Duration::from_secs(2),
      "must not wait for capacity"
    );
    // Draining frees the slot: the next bounded send succeeds.
    assert!(rx.try_recv().is_ok());
    r.send_timeout(Message::from("reply"), Duration::from_millis(50))
      .await
      .unwrap();
  }

  #[tokio::test]
  async fn hub_streams_events_in_order() {
    use futures_util::StreamExt;
    let (tx, rx) = mpsc::channel(8);
    let mut hub = EventHub::new(rx);
    tx.send(Event::Disconnect(3, DisconnectReason::Clean))
      .await
      .unwrap();
    drop(tx);
    let first = hub.next().await.unwrap();
    assert!(matches!(first, Event::Disconnect(3, _)));
    assert!(hub.next_event().await.is_none());
  }
}
