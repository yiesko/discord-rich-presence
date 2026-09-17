//! Async server: capped accept loop, one task per connection, clean shutdown.
//!
//! Design notes:
//! - `Server::bind` runs on the **caller's** Tokio runtime (no hidden
//!   `Runtime::new`, no listener thread): one runtime per process.
//! - The accept loop never spawns unbounded tasks: an [`tokio::sync::Semaphore`]
//!   caps connections; over-limit peers get a polite 1013 close.
//! - Each connection task is a single `select!` over socket input, outbox,
//!   keepalive ticks and the shutdown token — never two racing futures like
//!   the old `try_join!(responder_events, events)` pair.
//! - `tungstenite` answers `Ping` with `Pong` internally; the task only tracks
//!   activity for the idle timeout.

use std::{
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{
  net::{TcpListener, TcpStream},
  sync::{Mutex, Semaphore, mpsc},
  task::JoinSet,
};
use tokio_tungstenite::accept_hdr_async_with_config;
use tokio_util::sync::CancellationToken;
use tungstenite::{
  Message as WsMessage,
  protocol::{
    WebSocketConfig,
    frame::{CloseFrame, coding::CloseCode as WsCloseCode},
  },
};

use crate::{
  ClientId, Error, Message, ServerConfig,
  hub::{CloseCode, Cmd, ConnectionDetails, DisconnectReason, Event, EventHub, Responder},
};

/// Writes for handshake + rejection paths.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound for graceful connection drain in [`Server::shutdown`].
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Async WebSocket server handle.
///
/// Obtained from [`bind`](Self::bind); owns the accept task and every
/// connection task. [`shutdown`](Self::shutdown) terminates all of them.
pub struct Server {
  token: CancellationToken,
  accept_task: tokio::task::JoinHandle<()>,
  conns: Arc<Mutex<JoinSet<()>>>,
  local_addr: SocketAddr,
}

impl std::fmt::Debug for Server {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Server")
      .field("local_addr", &self.local_addr)
      .finish_non_exhaustive()
  }
}

impl Server {
  /// Bind `config.bind` and start accepting.
  ///
  /// Must be called within a Tokio runtime; the server lives on it instead
  /// of spawning a hidden runtime + thread (old design).
  ///
  /// # Errors
  ///
  /// [`Error::Bind`] when the address cannot be bound (source keeps
  /// `AddrInUse` etc.), [`Error::Runtime`] when the bound address cannot
  /// be read back.
  pub async fn bind(config: ServerConfig) -> Result<(Self, EventHub), Error> {
    let listener = TcpListener::bind(config.bind).await.map_err(Error::Bind)?;
    let local_addr = listener.local_addr().map_err(Error::Runtime)?;

    let (event_tx, event_rx) = mpsc::channel(config.event_queue);
    let token = CancellationToken::new();
    let semaphore = Arc::new(Semaphore::new(config.max_connections));
    let ids = Arc::new(AtomicU64::new(0));
    let conns = Arc::new(Mutex::new(JoinSet::new()));

    let ws_config = WebSocketConfig::default()
      .max_message_size(config.max_message_size)
      .max_frame_size(config.max_frame_size);

    let accept_ctx = AcceptCtx {
      listener,
      token: token.clone(),
      semaphore,
      ids,
      event_tx,
      conns: Arc::clone(&conns),
      ws_config,
      per_client_queue: config.per_client_queue,
      keepalive_interval: config.keepalive_interval,
      idle_timeout: config.idle_timeout,
    };
    let accept_task = tokio::spawn(async move { accept_ctx.run().await });

    Ok((
      Self {
        token,
        accept_task,
        conns,
        local_addr,
      },
      EventHub::new(event_rx),
    ))
  }

  /// Bound local address (useful with port `0`).
  #[must_use]
  pub fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }

  /// Graceful shutdown: stop accepting, close connections, wait for tasks.
  ///
  /// Bounded by a fixed drain timeout; stragglers are aborted. After
  /// this returns, the [`EventHub`] yields its remaining events then `None`.
  pub async fn shutdown(self) {
    self.token.cancel();
    let _ = tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, self.accept_task).await;
    // Take the set out under a short lock; join without holding it
    // (never hold a lock across `.await` on task completion).
    let mut owned = {
      let mut guard = self.conns.lock().await;
      std::mem::take(&mut *guard)
    };
    let deadline = Instant::now() + SHUTDOWN_DRAIN_TIMEOUT;
    while !owned.is_empty() {
      let remaining = deadline.saturating_duration_since(Instant::now());
      if remaining.is_zero() {
        break;
      }
      // One task at a time; a hang hits the deadline instead of us.
      match tokio::time::timeout(remaining, owned.join_next()).await {
        Ok(_) => {}
        Err(_) => break,
      }
    }
    owned.abort_all();
  }
}

/// Shared accept-loop state (moved into the accept task).
struct AcceptCtx {
  listener: TcpListener,
  token: CancellationToken,
  semaphore: Arc<Semaphore>,
  ids: Arc<AtomicU64>,
  event_tx: mpsc::Sender<Event>,
  conns: Arc<Mutex<JoinSet<()>>>,
  ws_config: WebSocketConfig,
  per_client_queue: usize,
  keepalive_interval: Duration,
  idle_timeout: Duration,
}

impl AcceptCtx {
  async fn run(self) {
    loop {
      tokio::select! {
        biased;
        () = self.token.cancelled() => break,
        accepted = self.listener.accept() => {
          let (stream, peer) = match accepted {
            Ok(pair) => pair,
            Err(_) => continue,
          };
          let _ = stream.set_nodelay(true);
          match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
              let id = self.ids.fetch_add(1, Ordering::Relaxed);
              let task = ConnTask {
                stream,
                peer,
                id,
                event_tx: self.event_tx.clone(),
                token: self.token.clone(),
                ws_config: self.ws_config,
                per_client_queue: self.per_client_queue,
                keepalive_interval: self.keepalive_interval,
                idle_timeout: self.idle_timeout,
                _permit: permit,
              };
              // Short critical section: reap + spawn are synchronous, no
              // await inside. Drained every accept: finished tasks would
              // otherwise pin their entries for the process lifetime.
              let mut conns = self.conns.lock().await;
              while conns.try_join_next().is_some() {}
              conns.spawn(async move { task.run().await });
            }
            Err(_) => {
              tokio::spawn(reject_overloaded(stream, self.ws_config));
            }
          }
        }
      }
    }
  }
}

/// Over-limit peer: complete the handshake, then close with 1013 (Again).
async fn reject_overloaded(stream: TcpStream, ws_config: WebSocketConfig) {
  let handshake = tokio::time::timeout(
    HANDSHAKE_TIMEOUT,
    tokio_tungstenite::accept_async_with_config(stream, Some(ws_config)),
  )
  .await;
  let mut ws = match handshake {
    Ok(Ok(ws)) => ws,
    _ => return,
  };
  send_close(&mut ws, CloseCode::TryAgainLater).await;
}

/// One connection: handshake, then a single `select!` pump.
struct ConnTask {
  stream: TcpStream,
  peer: SocketAddr,
  id: ClientId,
  event_tx: mpsc::Sender<Event>,
  token: CancellationToken,
  ws_config: WebSocketConfig,
  per_client_queue: usize,
  keepalive_interval: Duration,
  idle_timeout: Duration,
  /// Held for the connection lifetime; released back on task exit.
  _permit: tokio::sync::OwnedSemaphorePermit,
}

impl ConnTask {
  // The handshake closure must return tungstenite's `ErrorResponse`
  // (136 bytes); the large-Err type is mandated by the API, not a choice
  // (same precedent as `lib/src/server/ipc_utils.rs`).
  #[allow(clippy::result_large_err)]
  async fn run(self) {
    let Self {
      stream,
      peer,
      id,
      event_tx,
      token,
      ws_config,
      per_client_queue,
      keepalive_interval,
      idle_timeout,
      _permit,
    } = self;

    let mut uri: Option<String> = None;
    let mut headers = None;
    let handshake = tokio::time::timeout(
      HANDSHAKE_TIMEOUT,
      accept_hdr_async_with_config(
        stream,
        |request: &tungstenite::http::Request<()>, response| {
          uri = Some(request.uri().to_string());
          headers = Some(request.headers().clone());
          Ok(response)
        },
        Some(ws_config),
      ),
    )
    .await;
    let ws_stream = match handshake {
      Ok(Ok(ws)) => ws,
      // Invalid handshake / timeout / non-WS client: silent, no event.
      _ => return,
    };

    let details = Arc::new(ConnectionDetails {
      peer,
      headers: Arc::new(headers.unwrap_or_default()),
      uri: uri.unwrap_or_default().into(),
    });
    let (resp_tx, mut resp_rx) = mpsc::channel(per_client_queue);
    let responder = Responder::new(resp_tx, id, details);
    if event_tx.send(Event::Connect(id, responder)).await.is_err() {
      return;
    }

    let (mut outgoing, mut incoming) = ws_stream.split();
    let mut last_seen = Instant::now();
    let mut keepalive = tokio::time::interval(keepalive_interval);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Consume the immediate first tick; pacing starts from here.
    keepalive.tick().await;
    // Idle expiry runs on its own deadline, never on the keepalive tick:
    // a silent peer with idle_timeout far below keepalive_interval must
    // still be reaped promptly. Reset on every sign of life below.
    let mut idle = Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
      last_seen + idle_timeout,
    )));
    // A dropped responder only retires the outbox; the connection keeps
    // serving incoming messages until the peer leaves or shutdown arrives.
    let mut outbox_open = true;

    let reason = loop {
      tokio::select! {
        biased;
        () = token.cancelled() => {
          send_close(&mut outgoing, CloseCode::Normal).await;
          break DisconnectReason::ServerShutdown;
        }
        cmd = resp_rx.recv(), if outbox_open => {
          match cmd {
            Some(Cmd::Message(message)) => {
              if outgoing.send(message.into_ws()).await.is_err() {
                break DisconnectReason::Error;
              }
            }
            Some(Cmd::Close(code)) => {
              send_close(&mut outgoing, code).await;
              break DisconnectReason::Clean;
            }
            None => {
              outbox_open = false;
            }
          }
        }
        next = incoming.next() => {
          match next {
            Some(Ok(WsMessage::Text(text))) => {
              last_seen = Instant::now();
              idle.as_mut().reset(tokio::time::Instant::from_std(last_seen + idle_timeout));
              // Moved, not copied: tungstenite already hands us refcounted text.
              let event = Event::Message(id, Message::Text(text));
              if event_tx.send(event).await.is_err() {
                break DisconnectReason::ServerShutdown;
              }
            }
            Some(Ok(WsMessage::Binary(bytes))) => {
              last_seen = Instant::now();
              idle.as_mut().reset(tokio::time::Instant::from_std(last_seen + idle_timeout));
              let event = Event::Message(id, Message::Binary(bytes));
              if event_tx.send(event).await.is_err() {
                break DisconnectReason::ServerShutdown;
              }
            }
            // Ping is auto-Ponged inside tungstenite; Pong needs nothing.
            Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_))) => {
              last_seen = Instant::now();
              idle.as_mut().reset(tokio::time::Instant::from_std(last_seen + idle_timeout));
            }
            // tungstenite never yields Frame from the read path.
            Some(Ok(WsMessage::Frame(_))) => {}
            Some(Ok(WsMessage::Close(_))) => break DisconnectReason::Clean,
            Some(Err(_)) => break DisconnectReason::Error,
            // TCP ended without a close frame (kill -9, crash, FIN).
            None => break DisconnectReason::Clean,
          }
        }
        _ = keepalive.tick() => {
          if outgoing.send(WsMessage::Ping(Bytes::new())).await.is_err() {
            break DisconnectReason::Error;
          }
        }
        () = &mut idle => {
          send_close(&mut outgoing, CloseCode::InternalError).await;
          break DisconnectReason::IdleTimeout;
        }
      }
    };

    // Hub gone (shutdown drained): best-effort, never blocks exit.
    let _ = event_tx.send(Event::Disconnect(id, reason)).await;
  }
}

impl Message {
  fn into_ws(self) -> WsMessage {
    match self {
      Self::Text(text) => WsMessage::Text(text),
      Self::Binary(bytes) => WsMessage::Binary(bytes),
    }
  }
}

/// Send a close frame plus transport close; every error is terminal anyway.
async fn send_close<S>(outgoing: &mut S, code: CloseCode)
where
  S: SinkExt<WsMessage> + Unpin,
  S::Error: std::fmt::Debug,
{
  let (ws_code, reason) = match code {
    CloseCode::Normal => (WsCloseCode::Normal, ""),
    CloseCode::TryAgainLater => (WsCloseCode::Again, "server overloaded, retry later"),
    CloseCode::InternalError => (WsCloseCode::Error, "internal error"),
  };
  let _ = outgoing
    .send(WsMessage::Close(Some(CloseFrame {
      code: ws_code,
      reason: reason.into(),
    })))
    .await;
  let _ = outgoing.close().await;
}
