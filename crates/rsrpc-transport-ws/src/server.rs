use std::{
  collections::VecDeque,
  net::{Ipv4Addr, SocketAddr},
  sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
  },
};

use rsrpc_protocol::error::{Result, RsrpcError};
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;
use rsrpc_ws::{ClientId, EventHub};
use rustc_hash::FxHashMap;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;

use crate::config::WsTransportConfig;
use crate::handlers;

use super::pump::{PumpCtx, pump_loop};
use super::sink::{ClientSlot, PendingClear, Sink};

/// Discord origins allowed to drive game commands. Absent `origin`
/// (non-browser clients) passes: only a mismatched origin is refused.
pub(crate) const ALLOWED_ORIGINS: [&str; 3] = [
  "https://discord.com",
  "https://canary.discord.com",
  "https://ptb.discord.com",
];

/// Browser origins must be Discord; absent origin (native clients) passes.
pub(crate) fn origin_allowed(origin: Option<&str>) -> bool {
  match origin {
    None => true,
    Some(value) => ALLOWED_ORIGINS.contains(&value),
  }
}

/// Shareable read handle: snapshots and counters without transport ownership.
///
/// `Clone + Send + Sync`: hand to telemetry tasks freely.
#[derive(Debug, Clone)]
pub struct TransportHandle {
  clients: Arc<RwLock<FxHashMap<ClientId, ClientSlot>>>,
  dropped: Arc<AtomicU64>,
}

impl TransportHandle {
  /// Current game-client count (short read lock, never stalls).
  pub async fn client_count(&self) -> usize {
    self.clients.read().await.len()
  }

  /// Commands shed by a full/closed sink since bind.
  #[must_use]
  pub fn dropped_total(&self) -> u64 {
    self.dropped.load(Ordering::Relaxed)
  }
}

/// Discord-facing game WebSocket transport.
///
/// Binds the first free loopback port in range, pumps validated game
/// commands into the sink, and answers lock-step replies. Shut down with
/// [`shutdown`](Self::shutdown); dropping aborts the pump (server tasks
/// wind down with the runtime).
pub struct WsTransport {
  server: Option<rsrpc_ws::Server>,
  pump: Option<JoinHandle<()>>,
  handle: TransportHandle,
  /// Monotonic live-client total for telemetry census (the clients map
  /// stays the dispatch source of truth; this counter is O(1) to read).
  total: Arc<AtomicU64>,
  /// Retained sink sender for census queue-depth sampling.
  sink_tx: mpsc::Sender<ActivityCmd>,
  /// Staged disconnect clears (see `MAX_PENDING_CLEARS`): flushed
  /// opportunistically on every pump event and drained on shutdown.
  pending_clears: Arc<Mutex<VecDeque<PendingClear>>>,
  bound_port: u16,
}

impl std::fmt::Debug for WsTransport {
  /// Bound port only; internal handles stay out of logs.
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("WsTransport")
      .field("bound_port", &self.bound_port)
      .finish_non_exhaustive()
  }
}

impl WsTransport {
  /// Bind the first free loopback port in `config` range and start pumping.
  ///
  /// Returns the transport plus the bounded sink receiver. Must be called
  /// within a Tokio runtime.
  ///
  /// # Errors
  ///
  /// [`RsrpcError::InvalidConfig`] for zero bounds, [`RsrpcError::WsBind`]
  /// (keeping the last `io::Error`) when every port is taken,
  /// [`RsrpcError::WsExhausted`] when the range yields no bindable port.
  pub async fn bind(
    config: WsTransportConfig,
    user: Arc<Mutex<RpcUser>>,
  ) -> Result<(Self, mpsc::Receiver<ActivityCmd>)> {
    if config.event_queue == 0 {
      return Err(RsrpcError::InvalidConfig("event_queue must be non-zero"));
    }
    if config.max_connections == 0 {
      return Err(RsrpcError::InvalidConfig(
        "max_connections must be non-zero",
      ));
    }
    if config.per_client_queue == 0 {
      return Err(RsrpcError::InvalidConfig(
        "per_client_queue must be non-zero",
      ));
    }

    let mut last_err = None;
    let mut bound: Option<(rsrpc_ws::Server, EventHub, u16)> = None;
    for port in config.port_start..=config.port_end {
      let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
      let ws_config = rsrpc_ws::ServerConfig::builder(addr)
        .max_connections(config.max_connections)
        .per_client_queue(config.per_client_queue)
        .keepalive_interval(config.keepalive_interval)
        .build()
        .map_err(|_| RsrpcError::InvalidConfig("rsrpc-ws bounds must be non-zero"))?;
      match rsrpc_ws::Server::bind(ws_config).await {
        Ok((server, hub)) => {
          // Port 0 asks the OS: report what we actually hold.
          let actual = server.local_addr().port();
          tracing::info!("[transport-ws] Game server on port {actual}");
          bound = Some((server, hub, actual));
          break;
        }
        Err(rsrpc_ws::Error::Bind(source)) => {
          if source.kind() == std::io::ErrorKind::AddrInUse {
            tracing::warn!("[transport-ws] Port {port} in use, trying next");
          } else {
            tracing::warn!("[transport-ws] Cannot bind port {port} ({source}), trying next");
          }
          last_err = Some(source);
        }
        // Handshake/config failures cannot happen pre-accept; treat as fatal
        // for this port and continue the scan.
        Err(other) => {
          tracing::warn!("[transport-ws] Failed to start server on port {port}: {other}");
        }
      }
    }

    let (server, hub, bound_port) = match bound {
      Some(bound) => bound,
      None => {
        tracing::error!("[transport-ws] Failed to bind any port");
        return match last_err {
          Some(source) => Err(RsrpcError::WsBind {
            start: config.port_start,
            end: config.port_end,
            source,
          }),
          None => Err(RsrpcError::WsExhausted {
            start: config.port_start,
            end: config.port_end,
          }),
        };
      }
    };

    let (tx, rx) = mpsc::channel(config.event_queue);
    let clients = Arc::new(RwLock::new(FxHashMap::default()));
    let dropped = Arc::new(AtomicU64::new(0));
    let total = Arc::new(AtomicU64::new(0));
    let pending_clears = Arc::new(Mutex::new(VecDeque::new()));
    let clear_retries = Arc::new(AtomicUsize::new(0));
    let pump = tokio::spawn(pump_loop(PumpCtx {
      hub,
      clients: Arc::clone(&clients),
      total: Arc::clone(&total),
      sink: Sink {
        tx: tx.clone(),
        dropped: Arc::clone(&dropped),
      },
      pending_clears: Arc::clone(&pending_clears),
      clear_retries,
      user,
      set_activity: config.set_activity,
      secondary_events: config.secondary_events,
    }));

    let handle = TransportHandle { clients, dropped };
    Ok((
      Self {
        server: Some(server),
        pump: Some(pump),
        handle: handle.clone(),
        total: Arc::clone(&total),
        sink_tx: tx,
        pending_clears,
        bound_port,
      },
      rx,
    ))
  }

  /// Actual bound port (differs from config when port `0` was requested).
  #[must_use]
  pub fn bound_port(&self) -> u16 {
    self.bound_port
  }

  /// Shareable read handle for telemetry.
  #[must_use]
  pub fn handle(&self) -> TransportHandle {
    self.handle.clone()
  }

  /// Live game-client total for the telemetry census (O(1) atomic read).
  #[must_use]
  pub fn client_total(&self) -> Arc<AtomicU64> {
    Arc::clone(&self.total)
  }

  /// Shared sink sender for census queue-depth sampling.
  #[must_use]
  pub fn sink_sender(&self) -> mpsc::Sender<ActivityCmd> {
    self.sink_tx.clone()
  }

  /// Current game-client count (short read lock, never stalls).
  pub async fn client_count(&self) -> usize {
    self.handle.client_count().await
  }

  /// Commands shed by a full/closed sink since bind.
  #[must_use]
  pub fn dropped_total(&self) -> u64 {
    self.handle.dropped_total()
  }

  /// Graceful shutdown: close the listener and connections, drain the pump
  /// (including disconnect clears for every tracked pid), then clear any
  /// residue the pump never saw (tasks aborted without `Disconnect`).
  /// This is the only path that cleans up publications: `Drop` merely
  /// aborts the pump (Rust forbids `await` there), so owners must call
  /// `shutdown()` — the daemon always does. Past-budget sheds stay bounded
  /// downstream by ghost reaping.
  pub async fn shutdown(mut self) {
    if let Some(server) = self.server.take() {
      server.shutdown().await;
    }
    if let Some(pump) = self.pump.take() {
      let _ = pump.await;
    }
    // Belt and braces: connection tasks aborted without `Disconnect` (or
    // prunes the pump never processed) leave tracked pids with no one left
    // to clear them. The pump is gone, so stage them into the shared queue
    // and drain it here. Normally empty: `Disconnect` already cleared
    // everything on the way down.
    let sink = Sink {
      tx: self.sink_tx.clone(),
      dropped: Arc::clone(&self.handle.dropped),
    };
    {
      let mut clients = self.handle.clients.write().await;
      for (_, slot) in clients.drain() {
        self.total.fetch_sub(1, Ordering::Relaxed);
        for published in &slot.published {
          sink.stage_clear(&self.pending_clears, handlers::clear_for_slot(published));
        }
      }
    }
    // Opportunistic flush first (instant when healthy), then bounded retry
    // per entry: each clear carries its own deadline, so this loop is
    // bounded by the youngest entry instead of stalling shutdown on a
    // wedged bridge. Past-budget leftovers shed counted.
    sink.flush_pending(&self.pending_clears);
    loop {
      let next = {
        let mut pending = self
          .pending_clears
          .lock()
          .unwrap_or_else(|e| e.into_inner());
        pending.pop_front()
      };
      let Some(next) = next else { break };
      sink.emit_clear_retry(next.cmd, next.deadline).await;
    }
  }
}

impl Drop for WsTransport {
  /// Abort a still-running pump so drops never park it forever.
  fn drop(&mut self) {
    // Best-effort: an explicit shutdown() drains gracefully; a drop must
    // at least not leave the pump parked forever.
    if let Some(pump) = &self.pump {
      pump.abort();
    }
  }
}
