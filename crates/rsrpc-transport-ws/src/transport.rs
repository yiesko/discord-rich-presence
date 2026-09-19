//! Game WebSocket transport: bind loop, event pump, graceful shutdown.
//!
//! The P1 fix, structurally: the client map behind
//! `Arc<RwLock<HashMap<..>>>` is only ever touched in short clone / insert /
//! remove sections. Every `.await` (socket I/O, sink send, responder send)
//! happens with **no guard alive** — a concurrent `client_count()` snapshot
//! can never stall behind message flow (see
//! `snapshots_under_flood_never_stall`).

use std::{
  net::{Ipv4Addr, SocketAddr},
  sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use rsrpc_protocol::error::{Result, RsrpcError};
use rsrpc_protocol::query::query_params;
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;
use rsrpc_ws::{ClientId, CloseCode, Event, EventHub, Message, Responder};
use rustc_hash::FxHashMap;
use serde_json::Value;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;

use crate::config::WsTransportConfig;
use crate::handlers;

/// How long the pump waits for sink capacity before shedding (counted).
const SINK_SEND_TIMEOUT: Duration = Duration::from_millis(250);

/// Discord origins allowed to drive game commands. Absent `origin`
/// (non-browser clients) passes: only a mismatched origin is refused.
const ALLOWED_ORIGINS: [&str; 3] = [
  "https://discord.com",
  "https://canary.discord.com",
  "https://ptb.discord.com",
];

/// Per-client slot: responder plus the publication history needed for
/// clear-on-disconnect.
///
/// No `Clone` derive (see `SlotSnapshot`): the history must only ever be
/// read under a short guard, never duplicated wholesale.
#[derive(Debug)]
struct ClientSlot {
  responder: Responder,
  /// Every pid published on this connection (slim records), oldest first.
  /// A single-pid client — the norm — keeps exactly one entry.
  published: Vec<handlers::PublishedSlot>,
  /// Evicted clears whose immediate enqueue failed (shed sink). Drained
  /// on disconnect alongside `published`, so a dropped eviction clear
  /// cannot ghost. Bounded like `published`; never holds the lock across
  /// an await.
  pending_clears: Vec<handlers::PublishedSlot>,
  query_client_id: Option<String>,
}

impl ClientSlot {
  /// Record a publication, refreshing a re-published pid as most recent.
  /// Returns the oldest entry when it is evicted past
  /// [`handlers::MAX_TRACKED_PIDS`]: the caller must clear it at once,
  /// or the evicted card ghosts (disconnect cleanup only sees tracked
  /// pids).
  fn note_published(
    &mut self,
    app_id: Option<String>,
    pid: u64,
    nonce: Value,
  ) -> Option<handlers::PublishedSlot> {
    if let Some(pos) = self.published.iter().position(|entry| entry.pid == pid) {
      self.published.remove(pos);
    } else if self.published.len() >= handlers::MAX_TRACKED_PIDS {
      let evicted = self.published.remove(0);
      self
        .published
        .push(handlers::PublishedSlot { app_id, pid, nonce });
      return Some(evicted);
    }
    self
      .published
      .push(handlers::PublishedSlot { app_id, pid, nonce });
    None
  }
}

/// Cheap per-message snapshot: `responder` is an `Arc` bump and the id is
/// a short string. Publication history is never cloned here — it is only
/// read on disconnect and appended on `SET_ACTIVITY`, both under short
/// write guards.
struct SlotSnapshot {
  responder: Responder,
  query_client_id: Option<String>,
}

/// Bounded event sink with a shed counter.
///
/// `send_timeout` waits briefly for genuine bursts, then drops (counted)
/// instead of stalling the pump — one slow bridge must not freeze every
/// game client. A closed sink (dead bridge) also counts and continues.
#[derive(Debug, Clone)]
pub(crate) struct Sink {
  tx: mpsc::Sender<ActivityCmd>,
  dropped: Arc<AtomicU64>,
}

impl Sink {
  /// Queue one command downstream, shedding (counted) instead of stalling
  /// the pump when the bridge stops draining.
  pub(crate) async fn emit(&self, cmd: ActivityCmd) {
    match self.tx.send_timeout(cmd, SINK_SEND_TIMEOUT).await {
      Ok(()) => {}
      Err(_) => {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        tracing::warn!("[transport-ws] Event sink full/closed, dropping command");
      }
    }
  }

  /// Queue one clear downstream, reporting whether it landed.
  ///
  /// Same shed-counted timeout as [`emit`](Self::emit): callers retain the
  /// slot in `pending_clears` before awaiting and confirm it only on
  /// `true`, so a shed eviction clear is retried at disconnect instead of
  /// ghosting.
  pub(crate) async fn emit_clear(&self, cmd: ActivityCmd) -> bool {
    match self.tx.send_timeout(cmd, SINK_SEND_TIMEOUT).await {
      Ok(()) => true,
      Err(_) => {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        tracing::warn!("[transport-ws] Event sink full/closed, dropping clear");
        false
      }
    }
  }
}

/// Register an evicted slot for disconnect cleanup. Bounded like
/// `published`: beyond the cap the oldest pending entry drops (shed,
/// counted downstream via `dropped`). Unbounded retention would let a
/// pathological publisher grow memory forever while the bridge is wedged;
/// every handoff/publish table in this workspace sheds oldest the same way.
fn retain_pending(pending: &mut Vec<handlers::PublishedSlot>, evicted: handlers::PublishedSlot) {
  if pending.len() >= handlers::MAX_TRACKED_PIDS {
    let dropped = pending.remove(0);
    tracing::warn!(
      "[transport-ws] Pending eviction clears full, shedding oldest (pid {})",
      dropped.pid
    );
  }
  pending.push(evicted);
}

/// Drop a pending clear once its enqueue succeeded. Matches the exact
/// `(pid, nonce)` pair so a re-published pid does not confirm a stale entry.
fn confirm_pending(pending: &mut Vec<handlers::PublishedSlot>, pid: u64, nonce: &Value) -> bool {
  if let Some(pos) = pending
    .iter()
    .position(|entry| entry.pid == pid && entry.nonce == *nonce)
  {
    pending.remove(pos);
    true
  } else {
    false
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
  bound_port: u16,
}

impl std::fmt::Debug for WsTransport {
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
    let pump = tokio::spawn(pump_loop(PumpCtx {
      hub,
      clients: Arc::clone(&clients),
      total: Arc::clone(&total),
      sink: Sink {
        tx: tx.clone(),
        dropped: Arc::clone(&dropped),
      },
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

  /// Graceful shutdown: close the listener and connections, drain the pump.
  pub async fn shutdown(mut self) {
    if let Some(server) = self.server.take() {
      server.shutdown().await;
    }
    if let Some(pump) = self.pump.take() {
      let _ = pump.await;
    }
  }
}

impl Drop for WsTransport {
  fn drop(&mut self) {
    // Best-effort: an explicit shutdown() drains gracefully; a drop must
    // at least not leave the pump parked forever.
    if let Some(pump) = &self.pump {
      pump.abort();
    }
  }
}

/// Pump-owned context (moved into the pump task).
struct PumpCtx {
  hub: EventHub,
  clients: Arc<RwLock<FxHashMap<ClientId, ClientSlot>>>,
  total: Arc<AtomicU64>,
  sink: Sink,
  user: Arc<Mutex<RpcUser>>,
  set_activity: bool,
  secondary_events: bool,
}

/// Event pump: the only task touching the map, one short guard per event.
async fn pump_loop(
  PumpCtx {
    mut hub,
    clients,
    total,
    sink,
    user,
    set_activity,
    secondary_events,
  }: PumpCtx,
) {
  while let Some(event) = hub.next_event().await {
    match event {
      Event::Connect(id, responder) => {
        on_connect(id, responder, &clients, &total, &user).await;
      }
      Event::Disconnect(id, _) => {
        remove_and_clear(id, &clients, &total, &sink).await;
      }
      Event::Message(id, message) => {
        on_message(
          id,
          message,
          &clients,
          &total,
          &sink,
          &user,
          set_activity,
          secondary_events,
        )
        .await;
      }
      // `Event` is non-exhaustive: future variants must not break the pump.
      _ => {}
    }
  }
}

/// Validate a new game client (version, encoding, origin), send READY and
/// register its slot. Rejected clients are closed before registration.
async fn on_connect(
  id: ClientId,
  responder: Responder,
  clients: &Arc<RwLock<FxHashMap<ClientId, ClientSlot>>>,
  total: &Arc<AtomicU64>,
  user: &Arc<Mutex<RpcUser>>,
) {
  // Parse + validate before any reply or insert.
  let query = query_params(responder.details().uri.as_ref());
  let version = query.get("v").map(String::as_str).unwrap_or("0");
  let encoding = query.get("encoding").map(String::as_str).unwrap_or("json");
  let query_client_id = query.get("client_id").cloned();
  if version != "1" || encoding != "json" {
    tracing::warn!("[transport-ws] Rejecting client {id} (v={version}, encoding={encoding})");
    responder.close(CloseCode::Normal).await;
    return;
  }
  // Origins are per-connection: refuse mismatches here, before any READY
  // or slot registration, instead of per message downstream.
  if !origin_allowed(
    responder
      .details()
      .headers
      .get("origin")
      .and_then(|v| v.to_str().ok()),
  ) {
    tracing::warn!("[transport-ws] Refused origin for client {id}");
    responder.close(CloseCode::Normal).await;
    return;
  }

  // Snapshot the identity under a short lock; the guard never crosses await.
  let ready = user
    .lock()
    .unwrap_or_else(|e| e.into_inner())
    .ready_payload();
  if responder
    .send_async(Message::Text(ready.into()))
    .await
    .is_err()
  {
    // Vanished mid-handshake: nothing to track.
    return;
  }
  clients.write().await.insert(
    id,
    ClientSlot {
      responder,
      published: Vec::new(),
      pending_clears: Vec::new(),
      query_client_id,
    },
  );
  total.fetch_add(1, Ordering::Relaxed);
}

/// Remove the slot and emit one clear per published pid (shared by
/// Disconnect and prune paths). A connection that published several pids
/// (multiplexing companion) clears every card, not just the latest.
/// Pending eviction clears (whose immediate enqueue was shed) ride along,
/// so a full sink cannot strand a ghost.
async fn remove_and_clear(
  id: ClientId,
  clients: &Arc<RwLock<FxHashMap<ClientId, ClientSlot>>>,
  total: &Arc<AtomicU64>,
  sink: &Sink,
) {
  let slot = clients.write().await.remove(&id);
  let Some(slot) = slot else { return };
  total.fetch_sub(1, Ordering::Relaxed);
  for published in &slot.published {
    sink.emit(handlers::clear_for_slot(published)).await;
  }
  for pending in &slot.pending_clears {
    sink.emit(handlers::clear_for_slot(pending)).await;
  }
}

#[allow(clippy::too_many_arguments)]
/// Dispatch one client message; `false` means the client died and its slot
/// must be pruned (disconnect clears follow downstream).
async fn on_message(
  id: ClientId,
  message: Message,
  clients: &Arc<RwLock<FxHashMap<ClientId, ClientSlot>>>,
  total: &Arc<AtomicU64>,
  sink: &Sink,
  user: &Arc<Mutex<RpcUser>>,
  set_activity: bool,
  secondary_events: bool,
) {
  // Snapshot the cheap fields; the read guard drops before any await
  // below (and the deep `last_cmd` is never cloned here).
  let slot = match clients.read().await.get(&id) {
    Some(slot) => SlotSnapshot {
      responder: slot.responder.clone(),
      query_client_id: slot.query_client_id.clone(),
    },
    None => {
      // Stale message for a removed slot (pruned or rejected client).
      return;
    }
  };
  let Message::Text(text) = message else {
    tracing::warn!("[transport-ws] Ignoring non-text message from client {id}");
    return;
  };
  let event: ActivityCmd = match serde_json::from_str(&text) {
    Ok(event) => event,
    Err(err) => {
      tracing::warn!("[transport-ws] Invalid message from client {id}: {err}");
      return;
    }
  };

  // Every arm reports liveness: `false` means the game client died without
  // a clean `Disconnect` and its slot must be released now.
  let alive = match event.cmd.as_str() {
    "INVITE_BROWSER" | "GUILD_TEMPLATE_BROWSER" | "GIFT_CODE_BROWSER" => {
      if !secondary_events {
        return;
      }
      handlers::handle_browser_command(&event, &slot.responder, sink).await
    }
    "DEEP_LINK" => {
      if !secondary_events {
        return;
      }
      handlers::handle_deep_link(&event, &slot.responder).await
    }
    "CONNECTIONS_CALLBACK" => {
      if !secondary_events {
        return;
      }
      handlers::handle_connections_callback(&event, &slot.responder).await
    }
    "SUBSCRIBE" | "UNSUBSCRIBE" => handlers::handle_subscribe(&event, &slot.responder).await,
    "GET_USER" => {
      let snapshot = user.lock().unwrap_or_else(|e| e.into_inner()).clone();
      handlers::handle_get_user(&event, &snapshot, &slot.responder).await
    }
    "SET_ACTIVITY" => {
      if !set_activity {
        return;
      }
      let alive = handlers::handle_set_activity(
        &event,
        slot.query_client_id.as_deref(),
        &slot.responder,
        sink,
      )
      .await;
      // Record the publication regardless of the reply outcome: the
      // command already reached the sink, and a stalled or gone client
      // is pruned right below — its removal must clear the pid or the
      // card ghosts. Applies the same connect-query client_id fallback
      // the forwarded command carries. Short write-back, no await inside.
      let app_id = event
        .application_id
        .clone()
        .or_else(|| slot.query_client_id.clone());
      let pid = event.args.as_ref().and_then(|a| a.pid).unwrap_or_default();
      // Short write-back, no await inside: record the publication and
      // stage a shed eviction clear before any await, so the client lock
      // never crosses the sink wait.
      let evicted = {
        let mut guard = clients.write().await;
        let evicted = guard
          .get_mut(&id)
          .and_then(|entry| entry.note_published(app_id, pid, event.nonce.clone()));
        if let Some(ref old) = evicted
          && let Some(entry) = guard.get_mut(&id)
        {
          retain_pending(&mut entry.pending_clears, old.clone());
        }
        evicted
      };
      if let Some(old) = evicted {
        // Bounded history evicted a live card: clear it at once, or it
        // ghosts (disconnect cleanup only ever sees tracked pids). On
        // shed, the staged pending entry survives for disconnect cleanup.
        if sink.emit_clear(handlers::clear_for_slot(&old)).await {
          let mut guard = clients.write().await;
          if let Some(entry) = guard.get_mut(&id) {
            confirm_pending(&mut entry.pending_clears, old.pid, &old.nonce);
          }
        }
      }
      alive
    }
    other => handlers::handle_unknown(other, &event, &slot.responder).await,
  };
  if !alive {
    tracing::info!("[transport-ws] Client {id} send failed, pruning");
    remove_and_clear(id, clients, total, sink).await;
  }
}

/// Browser origins must be Discord; absent origin (native clients) passes.
fn origin_allowed(origin: Option<&str>) -> bool {
  match origin {
    None => true,
    Some(value) => ALLOWED_ORIGINS.contains(&value),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn emit_clear_reports_enqueue_result() {
    // Closed sink: enqueue fails, counted, returns false.
    let (tx, rx) = mpsc::channel(1);
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
    assert!(!sink.emit_clear(cmd).await);
    assert_eq!(sink.dropped.load(Ordering::Relaxed), 1);
  }

  #[tokio::test]
  async fn emit_clear_succeeds_when_capacity_free() {
    let (tx, mut rx) = mpsc::channel(1);
    let sink = Sink {
      tx,
      dropped: Arc::new(AtomicU64::new(0)),
    };
    let cmd = handlers::clear_for_slot(&handlers::PublishedSlot {
      app_id: None,
      pid: 9,
      nonce: Value::Null,
    });
    assert!(sink.emit_clear(cmd).await);
    assert!(rx.try_recv().is_ok());
    assert_eq!(sink.dropped.load(Ordering::Relaxed), 0);
  }

  #[test]
  fn evicted_clear_is_retained_until_confirmed() {
    let mut pending: Vec<handlers::PublishedSlot> = Vec::new();
    let evicted = handlers::PublishedSlot {
      app_id: None,
      pid: 1,
      nonce: Value::Null,
    };
    retain_pending(&mut pending, evicted.clone());
    assert_eq!(pending.len(), 1);
    assert!(confirm_pending(&mut pending, evicted.pid, &evicted.nonce));
    assert!(pending.is_empty());
  }

  #[test]
  fn pending_clears_stay_bounded() {
    let mut pending: Vec<handlers::PublishedSlot> = Vec::new();
    for pid in 0..(handlers::MAX_TRACKED_PIDS + 5) as u64 {
      retain_pending(
        &mut pending,
        handlers::PublishedSlot {
          app_id: None,
          pid,
          nonce: Value::Null,
        },
      );
    }
    assert!(pending.len() <= handlers::MAX_TRACKED_PIDS);
  }

  #[test]
  fn origin_policy() {
    assert!(origin_allowed(None));
    assert!(origin_allowed(Some("https://discord.com")));
    assert!(origin_allowed(Some("https://canary.discord.com")));
    assert!(origin_allowed(Some("https://ptb.discord.com")));
    assert!(!origin_allowed(Some("https://evil.example")));
    assert!(!origin_allowed(Some("")));
  }

  #[test]
  fn query_params_match_legacy_semantics() {
    // Canonical parser lives in rsrpc-protocol (tested there); this pins
    // the import still resolves to the same behavior here.
    let params = query_params("/?v=1&client_id=abc");
    assert_eq!(params.get("v").map(String::as_str), Some("1"));
    assert_eq!(params.get("client_id").map(String::as_str), Some("abc"));
  }
}
