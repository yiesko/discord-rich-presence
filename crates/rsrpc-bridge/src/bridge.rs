//! arRPC-compatible activity bridge: fan-out, replay cache, handoff.
//!
//! Owns the JSON (1337) and MessagePack (1338) consumer servers plus every
//! pump: two bridge-user loops, two command loops (IPC + game transports),
//! one process loop, one refresh loop and one persist loop — all Tokio
//! tasks in a [`JoinSet`] under one [`CancellationToken`], replacing the
//! legacy seven detached `std::thread`s with no shutdown path.
//!
//! Two deliberate improvements over the legacy connector:
//! - Snapshots persist dirty-gated on a cadence (default 5s), never on
//!   every publish: a flooding client used to force a
//!   `write+sync_all+rename` per frame.
//! - Refresh rebroadcasts share the cached `Arc` instead of cloning whole
//!   payloads into a scratch `Vec`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
  Arc, Mutex,
  atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use rsrpc_protocol::commands::{self, CachedActivity, RecentActivities};
use rsrpc_protocol::error::{Result, RsrpcError};
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;
use rsrpc_types::{AppId, SocketId};
use rsrpc_ws::{ClientId, CloseCode, Event, EventHub, Message, Responder};
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::BridgeConfig;
use crate::consumer::{BridgeProtocol, send_cached, send_message};
use crate::control::handle_bridge_control;
use crate::handoff::{HandoffState, ProcInput, is_process_alive, track_process_publication};
use crate::origin::origin_allowed;
use crate::replay::{ReplayCache, cache_entry_pid};
use crate::router::{
  channel_depth, generic_payload, table_transition, take_matching_process, take_process_clear,
};

/// Upper bound for graceful drain in [`Bridge::shutdown`].
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Input channels, one per producer leg. Bounds live with the producers:
/// IPC 64 (backpressure per connection), game transports 1024 (shed
/// counted), process scanner 512. The bridge never blocks a producer:
/// full channels shed at the producer (counted), closes end the pumps.
pub struct BridgeInputs {
  /// Validated game commands from the IPC transport.
  pub ipc_rx: mpsc::Receiver<ActivityCmd>,
  /// Validated game commands from the game WebSocket transport.
  pub game_rx: mpsc::Receiver<ActivityCmd>,
  /// Scanner reports (generic presence).
  pub proc_rx: mpsc::Receiver<ProcInput>,
  /// Sender clones for census queue-depth sampling
  /// (`max_capacity - capacity` = queued). `None` when the leg is off.
  pub ipc_tx: Option<mpsc::Sender<ActivityCmd>>,
  /// Sender clone for the game-transport leg census depth.
  pub game_tx: Option<mpsc::Sender<ActivityCmd>>,
  /// Sender clone for the scanner leg census depth.
  pub proc_tx: Option<mpsc::Sender<ProcInput>>,
  /// Live game-client total, maintained by the game transport.
  /// `None` when the game transport is off.
  pub game_clients: Option<Arc<AtomicU64>>,
}

/// Workhorse state shared by every pump (all guards short, never `.await`
/// while held — verified by the flood integration test). Fields are
/// crate-visible so the consumer/router/snapshot units in sibling modules
/// can implement on it; all mutation still funnels through short guards.
pub(crate) struct Shared {
  pub(crate) json_clients: Mutex<FxHashMap<ClientId, Responder>>,
  pub(crate) msgpack_clients: Mutex<FxHashMap<ClientId, Responder>>,
  /// Resolved encoding per consumer (the `format=` override wins over the
  /// port default): every later lookup keys off this, never the default.
  pub(crate) consumer_protocol: Mutex<FxHashMap<ClientId, BridgeProtocol>>,
  pub(crate) cache: Mutex<ReplayCache>,
  pub(crate) activity_seq: Mutex<u64>,
  pub(crate) last_process: Mutex<HashMap<AppId, u64>>,
  pub(crate) handoff: Mutex<HandoffState>,
  pub(crate) recent: Mutex<RecentActivities>,
  pub(crate) user: Arc<Mutex<RpcUser>>,
  pub(crate) dirty: AtomicBool,
  pub(crate) dropped_broadcasts: AtomicU64,
  pub(crate) app_version: String,
  pub(crate) state_path: Option<PathBuf>,
  pub(crate) json_port: u16,
  pub(crate) msgpack_port: u16,
  pub(crate) ws_port: Option<u16>,
  pub(crate) ipc_path: Option<String>,
  /// Sender clones for census queue-depth sampling (see [`BridgeInputs`]).
  pub(crate) ipc_tx: Option<mpsc::Sender<ActivityCmd>>,
  pub(crate) game_tx: Option<mpsc::Sender<ActivityCmd>>,
  pub(crate) proc_tx: Option<mpsc::Sender<ProcInput>>,
  /// Live game-client total from the game transport.
  pub(crate) game_clients: Option<Arc<AtomicU64>>,
  /// Extra browser origins allowed to drive commands (see `origin`).
  pub(crate) allowed_origins: Vec<String>,
}

/// Hourly resource census cadence: distinguishes a growing queue backlog
/// (producer outrunning consumer) from allocator retention (flat queues
/// but climbing RSS) in long sessions.
const STATS_INTERVAL: Duration = Duration::from_secs(3600);

/// arRPC-compatible activity bridge.
pub struct Bridge {
  token: CancellationToken,
  tasks: Arc<tokio::sync::Mutex<JoinSet<()>>>,
  json_server: Option<rsrpc_ws::Server>,
  msgpack_server: Option<rsrpc_ws::Server>,
  shared: Arc<Shared>,
  json_port: u16,
  msgpack_port: u16,
  state_path: Option<PathBuf>,
}

impl std::fmt::Debug for Bridge {
  /// Ports only; shared state stays out of logs.
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Bridge")
      .field("json_port", &self.json_port)
      .field("msgpack_port", &self.msgpack_port)
      .finish_non_exhaustive()
  }
}

impl Bridge {
  /// Bind both bridge protocols and start every pump.
  ///
  /// Must be called within a Tokio runtime. Each range binds its first
  /// free loopback port; the MessagePack scan skips the claimed JSON port.
  ///
  /// # Errors
  ///
  /// [`RsrpcError::InvalidConfig`] for zero cadences,
  /// [`RsrpcError::BridgeBind`] when a range is exhausted.
  pub async fn bind(
    config: BridgeConfig,
    user: Arc<Mutex<RpcUser>>,
    inputs: BridgeInputs,
  ) -> Result<Self> {
    if config.persist_interval.is_zero() {
      return Err(RsrpcError::InvalidConfig(
        "persist_interval must be non-zero",
      ));
    }
    if config.refresh_interval.is_zero() {
      return Err(RsrpcError::InvalidConfig(
        "refresh_interval must be non-zero",
      ));
    }

    let (json_server, json_hub, json_port) =
      bind_range(config.json_port_start, config.json_port_end, None, "json").await?;
    let skip = (config.msgpack_port_start == json_port).then_some(json_port);
    let (msgpack_server, msgpack_hub, msgpack_port) = bind_range(
      config.msgpack_port_start,
      config.msgpack_port_end,
      skip,
      "msgpack",
    )
    .await?;

    let state_path = config.state_dir.as_deref().and_then(|dir| {
      let path = crate::state::select_slot(dir, crate::state::now_secs());
      match &path {
        Some(path) => tracing::info!("[bridge] State snapshot: {}", path.display()),
        None => tracing::warn!("[bridge] State dir set but no free state slot"),
      }
      path
    });

    let token = CancellationToken::new();
    let tasks = Arc::new(tokio::sync::Mutex::new(JoinSet::new()));
    let shared = Arc::new(Shared {
      json_clients: Mutex::new(FxHashMap::default()),
      msgpack_clients: Mutex::new(FxHashMap::default()),
      consumer_protocol: Mutex::new(FxHashMap::default()),
      cache: Mutex::new(HashMap::new()),
      activity_seq: Mutex::new(0),
      last_process: Mutex::new(HashMap::new()),
      handoff: Mutex::new(HandoffState::default()),
      recent: Mutex::new(RecentActivities::default()),
      user,
      dirty: AtomicBool::new(true),
      dropped_broadcasts: AtomicU64::new(0),
      app_version: config.app_version.clone(),
      state_path: state_path.clone(),
      json_port,
      msgpack_port,
      ws_port: config.ws_port,
      ipc_path: config.ipc_path.clone(),
      ipc_tx: inputs.ipc_tx,
      game_tx: inputs.game_tx,
      proc_tx: inputs.proc_tx,
      game_clients: inputs.game_clients,
      allowed_origins: config.allowed_origins.clone(),
    });

    {
      let mut tasks = tasks.lock().await;
      tasks.spawn(bridge_pump(
        json_hub,
        Arc::clone(&shared),
        BridgeProtocol::Json,
      ));
      tasks.spawn(bridge_pump(
        msgpack_hub,
        Arc::clone(&shared),
        BridgeProtocol::MsgPack,
      ));
      tasks.spawn(command_pump(
        inputs.ipc_rx,
        Arc::clone(&shared),
        token.clone(),
      ));
      tasks.spawn(command_pump(
        inputs.game_rx,
        Arc::clone(&shared),
        token.clone(),
      ));
      tasks.spawn(proc_pump(
        inputs.proc_rx,
        Arc::clone(&shared),
        token.clone(),
      ));
      tasks.spawn(refresh_task(
        Arc::clone(&shared),
        config.refresh_interval,
        token.clone(),
      ));
      tasks.spawn(persist_task(
        Arc::clone(&shared),
        config.persist_interval,
        token.clone(),
      ));
      tasks.spawn(stats_task(Arc::clone(&shared), token.clone()));
    }

    // Snapshot the (empty) presence + bound servers immediately, so
    // external tooling sees us before the first game appears.
    shared.persist_now().await;

    Ok(Self {
      token,
      tasks,
      json_server: Some(json_server),
      msgpack_server: Some(msgpack_server),
      shared,
      json_port,
      msgpack_port,
      state_path,
    })
  }

  /// Bound JSON port (differs from config when port `0` was requested).
  #[must_use]
  pub fn json_port(&self) -> u16 {
    self.json_port
  }

  /// Bound MessagePack port.
  #[must_use]
  pub fn msgpack_port(&self) -> u16 {
    self.msgpack_port
  }

  /// Selected state-snapshot path, if snapshots are enabled.
  #[must_use]
  pub fn state_path(&self) -> Option<&std::path::Path> {
    self.state_path.as_deref()
  }

  /// Broadcasts shed by full consumer outboxes since bind.
  #[must_use]
  pub fn dropped_total(&self) -> u64 {
    self.shared.dropped_broadcasts.load(Ordering::Relaxed)
  }

  /// Graceful shutdown: stop accepting, drain every pump with a deadline,
  /// then release the state slot (mirrors the legacy ownership: fresh
  /// mtime blocks reuse, so a later daemon reuses it immediately).
  pub async fn shutdown(mut self) {
    self.token.cancel();
    if let Some(server) = self.json_server.take() {
      server.shutdown().await;
    }
    if let Some(server) = self.msgpack_server.take() {
      server.shutdown().await;
    }
    let mut owned = {
      let mut guard = self.tasks.lock().await;
      std::mem::take(&mut *guard)
    };
    let deadline = std::time::Instant::now() + SHUTDOWN_DRAIN_TIMEOUT;
    while !owned.is_empty() {
      let remaining = deadline.saturating_duration_since(std::time::Instant::now());
      if remaining.is_zero() {
        break;
      }
      match tokio::time::timeout(remaining, owned.join_next()).await {
        Ok(_) => {}
        Err(_) => break,
      }
    }
    owned.abort_all();
    if let Some(path) = self.state_path.as_ref() {
      let path = path.clone();
      let _ = tokio::task::spawn_blocking(move || std::fs::remove_file(path)).await;
    }
  }
}

impl Drop for Bridge {
  /// Cancel every pump so drops never outlive the bridge.
  fn drop(&mut self) {
    // Best-effort: shutdown() drains gracefully; a drop at least stops
    // every pump (hubs close once the servers below drop).
    self.token.cancel();
  }
}

/// Bind the first free loopback port in `start..=end`, skipping `skip`.
async fn bind_range(
  start: u16,
  end: u16,
  skip: Option<u16>,
  name: &'static str,
) -> Result<(rsrpc_ws::Server, EventHub, u16)> {
  let end = end.max(start);
  for port in start..=end {
    if Some(port) == skip {
      continue;
    }
    let ws_config =
      rsrpc_ws::ServerConfig::builder(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
        .build()
        .map_err(|_| RsrpcError::InvalidConfig("bridge server bounds must be non-zero"))?;
    match rsrpc_ws::Server::bind(ws_config).await {
      Ok((server, hub)) => {
        let actual = server.local_addr().port();
        tracing::info!("[bridge] {name} bridge on port {actual}");
        return Ok((server, hub, actual));
      }
      Err(rsrpc_ws::Error::Bind(source)) => {
        tracing::warn!("[bridge] Failed to bind {name} on port {port}: {source}, trying next");
      }
      Err(err) => {
        tracing::warn!("[bridge] Failed to launch {name} on port {port}: {err}, trying next");
      }
    }
  }
  Err(RsrpcError::BridgeBind { name, start, end })
}

/// Consumer pump for one bridge server: READY + replay on connect, control
/// or echo on message, removal on disconnect.
async fn bridge_pump(hub: EventHub, shared: Arc<Shared>, default_protocol: BridgeProtocol) {
  let mut hub = hub;
  while let Some(event) = hub.next_event().await {
    match event {
      Event::Connect(id, responder) => {
        // Origins are per-connection: refuse mismatches here, before any
        // READY or slot registration, instead of per message downstream
        // (same policy as the game transport).
        if !origin_allowed(
          responder
            .details()
            .headers
            .get("origin")
            .and_then(|v| v.to_str().ok()),
          &shared.allowed_origins,
        ) {
          tracing::warn!("[bridge] Refused origin for consumer {id}");
          responder.close(CloseCode::Normal).await;
          continue;
        }
        tracing::info!("[bridge] Consumer {id} connected");
        let protocol =
          BridgeProtocol::from_query(responder.details().uri.as_ref(), default_protocol);
        // READY snapshot under a short lock; no await while held.
        let ready = shared
          .user
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .ready_payload();
        send_message(&responder, &ready, protocol);
        // Replay for late joiners.
        let cached: Vec<Arc<CachedActivity>> = shared
          .cache
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .values()
          .map(|(payload, _)| Arc::clone(payload))
          .collect();
        for payload in &cached {
          send_cached(&responder, payload, protocol);
        }
        shared
          .consumer_protocol
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .insert(id, protocol);
        shared
          .clients_for(protocol)
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .insert(id, responder);
      }
      Event::Disconnect(id, _) => {
        tracing::info!("[bridge] Consumer {id} disconnected");
        let known = shared
          .consumer_protocol
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .remove(&id);
        match known {
          Some(protocol) => {
            shared
              .clients_for(protocol)
              .lock()
              .unwrap_or_else(|e| e.into_inner())
              .remove(&id);
          }
          // Untracked (cannot happen): sweep both maps so no slot leaks.
          None => {
            for protocol in [BridgeProtocol::Json, BridgeProtocol::MsgPack] {
              shared
                .clients_for(protocol)
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            }
          }
        }
      }
      Event::Message(id, message) => {
        // Unregistered senders drive nothing: refused origins never
        // register, and a pipelined frame can outrun its own close.
        if !shared.is_registered(id) {
          continue;
        }
        // Bridge control messages (JSON text) are answered, everything
        // else echoes to the sender as before. Identity changes fan out
        // as CURRENT_USER_UPDATE to every consumer on THIS server (JSON
        // and MessagePack loops are independent; control traffic
        // practically only arrives on the JSON port).
        match message {
          Message::Text(text) => match handle_bridge_control(&shared.user, text.as_str()) {
            Some((ack, changed)) => {
              let protocol = shared.protocol_for(id, default_protocol);
              if let Some(responder) = shared
                .clients_for(protocol)
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&id)
              {
                send_message(responder, &ack, protocol);
              }
              if let Some(user) = changed {
                let dispatch = commands::current_user_update(&user);
                for protocol in [BridgeProtocol::Json, BridgeProtocol::MsgPack] {
                  let clients = shared.clients_for(protocol);
                  let mut clients = clients.lock().unwrap_or_else(|e| e.into_inner());
                  let dead: Vec<ClientId> = clients
                    .iter()
                    .filter_map(|(id, responder)| {
                      (!send_message(responder, &dispatch, protocol)).then_some(*id)
                    })
                    .collect();
                  for id in dead {
                    tracing::warn!("[bridge] Pruning dead consumer {id}");
                    clients.remove(&id);
                    // Paired table: prune both or dead ids pin protocol
                    // entries forever (same as the broadcast prunes).
                    shared
                      .consumer_protocol
                      .lock()
                      .unwrap_or_else(|e| e.into_inner())
                      .remove(&id);
                  }
                }
              }
            }
            None => {
              let protocol = shared.protocol_for(id, default_protocol);
              if let Some(responder) = shared
                .clients_for(protocol)
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&id)
              {
                let _ = responder.try_send(Message::Text(text));
              }
            }
          },
          other => {
            let protocol = shared.protocol_for(id, default_protocol);
            if let Some(responder) = shared
              .clients_for(protocol)
              .lock()
              .unwrap_or_else(|e| e.into_inner())
              .get(&id)
            {
              let _ = responder.try_send(other);
            }
          }
        }
      }
      // `Event` is non-exhaustive: future variants must not break the pump.
      _ => {}
    }
  }
}

/// Command pump shared by the IPC and game legs. Ends on token cancel
/// (shutdown) or when the producer drops its sender.
async fn command_pump(
  mut rx: mpsc::Receiver<ActivityCmd>,
  shared: Arc<Shared>,
  token: CancellationToken,
) {
  loop {
    tokio::select! {
      biased;
      () = token.cancelled() => break,
      cmd = rx.recv() => {
        let Some(cmd) = cmd else { break };
        if cmd.cmd != "SET_ACTIVITY" {
          // Non-activity events (INVITE_BROWSER, ...) fan out as-is.
          shared.broadcast_raw(&cmd);
          continue;
        }
        shared.handle_set_activity(cmd).await;
      }
    }
  }
}

/// Process pump: generic presence from the scanner with IPC-wins handoff.
/// Ends on token cancel (shutdown) or when the scanner drops its sender.
impl Shared {
  /// Assemble the resource census from live state: client counts, input
  /// queue depths and self-RSS. Short guards only, never `.await`.
  fn census_snapshot(&self) -> rsrpc_telemetry::StatsSnapshot {
    let json_clients = self.json_clients.lock().unwrap_or_else(|e| e.into_inner());
    let msgpack_clients = self
      .msgpack_clients
      .lock()
      .unwrap_or_else(|e| e.into_inner());
    rsrpc_telemetry::StatsSnapshot {
      rss_bytes: rsrpc_telemetry::rss_bytes(),
      bridge_json: json_clients.len(),
      bridge_msgpack: msgpack_clients.len(),
      ws: self
        .game_clients
        .as_ref()
        .map(|total| usize::try_from(total.load(Ordering::Relaxed)).unwrap_or(0))
        .unwrap_or(0),
      // `watch` is the scanner→bridge leg here (proc-events feed the
      // scanner internally); `proc`/`ws` are the bridge input queues.
      watch_depth: channel_depth(self.proc_tx.as_ref()),
      proc_depth: channel_depth(self.ipc_tx.as_ref()),
      ws_depth: channel_depth(self.game_tx.as_ref()),
    }
  }

  /// Render the census line for `reason` (`hourly`, `game-start`, ...).
  fn census_line(&self, reason: &str) -> String {
    rsrpc_telemetry::format_resource_stats(reason, &self.census_snapshot())
  }
}

impl Bridge {
  /// Render the current resource census line: bridge/ws consumer counts,
  /// input queue depths and self-RSS (see the telemetry crate).
  #[must_use]
  pub fn census(&self, reason: &str) -> String {
    self.shared.census_line(reason)
  }
}

/// Hourly resource census plus the session-boundary lines emitted by the
/// process pump. Read-only diagnostics: never changes runtime behavior.
/// Ends on token cancel (shutdown).
async fn stats_task(shared: Arc<Shared>, token: CancellationToken) {
  let mut tick = tokio::time::interval(STATS_INTERVAL);
  tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
  // Skip the immediate first tick: boot already logs its inventory.
  tick.tick().await;
  loop {
    tokio::select! {
      biased;
      () = token.cancelled() => break,
      _ = tick.tick() => {
        tracing::info!("{}", shared.census_line("hourly"));
      }
    }
  }
}

/// Translate scanner reports into bridge events until shutdown.
async fn proc_pump(
  mut rx: mpsc::Receiver<ProcInput>,
  shared: Arc<Shared>,
  token: CancellationToken,
) {
  // Process-table occupancy for session-boundary census lines.
  let mut had_games = false;
  loop {
    tokio::select! {
      biased;
      () = token.cancelled() => break,
      input = rx.recv() => {
        let Some(input) = input else { break };
        if let Some(reason) = table_transition(had_games, &input) {
          tracing::info!("{}", shared.census_line(reason));
        }
        // Per-slot churn stays quiet: only empty<->non-empty edges log.
        // `Removed` preserves occupancy (other games may remain).
        had_games = match &input {
          ProcInput::Detected(_) => true,
          ProcInput::Cleared => false,
          ProcInput::Removed(..) => had_games,
        };
        match input {
      ProcInput::Cleared => {
        shared.handoff.lock().unwrap_or_else(|e| e.into_inner()).note_scan(None);
        // Clear every outstanding process publication (multi-game scans
        // publish per slot; one clear means the table is empty). Consumed
        // once: repeated clears go quiet.
        let outstanding = take_process_clear(&shared);
        for (pid, app_id) in outstanding {
          tracing::info!("[bridge] Sending empty payload");
          let socket_id = SocketId::from(app_id);
          let Some(payload) = commands::empty_cached(pid, socket_id.clone()) else {
            tracing::warn!("[bridge] Dropping unencodable clear payload");
            shared.dropped_broadcasts.fetch_add(1, Ordering::Relaxed);
            continue;
          };
          shared.broadcast_activity(payload, socket_id);
        }
        // Reap replay-cache ghosts: cards whose pid is provably dead but
        // which never got a clear (abrupt companion death + game exit).
        // Without this the refresh loop re-asserts them every 30s forever
        // and late joiners replay a dead presence.
        let ghosts: Vec<(SocketId, u64)> = {
          let cache = shared.cache.lock().unwrap_or_else(|e| e.into_inner());
          cache
            .iter()
            .filter_map(|(id, (payload, _))| {
              cache_entry_pid(id, payload)
                .filter(|pid| !is_process_alive(*pid))
                .map(|pid| (id.clone(), pid))
            })
            .collect()
        };
        for (socket_id, pid) in ghosts {
          tracing::info!("[bridge] Reaping ghost card for dead pid {pid}");
          let Some(payload) = commands::empty_cached(pid, socket_id.clone()) else {
            tracing::warn!("[bridge] Dropping unencodable clear payload");
            shared.dropped_broadcasts.fetch_add(1, Ordering::Relaxed);
            continue;
          };
          shared.broadcast_activity(payload, socket_id);
        }
      }
      ProcInput::Detected(game) => {
        // Remember the scan for the handoff: a clear hands the slot back
        // to exactly this game (the scanner won't re-emit it).
        shared.handoff.lock().unwrap_or_else(|e| e.into_inner()).note_scan(Some(game.clone()));

        // IPC-wins: a live SDK presence owns this slot — withdraw our
        // generic card if shown and stay out until that source clears.
        // Strictly per-slot: co-running games keep theirs.
        if shared.handoff.lock().unwrap_or_else(|e| e.into_inner()).is_suppressed(game.id.as_ref()) {
          let withdrawn = shared
            .last_process
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&game.id);
          if let Some(pid) = withdrawn {
            if let Some(payload) = commands::empty_cached(pid, SocketId::from(&game.id)) {
              shared.broadcast_activity(payload, SocketId::from(&game.id));
              tracing::debug!("[bridge] Yielding {} to live IPC presence", game.name);
            } else {
              tracing::warn!("[bridge] Dropping unencodable clear payload");
              shared.dropped_broadcasts.fetch_add(1, Ordering::Relaxed);
            }
          } else {
            tracing::debug!("[bridge] Deferring to live IPC presence for: {}", game.name);
          }
          continue;
        }

        // Already showing this slot: repeats dedup here instead of
        // flapping the display.
        if shared.last_process.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&game.id) {
          tracing::debug!("[bridge] Already sent payload for activity: {}", game.name);
          continue;
        }

        track_process_publication(
          &mut shared.last_process.lock().unwrap_or_else(|e| e.into_inner()),
          game.id.clone(),
          game.pid,
        );
        tracing::debug!("[bridge] Publishing generic presence for activity: {}", game.name);
        if let Some(payload) = generic_payload(&game) {
          shared.broadcast_activity(payload, SocketId::from(&game.id));
        } else {
          tracing::warn!("[bridge] Dropping unencodable generic payload");
          shared.dropped_broadcasts.fetch_add(1, Ordering::Relaxed);
        }
      }
      ProcInput::Removed(app_id, pid) => {
        // One `(app, pid)` pair vanished while others remain: clear exactly
        // this card. The pid gates both removals: a stale event (pid reuse,
        // EXEC-vs-poll race) must never clear a newer detection that already
        // re-armed the slot under a fresh pid.
        // Suppressed slots (live IPC owner) show no generic card, so there
        // is nothing to broadcast — but scanner memory must still drop a
        // matching slot, or a later IPC clear would resurrect stale state.
        shared
          .handoff
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .note_remove(app_id.as_ref(), pid);
        let outstanding = take_matching_process(
          &mut shared
            .last_process
            .lock()
            .unwrap_or_else(|e| e.into_inner()),
          &app_id,
          pid,
        );
        if let Some(pid) = outstanding {
          tracing::info!("[bridge] Clearing removed game slot");
          if let Some(payload) = commands::empty_cached(pid, SocketId::from(&app_id)) {
            shared.broadcast_activity(payload, SocketId::from(&app_id));
          } else {
            tracing::warn!("[bridge] Dropping unencodable clear payload");
            shared.dropped_broadcasts.fetch_add(1, Ordering::Relaxed);
          }
        } else {
          tracing::debug!("[bridge] Removed slot had no matching generic card");
        }
      }
        }
      }
    }
  }
}

/// Periodic rebroadcast: consumers that missed a frame converge on the
/// cached presence. Shares the cached `Arc` — no payload clones.
/// Ends on token cancel (shutdown).
async fn refresh_task(shared: Arc<Shared>, interval: Duration, token: CancellationToken) {
  let mut tick = tokio::time::interval(interval);
  tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
  loop {
    tokio::select! {
      biased;
      () = token.cancelled() => break,
      _ = tick.tick() => {
        refresh_once(&shared);
      }
    }
  }
}

/// Re-broadcast cached activities so late joiners converge; always marks
/// the snapshot dirty, even when there is nothing to send.
fn refresh_once(shared: &Arc<Shared>) {
  let payloads: Vec<Arc<CachedActivity>> = shared
    .cache
    .lock()
    .unwrap_or_else(|e| e.into_inner())
    .values()
    .map(|(payload, _)| Arc::clone(payload))
    .collect();
  if payloads.is_empty() {
    // No presence to rebroadcast, but still refresh the snapshot mtime
    // so a live-but-idle daemon never looks stale to slot reuse.
    shared.dirty.store(true, Ordering::Relaxed);
    return;
  }
  tracing::debug!("[bridge] Refreshing {} cached activities", payloads.len());
  for payload in &payloads {
    shared.send_to_all(payload);
  }
  shared.dirty.store(true, Ordering::Relaxed);
}

/// Dirty-gated snapshot writer: at most one write per interval no matter
/// the publish rate (the legacy wrote on *every* publish).
/// Ends on token cancel (shutdown).
async fn persist_task(shared: Arc<Shared>, interval: Duration, token: CancellationToken) {
  let mut tick = tokio::time::interval(interval);
  tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
  // Skip the immediate first tick: bind() already persisted once.
  tick.tick().await;
  loop {
    tokio::select! {
      biased;
      () = token.cancelled() => break,
      _ = tick.tick() => {
        if shared.dirty.swap(false, Ordering::SeqCst) {
          shared.persist_now().await;
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::handoff::ScannedGame;

  /// Registration means presence in the protocol table: refused origins
  /// never insert, so their ids stay unknown even if a frame arrives.
  /// (Guards the `is_registered` gate in the message pump.)
  #[test]
  fn registration_means_protocol_table_presence() {
    let shared = Shared {
      json_clients: Mutex::new(FxHashMap::default()),
      msgpack_clients: Mutex::new(FxHashMap::default()),
      consumer_protocol: Mutex::new(FxHashMap::default()),
      cache: Mutex::new(HashMap::new()),
      activity_seq: Mutex::new(0),
      last_process: Mutex::new(HashMap::new()),
      handoff: Mutex::new(HandoffState::default()),
      recent: Mutex::new(RecentActivities::default()),
      user: Arc::new(Mutex::new(RpcUser::default())),
      dirty: AtomicBool::new(false),
      dropped_broadcasts: AtomicU64::new(0),
      app_version: String::new(),
      state_path: None,
      json_port: 0,
      msgpack_port: 0,
      ws_port: None,
      ipc_path: None,
      ipc_tx: None,
      game_tx: None,
      proc_tx: None,
      game_clients: None,
      allowed_origins: Vec::new(),
    };
    assert!(!shared.is_registered(7));
    shared
      .consumer_protocol
      .lock()
      .unwrap()
      .insert(7, BridgeProtocol::Json);
    assert!(shared.is_registered(7));
    assert!(!shared.is_registered(8));
  }

  /// Publishing past the replay bound keeps the cache within it: steady
  /// memory no matter how many distinct publishers arrive (same
  /// regression net as `prune_keeps_cache_within_bound`, through the
  /// real broadcast path).
  #[test]
  fn broadcast_keeps_replay_cache_bounded() {
    let shared = Shared {
      json_clients: Mutex::new(FxHashMap::default()),
      msgpack_clients: Mutex::new(FxHashMap::default()),
      consumer_protocol: Mutex::new(FxHashMap::default()),
      cache: Mutex::new(HashMap::new()),
      activity_seq: Mutex::new(0),
      last_process: Mutex::new(HashMap::new()),
      handoff: Mutex::new(HandoffState::default()),
      recent: Mutex::new(RecentActivities::default()),
      user: Arc::new(Mutex::new(RpcUser::default())),
      dirty: AtomicBool::new(false),
      dropped_broadcasts: AtomicU64::new(0),
      app_version: String::new(),
      state_path: None,
      json_port: 0,
      msgpack_port: 0,
      ws_port: None,
      ipc_path: None,
      ipc_tx: None,
      game_tx: None,
      proc_tx: None,
      game_clients: None,
      allowed_origins: Vec::new(),
    };
    for i in 0..(crate::config::MAX_CACHED_ACTIVITIES * 2) {
      let mut cmd: ActivityCmd = serde_json::from_value(serde_json::json!({
        "cmd": "SET_ACTIVITY",
        "application_id": format!("app-{i}"),
        "args": {"pid": i, "activity": {"name": "G", "type": 0}},
        "nonce": "n",
      }))
      .expect("test command builds");
      let payload =
        rsrpc_protocol::commands::cached_activity(&mut cmd, None).expect("fixed shapes build");
      shared.broadcast_activity(payload, SocketId::from(i.to_string()));
    }
    let len = shared.cache.lock().unwrap_or_else(|e| e.into_inner()).len();
    assert!(
      len <= crate::config::MAX_CACHED_ACTIVITIES,
      "cache must stay bounded, got {len}"
    );
  }

  /// Session lines fire only on empty<->non-empty edges; slot churn stays quiet.
  #[test]
  fn table_transition_fires_only_on_state_edges() {
    let game = ScannedGame {
      id: AppId::from("1"),
      name: "G".to_string(),
      pid: 7,
      start: 0,
    };
    // Empty -> game: session start; repeats stay quiet.
    assert_eq!(
      table_transition(false, &ProcInput::Detected(game.clone())),
      Some("game-start")
    );
    assert_eq!(table_transition(true, &ProcInput::Detected(game)), None);
    // Stale removals (pid mismatch) release nothing; matching ones do.
    let mut table = HashMap::new();
    table.insert(AppId::from("1"), 7u64);
    assert_eq!(
      take_matching_process(&mut table, &AppId::from("1"), 8),
      None
    );
    assert!(table.contains_key(&AppId::from("1")));
    assert_eq!(
      take_matching_process(&mut table, &AppId::from("1"), 7),
      Some(7)
    );
    assert!(!table.contains_key(&AppId::from("1")));
    assert_eq!(
      take_matching_process(&mut table, &AppId::from("9"), 7),
      None
    );
    // Game -> empty: session end; repeats stay quiet.
    assert_eq!(
      table_transition(true, &ProcInput::Cleared),
      Some("game-end")
    );
    assert_eq!(table_transition(false, &ProcInput::Cleared), None);
    // Per-slot churn stays quiet in both occupancy states.
    assert_eq!(
      table_transition(true, &ProcInput::Removed(AppId::from("1"), 7)),
      None
    );
    assert_eq!(
      table_transition(false, &ProcInput::Removed(AppId::from("1"), 7)),
      None
    );
  }
}
