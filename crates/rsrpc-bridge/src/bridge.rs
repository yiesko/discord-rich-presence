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
use rsrpc_protocol::query::query_params;
use rsrpc_state::{StateActivity, StateServer, StateServers, StateSnapshot};
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;
use rsrpc_types::{AppId, SocketId};
use rsrpc_ws::{ClientId, Event, EventHub, Message, Responder};
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::BridgeConfig;
use crate::handoff::{
  HandoffState, ProcInput, ScannedGame, is_process_alive, track_process_publication,
};

/// Replay cache: socket id → (shared payload, sequence).
pub(crate) type ReplayCache = HashMap<SocketId, (Arc<CachedActivity>, u64)>;

/// Upper bound for graceful drain in [`Bridge::shutdown`].
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Which wire encoding a bridge consumer speaks.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum BridgeProtocol {
  /// JSON text frames (port 1337, the arRPC-compatible bridge).
  Json,
  /// MessagePack binary frames (port 1338).
  MsgPack,
}

impl BridgeProtocol {
  /// The protocol requested by the consumer's `format=` query parameter,
  /// falling back to the per-port default when absent.
  fn from_query(uri: &str, default: BridgeProtocol) -> BridgeProtocol {
    match query_params(uri).get("format").map(String::as_str) {
      Some("msgpack") | Some("messagepack") => BridgeProtocol::MsgPack,
      Some("json") => BridgeProtocol::Json,
      _ => default,
    }
  }
}

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
/// while held — verified by the flood integration test).
struct Shared {
  json_clients: Mutex<FxHashMap<ClientId, Responder>>,
  msgpack_clients: Mutex<FxHashMap<ClientId, Responder>>,
  /// Resolved encoding per consumer (the `format=` override wins over the
  /// port default): every later lookup keys off this, never the default.
  consumer_protocol: Mutex<FxHashMap<ClientId, BridgeProtocol>>,
  cache: Mutex<ReplayCache>,
  activity_seq: Mutex<u64>,
  last_process: Mutex<HashMap<AppId, u64>>,
  handoff: Mutex<HandoffState>,
  recent: Mutex<RecentActivities>,
  user: Arc<Mutex<RpcUser>>,
  dirty: AtomicBool,
  dropped_broadcasts: AtomicU64,
  app_version: String,
  state_path: Option<PathBuf>,
  json_port: u16,
  msgpack_port: u16,
  ws_port: Option<u16>,
  ipc_path: Option<String>,
  /// Sender clones for census queue-depth sampling (see [`BridgeInputs`]).
  ipc_tx: Option<mpsc::Sender<ActivityCmd>>,
  game_tx: Option<mpsc::Sender<ActivityCmd>>,
  proc_tx: Option<mpsc::Sender<ProcInput>>,
  /// Live game-client total from the game transport.
  game_clients: Option<Arc<AtomicU64>>,
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
      let path = rsrpc_state::select_slot(dir, rsrpc_state::now_secs());
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
/// Session-boundary reason for a process-table event: the table went
/// from empty to non-empty (`game-start`) or back to empty (`game-end`).
/// Repeats within a state stay quiet.
fn table_transition(had_games: bool, input: &ProcInput) -> Option<&'static str> {
  match input {
    ProcInput::Detected(_) if !had_games => Some("game-start"),
    ProcInput::Cleared if had_games => Some("game-end"),
    _ => None,
  }
}

/// Queued depth of a census sender (`max - free`).
fn channel_depth<T>(tx: Option<&mpsc::Sender<T>>) -> usize {
  tx.map(|tx| tx.max_capacity().saturating_sub(tx.capacity()))
    .unwrap_or(0)
}

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
        had_games = matches!(input, ProcInput::Detected(_));
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
          let payload = commands::empty_cached(pid, socket_id.clone());
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
          shared.broadcast_activity(
            commands::empty_cached(pid, socket_id.clone()),
            socket_id,
          );
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
            shared.broadcast_activity(
              commands::empty_cached(pid, SocketId::from(&game.id)),
              SocketId::from(&game.id),
            );
            tracing::debug!("[bridge] Yielding {} to live IPC presence", game.name);
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
        shared.broadcast_activity(generic_payload(&game), SocketId::from(&game.id));
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

impl Shared {
  fn clients_for(&self, protocol: BridgeProtocol) -> &Mutex<FxHashMap<ClientId, Responder>> {
    match protocol {
      BridgeProtocol::Json => &self.json_clients,
      BridgeProtocol::MsgPack => &self.msgpack_clients,
    }
  }

  /// Encoding a consumer speaks: the resolved `format=` override recorded
  /// at connect, falling back to the port default for unknown ids (which
  /// cannot happen: every insert is paired with a table entry).
  fn protocol_for(&self, id: ClientId, default: BridgeProtocol) -> BridgeProtocol {
    self
      .consumer_protocol
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .get(&id)
      .copied()
      .unwrap_or(default)
  }

  /// Flood guard: byte-identical republishes inside the window are
  /// dropped — before any broadcast, cache write or log line.
  /// Takes the precomputed fingerprint so dropped publishes never pay
  /// for the envelope build.
  fn flood_dropped(&self, cmd: &ActivityCmd, fingerprint: Option<&[u8]>) -> bool {
    let args = cmd.args.as_ref();
    let pid = args.and_then(|args| args.pid).unwrap_or_default();
    self
      .recent
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .should_drop(
        cmd.application_id.as_deref().unwrap_or(""),
        pid,
        fingerprint,
        std::time::Instant::now(),
      )
  }

  /// IPC-wins handoff: a live SDK presence takes over this app slot
  /// from generic detection (last publisher wins across companions).
  fn note_sdk_publish(&self, cmd: &ActivityCmd) {
    let args = cmd.args.as_ref();
    let pid = args.and_then(|args| args.pid).unwrap_or_default();
    if let Some(app) = args
      .and_then(|args| args.activity.as_ref())
      .and_then(|activity| activity.application_id.clone())
    {
      self
        .handoff
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .note_publish(&app, pid);
    }
  }

  /// Compare the current fingerprint against the cached entry, so first
  /// publishes, real changes and effective clears still log once.
  /// Byte compare: both sides come from the same serializer and the
  /// stored bytes travel with the build — no re-serialize, no re-parse.
  fn activity_changed(&self, pid: u64, fingerprint: Option<&[u8]>) -> bool {
    // Clone the two small shape facts we need (flag + bytes) and let the
    // guard drop at the semicolon: nothing below holds the map lock.
    let cached = self
      .cache
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .get(&SocketId::from(pid.to_string()))
      .map(|(entry, _)| (entry.is_clear, entry.activity_json.clone()));
    match (cached, fingerprint) {
      (None, None) => false,
      (None, Some(_)) => true,
      (Some((is_clear, _)), None) => !is_clear,
      (Some((is_clear, stored)), Some(fp)) => is_clear || stored.as_ref() != fp,
    }
  }

  /// A genuine clear (null activity) from a real connection means the
  /// SDK source went away: hand the slot back to generic detection so
  /// the scanner re-asserts the still-running game. pid == 0 means no
  /// game was ever identified — ignore those, or every fresh
  /// connection would flap the display.
  fn resume_after_clear(&self, cmd: &ActivityCmd, changed: bool) {
    if !is_genuine_clear(cmd) {
      return;
    }
    let pid = cmd
      .args
      .as_ref()
      .and_then(|args| args.pid)
      .unwrap_or_default();
    let resume: Vec<ScannedGame> = {
      let mut handoff = self.handoff.lock().unwrap_or_else(|e| e.into_inner());
      match cmd.application_id.clone() {
        Some(app) if handoff.note_clear(&app, pid) => {
          handoff.resume_for(app.as_ref()).into_iter().collect()
        }
        Some(_) => Vec::new(),
        // No app id: abrupt close (socket died without CLEAR).
        // Release every slot this pid owned, or their generics stay
        // suppressed by a dead owner forever.
        None => handoff
          .note_clear_pid(pid)
          .into_iter()
          .filter_map(|app| handoff.resume_for(app.as_ref()))
          .collect(),
      }
    };
    for game in resume.into_iter().filter(|game| is_process_alive(game.pid)) {
      self.resume_generic(&game);
    }
    if changed {
      tracing::info!("[bridge] Source cleared, resuming process detection");
    } else {
      tracing::debug!("[bridge] Duplicate clear ignored (pid {pid})");
    }
  }

  /// Handle one `SET_ACTIVITY` command: fingerprint, flood-guard,
  /// envelope build, handoff, change detection, genuine-clear resume,
  /// conditional fan-out.
  async fn handle_set_activity(&self, mut cmd: ActivityCmd) {
    // Fingerprint first (runs `fix()`): flood-dropped publishes return
    // before the envelope (JSON + MessagePack) is ever built.
    let fingerprint = commands::activity_fingerprint(&mut cmd);
    let pid = cmd
      .args
      .as_ref()
      .and_then(|args| args.pid)
      .unwrap_or_default();
    if self.flood_dropped(&cmd, fingerprint.as_deref()) {
      let app_key = cmd.application_id.as_deref().unwrap_or("");
      tracing::debug!("[bridge] Dropping duplicate SET_ACTIVITY (app {app_key}, pid {pid})");
      return;
    }
    let Some(payload) = commands::cached_activity(&mut cmd, fingerprint.clone()) else {
      tracing::warn!("[bridge] Invalid activity command, skipping");
      return;
    };
    let app_key = cmd.application_id.as_deref().unwrap_or("");
    let activity = cmd.args.as_ref().and_then(|args| args.activity.as_ref());
    self.note_sdk_publish(&cmd);
    // NOTE: no ignore-list filtering here by design. Forwarded client
    // frames are indistinguishable on this path — filtering would kill
    // the companion this bridge exists to carry.
    let changed = self.activity_changed(pid, fingerprint.as_deref());
    match activity {
      Some(activity) => {
        if changed {
          tracing::info!(
            "[bridge] Published: {} (app {}, pid {})",
            activity.display_name(),
            activity.application_id.as_deref().unwrap_or("?"),
            pid
          );
        } else {
          tracing::debug!(
            "[bridge] Published: {} (app {}, pid {})",
            activity.display_name(),
            activity.application_id.as_deref().unwrap_or("?"),
            pid
          );
        }
      }
      None => {
        if changed {
          tracing::info!("[bridge] Published clear (pid {pid})");
        } else {
          tracing::debug!("[bridge] Published clear (pid {pid})");
        }
      }
    }
    // A genuine clear (null activity) from a real connection means the
    // SDK source went away: hand the slot back to generic detection.
    self.resume_after_clear(&cmd, changed);
    // Identical republishes change nothing observable: the replay cache
    // already holds these exact bytes (late joiners replay them) and the
    // refresh re-asserts them — so skip the fan-out. First publishes,
    // real changes and effective clears always pass.
    if changed {
      self.broadcast_activity(payload, SocketId::from(pid.to_string()));
    } else {
      tracing::debug!(
        "[bridge] Already published identical activity (app {app_key}, pid {pid}), skipping fan-out"
      );
    }
  }

  /// Re-assert generic process presence after its source cleared. The
  /// scanner only emits on *changes*, so without this the slot would stay
  /// dark until the next game switch.
  fn resume_generic(&self, game: &ScannedGame) {
    track_process_publication(
      &mut self.last_process.lock().unwrap_or_else(|e| e.into_inner()),
      game.id.clone(),
      game.pid,
    );
    tracing::debug!(
      "[bridge] Resuming generic presence for {} ({})",
      game.name,
      game.id
    );
    self.broadcast_activity(generic_payload(game), SocketId::from(&game.id));
  }

  /// Broadcast an activity payload, updating the replay cache (clears
  /// evict) and marking the snapshot dirty — never persisting inline.
  fn broadcast_activity(&self, payload: Arc<CachedActivity>, socket_id: SocketId) {
    // Keep the replay cache in sync, pruning cleared activities. The flag
    // travels with the build — no re-parse of our own serialization.
    let is_clear = payload.is_clear;
    {
      let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
      if is_clear {
        cache.remove(&socket_id);
      } else {
        let mut seq = self.activity_seq.lock().unwrap_or_else(|e| e.into_inner());
        *seq = seq.saturating_add(1);
        cache.insert(socket_id, (Arc::clone(&payload), *seq));
        prune_cache(&mut cache);
      }
    }
    self.send_to_all(&payload);
    self.dirty.store(true, Ordering::Relaxed);
  }

  /// Send one payload to every connected consumer, pruning dead ones so a
  /// stuck client cannot pin memory (its queued frames) forever.
  fn send_to_all(&self, payload: &CachedActivity) {
    let mut json_clients = self.json_clients.lock().unwrap_or_else(|e| e.into_inner());
    let mut msgpack_clients = self
      .msgpack_clients
      .lock()
      .unwrap_or_else(|e| e.into_inner());
    if json_clients.is_empty() && msgpack_clients.is_empty() {
      tracing::debug!("[bridge] No consumers connected, skipping");
      return;
    }
    for (clients, payload) in [
      (&mut json_clients, Message::Text(payload.json.clone())),
      (
        &mut msgpack_clients,
        Message::Binary(payload.msgpack.clone()),
      ),
    ] {
      // try_send never blocks: a full outbox reports Full here and the
      // slot is released now, not pinned until a lagging Disconnect.
      let dead: Vec<ClientId> = clients
        .iter()
        .filter_map(|(id, responder)| responder.try_send(payload.clone()).err().map(|_| *id))
        .collect();
      self
        .dropped_broadcasts
        .fetch_add(dead.len() as u64, Ordering::Relaxed);
      for id in dead {
        tracing::warn!("[bridge] Pruning dead consumer {id}");
        clients.remove(&id);
      }
    }
  }

  /// Broadcast a non-activity event as-is, serializing once per encoding.
  /// A frame that cannot encode is dropped loudly (not silently).
  fn broadcast_raw(&self, cmd: &ActivityCmd) {
    let mut json_clients = self.json_clients.lock().unwrap_or_else(|e| e.into_inner());
    let mut msgpack_clients = self
      .msgpack_clients
      .lock()
      .unwrap_or_else(|e| e.into_inner());
    if json_clients.is_empty() && msgpack_clients.is_empty() {
      tracing::debug!("[bridge] No consumers connected, skipping");
      return;
    }
    let json_payload = if json_clients.is_empty() {
      None
    } else {
      match serde_json::to_string(cmd) {
        Ok(payload) => Some(payload),
        Err(err) => {
          tracing::debug!("[bridge] Dropping unserializable fan-out frame: {err}");
          None
        }
      }
    };
    let msgpack_payload = if msgpack_clients.is_empty() {
      None
    } else {
      match rmp_serde::to_vec_named(cmd) {
        Ok(payload) => Some(payload),
        Err(err) => {
          tracing::debug!("[bridge] Dropping unserializable fan-out frame: {err}");
          None
        }
      }
    };
    if let Some(payload) = json_payload {
      let dead: Vec<ClientId> = json_clients
        .iter()
        .filter_map(|(id, responder)| {
          responder
            .try_send(Message::Text(payload.clone().into()))
            .err()
            .map(|_| *id)
        })
        .collect();
      for id in dead {
        tracing::warn!("[bridge] Pruning dead consumer {id}");
        json_clients.remove(&id);
      }
    }
    if let Some(payload) = msgpack_payload {
      let dead: Vec<ClientId> = msgpack_clients
        .iter()
        .filter_map(|(id, responder)| {
          responder
            .try_send(Message::Binary(bytes::Bytes::from(payload.clone())))
            .err()
            .map(|_| *id)
        })
        .collect();
      for id in dead {
        tracing::warn!("[bridge] Pruning dead consumer {id}");
        msgpack_clients.remove(&id);
      }
    }
  }

  /// Write the snapshot when dirty (spawn_blocking: sync temp+rename+fsync
  /// must not sit on an async worker). Best-effort: failures stay in
  /// diagnostics so a full tmpfs never breaks presence.
  async fn persist_now(&self) {
    let Some(path) = self.state_path.clone() else {
      return;
    };
    let servers = StateServers {
      bridge: Some(StateServer {
        host: "127.0.0.1".to_string(),
        port: self.json_port,
      }),
      msgpack: Some(StateServer {
        host: "127.0.0.1".to_string(),
        port: self.msgpack_port,
      }),
      websocket: self.ws_port.map(|port| StateServer {
        host: "127.0.0.1".to_string(),
        port,
      }),
      ipc: self.ipc_path.clone(),
    };
    let activities = state_activities(&self.cache.lock().unwrap_or_else(|e| e.into_inner()));
    let snapshot = StateSnapshot::new(&self.app_version, servers, activities);
    let result =
      tokio::task::spawn_blocking(move || rsrpc_state::write_snapshot(&path, &snapshot)).await;
    match result {
      Ok(Ok(())) => {}
      Ok(Err(err)) => tracing::debug!("[bridge] State snapshot failed: {err}"),
      Err(err) => tracing::debug!("[bridge] Snapshot task failed: {err}"),
    }
  }
}

/// Whether a command is a genuine clear from a real connection (null
/// activity + nonzero pid). Messages that never identified a game (pid 0)
/// are not clears.
fn is_genuine_clear(cmd: &ActivityCmd) -> bool {
  match cmd.args.as_ref().and_then(|args| args.pid) {
    Some(pid) if pid != 0 => cmd
      .args
      .as_ref()
      .is_some_and(|args| args.activity.is_none()),
    _ => false,
  }
}

/// Resolve the owning pid of a replay-cache entry for ghost reaping:
/// the pid rides in the JSON body, falling back to numeric socket ids
/// (SDK connections use the pid verbatim). The body is authoritative
/// because generic scanner cards are cached under the numeric
/// application id: resolving the socket id first would mistake the app
/// id for a dead pid and evict live generic cards on every null sweep.
/// Returns `None` when neither yields a usable pid — pid 0 included:
/// unidentifiable publishers can never be proven dead, and clearing
/// them risks darkening a live-but-broken client (same convention as
/// the private `is_genuine_clear`).
pub fn cache_entry_pid(socket_id: &SocketId, payload: &CachedActivity) -> Option<u64> {
  let body_pid = serde_json::from_str::<serde_json::Value>(&payload.json)
    .ok()
    .and_then(|body| body.get("pid").and_then(serde_json::Value::as_u64))
    .filter(|pid| *pid != 0);
  if let Some(pid) = body_pid {
    return Some(pid);
  }
  if let Ok(pid) = socket_id.as_ref().parse::<u64>() {
    return (pid != 0).then_some(pid);
  }
  None
}

/// Handle a bridge control message (`SET_USER`/`RESET_USER`, arRPC parity).
/// Returns the ACK text plus the new identity when it changed, `None` for
/// anything else (the caller echoes those to the sender untouched).
fn handle_bridge_control(
  user: &Arc<Mutex<RpcUser>>,
  text: &str,
) -> Option<(String, Option<RpcUser>)> {
  let body: serde_json::Value = serde_json::from_str(text).ok()?;
  let msg_type = body.get("type")?.as_str()?;
  if !matches!(msg_type, "SET_USER" | "RESET_USER") {
    return None;
  }
  let nonce = body
    .get("nonce")
    .cloned()
    .unwrap_or(serde_json::Value::Null);
  // One critical section for the whole read-modify-read: patch/reset
  // and both snapshots share a single guard.
  let (before, after) = {
    let mut guard = user.lock().unwrap_or_else(|e| e.into_inner());
    let before = guard.clone();
    if msg_type == "SET_USER" {
      // `patch` (arRPC shape) or `data` (defensive alias) carry the patch.
      if let Some(patch) = body.get("patch").or_else(|| body.get("data")) {
        guard.patch(patch);
      }
    } else {
      // Reset to the startup identity (defaults + `RSRPC_USER_*`).
      *guard = RpcUser::from_env();
    }
    let after = guard.clone();
    (before, after)
  };
  let changed = (before != after).then_some(after.clone());
  let user_value = serde_json::to_value(after).unwrap_or(serde_json::Value::Null);
  let ack = serde_json::json!({
    "type": format!("{msg_type}_ACK"),
    "nonce": nonce,
    "data": { "success": true, "user": user_value },
  })
  .to_string();
  Some((ack, changed))
}

/// Build the generic process-detection payload for a scanned game.
fn generic_payload(game: &ScannedGame) -> Arc<CachedActivity> {
  let payload_struct = commands::ProcessPayload {
    activity: commands::ProcessActivity {
      application_id: game.id.clone(),
      name: game.name.clone(),
      timestamps: commands::ProcessTimestamps { start: game.start },
      r#type: 0,
      metadata: HashMap::new(),
      flags: 0,
    },
    pid: game.pid,
    socket_id: SocketId::from(&game.id),
  };
  // Same fixed-shape guarantee as `empty_cached` (String/int only):
  // encode failure is a future-field bug — degrade loudly in diagnostics,
  // never panic the broadcast path.
  let activity_json = serde_json::to_vec(&payload_struct.activity).unwrap_or_default();
  Arc::new(commands::CachedActivity {
    json: serde_json::to_string(&payload_struct)
      .map(tungstenite::Utf8Bytes::from)
      .unwrap_or_else(|err| {
        tracing::debug!("[bridge] Generic payload encode failed: {err}");
        tungstenite::Utf8Bytes::from_static("")
      }),
    msgpack: rmp_serde::to_vec_named(&payload_struct)
      .map(bytes::Bytes::from)
      .unwrap_or_else(|err| {
        tracing::debug!("[bridge] Generic payload encode failed: {err}");
        bytes::Bytes::new()
      }),
    // Always built with `activity: Some` above.
    is_clear: false,
    activity_json: bytes::Bytes::from(activity_json),
  })
}

/// Flatten the replay cache into state-snapshot activities (best-effort:
/// unparseable entries contribute their socket id only).
fn state_activities(cache: &ReplayCache) -> Vec<StateActivity> {
  let mut out = Vec::with_capacity(cache.len());
  out.extend(cache.iter().map(|(socket_id, (payload, _))| {
    let body: serde_json::Value =
      serde_json::from_str(&payload.json).unwrap_or(serde_json::Value::Null);
    let activity = body.get("activity");
    StateActivity {
      socket_id: socket_id.to_string(),
      name: activity
        .and_then(|item| item.get("name"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string),
      application_id: activity
        .and_then(|item| item.get("application_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string),
      pid: body.get("pid").and_then(serde_json::Value::as_u64),
      start_time: activity
        .and_then(|item| item.get("timestamps"))
        .and_then(|item| item.get("start"))
        .map(|value| match value {
          serde_json::Value::String(text) => text.clone(),
          other => other.to_string(),
        }),
    }
  }));
  out
}

/// Evict the oldest entries while the replay cache exceeds
/// [`MAX_CACHED_ACTIVITIES`][crate::config::MAX_CACHED_ACTIVITIES].
/// Pure map operation (no locks taken here).
fn prune_cache(cache: &mut ReplayCache) {
  while cache.len() > crate::config::MAX_CACHED_ACTIVITIES {
    let oldest = cache
      .iter()
      .min_by_key(|(_, (_, seq))| *seq)
      .map(|(key, _)| key.clone());
    match oldest {
      Some(key) => {
        cache.remove(&key);
      }
      None => break,
    }
  }
}

/// Consume the outstanding process publications for clearing, if any.
/// Returns `(pid, app_id)` pairs, sorted for deterministic clears.
/// Single-shot by construction (`drain`): repeated null scans clear once
/// and then skip.
fn take_process_clear(shared: &Shared) -> Vec<(u64, AppId)> {
  let mut outstanding: Vec<(u64, AppId)> = shared
    .last_process
    .lock()
    .unwrap_or_else(|e| e.into_inner())
    .drain()
    .map(|(app_id, pid)| (pid, app_id))
    .collect();
  outstanding.sort();
  outstanding
}

/// Send a JSON string, encoding it to MessagePack when the consumer speaks
/// MessagePack. Returns whether the frame was queued.
fn send_message(responder: &Responder, data: &str, protocol: BridgeProtocol) -> bool {
  match protocol {
    BridgeProtocol::Json => responder
      .try_send(Message::Text(data.to_string().into()))
      .is_ok(),
    BridgeProtocol::MsgPack => {
      if let Ok(value) = serde_json::from_str::<serde_json::Value>(data)
        && let Ok(bytes) = rmp_serde::to_vec_named(&value)
      {
        responder
          .try_send(Message::Binary(bytes::Bytes::from(bytes)))
          .is_ok()
      } else {
        false
      }
    }
  }
}

/// Send an already dual-encoded activity payload to a consumer.
fn send_cached(responder: &Responder, payload: &CachedActivity, protocol: BridgeProtocol) {
  match protocol {
    BridgeProtocol::Json => {
      let _ = responder.try_send(Message::Text(payload.json.clone()));
    }
    BridgeProtocol::MsgPack => {
      let _ = responder.try_send(Message::Binary(payload.msgpack.clone()));
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

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
    // Game -> empty: session end; repeats stay quiet.
    assert_eq!(
      table_transition(true, &ProcInput::Cleared),
      Some("game-end")
    );
    assert_eq!(table_transition(false, &ProcInput::Cleared), None);
  }
}
