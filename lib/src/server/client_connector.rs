use std::{
  collections::HashMap,
  sync::{Arc, Mutex},
  time::Instant,
};

use serde_json::Value;
use simple_websockets::{Event, EventHub, Message, Responder};

use crate::{
  cmd::ActivityCmd,
  commands, debug, log,
  state::{self, StateActivity, StateServer, StateServers, StateSnapshot},
  url_params::get_url_params,
  user::RpcUser,
  warn,
};

use super::process::ProcessDetectedEvent;

/// How many bridge ports to scan when the first choice is taken
/// (`port..=port_end`, arRPC scans `1337-1347` the same way).
pub(crate) const BRIDGE_PORT_SCAN_SPAN: u16 = 10;
/// Cap on replayed activities (arRPC keeps 50): bounds memory when many
/// distinct pids publish without clearing.
pub(crate) const MAX_CACHED_ACTIVITIES: usize = 50;
/// How often cached activities are rebroadcast so bridge clients that
/// missed a frame converge (arRPC refreshes every 30s).
pub(crate) const BRIDGE_REFRESH_INTERVAL_SECS: u64 = 30;
// Note: the SET_ACTIVITY flood-guard window/cap live on
// [`commands::RecentActivities`] (`DEFAULT_WINDOW`/`DEFAULT_CAP`); the
// table is built with `::default()`.

/// Which wire protocol a connected bridge client speaks.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum BridgeProtocol {
  /// JSON text frames (port 1337, the arRPC-compatible bridge).
  Json,
  /// MessagePack binary frames (port 1338).
  MsgPack,
}

impl BridgeProtocol {
  /// The protocol requested by the client's `format=` query parameter,
  /// falling back to the per-port default when absent.
  fn from_query(uri: &str, default: BridgeProtocol) -> BridgeProtocol {
    match get_url_params(uri.to_string())
      .get("format")
      .map(String::as_str)
    {
      Some("msgpack") | Some("messagepack") => BridgeProtocol::MsgPack,
      Some("json") => BridgeProtocol::Json,
      _ => default,
    }
  }
}

/// One process-detected game, remembered so an IPC clear can hand the
/// slot back to generic detection (the scanner only emits on *changes*,
/// so without this the slot would stay dark until the next game switch).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScannedGame {
  pub(crate) id: crate::AppId,
  pub(crate) name: String,
  pub(crate) pid: u64,
  pub(crate) start: u64,
}

/// IPC-wins handoff: generic process detection yields its slot to a live
/// game-SDK presence and reclaims it when that source clears. One lock for
/// the whole state (short critical sections, no I/O under it), shared by
/// the event (IPC/WS) and process loops.
///
/// Retro-compat rule (several companions may target one app, e.g. wwrpc
/// plus a community fork): **last publisher wins**. Each app maps to the
/// pid of its current owner; a publish replaces the owner, and a clear
/// only releases the slot when it comes from that same owner — a stale
/// close from a superseded companion is ignored instead of wrongly
/// resuming the generic card.
#[derive(Clone, Debug, Default)]
pub(crate) struct HandoffState {
  /// App id → pid of its current IPC/WS owner (`SET_ACTIVITY` with activity).
  live_ipc: HashMap<crate::AppId, u64>,
  /// Last games the scanner reported, by app id (a null/clear event wipes
  /// the table).
  last_scans: HashMap<crate::AppId, ScannedGame>,
}

/// Replay cache: socket id → (shared payload, sequence).
/// The `Arc` shares one build across cache, refresh and broadcast instead
/// of cloning the payload per use (`mem-zero-copy`).
type ReplayCache = HashMap<crate::SocketId, (Arc<commands::CachedActivity>, u64)>;

/// Cap for the handoff tables: distinct live app-ids are tiny in practice
/// (co-running games plus their companions). The cap only bites a client
/// publishing hundreds of ids without clearing (malicious or buggy) —
/// without it, memory grows forever on untrusted input. Enforcement
/// purges dead owners first (the actual garbage), so live slots are only
/// evicted in pathological cases, and even then the next scan or publish
/// re-arms them (self-healing).
pub(crate) const MAX_HANDOFF_ENTRIES: usize = 64;

/// Track a generic publication for its later clear, bounded like the
/// handoff tables above (purge dead pids first, then evict arbitrarily).
/// Evicting a live entry only drops its future clear — the next scan
/// re-arms it (self-healing).
fn track_process_publication(map: &mut HashMap<crate::AppId, u64>, app_id: crate::AppId, pid: u64) {
  if map.len() >= MAX_HANDOFF_ENTRIES {
    map.retain(|_, known| is_process_alive(*known));
  }
  map.insert(app_id, pid);
  while map.len() > MAX_HANDOFF_ENTRIES {
    let Some(victim) = map.keys().next().cloned() else {
      break;
    };
    map.remove(&victim);
  }
}

impl HandoffState {
  pub(crate) fn note_publish(&mut self, app_id: &str, pid: u64) {
    if self.live_ipc.len() >= MAX_HANDOFF_ENTRIES {
      // Purge dead owners first (the actual garbage: crashed companions
      // that never cleared). Whatever remains is live.
      self.live_ipc.retain(|_, owner| is_process_alive(*owner));
    }
    self.live_ipc.insert(crate::AppId::from(app_id), pid);
    // Hard bound: even all-live flooding (one pid, infinite ids) stops
    // here. Evicting a live slot only desuppresses its generic until the
    // next publish re-arms it — unreachable in legitimate use (<5 ids).
    while self.live_ipc.len() > MAX_HANDOFF_ENTRIES {
      let Some(victim) = self.live_ipc.keys().next().cloned() else {
        break;
      };
      self.live_ipc.remove(&victim);
    }
  }

  /// Returns true when the slot was actually released (owner cleared).
  pub(crate) fn note_clear(&mut self, app_id: &str, pid: u64) -> bool {
    if self.live_ipc.get(app_id).is_some_and(|owner| *owner == pid) {
      self.live_ipc.remove(app_id);
      true
    } else {
      false
    }
  }

  pub(crate) fn note_scan(&mut self, game: Option<ScannedGame>) {
    match game {
      Some(game) => {
        if self.last_scans.len() >= MAX_HANDOFF_ENTRIES {
          self
            .last_scans
            .retain(|_, known| is_process_alive(known.pid));
        }
        self.last_scans.insert(game.id.clone(), game);
        while self.last_scans.len() > MAX_HANDOFF_ENTRIES {
          let Some(victim) = self.last_scans.keys().next().cloned() else {
            break;
          };
          self.last_scans.remove(&victim);
        }
      }
      // Null scan: the table is empty, forget every game.
      None => self.last_scans.clear(),
    }
  }

  /// Release every slot owned by `pid` (abrupt close without CLEAR) and
  /// return the released app ids. Without this, a dead owner is_suppressed
  /// its slots' generics forever.
  pub(crate) fn note_clear_pid(&mut self, pid: u64) -> Vec<crate::AppId> {
    self
      .live_ipc
      .extract_if(|_, owner| *owner == pid)
      .map(|(app, _)| app)
      .collect()
  }

  /// Whether generic detection must stay out of this slot right now.
  pub(crate) fn is_suppressed(&self, app_id: &str) -> bool {
    self.live_ipc.contains_key(app_id)
  }

  /// Table sizes (bounded by [`MAX_HANDOFF_ENTRIES`]; test-only probe).
  #[cfg(test)]
  pub(crate) fn live_ipc_len(&self) -> usize {
    self.live_ipc.len()
  }

  /// Table sizes (bounded by [`MAX_HANDOFF_ENTRIES`]; test-only probe).
  #[cfg(test)]
  pub(crate) fn last_scans_len(&self) -> usize {
    self.last_scans.len()
  }

  /// The game to re-assert when `app_id`'s IPC source cleared, if the
  /// scanner still reports that same game.
  pub(crate) fn resume_for(&self, app_id: &str) -> Option<ScannedGame> {
    self.last_scans.get(app_id).cloned()
  }
}

/// Best-effort liveness probe so an IPC clear for an already-dead game
/// doesn't flash the generic card on the way out (the scanner's null
/// event clears the slot anyway).
pub(crate) fn is_process_alive(pid: u64) -> bool {
  if pid == 0 {
    return false;
  }
  if cfg!(target_os = "linux") {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
  } else {
    true
  }
}

/// Build the generic process-detection payload for a scanned game (the
/// shape `process_loop` broadcasts; reused by the handoff resume path).
pub(crate) fn generic_payload(game: &ScannedGame) -> Arc<commands::CachedActivity> {
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
    socket_id: crate::SocketId::from(&game.id),
  };
  // Same fixed-shape guarantee as `empty_cached` (String/int only):
  // encode failure is a future-field bug, logged loudly instead of
  // broadcasting an empty presence frame.
  commands::CachedActivity {
    json: serde_json::to_string(&payload_struct).unwrap_or_else(|err| {
      debug!("[Client Connector] Generic payload encode failed: {}", err);
      String::new()
    }),
    msgpack: rmp_serde::to_vec_named(&payload_struct).unwrap_or_else(|err| {
      debug!("[Client Connector] Generic payload encode failed: {}", err);
      Vec::new()
    }),
  }
  .into()
}

#[derive(Clone)]
pub(crate) struct ClientConnector {
  pub port: u16,
  pub msgpack_port: u16,

  json_server: Arc<Mutex<Option<EventHub>>>,
  msgpack_server: Arc<Mutex<Option<EventHub>>>,

  pub json_clients: Arc<Mutex<HashMap<u64, Responder>>>,
  pub msgpack_clients: Arc<Mutex<HashMap<u64, Responder>>>,

  /// Shared identity for READY frames (startup `RSRPC_USER_*`, runtime
  /// `SET_USER`/`RESET_USER` from bridge clients).
  user: Arc<Mutex<RpcUser>>,
  /// State-snapshot slot (`RSRPC_STATE_FILE` set), written on broadcasts.
  state_path: Option<std::path::PathBuf>,
  /// Extra servers for the snapshot, filled via [`set_extra_servers`]
  /// before [`start`](Self::start) (the connectors bind in their own
  /// constructors, so this struct learns them after the fact).
  ws_port: Option<u16>,
  ipc_path: Option<String>,

  /// Cache of the last activity payload per socket id, replayed to web
  /// clients that connect after the activity was set (like arRPC). Each
  /// entry carries a sequence number so the cache can evict the oldest
  /// first when it hits [`MAX_CACHED_ACTIVITIES`].
  last_activities: Arc<Mutex<ReplayCache>>,
  /// Monotonic sequence for replay-cache recency (LRU eviction order).
  activity_seq: Arc<Mutex<u64>>,

  /// Last process-detected activities broadcast but not yet cleared, by
  /// app id (each with its pid for the clear frame). Consumed (`drain`)
  /// by the null-scan path to clear exactly once. The two keyspaces
  /// differ (IPC clears are pid-keyed, process clears are app-id-keyed),
  /// so IPC clears never touch this map: an entry lives exactly from its
  /// generic publication to its process clear, and every later bridge
  /// client replays only live games.
  pub last_process: Arc<Mutex<HashMap<crate::AppId, u64>>>,
  /// IPC-wins handoff state (see [`HandoffState`]).
  handoff: Arc<Mutex<HandoffState>>,
  /// Flood guard for `SET_ACTIVITY` duplicates, shared by the event loop
  /// (short critical sections, no I/O under the lock).
  recent: Arc<Mutex<commands::RecentActivities>>,

  pub ipc_event_rec: Arc<Mutex<Option<std::sync::mpsc::Receiver<ActivityCmd>>>>,
  pub proc_event_rec: Arc<Mutex<Option<super::utils::GaugeReceiver<ProcessDetectedEvent>>>>,
  pub ws_event_rec: Arc<Mutex<Option<super::utils::GaugeReceiver<ActivityCmd>>>>,
  /// Resource census handles, installed by the daemon once all channels
  /// exist (see [`ClientConnector::set_stats_ctx`]). `None` until then:
  /// transition lines are skipped, never fabricated.
  stats_ctx: Option<super::utils::StatsCtx>,
}

impl ClientConnector {
  /// Bind both bridge protocols (JSON + MessagePack), each on its port
  /// range.
  ///
  /// # Errors
  ///
  /// Returns the launch error when every candidate port is taken (both
  /// ranges are exhausted): the caller surfaces it instead of exiting.
  pub(crate) fn new(
    port_start: u16,
    port_end: u16,
    msgpack_port: u16,
    user: Arc<Mutex<RpcUser>>,
    ipc_event_rec: std::sync::mpsc::Receiver<ActivityCmd>,
    proc_event_rec: super::utils::GaugeReceiver<ProcessDetectedEvent>,
    ws_event_rec: super::utils::GaugeReceiver<ActivityCmd>,
  ) -> crate::error::Result<ClientConnector> {
    let (json_server, port) = launch_in_range(port_start, port_end, None, "JSON bridge")?;
    // The MessagePack port keeps its configured value unless it collides
    // with the claimed JSON port (e.g. defaults 1337/1338 are adjacent, or
    // a custom --bridge-port lands on 1338): then scan forward instead of
    // dying.
    let skip = (msgpack_port == port).then_some(port);
    let (msgpack_server, msgpack_port) = launch_in_range(
      msgpack_port,
      msgpack_port.saturating_add(BRIDGE_PORT_SCAN_SPAN),
      skip,
      "MessagePack bridge",
    )?;

    // Optional presence snapshot for external tooling (arRPC-compatible
    // layout, `rsrpc-` prefix so both daemons coexist).
    let state_path = std::env::var("RSRPC_STATE_FILE").ok().and_then(|_| {
      let path = state::select_slot(&std::env::temp_dir(), state::now_secs());
      match &path {
        Some(path) => log!("[Client Connector] State snapshot: {}", path.display()),
        None => warn!("[Client Connector] RSRPC_STATE_FILE set but no free state slot"),
      }
      path
    });

    Ok(ClientConnector {
      json_server: Arc::new(Mutex::new(Some(json_server))),
      msgpack_server: Arc::new(Mutex::new(Some(msgpack_server))),

      json_clients: Arc::new(Mutex::new(HashMap::new())),
      msgpack_clients: Arc::new(Mutex::new(HashMap::new())),
      user,
      state_path,
      ws_port: None,
      ipc_path: None,
      port,
      msgpack_port,

      last_activities: Arc::new(Mutex::new(HashMap::new())),
      activity_seq: Arc::new(Mutex::new(0)),

      last_process: Arc::new(Mutex::new(HashMap::new())),
      handoff: Arc::new(Mutex::new(HandoffState::default())),
      recent: Arc::new(Mutex::new(commands::RecentActivities::default())),

      ipc_event_rec: Arc::new(Mutex::new(Some(ipc_event_rec))),
      proc_event_rec: Arc::new(Mutex::new(Some(proc_event_rec))),
      ws_event_rec: Arc::new(Mutex::new(Some(ws_event_rec))),
      stats_ctx: None,
    })
  }

  /// Install the resource census handles (called once, before
  /// [`start`](Self::start), when every channel and client map exists).
  pub(crate) fn set_stats_ctx(&mut self, stats: super::utils::StatsCtx) {
    self.stats_ctx = Some(stats);
  }

  /// Fill in the servers this struct does not bind itself (called once,
  /// before [`start`](Self::start)).
  pub(crate) fn set_extra_servers(&mut self, ws_port: Option<u16>, ipc_path: Option<String>) {
    self.ws_port = ws_port;
    self.ipc_path = ipc_path;
  }

  pub(crate) fn start(&mut self) {
    // Double-start is a caller bug: the taken server below marks it.
    // Ignore fail-safe instead of panicking on the takes.
    if self
      .json_server
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .is_none()
    {
      warn!("[Client Connector] Already started, ignoring duplicate start");
      return;
    }
    let json_server = self
      .json_server
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .take()
      .expect("[bug] json server checked above");
    let msgpack_server = self
      .msgpack_server
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .take()
      .expect("[bug] msgpack server taken with json server");

    let json_clients = self.json_clients.clone();
    let msgpack_clients = self.msgpack_clients.clone();
    let user = self.user.clone();
    let last_activities = self.last_activities.clone();

    // One poll loop per bridge protocol/port
    std::thread::spawn({
      let last_activities = last_activities.clone();
      let user = user.clone();
      move || {
        Self::poll_loop(
          json_server,
          json_clients,
          last_activities,
          user,
          BridgeProtocol::Json,
        )
      }
    });
    std::thread::spawn(move || {
      Self::poll_loop(
        msgpack_server,
        msgpack_clients,
        last_activities,
        user,
        BridgeProtocol::MsgPack,
      )
    });

    // Create a thread for each reciever
    let ipc_event_rec = self
      .ipc_event_rec
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .take()
      .expect("[bug] receivers taken with servers");
    let proc_event_rec = self
      .proc_event_rec
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .take()
      .expect("[bug] receivers taken with servers");
    let ws_event_rec = self
      .ws_event_rec
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .take()
      .expect("[bug] receivers taken with servers");

    let ipc_clone = self.clone();
    let proc_clone = self.clone();
    let ws_clone = self.clone();
    let refresh_clone = self.clone();

    std::thread::spawn(move || Self::event_loop(ipc_event_rec, ipc_clone));
    std::thread::spawn(move || Self::event_loop(ws_event_rec, ws_clone));
    let stats_ctx = self.stats_ctx.clone();
    std::thread::spawn(move || Self::process_loop(proc_event_rec, proc_clone, stats_ctx));
    // Periodic rebroadcast: bridge clients that missed a frame (or
    // connected between frames) converge on the cached presence.
    std::thread::spawn(move || {
      loop {
        std::thread::sleep(std::time::Duration::from_secs(BRIDGE_REFRESH_INTERVAL_SECS));
        refresh_clone.refresh_clients();
      }
    });

    // Snapshot the (empty) presence + bound servers immediately, so
    // external tooling sees us before the first game appears.
    self.persist_state();
  }

  fn poll_loop(
    server: EventHub,
    clients: Arc<Mutex<HashMap<u64, Responder>>>,
    last_activities: Arc<Mutex<ReplayCache>>,
    user: Arc<Mutex<RpcUser>>,
    default_protocol: BridgeProtocol,
  ) {
    loop {
      match server.poll_event() {
        Event::Connect(client_id, responder) => {
          log!("[Client Connector] Client {} connected", client_id);

          let uri = responder.connection_details().uri.clone();
          let protocol = BridgeProtocol::from_query(&uri, default_protocol);

          log!(
            "[Client Connector] Client {} using protocol {:?}",
            client_id,
            protocol
          );

          // Send initial connection data
          send_message(
            &responder,
            &user
              .lock()
              .unwrap_or_else(|e| e.into_inner())
              .ready_payload(),
            protocol,
          );

          // Send any cached activities so late joiners see current presence
          for (payload, _) in last_activities
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
          {
            send_cached_activity(&responder, payload, protocol);
          }

          clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(client_id, responder);
        }
        Event::Disconnect(client_id) => {
          log!("[Client Connector] Client {} disconnected", client_id);
          clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&client_id);
        }
        Event::Message(client_id, message) => {
          debug!(
            "[Client Connector] Received message from client {}: {:?}",
            client_id, message
          );
          // Bridge control messages (JSON text) are answered, everything
          // else echoes to the sender as before. Identity changes also
          // fan out as CURRENT_USER_UPDATE (official event) to every
          // client on THIS port loop (JSON and MessagePack loops are
          // independent; control traffic practically only arrives on the
          // JSON port). IPC/WS game clients learn it on their next
          // handshake (no reverse channel exists there by design).
          match &message {
            Message::Text(text) => match handle_bridge_control(&user, text) {
              Some((ack, changed)) => {
                let mut clients = clients.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(responder) = clients.get(&client_id) {
                  responder.send(Message::Text(ack));
                }
                if let Some(user) = changed {
                  let dispatch = commands::current_user_update(&user);
                  let dead: Vec<u64> = clients
                    .iter()
                    .filter_map(|(id, responder)| {
                      (!responder.send(Message::Text(dispatch.clone()))).then_some(*id)
                    })
                    .collect();
                  for id in dead {
                    warn!("[Client Connector] Pruning dead bridge client {id}");
                    clients.remove(&id);
                  }
                }
              }
              None => {
                if let Some(responder) = clients
                  .lock()
                  .unwrap_or_else(|e| e.into_inner())
                  .get(&client_id)
                {
                  responder.send(message);
                }
              }
            },
            _ => {
              if let Some(responder) = clients
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&client_id)
              {
                responder.send(message);
              }
            }
          }
        }
      }
    }
  }

  /// Handle activity commands coming from the IPC and WebSocket connectors.
  /// `SET_ACTIVITY` commands are translated into bridge payloads, everything
  /// else (INVITE_BROWSER, DEEP_LINK, ...) is forwarded as-is.
  fn event_loop(rec: impl super::utils::RecvQueue<ActivityCmd>, connector: ClientConnector) {
    while let Ok(cmd) = rec.recv_q() {
      if cmd.cmd != "SET_ACTIVITY" {
        // Just send the event as-is, there isn't really anything to go off of here
        connector.broadcast_raw(&cmd);
        continue;
      }

      let mut cmd = cmd;
      match commands::cached_activity(&mut cmd) {
        Some(payload) => {
          let args = cmd.args.as_ref();
          let pid = args.and_then(|args| args.pid).unwrap_or_default();
          // Flood guard: byte-identical republishes inside the window are
          // dropped here — after the game got its echo upstream, before
          // any broadcast, cache write or log line. Any changed byte, and
          // every clear, passes and re-arms the slot.
          let fingerprint = args
            .and_then(|args| args.activity.as_ref())
            .and_then(|activity| serde_json::to_vec(activity).ok());
          let app_key = cmd.application_id.as_deref().unwrap_or("");
          if connector
            .recent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .should_drop(app_key, pid, fingerprint.as_deref(), Instant::now())
          {
            debug!(
              "[Client Connector] Dropping duplicate SET_ACTIVITY (app {}, pid {})",
              app_key, pid
            );
            continue;
          }
          let activity = args.and_then(|args| args.activity.as_ref());
          // IPC-wins handoff: a live SDK presence takes over this app slot
          // from generic detection (last publisher wins across companions).
          if let Some(app) = activity.and_then(|activity| activity.application_id.clone()) {
            connector
              .handoff
              .lock()
              .unwrap_or_else(|e| e.into_inner())
              .note_publish(&app, pid);
          }
          // NOTE: no ignore-list filtering here by design. Forwarded client
          // frames (companions, native game presences) are indistinguishable
          // on this path — filtering would kill the companion this bridge
          // exists to carry. `--ignore-ids` applies to process detection
          // only (see apply_ignore_list).
          // Identical republishes (apps re-send every few minutes) and
          // duplicate clears stay in debug: compare against the cached
          // activity for this pid, so first publishes, real changes and
          // effective clears still log once.
          let changed = {
            let cached = connector
              .last_activities
              .lock()
              .unwrap_or_else(|e| e.into_inner())
              .get(&crate::SocketId::from(pid.to_string()))
              .and_then(|(cached, _)| serde_json::from_str::<Value>(&cached.json).ok())
              .and_then(|body| body.get("activity").cloned());
            let current = activity
              .and_then(|activity| serde_json::to_value(activity).ok())
              .unwrap_or(Value::Null);
            match cached {
              None => !current.is_null(),
              Some(old) => old != current,
            }
          };
          // Who published what, in one line: best available label (name,
          // else details/state) + app id + pid. Labels and pids only.
          match activity {
            Some(activity) => {
              if changed {
                log!(
                  "[Client Connector] Published: {} (app {}, pid {})",
                  activity.display_name(),
                  activity.application_id.as_deref().unwrap_or("?"),
                  pid
                );
              } else {
                debug!(
                  "[Client Connector] Published: {} (app {}, pid {})",
                  activity.display_name(),
                  activity.application_id.as_deref().unwrap_or("?"),
                  pid
                );
              }
            }
            None => {
              if changed {
                log!("[Client Connector] Published clear (pid {})", pid);
              } else {
                debug!("[Client Connector] Published clear (pid {})", pid);
              }
            }
          }
          // A genuine clear (null activity) from a real connection means the
          // SDK source went away: the slot below is handed back to generic
          // detection (resume), so the scanner re-asserts the still-running
          // game (e.g. How to Fish comes back after Sober/Roblox closes).
          // pid == 0 means no game was ever identified on this connection
          // (e.g. SUBSCRIBE before any SET_ACTIVITY) — ignore those, or
          // every fresh connection would flap the display.
          if is_genuine_clear(&cmd) {
            // Hand the slot(s) back to generic detection when the owning
            // SDK source cleared (stale closes from superseded companions
            // are ignored); the game must still be alive, or the card
            // would flash on the way out (the scanner's null event clears
            // anyway).
            let resume: Vec<ScannedGame> = {
              let mut handoff = connector.handoff.lock().unwrap_or_else(|e| e.into_inner());
              match cmd.application_id.clone() {
                Some(app) if handoff.note_clear(&app, pid) => {
                  handoff.resume_for(app.as_ref()).into_iter().collect()
                }
                Some(_) => Vec::new(),
                // No app id: abrupt close (socket died without CLEAR).
                // Release every slot this pid owned, or their generics
                // stay suppressed by a dead owner forever.
                None => handoff
                  .note_clear_pid(pid)
                  .into_iter()
                  .filter_map(|app| handoff.resume_for(app.as_ref()))
                  .collect(),
              }
            };
            for game in resume.into_iter().filter(|game| is_process_alive(game.pid)) {
              connector.resume_generic(&game);
            }
            if changed {
              log!("[Client Connector] IPC/WS source cleared, resuming process detection");
            } else {
              debug!("[Client Connector] Duplicate clear ignored (pid {})", pid);
            }
          }
          // Identical republishes change nothing observable: the replay
          // cache already holds these exact bytes (late joiners replay
          // them) and the 30s refresh re-asserts them to live clients —
          // so skip the fan-out, cache churn and snapshot rewrite. First
          // publishes, real changes and effective clears always pass.
          if changed {
            connector.broadcast_activity(payload, crate::SocketId::from(pid.to_string()));
          } else {
            debug!(
              "[Client Connector] Already published identical activity (app {}, pid {}), skipping fan-out",
              app_key, pid
            );
          }
        }
        None => warn!("[Client Connector] Invalid activity command, skipping"),
      }
    }
  }

  fn process_loop(
    rec: super::utils::GaugeReceiver<ProcessDetectedEvent>,
    connector: ClientConnector,
    stats: Option<super::utils::StatsCtx>,
  ) {
    while let Ok(proc_event) = rec.recv() {
      let Some(hit) = proc_event.hit else {
        connector
          .handoff
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .note_scan(None);
        // Clear every outstanding process publication (multi-game scans
        // publish per slot; a lone null means the table is empty). The
        // two keyspaces differ (IPC clears are pid-keyed, process clears
        // are app-id-keyed), so IPC clears never disarm these.
        let outstanding = take_process_clear(&connector);
        if outstanding.is_empty() {
          continue;
        }

        let cleared = outstanding.len();
        for (pid, app_id) in outstanding {
          // Send an empty payload
          log!("[Client Connector] Sending empty payload");

          let socket_id = crate::SocketId::from(app_id);
          let payload = commands::empty_cached(pid, socket_id.clone());

          connector.broadcast_activity(payload, socket_id);
        }
        // Session boundary for the resource census: a game ended here, so
        // RSS/queue movement around this timestamp correlates with churn.
        if let Some(stats) = &stats {
          log!(
            "{}",
            stats.snapshot(&format!("game-end {cleared} slot(s) cleared"))
          );
        }

        continue;
      };

      // Remember the scan for the handoff: an IPC clear hands the slot
      // back to exactly this game (the scanner won't re-emit it).
      let game = ScannedGame {
        id: crate::AppId::from(hit.entry.id.clone()),
        name: hit.entry.name.to_string(),
        pid: hit.pid,
        start: hit.start,
      };
      connector
        .handoff
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .note_scan(Some(game.clone()));

      // IPC-wins handoff: a live SDK presence owns this slot — withdraw
      // our generic card if shown and stay out until that source clears.
      // Strictly per-slot: other games' cards are untouched, so co-running
      // games each keep theirs.
      if connector
        .handoff
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_suppressed(game.id.as_ref())
      {
        // The armed entry carries its own pid for the clear frame.
        if let Some(pid) = connector
          .last_process
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .remove(&game.id)
        {
          connector.broadcast_activity(
            commands::empty_cached(pid, crate::SocketId::from(&game.id)),
            crate::SocketId::from(&game.id),
          );
          debug!(
            "[Client Connector] Yielding {} to live IPC presence",
            game.name
          );
        } else {
          debug!(
            "[Client Connector] Deferring to live IPC presence for: {}",
            game.name
          );
        }
        continue;
      }

      // Already showing this slot: repeats dedup here instead of
      // flapping the display (the scanner only forwards deltas, but the
      // EXEC fast path can still re-emit a shown game).
      if connector
        .last_process
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains_key(&game.id)
      {
        debug!(
          "[Client Connector] Already sent payload for activity: {}",
          game.name
        );
        continue;
      }

      track_process_publication(
        &mut connector
          .last_process
          .lock()
          .unwrap_or_else(|e| e.into_inner()),
        game.id.clone(),
        game.pid,
      );

      debug!(
        "[Client Connector] Publishing generic presence for activity: {}",
        game.name
      );

      // Session boundary for the resource census: a game started here, so
      // RSS/queue movement around this timestamp correlates with churn.
      if let Some(stats) = &stats {
        log!(
          "{}",
          stats.snapshot(&format!("game-start {} ({})", game.name, game.id.as_ref()))
        );
      }

      // Same bytes as the handoff resume path (one construction site).
      connector.broadcast_activity(generic_payload(&game), crate::SocketId::from(&game.id));
    }
  }

  /// Re-assert generic process presence for `game` after its IPC source
  /// cleared (handoff back). The scanner only emits on *changes*, so
  /// without this the slot would stay dark until the next game switch.
  fn resume_generic(&self, game: &ScannedGame) {
    track_process_publication(
      &mut self.last_process.lock().unwrap_or_else(|e| e.into_inner()),
      game.id.clone(),
      game.pid,
    );
    debug!(
      "[Client Connector] Resuming generic presence for {} ({})",
      game.name, game.id
    );
    self.broadcast_activity(generic_payload(game), crate::SocketId::from(&game.id));
  }

  /// Broadcast an activity payload to all connected clients, updating the
  /// replay cache so clients connecting later catch up on the current presence.
  #[hotpath::measure]
  fn broadcast_activity(&self, payload: Arc<commands::CachedActivity>, socket_id: crate::SocketId) {
    // Keep the replay cache in sync, pruning cleared activities
    let is_clear = serde_json::from_str::<Value>(&payload.json)
      .ok()
      .and_then(|value| value.get("activity").cloned())
      .map(|activity| activity.is_null())
      .unwrap_or(false);

    {
      let mut last_activities = self
        .last_activities
        .lock()
        .unwrap_or_else(|e| e.into_inner());
      if is_clear {
        last_activities.remove(&socket_id);
      } else {
        let mut seq = self.activity_seq.lock().unwrap_or_else(|e| e.into_inner());
        *seq = seq.saturating_add(1);
        last_activities.insert(socket_id, (payload.clone(), *seq));
        prune_cache(&mut last_activities);
      }
    }

    self.send_to_all(&payload);
    self.persist_state();
  }

  /// Rebroadcast every cached activity (refresh tick): no cache bookkeeping,
  /// just convergence for clients that missed a frame.
  fn refresh_clients(&self) {
    let payloads: Vec<Arc<commands::CachedActivity>> = self
      .last_activities
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .values()
      .map(|(payload, _)| payload.clone())
      .collect();
    if payloads.is_empty() {
      // No presence to rebroadcast, but still refresh the snapshot mtime
      // so a live-but-idle daemon never looks stale to slot reuse.
      self.persist_state();
      return;
    }
    debug!(
      "[Client Connector] Refreshing {} cached activities",
      payloads.len()
    );
    for payload in &payloads {
      self.send_to_all(payload);
    }
    self.persist_state();
  }

  /// Persist the state snapshot when enabled (`RSRPC_STATE_FILE`).
  /// Best-effort: failures stay in debug so a full tmpfs never breaks
  /// presence.
  fn persist_state(&self) {
    let Some(path) = self.state_path.as_ref() else {
      return;
    };
    let servers = StateServers {
      bridge: Some(StateServer {
        host: "127.0.0.1".to_string(),
        port: self.port,
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
    let activities = state_activities(
      &self
        .last_activities
        .lock()
        .unwrap_or_else(|e| e.into_inner()),
    );
    let snapshot = StateSnapshot::new(servers, activities);
    if let Err(err) = state::write_snapshot(path, &snapshot) {
      debug!("[Client Connector] State snapshot failed: {}", err);
    }
  }

  /// Send one payload to every connected bridge client, pruning dead ones.
  #[hotpath::measure]
  fn send_to_all(&self, payload: &commands::CachedActivity) {
    let json_clients = self.json_clients.lock().unwrap_or_else(|e| e.into_inner());
    let msgpack_clients = self
      .msgpack_clients
      .lock()
      .unwrap_or_else(|e| e.into_inner());
    if json_clients.is_empty() && msgpack_clients.is_empty() {
      debug!("[Client Connector] No clients connected, skipping");
      return;
    }
    drop(json_clients);
    drop(msgpack_clients);

    // Backpressure: Responder::send reports dead clients. Prune them so a
    // stuck bridge client cannot pin memory (its queued frames) forever.
    // One log per pruned client: removal means it never logs again.
    for (clients, payload) in [
      (&self.json_clients, Message::Text(payload.json.clone())),
      (
        &self.msgpack_clients,
        Message::Binary(payload.msgpack.clone()),
      ),
    ] {
      let mut clients = clients.lock().unwrap_or_else(|e| e.into_inner());
      let dead: Vec<u64> = clients
        .iter()
        .filter_map(|(id, responder)| (!responder.send(payload.clone())).then_some(*id))
        .collect();
      for id in dead {
        warn!("[Client Connector] Pruning dead bridge client {id}");
        clients.remove(&id);
      }
    }
  }

  /// Broadcast a non-activity event (e.g. INVITE_BROWSER) as-is to all clients.
  pub(crate) fn broadcast_raw(&self, cmd: &ActivityCmd) {
    let mut json_clients = self.json_clients.lock().unwrap_or_else(|e| e.into_inner());
    let mut msgpack_clients = self
      .msgpack_clients
      .lock()
      .unwrap_or_else(|e| e.into_inner());
    if json_clients.is_empty() && msgpack_clients.is_empty() {
      debug!("[Client Connector] No clients connected, skipping");
      return;
    }

    // Serialize once per encoding, not once per client: clones are
    // orders of magnitude cheaper than re-serializing the same frame.
    // A frame that cannot encode is dropped loudly (not silently).
    let json_payload = if json_clients.is_empty() {
      None
    } else {
      match serde_json::to_string(cmd) {
        Ok(payload) => Some(payload),
        Err(err) => {
          debug!(
            "[Client Connector] Dropping unserializable fan-out frame: {}",
            err
          );
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
          debug!(
            "[Client Connector] Dropping unserializable fan-out frame: {}",
            err
          );
          None
        }
      }
    };
    // Backpressure: same dead-client prune as `send_to_all` — a bridge
    // client that died without a clean `Disconnect` (kill -9, dead tab)
    // reports `false` here and must not pin its queued frames forever.
    if let Some(payload) = json_payload {
      let dead: Vec<u64> = json_clients
        .iter()
        .filter_map(|(id, responder)| {
          (!responder.send(Message::Text(payload.clone()))).then_some(*id)
        })
        .collect();
      for id in dead {
        warn!("[Client Connector] Pruning dead bridge client {id}");
        json_clients.remove(&id);
      }
    }
    if let Some(payload) = msgpack_payload {
      let dead: Vec<u64> = msgpack_clients
        .iter()
        .filter_map(|(id, responder)| {
          (!responder.send(Message::Binary(payload.clone()))).then_some(*id)
        })
        .collect();
      for id in dead {
        warn!("[Client Connector] Pruning dead bridge client {id}");
        msgpack_clients.remove(&id);
      }
    }
  }
}

/// Consume the outstanding process publications for clearing, if any.
/// Returns `(pid, socket_id)` pairs, sorted for deterministic clears.
/// Single-shot by construction (`drain`): repeated null scans clear once
/// and then skip. IPC/WS clears never touch this map (pid-keyed vs
/// app-id-keyed keyspaces), so they cannot disarm it either.
pub(crate) fn take_process_clear(connector: &ClientConnector) -> Vec<(u64, crate::AppId)> {
  let mut outstanding: Vec<(u64, crate::AppId)> = connector
    .last_process
    .lock()
    .unwrap_or_else(|e| e.into_inner())
    .drain()
    .map(|(socket_id, pid)| (pid, socket_id))
    .collect();
  outstanding.sort();
  outstanding
}

impl Drop for ClientConnector {
  fn drop(&mut self) {
    if let Ok(mut server) = self.json_server.lock() {
      drop(server.take());
    }
    if let Ok(mut server) = self.msgpack_server.lock() {
      drop(server.take());
    }
    // State slots are owned while alive (fresh mtime blocks reuse):
    // remove ours so a later daemon reuses the slot immediately.
    if let Some(path) = self.state_path.as_ref() {
      let _ = std::fs::remove_file(path);
    }
  }
}

/// Whether an IPC/WS command is a genuine clear from a real connection
/// (null activity + nonzero pid, e.g. game disconnect/close). SUBSCRIBE-style
/// messages that never identified a game (pid 0) are not clears.
pub(crate) fn is_genuine_clear(cmd: &ActivityCmd) -> bool {
  match cmd.args.as_ref().and_then(|args| args.pid) {
    Some(pid) if pid != 0 => cmd
      .args
      .as_ref()
      .is_some_and(|args| args.activity.is_none()),
    _ => false,
  }
}

/// Handle a bridge control message (`SET_USER`/`RESET_USER`, arRPC parity).
/// Returns the ACK text plus the new identity when it changed, `None` for
/// anything else (the caller echoes those to the sender untouched).
pub(crate) fn handle_bridge_control(
  user: &Arc<Mutex<RpcUser>>,
  text: &str,
) -> Option<(String, Option<RpcUser>)> {
  let body: Value = serde_json::from_str(text).ok()?;
  let msg_type = body.get("type")?.as_str()?;
  if !matches!(msg_type, "SET_USER" | "RESET_USER") {
    return None;
  }
  let nonce = body.get("nonce").cloned().unwrap_or(Value::Null);
  let before = user.lock().unwrap_or_else(|e| e.into_inner()).clone();
  if msg_type == "SET_USER" {
    // `patch` (arRPC shape) or `data` (defensive alias) carry the patch.
    if let Some(patch) = body.get("patch").or_else(|| body.get("data")) {
      user.lock().unwrap_or_else(|e| e.into_inner()).patch(patch);
    }
  } else {
    // Reset to the startup identity (defaults + `RSRPC_USER_*`).
    *user.lock().unwrap_or_else(|e| e.into_inner()) = RpcUser::from_env();
  }
  let after = user.lock().unwrap_or_else(|e| e.into_inner()).clone();
  let changed = (before != after).then_some(after.clone());
  let user_value = serde_json::to_value(after).unwrap_or(Value::Null);
  let ack_type = format!("{msg_type}_ACK");
  let ack = serde_json::json!({
    "type": ack_type,
    "nonce": nonce,
    "data": { "success": true, "user": user_value },
  })
  .to_string();
  Some((ack, changed))
}

/// Flatten the replay cache into state-snapshot activities (best-effort:
/// unparseable entries contribute their socket id only).
pub(crate) fn state_activities(cache: &ReplayCache) -> Vec<StateActivity> {
  let mut out = Vec::with_capacity(cache.len());
  out.extend(cache.iter().map(|(socket_id, (payload, _))| {
    let body: Value = serde_json::from_str(&payload.json).unwrap_or(Value::Null);
    let activity = body.get("activity");
    StateActivity {
      socket_id: socket_id.to_string(),
      name: activity
        .and_then(|item| item.get("name"))
        .and_then(Value::as_str)
        .map(str::to_string),
      application_id: activity
        .and_then(|item| item.get("application_id"))
        .and_then(Value::as_str)
        .map(str::to_string),
      pid: body.get("pid").and_then(Value::as_u64),
      start_time: activity
        .and_then(|item| item.get("timestamps"))
        .and_then(|item| item.get("start"))
        .map(|value| match value {
          Value::String(text) => text.clone(),
          other => other.to_string(),
        }),
    }
  }));
  out
}

/// Evict the oldest entries while the replay cache exceeds
/// [`MAX_CACHED_ACTIVITIES`]. Pure map operation (no locks taken here).
pub(crate) fn prune_cache(cache: &mut ReplayCache) {
  while cache.len() > MAX_CACHED_ACTIVITIES {
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

fn launch_in_range(
  start: u16,
  end: u16,
  skip: Option<u16>,
  name: &'static str,
) -> crate::error::Result<(EventHub, u16)> {
  let end = end.max(start);
  for port in start..=end {
    if Some(port) == skip {
      continue;
    }
    match simple_websockets::launch(port) {
      Ok(server) => {
        log!("[Client Connector] {} on port {}", name, port);
        return Ok((server, port));
      }
      Err(err) => {
        warn!(
          "[Client Connector] Failed to launch {} on port {}, trying next: {:?}",
          name, port, err
        );
      }
    }
  }

  Err(crate::error::RsrpcError::BridgeBind { name, start, end })
}

/// Send a raw JSON string, encoding it to MessagePack when the client speaks
/// MessagePack.
fn send_message(responder: &Responder, data: &str, protocol: BridgeProtocol) {
  match protocol {
    BridgeProtocol::Json => {
      responder.send(Message::Text(data.to_string()));
    }
    BridgeProtocol::MsgPack => {
      if let Ok(value) = serde_json::from_str::<Value>(data)
        && let Ok(bytes) = rmp_serde::to_vec_named(&value)
      {
        responder.send(Message::Binary(bytes));
      }
    }
  }
}

/// Send an already dual-encoded activity payload to a client.
fn send_cached_activity(
  responder: &Responder,
  payload: &commands::CachedActivity,
  protocol: BridgeProtocol,
) {
  match protocol {
    BridgeProtocol::Json => {
      responder.send(Message::Text(payload.json.clone()));
    }
    BridgeProtocol::MsgPack => {
      responder.send(Message::Binary(payload.msgpack.clone()));
    }
  }
}
