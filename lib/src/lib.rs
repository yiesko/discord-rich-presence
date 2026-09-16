use detection::DetectableActivity;
use server::{
  client_connector::ClientConnector,
  ipc::IpcConnector,
  ipc_utils::IpcFacilitator,
  process::{ProcessEventListeners, ProcessScanState, ProcessServer, RefreshConfig},
  utils::{QueueGauge, STATS_INTERVAL_SECS, StatsCtx},
  websocket::WebsocketConnector,
};
use std::{
  path::PathBuf,
  sync::{Arc, Mutex, mpsc},
};

use user::RpcUser;

pub mod cmd;
pub mod commands;
pub mod detection;
pub mod error;
mod logger;
pub mod overrides;
mod server;
pub mod state;
mod url_params;
pub mod user;

#[cfg(test)]
mod tests;

pub type ProcessCallback = dyn FnMut(ProcessScanState) + Send + Sync;

/// Depth of the client→bridge event queues (IPC and WebSocket legs).
/// Bounded on purpose (see `start`): backpressure instead of unbounded
/// growth under a spinning local client.
pub(crate) const EVENT_QUEUE_BOUND: usize = 64;

/// Discord application id: identifies a game/activity slot. Newtyped so
/// socket ids, pids and raw strings can never mix at compile time; same
/// wire format as the inner string.
#[derive(
  Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct AppId(pub String);

/// Bridge socket id: identifies one client connection slot. See [`AppId`].
#[derive(
  Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct SocketId(pub String);

impl std::fmt::Display for AppId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.0.fmt(f)
  }
}

impl std::fmt::Display for SocketId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.0.fmt(f)
  }
}

impl AsRef<str> for AppId {
  fn as_ref(&self) -> &str {
    &self.0
  }
}

/// Borrow as `str` so `HashMap<AppId, _>` lookups accept `&str` without
/// allocating an owned key on the hot path.
impl std::borrow::Borrow<str> for AppId {
  fn borrow(&self) -> &str {
    &self.0
  }
}

/// Same as [`AppId`]: socket maps accept `&str` lookups directly.
impl std::borrow::Borrow<str> for SocketId {
  fn borrow(&self) -> &str {
    &self.0
  }
}

impl AsRef<str> for SocketId {
  fn as_ref(&self) -> &str {
    &self.0
  }
}

impl From<String> for AppId {
  fn from(id: String) -> Self {
    Self(id)
  }
}

impl From<&str> for AppId {
  fn from(id: &str) -> Self {
    Self(id.to_string())
  }
}

impl From<String> for SocketId {
  fn from(id: String) -> Self {
    Self(id)
  }
}

impl From<&str> for SocketId {
  fn from(id: &str) -> Self {
    Self(id.to_string())
  }
}

/// An app slot doubles as its own socket on the generic (scanner-driven)
/// path: move the id across instead of cloning it.
impl From<AppId> for SocketId {
  fn from(id: AppId) -> Self {
    Self(id.0)
  }
}

/// Borrow an app id as its socket without touching the inner string.
impl From<&AppId> for SocketId {
  fn from(id: &AppId) -> Self {
    Self(id.0.clone())
  }
}

/// Unwrap back to the wire string (Display also works for formatting).
impl From<AppId> for String {
  fn from(id: AppId) -> Self {
    id.0
  }
}

/// Unwrap back to the wire string (Display also works for formatting).
impl From<SocketId> for String {
  fn from(id: SocketId) -> Self {
    id.0
  }
}

/// HTTP agent for Discord fetches (database, exclusions) with a global
/// timeout: without it, a blackholed endpoint hangs the hourly refresh
/// thread — or daemon boot — forever. Callers add their own size caps.
pub fn http_agent(timeout: std::time::Duration) -> ureq::Agent {
  ureq::Agent::config_builder()
    .timeout_global(Some(timeout))
    .build()
    .into()
}

/// Minimal game info returned by [`RPCServer::detect_once`].
#[derive(Clone, Debug)]
pub struct DetectedGame {
  pub id: String,
  pub name: String,
  pub pid: Option<u64>,
}

/// Minimal database entry for `--list-database` diagnostics.
#[derive(Clone, Debug)]
pub struct DetectableSummary {
  pub id: String,
  pub name: String,
  pub executables: usize,
}

#[derive(Clone, Debug)]
pub struct RPCConfig {
  pub enable_process_scanner: bool,
  /// Event-driven proc-events watcher (netlink `cn_proc` EXEC/EXIT fast
  /// path on Linux). Best-effort by kernel nature and occasionally
  /// silent — polling backstops it either way. `false` skips the watcher
  /// thread entirely (`--no-proc-events` / `RSRPC_NO_PROC_EVENTS`).
  pub enable_proc_events: bool,
  pub enable_ipc_connector: bool,
  pub enable_websocket_connector: bool,
  pub enable_secondary_events: bool,
  pub port: u16,
  pub msgpack_port: u16,
  /// End of the JSON bridge port scan range (`port..=bridge_port_end`,
  /// arRPC-compatible `1337-1347` by default).
  pub bridge_port_end: u16,
  pub ws_port_start: u16,
  pub ws_port_end: u16,
  pub scan_interval_secs: u64,
  pub db_url: Option<String>,
  pub enable_db_update: bool,
  /// ETag captured by the startup fetch, so the first hourly refresh is
  /// conditional too (otherwise every restart pays one full redundant
  /// rebuild before learning the tag). `None` means unconditional.
  pub initial_db_etag: Option<String>,
  /// Content hashes captured by the startup fetch — `(raw body, canonical
  /// trimmed)` — so the first hourly refresh can skip the parse (same
  /// bytes) or the rebuild (same games) exactly like later checks.
  /// Without this, every restart pays one redundant rebuild before the
  /// guard learns the content. `None` means unseeded (first check always
  /// rebuilds, like before).
  pub initial_db_content_hash: Option<(u64, u64)>,
  /// Application IDs the process scanner must never publish (coexistence
  /// with a richer publisher owning those slots). Scan-only by design:
  /// forwarded client frames always pass (a companion and a native frame
  /// are indistinguishable on that path, and the companion must flow).
  /// Ignored games behave as absent: null event, clear.
  pub ignored_ids: Vec<String>,
  /// Source URL for Discord's detection exclusions (installer/crash
  /// reporter names + regexes), refreshed hourly alongside the DB when
  /// `enable_db_update` is set. `None` disables the fetch (empty set =
  /// current behavior).
  pub exclusions_url: Option<String>,
}

impl Default for RPCConfig {
  fn default() -> Self {
    Self {
      enable_process_scanner: true,
      enable_proc_events: true,
      enable_ipc_connector: true,
      enable_websocket_connector: true,
      enable_secondary_events: true,
      port: 1337,
      bridge_port_end: 1347,
      msgpack_port: 1338,
      ws_port_start: 6463,
      ws_port_end: 6472,
      scan_interval_secs: 5,
      db_url: None,
      enable_db_update: false,
      initial_db_etag: None,
      initial_db_content_hash: None,
      ignored_ids: Vec::new(),
      exclusions_url: None,
    }
  }
}

#[derive(Clone)]
pub(crate) struct Connectors {
  process_server: Arc<Mutex<ProcessServer>>,
  client_connector: Arc<Mutex<ClientConnector>>,
  ipc_connector: Arc<Mutex<IpcConnector>>,
  ws_connector: Arc<Mutex<WebsocketConnector>>,
}

pub struct RPCServer {
  /// Parsed database. Ownership moves to the `ProcessServer` on [`start`](RPCServer::start)
  /// (single generation alive, never duplicated); after that this is empty
  /// and [`detect_once`](RPCServer::detect_once) on a started server finds
  /// nothing. One-shot users (CLI `--list-detected`, per-tick scanners that
  /// never start) are unaffected: they read it before any `start()`.
  detectable: Arc<Mutex<Vec<Arc<DetectableActivity>>>>,
  connectors: Option<Connectors>,
  config: RPCConfig,
  /// Overrides staged before [`start`](RPCServer::start): applied to the
  /// live scanner on start AND to [`detect_once`](RPCServer::detect_once),
  /// so one-shot diagnostics see exactly what the daemon would.
  staged_overrides: Vec<DetectableActivity>,

  on_process_scan_complete: Option<Arc<Mutex<ProcessCallback>>>,
}

impl RPCServer {
  /// # Errors
  ///
  /// Returns [`RsrpcError::InvalidJson`](crate::error::RsrpcError::InvalidJson)
  /// when the database is not a JSON array of detectable activities.
  pub fn from_json_str(
    detectable: impl AsRef<str>,
    config: RPCConfig,
  ) -> crate::error::Result<Self> {
    // Parse as DetectableActivity vector; invalid JSON is a caller error,
    // propagated (never panics: this is a library constructor).
    let detectable: Vec<DetectableActivity> = serde_json::from_str(detectable.as_ref())
      .map_err(|err: serde_json::Error| crate::error::RsrpcError::InvalidJson { source: err })?;

    Ok(Self::from_parsed(detectable, config))
  }

  /// Create a server from already-parsed activities (no parsing involved,
  /// so infallible): lets callers that already hold a `Vec` — e.g. a
  /// fetched body parsed once for validation — skip the second parse
  /// `from_json_str` would perform.
  pub fn from_parsed(detectable: Vec<DetectableActivity>, config: RPCConfig) -> Self {
    let detectable: Vec<Arc<DetectableActivity>> = detectable.into_iter().map(Arc::new).collect();

    Self {
      detectable: Arc::new(Mutex::new(detectable)),

      // Default to empty servers
      connectors: None,
      config,
      staged_overrides: Vec::new(),

      // Event listeners
      on_process_scan_complete: None,
    }
  }

  /// Create a new RPCServer and read the detectable games list from file.
  /// # Errors
  ///
  /// Returns [`RsrpcError::UnreadableFile`](crate::error::RsrpcError::UnreadableFile)
  /// when the file is missing or unreadable, or `InvalidJson` when it does
  /// not parse.
  pub fn from_file(file: PathBuf, config: RPCConfig) -> crate::error::Result<Self> {
    // Read the detectable games list from file.
    let detectable =
      std::fs::read_to_string(&file).map_err(|err| crate::error::RsrpcError::UnreadableFile {
        path: file.clone(),
        source: err,
      })?;

    Self::from_json_str(detectable.as_str(), config)
  }

  /// Create a new RPCServer using the bundled snapshot of Discord's detectable
  /// games database. This works fully offline.
  /// # Errors
  ///
  /// Essentially infallible (the snapshot is validated at release time);
  /// bubbles `InvalidJson` only if the embedded data is corrupt.
  pub fn from_bundled(config: RPCConfig) -> crate::error::Result<Self> {
    Self::from_json_str(detection::BUNDLED_DETECTABLE, config)
  }

  /// Run a single process scan without starting any threads/connectors.
  /// Used by `--list-detected` diagnostics. Reads the database held by
  /// this server (call before [`start`](RPCServer::start): startup moves
  /// the database to the scanner, leaving this side empty), applies staged
  /// overrides and the ignore-list exactly like the daemon would — what
  /// you see here is what running would publish.
  /// # Errors
  ///
  /// Propagates scan failures (`/proc` unreadable) and poisoned internal
  /// locks as [`RsrpcError`](crate::error::RsrpcError) variants.
  pub fn detect_once(&self) -> crate::error::Result<Vec<DetectedGame>> {
    let (tx, _rx) = QueueGauge::pair();
    // Staged overrides fold into the one-shot build, exactly like the
    // daemon's initial build — what you see here is what running publishes.
    let staged = self.staged_overrides.clone();
    let server = ProcessServer::new_with_custom(
      self
        .detectable
        .lock()
        .map_err(|e| crate::error::RsrpcError::Poisoned("detectable", e.to_string()))?
        .to_vec(),
      staged,
      tx,
      ProcessEventListeners::default(),
      RefreshConfig::default(),
      Vec::new(),
    );
    // Exclusions parity with the daemon: with hourly DB updates on, the
    // daemon filters installers/crash-reporters — fetch the same set
    // best-effort (fail-open) so diagnostics match what running publishes.
    // Without `enable_db_update` the daemon never fetches either (empty
    // set), so skipping here is parity, not a gap.
    if self.config.enable_db_update
      && let Some(url) = self.config.exclusions_url.clone()
    {
      match server::process::fetch_exclusions(&url) {
        Ok(exclusions) => server.set_exclusions(exclusions),
        Err(err) => debug!(
          "[RPC Server] Exclusions fetch failed, diagnostics unfiltered: {}",
          err
        ),
      }
    }

    let mut found = server.scan_for_processes()?;
    // One-shot path: build the set once (the daemon builds it once at startup).
    let ignored: std::collections::HashSet<String> =
      self.config.ignored_ids.iter().cloned().collect();
    found = server::process::apply_ignore_list(found, &ignored);

    Ok(
      found
        .iter()
        .map(|hit| DetectedGame {
          id: hit.entry.id.to_string(),
          name: hit.entry.name.to_string(),
          pid: Some(hit.pid),
        })
        .collect(),
    )
  }

  /// Summarize the held database (entry count, executable count, names).
  /// Like [`detect_once`](RPCServer::detect_once), call this before
  /// [`start`](RPCServer::start): startup moves the database to the
  /// scanner, leaving this side empty.
  ///
  /// # Errors
  ///
  /// Returns [`RsrpcError::Poisoned`](crate::error::RsrpcError::Poisoned)
  /// when the database lock was poisoned by a previous panic.
  pub fn database_summary(&self) -> crate::error::Result<Vec<DetectableSummary>> {
    let detectable = self
      .detectable
      .lock()
      .map_err(|err| crate::error::RsrpcError::Poisoned("detectable", err.to_string()))?;
    Ok(
      detectable
        .iter()
        .map(|entry| DetectableSummary {
          id: entry.id.clone(),
          name: entry.name.clone(),
          executables: entry.executables.as_ref().map(Vec::len).unwrap_or(0),
        })
        .collect(),
    )
  }

  /// Add new detectable processes on-the-fly. Before [`start`](RPCServer::start)
  /// this stages them (applied to the live scanner on start AND to
  /// [`detect_once`](RPCServer::detect_once)); after `start()` it applies
  /// them to the running scanner directly, as before.
  pub fn append_detectables(&mut self, detectable: Vec<DetectableActivity>) {
    if self.connectors.is_none() {
      log!(
        "[RPC Server] Staging {} detectable(s) for start",
        detectable.len()
      );
      self.staged_overrides.extend(detectable);
      return;
    }

    let Some(connectors) = self.connectors.as_mut() else {
      // Unreachable in practice (checked above): kept so a future
      // refactor removing the guard fails safe instead of panicking.
      log!("[RPC Server] Cannot append detectables, connectors are not initialized");
      return;
    };
    connectors
      .process_server
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .append_detectables(detectable);
  }

  /// Remove a detectable process by name.
  pub fn remove_detectable_by_name(&mut self, name: &str) {
    if self.connectors.is_none() {
      log!("[RPC Server] Cannot remove detectable, connectors are not initialized");
      return;
    }

    let Some(connectors) = self.connectors.as_mut() else {
      log!("[RPC Server] Cannot remove detectable, connectors are not initialized");
      return;
    };
    connectors
      .process_server
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .remove_detectable_by_name(name);
  }

  /// Manually trigger a scan for processes. This should be run AFTER start().
  /// Logs (instead of failing) when connectors are missing or the scan
  /// errors; never panics.
  pub fn scan_for_processes(&mut self) {
    if self.connectors.is_none() {
      log!("[RPC Server] Cannot scan processes, connectors are not initialized");
      return;
    }

    let Some(connectors) = self.connectors.as_mut() else {
      log!("[RPC Server] Cannot scan processes, connectors are not initialized");
      return;
    };
    let process_server = connectors
      .process_server
      .lock()
      .unwrap_or_else(|e| e.into_inner());

    match process_server.scan_for_processes() {
      Ok(_) => {}
      Err(err) => {
        log!("[RPC Server] Error while scanning processes: {}", err);
      }
    }
  }

  pub fn on_process_scan_complete(
    &mut self,
    callback: impl FnMut(ProcessScanState) + Send + Sync + 'static,
  ) {
    if self.connectors.is_some() {
      warn!("[RPC Server] Cannot set on_process_scan_complete, connectors are already initialized");
      return;
    }

    self.on_process_scan_complete = Some(Arc::new(Mutex::new(callback)));
  }

  /// Move the parsed database out for the scanner, leaving this server
  /// empty. Single ownership by construction: cloning here would pin a
  /// second live generation beside the scanner's (the retained memory the
  /// hourly rebuilds used to accumulate).
  fn take_detectables(&mut self) -> Vec<Arc<DetectableActivity>> {
    std::mem::take(&mut *self.detectable.lock().unwrap_or_else(|e| e.into_inner()))
  }
  /// Binds bridge ports and spawns every connector thread.
  ///
  /// # Errors
  ///
  /// Returns [`RsrpcError`](crate::error::RsrpcError) when no bridge,
  /// websocket or IPC socket in the configured ranges can be bound
  /// ([`RsrpcError::IpcBind`](crate::error::RsrpcError::IpcBind) /
  /// [`RsrpcError::WsBind`](crate::error::RsrpcError::WsBind) keep the
  /// last `io::Error` as source), or a poisoned internal lock is met.
  /// No process is killed: the caller (e.g. `cli/src/main.rs`) decides
  /// whether to exit. Never panics.
  pub fn start(&mut self) -> crate::error::Result<()> {
    let (proc_event_sender, proc_event_receiver) = QueueGauge::pair();
    let proc_gauge = proc_event_sender.gauge();
    // Bounded IPC queue (backpressure, not drops): a spinning local
    // client throttles to bridge speed on a full queue instead of growing
    // it without bound. 64 absorbs any legitimate burst; ordering and
    // clears are preserved (blocking sender, never a drop). Each IPC
    // connection runs its own handler thread, so a stalled sender only
    // ever stalls that same connection — no shared resource is held.
    // The WebSocket leg deliberately stays unbounded: its single poll
    // thread serves every client, so blocking it on a full queue would
    // stall handshakes, echoes and disconnects for innocent clients too
    // (pre-change behavior, which never exhibited growth: the bridge
    // drains continuously and `Responder::send` never blocks).
    let (ipc_event_sender, ipc_event_receiver) = mpsc::sync_channel(EVENT_QUEUE_BOUND);
    let (ws_event_sender, ws_event_reciever) = QueueGauge::pair();
    let ws_gauge = ws_event_sender.gauge();
    // Filled by `ProcessServer::start` once the watcher spawns (stays
    // zero when the watcher is disabled or unsupported).
    let watch_slot = Arc::new(Mutex::new(QueueGauge::new()));

    // Shared READY identity (startup RSRPC_USER_* + runtime SET_USER).
    let user = Arc::new(Mutex::new(RpcUser::from_env()));

    // Bind the edge connectors first: their bound addresses feed the
    // bridge's state snapshot.
    let ipc_connector = IpcConnector::new(ipc_event_sender, user.clone())?;
    let ws_connector = WebsocketConnector::new(
      ws_event_sender,
      self.config.ws_port_start,
      self.config.ws_port_end,
      user.clone(),
    )?;
    let mut client_connector = ClientConnector::new(
      self.config.port,
      self.config.bridge_port_end,
      self.config.msgpack_port,
      user,
      ipc_event_receiver,
      proc_event_receiver,
      ws_event_reciever,
    )?;
    client_connector.set_extra_servers(ws_connector.bound_port, Some(ipc_connector.socket_path()));

    // Resource census handles: every handle is a shared Arc, so snapshots
    // read the live state no matter when they run. The watch slot fills
    // once `ProcessServer::start` spawns the watcher below.
    let stats = StatsCtx {
      watch: watch_slot,
      proc_events: proc_gauge,
      ws_events: ws_gauge,
      bridge_json: client_connector.json_clients.clone(),
      bridge_msgpack: client_connector.msgpack_clients.clone(),
      ws_clients: ws_connector.clients.clone(),
    };
    client_connector.set_stats_ctx(stats.clone());

    // Staged overrides fold into the initial scanner build below: one
    // automaton construction per boot instead of build-then-rebuild.
    let staged = std::mem::take(&mut self.staged_overrides);
    if !staged.is_empty() {
      log!(
        "[RPC Server] Folding {} staged override(s) into the initial build",
        staged.len()
      );
    }
    // Hourly refresh inputs travel as one value (see `RefreshConfig`).
    let refresh = RefreshConfig {
      db_url: self.config.db_url.clone(),
      enable: self.config.enable_db_update,
      etag: self.config.initial_db_etag.clone(),
      content_hash: self.config.initial_db_content_hash,
      exclusions_url: self.config.exclusions_url.clone(),
    };
    let connectors = Connectors {
      process_server: Arc::new(Mutex::new(ProcessServer::new_with_custom(
        // Move, never clone: a second live generation here is exactly the
        // retained ~30MB the hourly rebuilds used to pin down.
        self.take_detectables(),
        // Staged overrides fold into the initial build: one automaton
        // construction per boot instead of build-then-rebuild.
        staged,
        proc_event_sender,
        ProcessEventListeners {
          on_process_scan_complete: self.on_process_scan_complete.clone(),
        },
        refresh,
        self.config.ignored_ids.clone(),
      ))),
      client_connector: Arc::new(Mutex::new(client_connector)),
      ipc_connector: Arc::new(Mutex::new(ipc_connector)),
      ws_connector: Arc::new(Mutex::new(ws_connector)),
    };

    log!(
      "[RPC Server] Starting client connector on port {}...",
      connectors
        .client_connector
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .port
    );
    log!(
      "[RPC Server] MessagePack bridge on port {}",
      connectors
        .client_connector
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .msgpack_port
    );
    connectors
      .client_connector
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .start();

    let config = self.config.clone();

    if config.enable_ipc_connector {
      log!("[RPC Server] Starting IPC connector...");
      connectors
        .ipc_connector
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .start();
    }

    if config.enable_process_scanner {
      log!("[RPC Server] Starting process server...");
      connectors
        .process_server
        .lock()
        .map_err(|e| crate::error::RsrpcError::Poisoned("process_server", e.to_string()))?
        .set_proc_events(config.enable_proc_events);
      connectors
        .process_server
        .lock()
        .map_err(|e| crate::error::RsrpcError::Poisoned("process_server", e.to_string()))?
        .start(
          std::time::Duration::from_secs(config.scan_interval_secs),
          &stats.watch,
        );
    }
    // Staged overrides were already folded into the initial scanner
    // build above (`new_with_custom`): post-start `append_detectables`
    // still rebuilds the live scanner directly when called later.

    if config.enable_websocket_connector || config.enable_secondary_events {
      log!("[RPC Server] Starting websocket connector...");
      connectors
        .ws_connector
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .start(
          config.enable_websocket_connector,
          config.enable_secondary_events,
        );
    }

    log!("[RPC Server] Done! Watching for activity...");
    log!("{}", stats.snapshot("boot"));
    // Best-effort: a census thread must never fail boot.
    if let Err(err) = std::thread::Builder::new()
      .name("rsrpc-stats".to_string())
      .spawn(move || {
        log!("{}", stats.snapshot("hourly-tick"));
        loop {
          std::thread::sleep(std::time::Duration::from_secs(STATS_INTERVAL_SECS));
          log!("{}", stats.snapshot("hourly"));
        }
      })
    {
      warn!(
        "[RPC Server] Stats thread failed to spawn ({}), continuing without census",
        err
      );
    }

    self.connectors = Some(connectors);
    Ok(())
  }
}
