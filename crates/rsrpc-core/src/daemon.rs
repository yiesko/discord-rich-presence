//! Daemon: database ownership, one-shot diagnostics, async run.
//!
//! [`Daemon`] holds the parsed database plus staged overrides and either
//! answers diagnostics (`detect_once`, `database_summary`) or consumes
//! itself into the running world (`run_until`): scanner threads, both
//! transports and the bridge on one Tokio runtime, torn down in reverse
//! on shutdown.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use rsrpc_bridge::{Bridge, BridgeConfig, BridgeInputs, ProcInput, ScannedGame};
use rsrpc_detect::db::DetectableActivity;
use rsrpc_detect::refresh::RefreshConfig;
use rsrpc_detect::server::ProcessServer;
use rsrpc_detect::types::{ProcessCallback, ProcessEventListeners, ProcessScanState};
use rsrpc_protocol::error::Result;
use rsrpc_telemetry::QueueGauge;
use rsrpc_transport_ipc::IpcTransport;
use rsrpc_transport_ws::{WsTransport, WsTransportConfig};
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;
use tokio::sync::mpsc;

use crate::config::RPCConfig;
use crate::database::{DetectableSummary, DetectedGame, summarize};

/// Scanner event channel bound (matches the legacy 64: backpressure per
/// producer, never unbounded growth).
const PROC_CHANNEL_BOUND: usize = 512;

/// The daemon: parsed database plus staged overrides, pre-run.
pub struct Daemon {
  detectable: Vec<Arc<DetectableActivity>>,
  config: RPCConfig,
  staged_overrides: Vec<DetectableActivity>,
  on_scan_complete: Option<Arc<Mutex<ProcessCallback>>>,
}

impl std::fmt::Debug for Daemon {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Daemon")
      .field("entries", &self.detectable.len())
      .field("staged", &self.staged_overrides.len())
      .finish_non_exhaustive()
  }
}

impl Daemon {
  /// Create from already-parsed activities (infallible).
  #[must_use]
  pub fn from_parsed(detectable: Vec<DetectableActivity>, config: RPCConfig) -> Self {
    let detectable = detectable.into_iter().map(Arc::new).collect();
    Self {
      detectable,
      config,
      staged_overrides: Vec::new(),
      on_scan_complete: None,
    }
  }

  /// Create from a JSON database body (direct parse, trimmed fallback).
  ///
  /// # Errors
  ///
  /// `InvalidJson` when neither shape parses.
  pub fn from_json_str(body: impl AsRef<str>, config: RPCConfig) -> Result<Self> {
    let (detectable, _) = crate::database::parse_body(body.as_ref())?;
    Ok(Self::from_parsed(detectable, config))
  }

  /// Create from a database file.
  ///
  /// # Errors
  ///
  /// `UnreadableFile` / `InvalidJson` (see [`crate::database::load_file`]).
  pub fn from_file(path: &std::path::Path, config: RPCConfig) -> Result<Self> {
    Ok(Self::from_parsed(crate::database::load_file(path)?, config))
  }

  /// Create from the bundled offline snapshot.
  ///
  /// # Errors
  ///
  /// `InvalidJson` only if the embedded data is corrupt.
  pub fn from_bundled(config: RPCConfig) -> Result<Self> {
    Ok(Self::from_parsed(crate::database::load_bundled()?, config))
  }

  /// Stage overrides applied at [`run_until`](Self::run_until) and visible
  /// to [`detect_once`](Self::detect_once) — what diagnostics see is what
  /// running would publish.
  pub fn append_detectables(&mut self, detectable: Vec<DetectableActivity>) {
    self.staged_overrides.extend(detectable);
  }

  /// Drop staged overrides by game name (pre-run only).
  pub fn remove_detectable_by_name(&mut self, name: &str) {
    self.staged_overrides.retain(|entry| entry.name != name);
  }

  /// Register a scan-tick callback (pre-run only).
  pub fn on_scan_complete(
    &mut self,
    callback: impl FnMut(ProcessScanState) + Send + Sync + 'static,
  ) {
    self.on_scan_complete = Some(Arc::new(Mutex::new(callback)));
  }

  /// Run a single process scan without starting anything. Reads the held
  /// database plus staged overrides and the ignore-list exactly like the
  /// daemon would — what you see here is what running publishes.
  ///
  /// # Errors
  ///
  /// Propagates scan failures (`/proc` unreadable). Exclusions parity
  /// with the daemon: with hourly updates on, the same set is fetched
  /// best-effort (fail-open).
  pub fn detect_once(&self) -> Result<Vec<DetectedGame>> {
    let (tx, _rx) = QueueGauge::pair();
    let server = ProcessServer::new_with_custom(
      self.detectable.to_vec(),
      self.staged_overrides.clone(),
      tx,
      ProcessEventListeners::default(),
      RefreshConfig::default(),
      Vec::new(),
    );
    if self.config.enable_db_update
      && let Some(url) = self.config.exclusions_url.clone()
    {
      match rsrpc_detect::refresh::fetch_exclusions(&url) {
        Ok(exclusions) => server.set_exclusions(exclusions),
        Err(err) => {
          tracing::debug!("[daemon] Exclusions fetch failed, diagnostics unfiltered: {err}")
        }
      }
    }
    let mut found = server.scan_for_processes()?;
    let ignored: HashSet<String> = self.config.ignored_ids.iter().cloned().collect();
    found = rsrpc_detect::scan::apply_ignore_list(found, &ignored);
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

  /// Summarize the held database (entry count, executable counts, names).
  #[must_use]
  pub fn database_summary(&self) -> Vec<DetectableSummary> {
    summarize(&self.detectable)
  }

  /// Run the daemon until `shutdown` completes: scanner threads, both
  /// transports and the bridge on the caller's Tokio runtime, torn down
  /// in reverse order afterwards.
  ///
  /// Must be called within a Tokio runtime. The scanner threads (scan
  /// loop, hourly refresh, proc-events watcher) live for the process
  /// lifetime by design: teardown stops the bridge and both transports,
  /// and the scan/refresh loops exit once their channels close, but the
  /// underlying threads are not joined — for the CLI this ends at
  /// process exit, and library callers should treat one `run_until` per
  /// process as the supported shape.
  ///
  /// # Errors
  ///
  /// Bind failures (`IpcBind`, `WsBind`, `BridgeBind`) surface here;
  /// nothing exits the process — the caller (CLI) decides.
  pub async fn run_until(
    mut self,
    shutdown: impl Future<Output = ()> + Send + 'static,
  ) -> Result<()> {
    let user = Arc::new(Mutex::new(RpcUser::from_env()));

    // Move (never clone) the database into the scanner build: the fat
    // structs convert once to the slim form and drop. Cloning here would
    // pin a second ~25MB generation beside the scanner's, and retaining
    // it afterwards would pin it for the daemon lifetime.
    let db = std::mem::take(&mut self.detectable);
    let staged = std::mem::take(&mut self.staged_overrides);

    // Scanner (sync threads, as today): its events cross into async via
    // a pump thread onto a bounded channel. Started first so game STARTs
    // during transport binds are still observed. The handle lives in this
    // frame until shutdown documents the ownership.
    let (_scanner, proc_rx, proc_tx) = self.start_scanner(db, staged).await;
    let mut ipc_transport = None;
    let mut game_transport = None;

    if self.config.enable_ipc_connector {
      let (transport, rx) = IpcTransport::bind(Arc::clone(&user)).await?;
      tracing::info!("[daemon] IPC transport on {}", transport.socket_path());
      ipc_transport = Some((transport, rx));
    }
    if self.config.enable_websocket_connector || self.config.enable_secondary_events {
      let ws_config = WsTransportConfig::new(self.config.ws_port_start, self.config.ws_port_end);
      let (transport, rx) = WsTransport::bind(ws_config, Arc::clone(&user)).await?;
      tracing::info!("[daemon] Game transport on port {}", transport.bound_port());
      game_transport = Some((transport, rx));
    }

    let (ipc_transport, ipc_rx) = split_option(ipc_transport);
    let (game_transport, game_rx) = split_option(game_transport);
    let bridge_config = BridgeConfig::new(
      self.config.port,
      self.config.bridge_port_end,
      self.config.msgpack_port,
      self.config.msgpack_port.saturating_add(10),
    )
    .app_version(self.config.app_version.clone())
    .ws_port(
      game_transport
        .as_ref()
        .map(|transport| transport.bound_port()),
    )
    .ipc_path(
      ipc_transport
        .as_ref()
        .map(|transport| transport.socket_path().to_string()),
    );
    let bridge = Bridge::bind(
      bridge_config,
      Arc::clone(&user),
      BridgeInputs {
        ipc_rx,
        game_rx,
        proc_rx,
        // Sender clones for census queue-depth sampling; game-client
        // total for the census consumer count. `None` when the leg is off.
        ipc_tx: ipc_transport
          .as_ref()
          .map(|transport| transport.event_sink().sender()),
        game_tx: game_transport
          .as_ref()
          .map(|transport| transport.sink_sender()),
        proc_tx,
        game_clients: game_transport
          .as_ref()
          .map(|transport| transport.client_total()),
      },
    )
    .await?;
    tracing::info!(
      "[daemon] Bridge on {} (msgpack {})",
      bridge.json_port(),
      bridge.msgpack_port()
    );

    shutdown.await;
    tracing::info!("[daemon] Shutting down...");

    // Reverse-order teardown: consumers first, producers last. Dropping
    // the bridge ends its pumps; transports close their listeners; the
    // scanner pump thread exits once its tokio sender fails.
    bridge.shutdown().await;
    if let Some(transport) = game_transport {
      transport.shutdown().await;
    }
    if let Some(transport) = ipc_transport {
      transport.shutdown().await;
    }
    Ok(())
  }

  /// Build and start the sync scanner, returning its handle (held by the
  /// caller for the daemon lifetime), its async event stream, and a
  /// sender clone for census queue-depth sampling (`None` when scanning
  /// is off).
  ///
  /// The automaton build runs in `spawn_blocking` (seconds of CPU);
  /// the pump thread translates scanner events and exits when the bridge
  /// drops the channel (shutdown).
  async fn start_scanner(
    &self,
    db: Vec<Arc<DetectableActivity>>,
    staged: Vec<DetectableActivity>,
  ) -> (
    Option<ProcessServer>,
    mpsc::Receiver<ProcInput>,
    Option<mpsc::Sender<ProcInput>>,
  ) {
    let (proc_tx, proc_rx) = mpsc::channel(PROC_CHANNEL_BOUND);
    if !self.config.enable_process_scanner {
      // No scanning: pre-closed stream, the bridge pump exits at once.
      // (The legacy no-scan path never built automata either.)
      drop(proc_tx);
      return (None, proc_rx, None);
    }
    let (scan_tx, scan_rx) = QueueGauge::pair();
    let refresh = RefreshConfig {
      db_url: self.config.db_url.clone(),
      enable: self.config.enable_db_update,
      etag: self.config.initial_db_etag.clone(),
      content_hash: self.config.initial_db_content_hash,
      exclusions_url: self.config.exclusions_url.clone(),
    };
    let listeners = ProcessEventListeners {
      on_process_scan_complete: self.on_scan_complete.clone(),
    };
    let ignored = self.config.ignored_ids.clone();
    let enable_proc_events = self.config.enable_proc_events;
    let scan_interval = std::time::Duration::from_secs(self.config.scan_interval_secs);
    // Heavy automaton build off the async workers. A build panic is a
    // bug (pure CPU over validated inputs), not a runtime error.
    let server = tokio::task::spawn_blocking(move || {
      let mut server =
        ProcessServer::new_with_custom(db, staged, scan_tx, listeners, refresh, ignored);
      server.set_proc_events(enable_proc_events);
      server
    })
    .await
    .expect("[bug] scanner build panicked");
    // Watch-slot gauge fed the old census; retained for the start() shape
    // until telemetry moves into the core.
    let watch_slot = Arc::new(Mutex::new(QueueGauge::new()));
    server.start(scan_interval, &watch_slot);
    // Pump: scanner events into the async world. `blocking_send` parks
    // this thread (not a worker) under backpressure, like the legacy
    // bounded queue; a closed channel means shutdown — exit quietly.
    let census_tx = proc_tx.clone();
    std::thread::spawn(move || {
      while let Ok(event) = scan_rx.recv() {
        let input = match event.hit {
          Some(hit) => ProcInput::Detected(ScannedGame {
            id: rsrpc_types::AppId::from(&*hit.entry.id),
            name: hit.entry.name.to_string(),
            pid: hit.pid,
            start: hit.start,
          }),
          None => ProcInput::Cleared,
        };
        if proc_tx.blocking_send(input).is_err() {
          break;
        }
      }
    });
    (Some(server), proc_rx, Some(census_tx))
  }
}

/// Split an optional bound transport into handle + receiver, or a
/// pre-closed receiver when the leg is disabled (its pump exits at once).
fn split_option<T>(
  bound: Option<(T, mpsc::Receiver<ActivityCmd>)>,
) -> (Option<T>, mpsc::Receiver<ActivityCmd>) {
  match bound {
    Some((transport, rx)) => (Some(transport), rx),
    None => {
      let (_, rx) = mpsc::channel(1);
      (None, rx)
    }
  }
}
