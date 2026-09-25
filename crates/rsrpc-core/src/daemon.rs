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
use rsrpc_detect::runtime::ShutdownHandle;
use rsrpc_detect::server::ProcessServer;
use rsrpc_detect::types::{ProcessCallback, ProcessEventListeners, ProcessScanState};
use rsrpc_protocol::error::{Result, RsrpcError};
use rsrpc_telemetry::QueueGauge;
use rsrpc_transport_ipc::IpcTransport;
use rsrpc_transport_ws::{WsTransport, WsTransportConfig};
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;
use tokio::sync::mpsc;

use crate::config::RPCConfig;
use crate::database::{DetectableSummary, DetectedGame, summarize};

/// Scanner event channel bound: per-producer backpressure, never
/// unbounded growth. Raised from the legacy 64 to absorb scan bursts.
const PROC_CHANNEL_BOUND: usize = 512;

/// The daemon: parsed database plus staged overrides, pre-run.
pub struct Daemon {
  detectable: Vec<Arc<DetectableActivity>>,
  config: RPCConfig,
  staged_overrides: Vec<DetectableActivity>,
  on_scan_complete: Option<Arc<Mutex<ProcessCallback>>>,
}

impl std::fmt::Debug for Daemon {
  /// Entry counts only; database contents stay out of logs.
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
    // One-shot diagnostic: throwaway reuse buffers (no hot loop here,
    // so nothing is retained — diagnostics observe production behavior).
    let mut processes = Vec::new();
    let mut scratch = rsrpc_detect::scan::ExecScratch::default();
    let mut match_scratch = rsrpc_detect::scan::MatchScratch::default();
    let mut found = server.scan_for_processes(&mut processes, &mut scratch, &mut match_scratch)?;
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

  /// Held database entry count, without materializing the summary.
  #[must_use]
  pub fn database_len(&self) -> usize {
    self.detectable.len()
  }

  /// Run the daemon until `shutdown` completes: scanner threads, both
  /// transports and the bridge on the caller's Tokio runtime, torn down
  /// in reverse order afterwards.
  ///
  /// Must be called within a Tokio runtime. Teardown is total: after
  /// the bridge and both transports stop, the scanner is shut down and
  /// joined (directed shutdown — no orphaned scan/refresh/watcher
  /// threads), then the server handle is dropped so the event pump's
  /// channel closes, and the pump itself is joined. When `run_until`
  /// returns, no rsRPC thread is still running: the CLI relies on this
  /// implicitly at process exit, and library callers may run another
  /// daemon afterwards. Bind failures tear the scanner down the same
  /// way: no path after `start_scanner` leaks threads.
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
    // frame until shutdown, when it is shut down, joined and dropped.
    let (scanner, proc_rx, proc_tx, pump) = self.start_scanner(db, staged).await?;
    // Abandonment guard: dropping this future (`select!`/timeout/abort)
    // still signals scanner shutdown; normal completion disarms it and
    // joins through `teardown_scanner` below. Every path — bind
    // failures included — tears the scanner down: `serve` owns the bind
    // flow, teardown always runs here.
    let mut cancel_guard = CancelGuard::disarmed();
    if let Some(server) = scanner.as_ref() {
      cancel_guard.arm(server);
    }
    let result = self.serve(proc_rx, proc_tx, user, shutdown).await;
    Self::teardown_scanner(scanner, pump).await;
    cancel_guard.disarm();
    result
  }

  /// Bind transports + bridge, serve until `shutdown`, stop the async
  /// legs. Partial binds clean themselves on drop (transports close
  /// sockets and abort tasks); scanner teardown is the caller's job so
  /// it runs on success AND error.
  async fn serve(
    &self,
    proc_rx: mpsc::Receiver<ProcInput>,
    proc_tx: Option<mpsc::Sender<ProcInput>>,
    user: Arc<Mutex<RpcUser>>,
    shutdown: impl Future<Output = ()> + Send + 'static,
  ) -> Result<()> {
    let mut ipc_transport = None;
    let mut game_transport = None;

    if self.config.enable_ipc_connector {
      let (transport, rx) = IpcTransport::bind(Arc::clone(&user)).await?;
      tracing::info!("[daemon] IPC transport on {}", transport.socket_path());
      ipc_transport = Some((transport, rx));
    }
    // Each flag disables only its own command class (mapped below); the
    // transport binds while either class is wanted, and stays down only
    // when both are off.
    if self.config.enable_websocket_connector || self.config.enable_secondary_events {
      let ws_config = ws_transport_config(&self.config);
      let (transport, rx) = WsTransport::bind(ws_config, Arc::clone(&user)).await?;
      tracing::info!("[daemon] Game transport on port {}", transport.bound_port());
      game_transport = Some((transport, rx));
    }

    let (ipc_transport, ipc_rx) = split_option(ipc_transport);
    let (game_transport, game_rx) = split_option(game_transport);
    let bridge_config = bridge_config_for(
      &self.config,
      game_transport
        .as_ref()
        .map(|transport| transport.bound_port()),
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

    // Reverse-order teardown of the async legs: consumers first,
    // producers last. Dropping the bridge ends its pumps; transports
    // close their listeners. Scanner teardown is the caller's job.
    bridge.shutdown().await;
    if let Some(transport) = game_transport {
      transport.shutdown().await;
    }
    if let Some(transport) = ipc_transport {
      transport.shutdown().await;
    }
    Ok(())
  }

  /// Stop scanner threads + pump on every `run_until` exit path
  /// (success and bind failures): signal inline, join off the async
  /// worker — blocking joins must never park a Tokio worker. Dropping
  /// the server closes the pump channel; the pump drains right after.
  async fn teardown_scanner(
    scanner: Option<ProcessServer>,
    pump: Option<std::thread::JoinHandle<()>>,
  ) {
    if let Some(scanner) = scanner {
      scanner.shutdown();
      let teardown = tokio::task::spawn_blocking(move || {
        scanner.join();
        drop(scanner);
        if let Some(pump) = pump
          && pump.join().is_err()
        {
          tracing::warn!("[daemon] scanner pump thread panicked during join");
        }
      });
      if teardown.await.is_err() {
        tracing::warn!("[daemon] scanner teardown task panicked during join");
      }
    }
  }

  /// Build and start the sync scanner, returning its handle (shut down,
  /// joined and dropped by the caller during teardown), its async event
  /// stream, a sender clone for census queue-depth sampling (`None` when
  /// scanning is off), and the pump thread handle (joined after the
  /// server handle drops and closes the event channel; `None` when
  /// scanning is off).
  ///
  /// The automaton build runs in `spawn_blocking` (seconds of CPU);
  /// the pump thread translates scanner events and exits when the bridge
  /// drops the channel (shutdown).
  ///
  /// # Errors
  ///
  /// [`RsrpcError::InvalidConfig`] for a zero `scan_interval_secs` (a zero
  /// cadence would hot-spin the scan loop): checked before the build.
  async fn start_scanner(
    &self,
    db: Vec<Arc<DetectableActivity>>,
    staged: Vec<DetectableActivity>,
  ) -> Result<(
    Option<ProcessServer>,
    mpsc::Receiver<ProcInput>,
    Option<mpsc::Sender<ProcInput>>,
    Option<std::thread::JoinHandle<()>>,
  )> {
    if self.config.enable_process_scanner && self.config.scan_interval_secs == 0 {
      return Err(RsrpcError::InvalidConfig(
        "scan_interval_secs must be non-zero",
      ));
    }
    let (proc_tx, proc_rx) = mpsc::channel(PROC_CHANNEL_BOUND);
    if !self.config.enable_process_scanner {
      // No scanning: pre-closed stream, the bridge pump exits at once.
      // (The legacy no-scan path never built automata either.)
      drop(proc_tx);
      return Ok((None, proc_rx, None, None));
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
    // The handle is joined during teardown, after the server handle
    // drops and closes the event channel.
    let census_tx = proc_tx.clone();
    let pump = std::thread::Builder::new()
      .name("rsrpc-pump".to_string())
      .spawn(move || {
        while let Ok(event) = scan_rx.recv() {
          let input = match event {
            rsrpc_detect::ProcessDetectedEvent::Detected(hit) => ProcInput::Detected(ScannedGame {
              id: rsrpc_types::AppId::from(&*hit.entry.id),
              name: hit.entry.name.to_string(),
              pid: hit.pid,
              start: hit.start,
            }),
            rsrpc_detect::ProcessDetectedEvent::Cleared => ProcInput::Cleared,
            rsrpc_detect::ProcessDetectedEvent::Removed { id, pid } => {
              ProcInput::Removed(rsrpc_types::AppId::from(&*id), pid)
            }
          };
          if proc_tx.blocking_send(input).is_err() {
            break;
          }
        }
      })
      .expect("scanner pump spawn failed");
    Ok((Some(server), proc_rx, Some(census_tx), Some(pump)))
  }
}

/// Signals scanner shutdown if `run_until`'s future is dropped before
/// teardown (caller abandonment via `select!`/timeout/abort): threads
/// exit on their own within one poll interval instead of leaking.
/// Holds only the signaling handle (no channel sender), so it can never
/// keep the event pump alive itself. Disarmed on the normal path before
/// returning.
struct CancelGuard {
  handle: Option<ShutdownHandle>,
}

impl CancelGuard {
  fn disarmed() -> Self {
    Self { handle: None }
  }

  fn arm(&mut self, server: &ProcessServer) {
    self.handle = Some(server.shutdown_handle());
  }

  fn disarm(&mut self) {
    self.handle = None;
  }
}

impl Drop for CancelGuard {
  fn drop(&mut self) {
    if let Some(handle) = self.handle.take() {
      handle.signal();
    }
  }
}

/// Map daemon connector flags onto the game transport config: each flag
/// gates only its own command class (`SET_ACTIVITY` vs secondary
/// browser/deep-link/callback commands).
fn ws_transport_config(config: &RPCConfig) -> WsTransportConfig {
  WsTransportConfig::new(config.ws_port_start, config.ws_port_end)
    .set_activity(config.enable_websocket_connector)
    .secondary_events(config.enable_secondary_events)
}

/// Bridge config from the daemon config: port ranges, version stamp and
/// the bound transport metadata; `--state-file` additionally points the
/// bridge at the temp-dir snapshot slots (`<tmpdir>/rsrpc-state-{0..9}`).
fn bridge_config_for(
  config: &RPCConfig,
  ws_port: Option<u16>,
  ipc_path: Option<String>,
) -> BridgeConfig {
  let bridge_config = BridgeConfig::new(
    config.port,
    config.bridge_port_end,
    config.msgpack_port,
    config.msgpack_port.saturating_add(10),
  )
  .app_version(config.app_version.clone())
  .ws_port(ws_port)
  .ipc_path(ipc_path);
  if config.state_file {
    bridge_config.state_dir(std::env::temp_dir())
  } else {
    bridge_config
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

#[cfg(test)]
mod tests {
  use super::*;

  /// Each daemon flag disables only its own command class; disabling both
  /// is expressed by not binding at all (see `start_scanner`).
  #[test]
  fn ws_config_maps_each_connector_flag_to_its_class() {
    let cfg = ws_transport_config(&RPCConfig::default());
    assert!(cfg.set_activity);
    assert!(cfg.secondary_events);

    let no_secondary = RPCConfig {
      enable_secondary_events: false,
      ..RPCConfig::default()
    };
    let cfg = ws_transport_config(&no_secondary);
    assert!(cfg.set_activity);
    assert!(!cfg.secondary_events);

    let no_ws = RPCConfig {
      enable_websocket_connector: false,
      ..RPCConfig::default()
    };
    let cfg = ws_transport_config(&no_ws);
    assert!(!cfg.set_activity);
    assert!(cfg.secondary_events);
  }

  /// `--state-file` routes bridge snapshots to the temp-dir slots; the
  /// default keeps snapshots off so ordinary runs never write them.
  #[test]
  fn state_file_routes_bridge_snapshots_to_tmpdir() {
    let off = bridge_config_for(&RPCConfig::default(), None, None);
    assert!(off.state_dir.is_none());

    let on = bridge_config_for(
      &RPCConfig {
        state_file: true,
        ..RPCConfig::default()
      },
      None,
      None,
    );
    assert_eq!(on.state_dir, Some(std::env::temp_dir()));
  }
}
