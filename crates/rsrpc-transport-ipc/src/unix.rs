//! Unix IPC server: async accept loop, blocking pumps in a [`JoinSet`].
//!
//! The listener isTokio-native (no 50ms poll sleep); each connection runs
//! `handle_stream` in `spawn_blocking` (blocking socket I/O must never sit
//! on an async worker). Shutdown cancels the accept loop, drains the set
//! with a deadline, then aborts stragglers.

use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::{
  Arc, Mutex,
  atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use rsrpc_protocol::error::{Result, RsrpcError};
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::frame::{IpcFacilitator, handle_stream};
use crate::paths::{
  fanout_socket_link, remove_socket_links, socket_dir_candidates, socket_file_name,
};
use crate::probe::socket_holder_alive;
use crate::sink::{DEFAULT_IPC_QUEUE, EventSink};

/// Upper bound for graceful connection drain in [`IpcTransport::shutdown`].
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-connection protocol state plus shared handles.
struct ConnFacilitator {
  handshake: bool,
  client_id: String,
  pid: u64,
  nonce: String,
  user: Arc<Mutex<RpcUser>>,
  sink: EventSink,
  /// Pids published on this connection, oldest first (bounded).
  published_pids: Vec<u64>,
}

impl ConnFacilitator {
  /// Blank per-connection state (handshake pending, no pid yet).
  fn fresh(user: Arc<Mutex<RpcUser>>, sink: EventSink) -> Self {
    Self {
      handshake: false,
      client_id: String::new(),
      pid: 0,
      nonce: String::new(),
      user,
      sink,
      published_pids: Vec::new(),
    }
  }
}

impl IpcFacilitator for ConnFacilitator {
  /// Whether the handshake completed on this connection.
  fn handshake(&self) -> bool {
    self.handshake
  }
  /// Record handshake completion.
  fn set_handshake(&mut self, handshake: bool) {
    self.handshake = handshake;
  }
  /// Application id from the handshake (empty until then).
  fn client_id(&self) -> String {
    self.client_id.clone()
  }
  /// Store the handshake application id.
  fn set_client_id(&mut self, client_id: String) {
    self.client_id = client_id;
  }
  /// Last pid seen on this connection (0 until the first activity).
  fn pid(&self) -> u64 {
    self.pid
  }
  /// Store the latest activity pid.
  fn set_pid(&mut self, pid: u64) {
    self.pid = pid;
  }
  /// Latest command nonce (lock-step replies echo it back).
  fn nonce(&self) -> String {
    self.nonce.clone()
  }
  /// Store the latest command nonce.
  fn set_nonce(&mut self, nonce: String) {
    self.nonce = nonce;
  }
  /// Current identity rendered as the READY payload.
  fn user_payload(&self) -> String {
    self
      .user
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .ready_payload()
  }
  /// Snapshot of the current identity for `GET_USER` answers.
  fn current_user(&self) -> RpcUser {
    self.user.lock().unwrap_or_else(|e| e.into_inner()).clone()
  }
  /// Downstream sink for validated commands and clears.
  fn sink(&self) -> &EventSink {
    &self.sink
  }
  /// Remember a published pid for disconnect cleanup (bounded history).
  fn note_published_pid(&mut self, pid: u64) {
    crate::frame::track_pid(&mut self.published_pids, pid);
  }
  /// Drain the published-pid history for disconnect clears.
  fn take_published_pids(&mut self) -> Vec<u64> {
    std::mem::take(&mut self.published_pids)
  }
}

/// Unix Discord IPC transport.
pub struct IpcTransport {
  token: CancellationToken,
  accept_task: Option<JoinHandle<()>>,
  conns: Arc<tokio::sync::Mutex<JoinSet<()>>>,
  /// Live connection sockets (one clone per pump): shutdown closes them
  /// so pumps parked in blocking reads exit instead of outliving the
  /// drain deadline (`abort_all` cannot interrupt a running closure).
  live: Arc<Mutex<Vec<(u64, std::os::unix::net::UnixStream)>>>,
  bound_path: String,
  dirs: Vec<PathBuf>,
  sink: EventSink,
}

impl std::fmt::Debug for IpcTransport {
  /// Bound path only; live sockets stay out of logs.
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("IpcTransport")
      .field("bound_path", &self.bound_path)
      .finish_non_exhaustive()
  }
}

impl IpcTransport {
  /// Bind `discord-ipc-0..=9` in the official candidate dirs.
  ///
  /// # Errors
  ///
  /// [`RsrpcError::IpcBind`] when every index is held by a live holder.
  pub async fn bind(user: Arc<Mutex<RpcUser>>) -> Result<(Self, mpsc::Receiver<ActivityCmd>)> {
    Self::bind_with_dirs(user, socket_dir_candidates()).await
  }

  /// Bind within explicit `dirs` (first = primary; tests point here at a
  /// scratch dir instead of the real runtime dirs).
  ///
  /// # Errors
  ///
  /// [`RsrpcError::IpcBind`] when every index is held by a live holder.
  pub async fn bind_with_dirs(
    user: Arc<Mutex<RpcUser>>,
    dirs: Vec<PathBuf>,
  ) -> Result<(Self, mpsc::Receiver<ActivityCmd>)> {
    let base = dirs
      .first()
      .map(|dir| format!("{}/discord-ipc", dir.display()))
      .unwrap_or_else(|| "/tmp/discord-ipc".to_string());
    // Blocking syscalls (bind, probe with its 1s budget, symlink fan-out)
    // run off the async workers; this executes once per (re)bind.
    let dirs_for_bind = dirs.clone();
    let (std_listener, bound_path) =
      tokio::task::spawn_blocking(move || create_socket(&base, &dirs_for_bind))
        .await
        .map_err(|_| RsrpcError::IpcBind {
          attempts: 10,
          source: std::io::Error::other("bind task panicked"),
        })??;
    std_listener
      .set_nonblocking(true)
      .map_err(|source| RsrpcError::IpcBind {
        attempts: 10,
        source,
      })?;
    let listener = UnixListener::from_std(std_listener).map_err(|source| RsrpcError::IpcBind {
      attempts: 10,
      source,
    })?;

    let (sink, rx) = EventSink::bounded(DEFAULT_IPC_QUEUE);
    let token = CancellationToken::new();
    let conns = Arc::new(tokio::sync::Mutex::new(JoinSet::new()));
    let live = Arc::new(Mutex::new(Vec::new()));
    let next_conn_id = Arc::new(AtomicU64::new(1));
    let accept_task = tokio::spawn(accept_loop(AcceptCtx {
      listener,
      token: token.clone(),
      conns: Arc::clone(&conns),
      live: Arc::clone(&live),
      next_conn_id: Arc::clone(&next_conn_id),
      user,
      sink: sink.clone(),
    }));

    Ok((
      Self {
        token,
        accept_task: Some(accept_task),
        conns,
        live,
        bound_path,
        dirs,
        sink,
      },
      rx,
    ))
  }

  /// Filesystem path of the bound socket (for snapshots/diagnostics).
  #[must_use]
  pub fn socket_path(&self) -> &str {
    &self.bound_path
  }

  /// Commands shed by a full/closed sink since bind.
  #[must_use]
  pub fn dropped_total(&self) -> u64 {
    self.sink.dropped_total()
  }

  /// Shared event sink for census queue-depth sampling.
  #[must_use]
  pub fn event_sink(&self) -> EventSink {
    self.sink.clone()
  }

  /// Graceful shutdown: stop accepting, unblock connection pumps,
  /// drain connections with a deadline, then remove the socket file and
  /// fan-out links (via [`Drop`]).
  pub async fn shutdown(mut self) {
    self.token.cancel();
    if let Some(task) = self.accept_task.take() {
      let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }
    // Close every live socket first: pumps parked in blocking reads exit
    // at once instead of holding their tasks past the drain deadline
    // (`abort_all` cannot interrupt a running closure).
    for (_, stream) in self
      .live
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .drain(..)
    {
      let _ = stream.shutdown(std::net::Shutdown::Both);
    }
    let mut owned = {
      let mut guard = self.conns.lock().await;
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
  }
}

impl Drop for IpcTransport {
  /// Abort a still-running accept task so drops never leak it.
  fn drop(&mut self) {
    if let Some(task) = &self.accept_task {
      task.abort();
    }
    // Best-effort filesystem cleanup (mirrors the legacy Drop): never
    // touch foreign files (same shape check inside).
    tracing::info!("[ipc] Cleaning up socket: {}", self.bound_path);
    remove_socket_links(&self.dirs, &self.bound_path);
    let _ = std::fs::remove_file(&self.bound_path);
  }
}

/// Bind `base-0..=9` with stale reclaim, then fan out links.
fn create_socket(
  base: &str,
  dirs: &[PathBuf],
) -> Result<(std::os::unix::net::UnixListener, String)> {
  let mut last_err = None;
  for tries in 0..=9_u8 {
    let socket_path = format!("{base}-{tries}");
    tracing::info!("[ipc] Creating socket: {socket_path}");
    match std::os::unix::net::UnixListener::bind(&socket_path) {
      Ok(socket) => {
        tracing::info!("[ipc] Created IPC socket: {socket_path}");
        fanout_socket_link(dirs, &socket_path, &socket_file_name(&socket_path));
        return Ok((socket, socket_path));
      }
      Err(err) => {
        if err.kind() == ErrorKind::AddrInUse {
          tracing::info!("[ipc] Socket {socket_path} already in use, checking if stale...");
          if socket_holder_alive(&socket_path) {
            tracing::warn!("[ipc] Socket {socket_path} is in use by another process");
          } else {
            tracing::warn!("[ipc] Socket {socket_path} is stale, removing and retrying...");
            let _ = std::fs::remove_file(&socket_path);
            match std::os::unix::net::UnixListener::bind(&socket_path) {
              Ok(socket) => {
                tracing::info!("[ipc] Created IPC socket after cleaning stale: {socket_path}");
                fanout_socket_link(dirs, &socket_path, &socket_file_name(&socket_path));
                return Ok((socket, socket_path));
              }
              Err(retry_err) => {
                tracing::warn!("[ipc] Rebind after stale-clean failed: {retry_err}");
              }
            }
          }
        } else {
          tracing::warn!("[ipc] Failed to create IPC socket, trying next: {err}");
        }
        last_err = Some(err);
      }
    }
  }
  Err(RsrpcError::IpcBind {
    attempts: 10,
    source: last_err.unwrap_or_else(|| std::io::Error::other("no socket bound")),
  })
}

/// Accept-loop state (moved into the accept task).
struct AcceptCtx {
  listener: UnixListener,
  token: CancellationToken,
  conns: Arc<tokio::sync::Mutex<JoinSet<()>>>,
  live: Arc<Mutex<Vec<(u64, std::os::unix::net::UnixStream)>>>,
  next_conn_id: Arc<AtomicU64>,
  user: Arc<Mutex<RpcUser>>,
  sink: EventSink,
}

/// Accept loop: spawn one pump per peer until token cancel (shutdown).
async fn accept_loop(
  AcceptCtx {
    listener,
    token,
    conns,
    live,
    next_conn_id,
    user,
    sink,
  }: AcceptCtx,
) {
  loop {
    tokio::select! {
      biased;
      () = token.cancelled() => break,
      accepted = listener.accept() => {
        let (tok_stream, _) = match accepted {
          Ok(pair) => pair,
          Err(err) => {
            tracing::warn!("[ipc] Accept failed: {err}");
            continue;
          }
        };
        let std_stream = match tok_stream.into_std() {
          Ok(stream) => stream,
          Err(err) => {
            tracing::warn!("[ipc] Failed to hand stream to blocking pump: {err}");
            continue;
          }
        };
        // tokio sockets are non-blocking; the pump does blocking I/O.
        if let Err(err) = std_stream.set_nonblocking(false) {
          tracing::warn!("[ipc] Failed to restore blocking mode: {err}");
          continue;
        };
        tracing::debug!("[ipc] Incoming stream...");
        let facil = ConnFacilitator::fresh(user.clone(), sink.clone());
        // Track one clone per pump so shutdown can close it (unblocking
        // the pump's read); the pump unregisters itself on exit, bounding
        // the list during normal operation.
        let conn_id = next_conn_id.fetch_add(1, Ordering::Relaxed);
        if let Ok(probe) = std_stream.try_clone() {
          live
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((conn_id, probe));
        }
        let live_exit = Arc::clone(&live);
        // Short critical section: spawn_blocking is synchronous. Exited
        // tasks are reaped here so connection churn cannot pin JoinSet
        // entries for the transport lifetime.
        let mut conns = conns.lock().await;
        while conns.try_join_next().is_some() {}
        conns.spawn_blocking(move || {
          let mut facil = facil;
          let mut stream = std_stream;
          handle_stream(&mut facil, &mut stream);
          live_exit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(id, _)| *id != conn_id);
        });
      }
    }
  }
}
