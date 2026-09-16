//! Windows IPC server: named-pipe listener, same JoinSet lifecycle.
//!
//! The pipe listener is blocking, so accepts run in one `spawn_blocking`
//! loop (non-blocking mode + token check, no unbounded sleep): accepted
//! streams cross a bounded channel to an async dispatcher that pumps them
//! in the shared [`JoinSet`]. Shutdown cancels the loop and drains the set
//! like the Unix server.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use interprocess::local_socket::traits::Listener as _;
use interprocess::local_socket::{Listener, ListenerNonblockingMode, ListenerOptions, ToFsName};
use interprocess::os::windows::local_socket::{ListenerOptionsExt, NamedPipe};
use interprocess::os::windows::security_descriptor::SecurityDescriptor;
use rsrpc_protocol::error::{Result, RsrpcError};
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::frame::{IpcFacilitator, handle_stream};
use crate::sink::{DEFAULT_IPC_QUEUE, EventSink};

/// Upper bound for graceful connection drain in [`IpcTransport::shutdown`].
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Pipe-name base (`\\.\pipe\discord-ipc-0..=9`, Discord convention).
const PIPE_BASE: &str = r"\\.\pipe\discord-ipc";

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
  fn handshake(&self) -> bool {
    self.handshake
  }
  fn set_handshake(&mut self, handshake: bool) {
    self.handshake = handshake;
  }
  fn client_id(&self) -> String {
    self.client_id.clone()
  }
  fn set_client_id(&mut self, client_id: String) {
    self.client_id = client_id;
  }
  fn pid(&self) -> u64 {
    self.pid
  }
  fn set_pid(&mut self, pid: u64) {
    self.pid = pid;
  }
  fn nonce(&self) -> String {
    self.nonce.clone()
  }
  fn set_nonce(&mut self, nonce: String) {
    self.nonce = nonce;
  }
  fn user_payload(&self) -> String {
    self
      .user
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .ready_payload()
  }
  fn current_user(&self) -> RpcUser {
    self.user.lock().unwrap_or_else(|e| e.into_inner()).clone()
  }
  fn sink(&self) -> &EventSink {
    &self.sink
  }
  fn note_published_pid(&mut self, pid: u64) {
    crate::frame::track_pid(&mut self.published_pids, pid);
  }
  fn take_published_pids(&mut self) -> Vec<u64> {
    std::mem::take(&mut self.published_pids)
  }
}

/// Windows Discord IPC transport.
pub struct IpcTransport {
  token: CancellationToken,
  accept_task: Option<JoinHandle<()>>,
  dispatch_task: Option<JoinHandle<()>>,
  conns: Arc<tokio::sync::Mutex<JoinSet<()>>>,
  pipe_path: String,
  sink: EventSink,
}

impl std::fmt::Debug for IpcTransport {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("IpcTransport")
      .field("pipe_path", &self.pipe_path)
      .finish_non_exhaustive()
  }
}

impl IpcTransport {
  /// Bind `\\.\pipe\discord-ipc-0..=9`.
  ///
  /// # Errors
  ///
  /// [`RsrpcError::IpcBind`] when every pipe name is taken.
  pub async fn bind(user: Arc<Mutex<RpcUser>>) -> Result<(Self, mpsc::Receiver<ActivityCmd>)> {
    let (listener, pipe_path) =
      tokio::task::spawn_blocking(create_pipe)
        .await
        .map_err(|_| RsrpcError::IpcBind {
          attempts: 10,
          source: std::io::Error::new(std::io::ErrorKind::Other, "bind task panicked"),
        })??;

    let (sink, rx) = EventSink::bounded(DEFAULT_IPC_QUEUE);
    let (stream_tx, stream_rx) = mpsc::channel::<interprocess::local_socket::Stream>(64);
    let token = CancellationToken::new();
    let conns = Arc::new(tokio::sync::Mutex::new(JoinSet::new()));

    let accept_token = token.clone();
    let accept_task =
      tokio::task::spawn_blocking(move || accept_loop(listener, accept_token, stream_tx));
    let dispatch_task = tokio::spawn(dispatch_loop(DispatchCtx {
      stream_rx,
      token: token.clone(),
      conns: Arc::clone(&conns),
      user,
      sink: sink.clone(),
    }));

    Ok((
      Self {
        token,
        accept_task: Some(accept_task),
        dispatch_task: Some(dispatch_task),
        conns,
        pipe_path,
        sink,
      },
      rx,
    ))
  }

  /// Named-pipe path of the bound socket (for snapshots/diagnostics).
  #[must_use]
  pub fn socket_path(&self) -> &str {
    &self.pipe_path
  }

  /// Commands shed by a full/closed sink since bind.
  #[must_use]
  pub fn dropped_total(&self) -> u64 {
    self.sink.dropped_total()
  }

  /// Graceful shutdown: stop accepting, drain connections with a deadline.
  pub async fn shutdown(mut self) {
    self.token.cancel();
    if let Some(task) = self.accept_task.take() {
      let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }
    if let Some(task) = self.dispatch_task.take() {
      let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
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
  fn drop(&mut self) {
    if let Some(task) = &self.accept_task {
      task.abort();
    }
    if let Some(task) = &self.dispatch_task {
      task.abort();
    }
  }
}

/// Bind the first free pipe name.
fn create_pipe() -> Result<(Listener, String)> {
  for tries in 0..=9_u8 {
    let pipe_path = format!("{PIPE_BASE}-{tries}");
    let listener = ListenerOptions::new()
      .name(pipe_path.clone().to_fs_name::<NamedPipe>()?)
      .security_descriptor(SecurityDescriptor::default());
    match listener.create_sync() {
      Ok(socket) => {
        tracing::info!("[ipc] Created IPC socket: {pipe_path}");
        return Ok((socket, pipe_path));
      }
      Err(err) => {
        tracing::warn!("[ipc] Failed to create IPC socket, trying next: {err}");
        if tries == 9 {
          return Err(RsrpcError::IpcBind {
            attempts: 10,
            source: err,
          });
        }
      }
    }
  }
  Err(RsrpcError::IpcBind {
    attempts: 10,
    source: std::io::Error::new(std::io::ErrorKind::Other, "no pipe bound"),
  })
}

/// Blocking accept loop: non-blocking accepts with a token check between
/// polls, forwarding streams to the async dispatcher.
fn accept_loop(
  listener: Listener,
  token: CancellationToken,
  stream_tx: mpsc::Sender<interprocess::local_socket::Stream>,
) {
  if listener
    .set_nonblocking(ListenerNonblockingMode::Accept)
    .is_err()
  {
    tracing::error!("[ipc] Failed to set pipe to non-blocking, accept loop dead");
    return;
  }
  loop {
    if token.is_cancelled() {
      break;
    }
    match listener.accept() {
      Ok(stream) => {
        tracing::debug!("[ipc] Incoming stream...");
        if stream_tx.blocking_send(stream).is_err() {
          break; // dispatcher gone (shutdown).
        }
      }
      Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
        std::thread::sleep(Duration::from_millis(50));
      }
      Err(err) => {
        tracing::error!("[ipc] Accept loop dying, no new game connections: {err}");
        break;
      }
    }
  }
}

struct DispatchCtx {
  stream_rx: mpsc::Receiver<interprocess::local_socket::Stream>,
  token: CancellationToken,
  conns: Arc<tokio::sync::Mutex<JoinSet<()>>>,
  user: Arc<Mutex<RpcUser>>,
  sink: EventSink,
}

async fn dispatch_loop(
  DispatchCtx {
    mut stream_rx,
    token,
    conns,
    user,
    sink,
  }: DispatchCtx,
) {
  loop {
    tokio::select! {
      biased;
      () = token.cancelled() => break,
      stream = stream_rx.recv() => {
        let Some(mut stream) = stream else { break };
        let facil = ConnFacilitator::fresh(user.clone(), sink.clone());
        // Short critical section: spawn_blocking is synchronous.
        conns.lock().await.spawn_blocking(move || {
          let mut facil = facil;
          handle_stream(&mut facil, &mut stream);
        });
      }
    }
  }
}
