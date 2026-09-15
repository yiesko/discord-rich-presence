use interprocess::local_socket::traits::ListenerExt;
use interprocess::local_socket::{Listener, ListenerOptions, ToFsName};
use interprocess::os::windows::local_socket::{ListenerOptionsExt, NamedPipe};
use interprocess::os::windows::security_descriptor::SecurityDescriptor;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use crate::cmd::ActivityCmd;
use crate::log;
use crate::user::RpcUser;

use super::ipc_utils::{IpcFacilitator, handle_stream};

#[derive(Clone)]
pub(crate) struct IpcConnector {
  socket: Arc<Mutex<Listener>>,
  socket_path: String,
  did_handshake: bool,
  pub client_id: String,
  pub pid: u64,
  pub nonce: String,
  user: Arc<Mutex<RpcUser>>,

  event_sender: mpsc::SyncSender<ActivityCmd>,
}

impl IpcFacilitator for IpcConnector {
  fn handshake(&self) -> bool {
    self.did_handshake
  }

  fn set_handshake(&mut self, handshake: bool) {
    self.did_handshake = handshake;
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

  fn recreate_socket(&mut self) -> crate::error::Result<()> {
    let (socket, socket_path) = Self::create_socket(0)?;
    *self.socket.lock().unwrap_or_else(|e| e.into_inner()) = socket;
    self.socket_path = socket_path;
    Ok(())
  }

  /**
   * Create a new thread that will recieve messages from the socket
   */
  fn start(&mut self) {
    let connector = self.clone();

    std::thread::spawn(move || {
      let socket = connector.socket.lock().unwrap_or_else(|e| e.into_inner());

      for stream in socket.incoming() {
        // Little baby delay to keep things smooth
        std::thread::sleep(std::time::Duration::from_millis(5));

        let mut clone = connector.clone();

        match stream {
          Ok(mut stream) => {
            log!("[IPC] Incoming stream...");
            std::thread::spawn(move || handle_stream(&mut clone, &mut stream));
          }
          Err(err) => {
            log!("[IPC] Error: {}", err);
            break;
          }
        }
      }
    });
  }

  fn event_sender(&mut self) -> &mut mpsc::SyncSender<ActivityCmd> {
    &mut self.event_sender
  }
}

impl IpcConnector {
  /**
   * Create a socket and return a new IpcConnector
   */
  pub(crate) fn new(
    event_sender: mpsc::SyncSender<ActivityCmd>,
    user: Arc<Mutex<RpcUser>>,
  ) -> crate::error::Result<Self> {
    let (socket, socket_path) = Self::create_socket(0)?;
    Ok(Self {
      socket: Arc::new(Mutex::new(socket)),
      socket_path,
      did_handshake: false,
      client_id: "".to_string(),
      pid: 0,
      nonce: "".to_string(),
      user,
      event_sender,
    })
  }

  /// Named-pipe path of the bound socket (for the state snapshot).
  pub(crate) fn socket_path(&self) -> String {
    self.socket_path.clone()
  }

  fn create_socket(tries: u8) -> crate::error::Result<(Listener, String)> {
    // Define the path to the named pipe
    let pipe_path = r"\\.\pipe\discord-ipc";

    // Append tried number to name
    let pipe_path = format!("{pipe_path}-{tries}");

    let listener = ListenerOptions::new()
      .name(pipe_path.clone().to_fs_name::<NamedPipe>()?)
      .security_descriptor(SecurityDescriptor::default());

    let socket = match listener.create_sync() {
      Ok(socket) => socket,
      Err(err) => {
        log!("[IPC] Failed to create IPC socket: {}", err);

        if tries < 9 {
          return Self::create_socket(tries + 1);
        }
        return Err(crate::error::RsrpcError::IpcBind {
          attempts: tries + 1,
          source: err,
        });
      }
    };

    log!("[IPC] Created IPC socket: {}", pipe_path);

    Ok((socket, pipe_path))
  }
}
