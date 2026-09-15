use std::{
  io::{Read, Write},
  sync::mpsc,
};

use interprocess::local_socket::Stream;
use serde_json::Value;

use crate::{
  cmd::{ActivityCmd, ActivityCmdArgs},
  commands, debug, log, warn,
};

pub(crate) trait IpcFacilitator {
  fn handshake(&self) -> bool;
  fn set_handshake(&mut self, handshake: bool);

  fn client_id(&self) -> String;
  fn set_client_id(&mut self, client_id: String);

  fn pid(&self) -> u64;
  fn set_pid(&mut self, pid: u64);

  fn nonce(&self) -> String;
  fn set_nonce(&mut self, nonce: String);

  /// The `DISPATCH`/`READY` frame for new connections (reflects the
  /// current shared user, including `SET_USER` patches).
  fn user_payload(&self) -> String;

  /// The current shared identity (for `GET_USER`).
  fn current_user(&self) -> crate::user::RpcUser;

  fn recreate_socket(&mut self) -> crate::error::Result<()>;

  fn start(&mut self);

  fn event_sender(&mut self) -> &mut mpsc::SyncSender<ActivityCmd>;
}

#[derive(Debug)]
pub(crate) enum PacketType {
  Handshake,
  Frame,
  Close,
  Ping,
  Pong,
}

/// Maximum IPC frame payload in bytes, matching arRPC/Discord (1 MiB).
/// Larger frames are refused with a `1003` close instead of being read.
pub(crate) const MAX_IPC_PAYLOAD: u32 = 1024 * 1024;

impl PacketType {
  /// `None` for out-of-range types: the caller must refuse those with a
  /// `1003 Unsupported` close (arRPC parity) instead of misreading them
  /// as frames.
  pub(crate) fn try_from_u32(value: u32) -> Option<Self> {
    match value {
      0 => Some(PacketType::Handshake),
      1 => Some(PacketType::Frame),
      2 => Some(PacketType::Close),
      3 => Some(PacketType::Ping),
      4 => Some(PacketType::Pong),
      _ => None,
    }
  }
}

#[derive(serde::Deserialize, serde::Serialize)]
pub(crate) struct Handshake {
  pub v: u32,
  pub client_id: String,
}

pub(crate) fn encode(r_type: PacketType, data: &str) -> Vec<u8> {
  let mut buffer: Vec<u8> = Vec::with_capacity(8 + data.len());

  // Write the packet type
  buffer.extend_from_slice(&u32::to_le_bytes(r_type as u32));

  // Write the data size
  buffer.extend_from_slice(&u32::to_le_bytes(data.len() as u32));

  // Write the data
  buffer.extend_from_slice(data.as_bytes());

  buffer
}

/// Encode a `Close` frame carrying a Discord-style `{code, message}` body.
pub(crate) fn close_frame(code: u16, message: &str) -> Vec<u8> {
  encode(
    PacketType::Close,
    &serde_json::json!({ "code": code, "message": message }).to_string(),
  )
}

/// Send a `Close` frame, ignoring write errors (the peer is going away).
fn send_close(stream: &mut Stream, code: u16, message: &str) {
  let _ = stream.write_all(&close_frame(code, message));
}

#[allow(clippy::result_large_err)]
pub(crate) fn send_empty(
  event_sender: &mut mpsc::SyncSender<ActivityCmd>,
  pid: u64,
) -> Result<(), mpsc::SendError<ActivityCmd>> {
  log!("[IPC] Sending empty activity");

  // NOTE: cmd MUST be SET_ACTIVITY — event_loop routes anything else to
  // broadcast_raw, which would silently drop the clear (stuck presence).
  let activity = ActivityCmd {
    cmd: "SET_ACTIVITY".to_string(),
    args: Some(ActivityCmdArgs {
      activity: None,
      code: None,
      user_id: None,
      pid: Some(pid),
    }),
    ..ActivityCmd::empty()
  };
  event_sender.send(activity)
}

pub(crate) fn handle_stream(ipc: &mut dyn IpcFacilitator, stream: &mut Stream) {
  loop {
    let current_pid = ipc.pid();
    // Read into buffer
    let mut buffer = std::io::BufReader::new(&mut *stream);

    // Read the packet type and size
    let mut packet_type = [0; 4];
    let mut data_size = [0; 4];

    match buffer.by_ref().take(4).read_exact(&mut packet_type) {
      Ok(_) => (),
      Err(err) => {
        // A peer that never handshaked (SDK probe that bailed, port
        // scanner, crashed launcher) is worth one INFO line: it is the
        // only trace of clients the bridge never identifies, e.g. a
        // native SDK rejecting our handshake observations. Graceful
        // closes arrive as Close frames (logged separately); post-
        // handshake abrupt closes stay in debug (routine: Alt+F4, kills).
        if !ipc.handshake() {
          log!("[IPC] Client disconnected before handshake: {}", err);
        } else {
          debug!(
            "[IPC] Error reading packet type: {}, socket likely closed",
            err
          );
        }

        // Send empty activity
        send_empty(ipc.event_sender(), current_pid)
          .unwrap_or_else(|e| warn!("[IPC] Error sending empty activity: {}", e));
        break;
      }
    }

    match buffer.by_ref().take(4).read_exact(&mut data_size) {
      Ok(_) => (),
      Err(err) => {
        debug!("[IPC] Error reading data size: {}", err);

        // Send empty activity
        send_empty(ipc.event_sender(), current_pid)
          .unwrap_or_else(|e| warn!("[IPC] Error sending empty activity: {}", e));
        break;
      }
    }

    // Convert the rest of the buffer to a string
    let mut message = String::new();

    let data_size = u32::from_le_bytes(data_size);
    if data_size > MAX_IPC_PAYLOAD {
      warn!(
        "[IPC] Frame of {} bytes exceeds the {} byte limit, closing",
        data_size, MAX_IPC_PAYLOAD
      );
      send_close(stream, 1003, "Payload too large");
      break;
    }

    match buffer
      .by_ref()
      .take(data_size as u64)
      .read_to_string(&mut message)
    {
      Ok(_) => (),
      Err(err) => {
        debug!("[IPC] Error reading data: {}", err);
        break;
      }
    }

    let r_type = match PacketType::try_from_u32(u32::from_le_bytes(packet_type)) {
      Some(r_type) => r_type,
      None => {
        warn!(
          "[IPC] Unknown packet type {}, closing",
          u32::from_le_bytes(packet_type)
        );
        send_close(stream, 1003, "Unsupported packet type");
        break;
      }
    };

    debug!("[IPC] Recieved message: {}", message);

    match r_type {
      PacketType::Handshake => {
        debug!("[IPC] Recieved handshake");
        let Ok(data) = serde_json::from_str::<Handshake>(&message) else {
          warn!("[IPC] Error parsing handshake");
          continue;
        };

        if data.v != 1 {
          warn!("[IPC] Invalid version: {}", data.v);
          send_close(stream, 4004, "Invalid version");
          break;
        }

        if data.client_id.is_empty() {
          warn!("[IPC] Invalid client_id (empty)");
          send_close(stream, 4000, "Invalid client_id");
          break;
        }

        ipc.set_handshake(true);
        ipc.set_client_id(data.client_id.clone());
        log!("[IPC] Client connected: {}", data.client_id);

        // Send CONNECTION_RESPONSE
        let resp = encode(PacketType::Frame, &ipc.user_payload());

        match stream.write_all(&resp) {
          Ok(_) => (),
          Err(err) => warn!("[IPC] Error sending connection response: {}", err),
        }
      }
      PacketType::Frame => {
        if !ipc.handshake() {
          debug!("[IPC] Did not handshake yet, ignoring frame");
          continue;
        }

        let mut activity_cmd = match serde_json::from_str::<ActivityCmd>(&message) {
          Ok(cmd) => cmd,
          Err(err) => {
            warn!("[IPC] Error parsing activity command: {}", err);
            let resp = encode(
              PacketType::Frame,
              &commands::rpc_error("", &Value::Null, 4005, "Invalid encoding"),
            );
            if let Err(err) = stream.write_all(&resp) {
              debug!("[IPC] Peer gone, dropping reply: {}", err);
            }
            continue;
          }
        };

        match activity_cmd.cmd.as_str() {
          // Subscriptions are acknowledged locally (arRPC parity): there is
          // no voice/guild backend to subscribe to, and forwarding them
          // would fake-subscribe bridge clients. pid 0 on these frames is
          // expected — never treat it as a presence clear downstream, so
          // they never reach the event sender.
          "SUBSCRIBE" | "UNSUBSCRIBE" => {
            let resp = encode(PacketType::Frame, &commands::subscribe_ack(&activity_cmd));
            if let Err(err) = stream.write_all(&resp) {
              warn!("[IPC] Error sending subscribe ack: {}", err);
            }
          }
          "SET_ACTIVITY" => {
            handle_set_activity(ipc, stream, &message, &mut activity_cmd, current_pid);
          }
          "GET_USER" => {
            // Official response: the user object, or null when the id
            // names somebody else (we only know our own identity). A
            // missing id resolves to self (lenient: game SDKs use this
            // to confirm who they are connected as).
            let wanted = activity_cmd
              .args
              .as_ref()
              .and_then(|args| args.user_id.as_ref());
            let user = ipc.current_user();
            let matched = wanted.is_none_or(|id| *id == user.id);
            let resp = encode(
              PacketType::Frame,
              &commands::user_response(&activity_cmd, matched.then_some(&user)),
            );
            if let Err(err) = stream.write_all(&resp) {
              debug!("[IPC] Peer gone, dropping reply: {}", err);
            }
          }
          "CONNECTIONS_CALLBACK" => {
            // Explicitly unsupported, like arRPC: answer the error the
            // client expects instead of dropping it silently.
            let resp = encode(
              PacketType::Frame,
              &commands::rpc_error(
                &activity_cmd.cmd,
                &activity_cmd.nonce,
                1000,
                "CONNECTIONS_CALLBACK is not supported",
              ),
            );
            if let Err(err) = stream.write_all(&resp) {
              debug!("[IPC] Peer gone, dropping reply: {}", err);
            }
          }
          "INVITE_BROWSER" | "GUILD_TEMPLATE_BROWSER" | "GIFT_CODE_BROWSER" | "DEEP_LINK" => {
            // Known secondary commands are forwarded to bridge clients and
            // acknowledged; the frame never touches presence state.
            activity_cmd.application_id = Some(ipc.client_id());
            if ipc.event_sender().send(activity_cmd.clone()).is_err() {
              warn!("[IPC] Event receiver gone, dropping command");
            }
            let resp = encode(PacketType::Frame, &commands::generic_ack(&activity_cmd));
            if let Err(err) = stream.write_all(&resp) {
              debug!("[IPC] Peer gone, dropping reply: {}", err);
            }
          }
          other => {
            // Known-but-unbacked commands (voice, guilds, OAuth...) get
            // their official error; anything else is genuinely unknown.
            // Neither is forwarded: both must not disturb presence state.
            let unsupported = commands::unsupported_command(other);
            if unsupported.is_none() {
              warn!("[IPC] Unknown command: {}", other);
            }
            let (code, message) = unsupported.unwrap_or((1000, "Unknown command"));
            let resp = encode(
              PacketType::Frame,
              &commands::rpc_error(&activity_cmd.cmd, &activity_cmd.nonce, code, message),
            );
            if let Err(err) = stream.write_all(&resp) {
              debug!("[IPC] Peer gone, dropping reply: {}", err);
            }
          }
        }
      }
      PacketType::Close => {
        log!("[IPC] Recieved close");

        // Send message with an empty activity
        let activity_cmd = ActivityCmd {
          application_id: Some(ipc.client_id()),
          cmd: "SET_ACTIVITY".to_string(),
          data: None,
          evt: None,
          args: Some(ActivityCmdArgs {
            pid: Some(ipc.pid()),
            activity: None,
            code: None,
            user_id: None,
          }),
          nonce: Value::String(ipc.nonce()),
        };

        match ipc.event_sender().send(activity_cmd) {
          Ok(_) => (),
          Err(err) => warn!("[IPC] Error sending activity command: {}", err),
        }

        // reset values
        ipc.set_handshake(false);
        ipc.set_client_id("".to_string());
        ipc.set_pid(0);

        ipc
          .recreate_socket()
          .unwrap_or_else(|e| warn!("[IPC] Error recreating socket: {}", e));

        break;
      }
      PacketType::Ping => {
        debug!("[IPC] Recieved ping");

        // Send a pong
        let resp = encode(PacketType::Pong, &message);

        match stream.write_all(&resp) {
          Ok(_) => (),
          Err(err) => log!("[IPC] Error sending pong: {}", err),
        };
      }
      PacketType::Pong => {
        debug!("[IPC] Recieved pong");
      }
    }
  }
}

/// Handle a `SET_ACTIVITY` frame: forward to the event loop and echo the
/// arRPC-shaped confirmation. `raw` is echoed back when the command has no
/// usable body, so lock-step clients never hang.
fn handle_set_activity(
  ipc: &mut dyn IpcFacilitator,
  stream: &mut Stream,
  raw: &str,
  activity_cmd: &mut ActivityCmd,
  current_pid: u64,
) {
  let args = match activity_cmd.args {
    Some(ref args) => args,
    None => {
      warn!("[IPC] Invalid activity command, skipping");

      // Send empty activity
      send_empty(ipc.event_sender(), current_pid)
        .unwrap_or_else(|e| warn!("[IPC] Error sending empty activity: {}", e));
      return;
    }
  };

  activity_cmd.application_id = Some(ipc.client_id());

  ipc.set_pid(args.pid.unwrap_or_default());
  ipc.set_nonce(activity_cmd.nonce.to_string());

  match ipc.event_sender().send(activity_cmd.clone()) {
    Ok(_) => (),
    Err(err) => warn!("[IPC] Error sending activity command: {}", err),
  }

  // "IPC will echo back every command you send as a response.
  //  Use this as a lock-step feature to avoid flooding messages.
  //  Can be used to validate messages such as the Presence or Subscribes."
  // Echo the activity back intact (official echo semantics): the
  // lock-step guarantee is the reply itself, not a rewritten body.
  activity_cmd.fix();
  let response = commands::set_activity_response(activity_cmd).unwrap_or(raw.to_string());
  let resp = encode(PacketType::Frame, &response);

  match stream.write_all(&resp) {
    Ok(_) => (),
    Err(err) => warn!("[IPC] Error sending connection response: {}", err),
  }
}
