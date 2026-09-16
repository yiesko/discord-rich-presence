//! Discord IPC wire protocol: framing, handshake, command dispatch.
//!
//! Ported from the legacy `ipc_utils` with two boundary changes:
//! - logging goes through `tracing` (no bespoke macros);
//! - events flow through [`EventSink`] (`try_send`, shed
//!   counted) instead of a blocking `SyncSender`, and the legacy
//!   mid-connection socket `recreate_socket` is gone (see crate docs).

use std::io::{Read, Write};

use rsrpc_protocol::commands;
use rsrpc_types::cmd::{ActivityCmd, ActivityCmdArgs};
use rsrpc_types::user::RpcUser;
use serde_json::Value;

use crate::EventSink;

/// Per-connection protocol state. Implemented by the server; object-safe so
/// `handle_stream` stays generic over platforms.
pub trait IpcFacilitator: Send {
  /// Whether the handshake completed on this connection.
  fn handshake(&self) -> bool;
  /// Mark the handshake complete (cleared on close).
  fn set_handshake(&mut self, handshake: bool);

  /// Client id from the handshake (routing enrichment for forwarded frames).
  fn client_id(&self) -> String;
  /// Remember the handshake client id.
  fn set_client_id(&mut self, client_id: String);

  /// Last pid seen on `SET_ACTIVITY` (clear attribution on abrupt close).
  fn pid(&self) -> u64;
  /// Remember the last pid.
  fn set_pid(&mut self, pid: u64);

  /// Last nonce seen (echoed on the close-clear).
  fn nonce(&self) -> String;
  /// Remember the last nonce.
  fn set_nonce(&mut self, nonce: String);

  /// The `DISPATCH`/`READY` frame for new connections (reflects the
  /// current shared user, including `SET_USER` patches).
  fn user_payload(&self) -> String;

  /// The current shared identity (for `GET_USER`).
  fn current_user(&self) -> RpcUser;

  /// Downstream sink for validated commands.
  fn sink(&self) -> &EventSink;

  /// Forward one command downstream (shed counted when the sink is full).
  /// Default impl suffices unless the server needs extra bookkeeping.
  fn send_event(&self, cmd: ActivityCmd) {
    self.sink().emit(cmd);
  }
}

/// Discord IPC packet types (little-endian `u32` header).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
  /// `0`: version + client_id handshake.
  Handshake,
  /// `1`: activity command frame.
  Frame,
  /// `2`: close (emits the clear, resets state).
  Close,
  /// `3`: ping (answered with pong).
  Ping,
  /// `4`: pong.
  Pong,
}

/// Maximum IPC frame payload in bytes, matching arRPC/Discord (1 MiB).
/// Larger frames are refused with a `1003` close instead of being read.
pub const MAX_IPC_PAYLOAD: u32 = 1024 * 1024;

impl PacketType {
  /// `None` for out-of-range types: the caller must refuse those with a
  /// `1003 Unsupported` close (arRPC parity) instead of misreading them
  /// as frames.
  #[must_use]
  pub fn try_from_u32(value: u32) -> Option<Self> {
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

/// Client handshake body: protocol version plus the routing id.
#[derive(serde::Deserialize, serde::Serialize)]
pub struct Handshake {
  /// Must be `1`; anything else is refused with a `4004` close.
  pub v: u32,
  /// Application id; empty is refused with a `4000` close.
  pub client_id: String,
}

/// Encode one frame: `u32` type + `u32` length + body (all little-endian).
#[must_use]
pub fn encode(r_type: PacketType, data: &str) -> Vec<u8> {
  let mut buffer: Vec<u8> = Vec::with_capacity(8 + data.len());
  buffer.extend_from_slice(&u32::to_le_bytes(r_type as u32));
  buffer.extend_from_slice(&u32::to_le_bytes(data.len() as u32));
  buffer.extend_from_slice(data.as_bytes());
  buffer
}

/// Encode a `Close` frame carrying a Discord-style `{code, message}` body.
#[must_use]
pub fn close_frame(code: u16, message: &str) -> Vec<u8> {
  encode(
    PacketType::Close,
    &serde_json::json!({ "code": code, "message": message }).to_string(),
  )
}

/// Send a `Close` frame, ignoring write errors (the peer is going away).
fn send_close(stream: &mut impl Write, code: u16, message: &str) {
  let _ = stream.write_all(&close_frame(code, message));
}

/// Queue a presence clear for `pid`.
///
/// NOTE: cmd MUST be `SET_ACTIVITY` — the bridge routes anything else to
/// `broadcast_raw`, which would silently drop the clear (stuck presence).
pub fn send_empty(sink: &EventSink, pid: u64) {
  tracing::info!("[ipc] Sending empty activity");
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
  sink.emit(activity);
}

/// Pump one connection until close, error, or peer loss.
///
/// `stream` is any synchronous duplex byte stream (socketpair, socket,
/// pipe): reads block the calling thread, so run this in `spawn_blocking`
/// or a dedicated thread — never on an async worker.
pub fn handle_stream(ipc: &mut dyn IpcFacilitator, stream: &mut (impl Read + Write)) {
  // Reused across frames: the 8 KiB read buffer and the message string
  // would otherwise reallocate on every frame (`mem-reuse-collections`).
  // Replies go through `buffer.get_mut()` (unbuffered writes bypass the
  // read buffer; full-duplex TCP keeps ordering correct).
  let mut buffer = std::io::BufReader::new(&mut *stream);
  let mut message = String::new();
  loop {
    let current_pid = ipc.pid();

    let mut packet_type = [0; 4];
    let mut data_size = [0; 4];

    if let Err(err) = buffer.by_ref().take(4).read_exact(&mut packet_type) {
      // A peer that never handshaked (SDK probe that bailed, port
      // scanner, crashed launcher) is worth one line: it is the only
      // trace of clients the bridge never identifies. Graceful closes
      // arrive as Close frames (logged separately); post-handshake
      // abrupt closes are routine (Alt+F4, kills).
      if !ipc.handshake() {
        tracing::info!("[ipc] Client disconnected before handshake: {err}");
      } else {
        tracing::debug!("[ipc] Error reading packet type: {err}, socket likely closed");
      }
      send_empty(ipc.sink(), current_pid);
      break;
    }

    if let Err(err) = buffer.by_ref().take(4).read_exact(&mut data_size) {
      tracing::debug!("[ipc] Error reading data size: {err}");
      send_empty(ipc.sink(), current_pid);
      break;
    }

    message.clear();
    let data_size = u32::from_le_bytes(data_size);
    if data_size > MAX_IPC_PAYLOAD {
      tracing::warn!(
        "[ipc] Frame of {data_size} bytes exceeds the {MAX_IPC_PAYLOAD} byte limit, closing"
      );
      send_close(buffer.get_mut(), 1003, "Payload too large");
      // The connection may have published before misbehaving: clear it
      // like every other connection loss, or the card sticks forever.
      send_empty(ipc.sink(), current_pid);
      break;
    }

    if let Err(err) = buffer
      .by_ref()
      .take(data_size as u64)
      .read_to_string(&mut message)
    {
      tracing::debug!("[ipc] Error reading data: {err}");
      break;
    }

    let r_type = match PacketType::try_from_u32(u32::from_le_bytes(packet_type)) {
      Some(r_type) => r_type,
      None => {
        tracing::warn!(
          "[ipc] Unknown packet type {}, closing",
          u32::from_le_bytes(packet_type)
        );
        send_close(buffer.get_mut(), 1003, "Unsupported packet type");
        // Same as above: a desynced client may hold a live card.
        send_empty(ipc.sink(), current_pid);
        break;
      }
    };

    tracing::debug!("[ipc] Recieved message: {message}");

    match r_type {
      PacketType::Handshake => {
        tracing::debug!("[ipc] Recieved handshake");
        let Ok(data) = serde_json::from_str::<Handshake>(&message) else {
          tracing::warn!("[ipc] Error parsing handshake");
          continue;
        };
        if data.v != 1 {
          tracing::warn!("[ipc] Invalid version: {}", data.v);
          send_close(buffer.get_mut(), 4004, "Invalid version");
          break;
        }
        if data.client_id.is_empty() {
          tracing::warn!("[ipc] Invalid client_id (empty)");
          send_close(buffer.get_mut(), 4000, "Invalid client_id");
          break;
        }
        ipc.set_handshake(true);
        ipc.set_client_id(data.client_id.clone());
        tracing::info!("[ipc] Client connected: {}", data.client_id);
        if let Err(err) = buffer
          .get_mut()
          .write_all(&encode(PacketType::Frame, &ipc.user_payload()))
        {
          tracing::warn!("[ipc] Error sending connection response: {err}");
        }
      }
      PacketType::Frame => {
        if !ipc.handshake() {
          tracing::debug!("[ipc] Did not handshake yet, ignoring frame");
          continue;
        }
        let mut activity_cmd = match serde_json::from_str::<ActivityCmd>(&message) {
          Ok(cmd) => cmd,
          Err(err) => {
            tracing::warn!("[ipc] Error parsing activity command: {err}");
            let resp = encode(
              PacketType::Frame,
              &commands::rpc_error("", &Value::Null, 4005, "Invalid encoding"),
            );
            if let Err(err) = buffer.get_mut().write_all(&resp) {
              tracing::debug!("[ipc] Peer gone, dropping reply: {err}");
            }
            continue;
          }
        };
        match activity_cmd.cmd.as_str() {
          // Subscriptions are acknowledged locally (arRPC parity): there is
          // no voice/guild backend to subscribe to, and forwarding them
          // would fake-subscribe bridge clients. pid 0 on these frames is
          // expected — never treat it as a presence clear downstream, so
          // they never reach the event sink.
          "SUBSCRIBE" | "UNSUBSCRIBE" => {
            let resp = encode(PacketType::Frame, &commands::subscribe_ack(&activity_cmd));
            if let Err(err) = buffer.get_mut().write_all(&resp) {
              tracing::warn!("[ipc] Error sending subscribe ack: {err}");
            }
          }
          "SET_ACTIVITY" => {
            handle_set_activity(
              ipc,
              buffer.get_mut(),
              &message,
              &mut activity_cmd,
              current_pid,
            );
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
            if let Err(err) = buffer.get_mut().write_all(&resp) {
              tracing::debug!("[ipc] Peer gone, dropping reply: {err}");
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
            if let Err(err) = buffer.get_mut().write_all(&resp) {
              tracing::debug!("[ipc] Peer gone, dropping reply: {err}");
            }
          }
          "INVITE_BROWSER" | "GUILD_TEMPLATE_BROWSER" | "GIFT_CODE_BROWSER" | "DEEP_LINK" => {
            // Known secondary commands are forwarded to bridge clients and
            // acknowledged; the frame never touches presence state.
            activity_cmd.application_id = Some(ipc.client_id());
            ipc.send_event(activity_cmd.clone());
            let resp = encode(PacketType::Frame, &commands::generic_ack(&activity_cmd));
            if let Err(err) = buffer.get_mut().write_all(&resp) {
              tracing::debug!("[ipc] Peer gone, dropping reply: {err}");
            }
          }
          other => {
            // Known-but-unbacked commands (voice, guilds, OAuth...) get
            // their official error; anything else is genuinely unknown.
            // Neither is forwarded: both must not disturb presence state.
            let unsupported = commands::unsupported_command(other);
            if unsupported.is_none() {
              tracing::warn!("[ipc] Unknown command: {other}");
            }
            let (code, message) = unsupported.unwrap_or((1000, "Unknown command"));
            let resp = encode(
              PacketType::Frame,
              &commands::rpc_error(&activity_cmd.cmd, &activity_cmd.nonce, code, message),
            );
            if let Err(err) = buffer.get_mut().write_all(&resp) {
              tracing::debug!("[ipc] Peer gone, dropping reply: {err}");
            }
          }
        }
      }
      PacketType::Close => {
        tracing::info!("[ipc] Recieved close");
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
        ipc.send_event(activity_cmd);
        // Reset for a potential reuse of this facilitator; the server
        // listener itself is never rebound (see crate docs).
        ipc.set_handshake(false);
        ipc.set_client_id(String::new());
        ipc.set_pid(0);
        break;
      }
      PacketType::Ping => {
        tracing::debug!("[ipc] Recieved ping");
        if let Err(err) = buffer
          .get_mut()
          .write_all(&encode(PacketType::Pong, &message))
        {
          tracing::info!("[ipc] Error sending pong: {err}");
        }
      }
      PacketType::Pong => {
        tracing::debug!("[ipc] Recieved pong");
      }
    }
  }
}

/// Handle a `SET_ACTIVITY` frame: forward to the event sink and echo the
/// arRPC-shaped confirmation. `raw` is echoed back when the command has no
/// usable body, so lock-step clients never hang.
fn handle_set_activity(
  ipc: &mut dyn IpcFacilitator,
  stream: &mut (impl Read + Write),
  raw: &str,
  activity_cmd: &mut ActivityCmd,
  current_pid: u64,
) {
  let args = match activity_cmd.args {
    Some(ref args) => args,
    None => {
      tracing::warn!("[ipc] Invalid activity command, skipping");
      send_empty(ipc.sink(), current_pid);
      return;
    }
  };

  activity_cmd.application_id = Some(ipc.client_id());
  ipc.set_pid(args.pid.unwrap_or_default());
  ipc.set_nonce(activity_cmd.nonce.to_string());
  ipc.send_event(activity_cmd.clone());

  // "IPC will echo back every command you send as a response.
  //  Use this as a lock-step feature to avoid flooding messages.
  //  Can be used to validate messages such as the Presence or Subscribes."
  // Echo the activity back intact (official echo semantics): the
  // lock-step guarantee is the reply itself, not a rewritten body.
  activity_cmd.fix();
  let response = commands::set_activity_response(activity_cmd).unwrap_or_else(|| raw.to_string());
  if let Err(err) = stream.write_all(&encode(PacketType::Frame, &response)) {
    tracing::warn!("[ipc] Error sending connection response: {err}");
  }
}
