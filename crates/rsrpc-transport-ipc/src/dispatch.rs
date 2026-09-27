//! IPC connection pump: one thread per peer, synchronous duplex.
//! Reads frames, dispatches handshake/ping/activity/close, and emits
//! clears on loss. Framing primitives live in [`crate::frame`].

use std::io::{Read, Write};

use rsrpc_protocol::commands;
use rsrpc_types::cmd::{ActivityCmd, ActivityCmdArgs};
use serde_json::Value;

use crate::EventSink;
use crate::frame::{
  Handshake, IpcFacilitator, MAX_IPC_PAYLOAD, MAX_TRACKED_PIDS, PacketType, close_frame, encode,
};

/// Pids needing clears on connection loss: tracked history (oldest
/// first) plus the current pid unless already listed. External
/// implementors using the trait defaults get exactly `[current_pid]` —
/// today's behavior, unchanged.
fn clear_pids(ipc: &mut dyn IpcFacilitator, current_pid: u64) -> Vec<u64> {
  let mut pids = ipc.take_published_pids();
  if !pids.contains(&current_pid) {
    pids.push(current_pid);
  }
  pids
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
  sink.emit_clear(activity);
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
      for pid in clear_pids(ipc, current_pid) {
        send_empty(ipc.sink(), pid);
      }
      break;
    }

    if let Err(err) = buffer.by_ref().take(4).read_exact(&mut data_size) {
      tracing::debug!("[ipc] Error reading data size: {err}");
      for pid in clear_pids(ipc, current_pid) {
        send_empty(ipc.sink(), pid);
      }
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
      for pid in clear_pids(ipc, current_pid) {
        send_empty(ipc.sink(), pid);
      }
      break;
    }

    if let Err(err) = buffer
      .by_ref()
      .take(data_size as u64)
      .read_to_string(&mut message)
    {
      tracing::debug!("[ipc] Error reading data: {err}");
      for pid in clear_pids(ipc, current_pid) {
        send_empty(ipc.sink(), pid);
      }
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
        for pid in clear_pids(ipc, current_pid) {
          send_empty(ipc.sink(), pid);
        }
        break;
      }
    };

    tracing::debug!("[ipc] Recieved message: {message}");

    match r_type {
      PacketType::Handshake => {
        if on_handshake(ipc, buffer.get_mut(), &message) {
          break;
        }
      }
      PacketType::Frame => {
        if on_frame(ipc, buffer.get_mut(), &message) {
          break;
        }
      }
      PacketType::Close => {
        on_close(ipc);
        break;
      }
      PacketType::Ping => {
        on_ping(buffer.get_mut(), &message);
      }
      PacketType::Pong => {
        tracing::debug!("[ipc] Recieved pong");
      }
    }
  }
}

/// Pump outcome: `true` stops the connection loop, `false` reads on.
fn on_handshake(
  ipc: &mut dyn IpcFacilitator,
  stream: &mut (impl Read + Write),
  message: &str,
) -> bool {
  tracing::debug!("[ipc] Recieved handshake");
  let Ok(data) = serde_json::from_str::<Handshake>(message) else {
    tracing::warn!("[ipc] Error parsing handshake");
    return false;
  };
  if data.v != 1 {
    tracing::warn!("[ipc] Invalid version: {}", data.v);
    send_close(stream, 4004, "Invalid version");
    return true;
  }
  if data.client_id.is_empty() {
    tracing::warn!("[ipc] Invalid client_id (empty)");
    send_close(stream, 4000, "Invalid client_id");
    return true;
  }
  ipc.set_handshake(true);
  ipc.set_client_id(data.client_id.clone());
  tracing::info!("[ipc] Client connected: {}", data.client_id);
  if let Err(err) = stream.write_all(&encode(PacketType::Frame, &ipc.user_payload())) {
    tracing::warn!("[ipc] Error sending connection response: {err}");
  }
  false
}

/// Answer a `Close` frame: full clear with identity plus clears for
/// older multiplexed pids, then reset the facilitator for reuse.
fn on_close(ipc: &mut dyn IpcFacilitator) {
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
  // Route through the clear path: a clean close must never be shed when
  // the queue is momentarily full, or the bridge keeps the card.
  ipc.sink().emit_clear(activity_cmd);
  // A multiplexing client may hold cards under older pids: clear
  // those too (app-less, like abrupt closes; the full clear above
  // already carried this pid with identity).
  let current = ipc.pid();
  for pid in ipc.take_published_pids() {
    if pid != current {
      send_empty(ipc.sink(), pid);
    }
  }
  // Reset for a potential reuse of this facilitator; the server
  // listener itself is never rebound (see crate docs).
  ipc.set_handshake(false);
  ipc.set_client_id(String::new());
  ipc.set_pid(0);
  ipc.set_nonce(String::new());
}

/// Answer a `Ping` frame with a `Pong` echo.
fn on_ping(stream: &mut (impl Read + Write), message: &str) {
  tracing::debug!("[ipc] Recieved ping");
  if let Err(err) = stream.write_all(&encode(PacketType::Pong, message)) {
    tracing::info!("[ipc] Error sending pong: {err}");
  }
}
/// Dispatch one `Frame` packet: sub-command handlers plus presence
/// forwarding. Returns whether the pump must stop (`true` = break).
fn on_frame(ipc: &mut dyn IpcFacilitator, stream: &mut (impl Read + Write), message: &str) -> bool {
  if !ipc.handshake() {
    tracing::debug!("[ipc] Did not handshake yet, ignoring frame");
    return false;
  }
  let mut activity_cmd = match serde_json::from_str::<ActivityCmd>(message) {
    Ok(cmd) => cmd,
    Err(err) => {
      tracing::warn!("[ipc] Error parsing activity command: {err}");
      let resp = encode(
        PacketType::Frame,
        &commands::rpc_error("", &Value::Null, 4005, "Invalid encoding"),
      );
      if let Err(err) = stream.write_all(&resp) {
        tracing::debug!("[ipc] Peer gone, dropping reply: {err}");
      }
      return false;
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
      if let Err(err) = stream.write_all(&resp) {
        tracing::warn!("[ipc] Error sending subscribe ack: {err}");
      }
    }
    "SET_ACTIVITY" => {
      handle_set_activity(ipc, stream, message, &mut activity_cmd);
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
      if let Err(err) = stream.write_all(&resp) {
        tracing::debug!("[ipc] Peer gone, dropping reply: {err}");
      }
    }
    "INVITE_BROWSER" | "GUILD_TEMPLATE_BROWSER" | "GIFT_CODE_BROWSER" | "DEEP_LINK" => {
      // Known secondary commands are forwarded to bridge clients and
      // acknowledged; the frame never touches presence state.
      activity_cmd.application_id = Some(ipc.client_id());
      ipc.send_event(activity_cmd.clone());
      let resp = encode(PacketType::Frame, &commands::generic_ack(&activity_cmd));
      if let Err(err) = stream.write_all(&resp) {
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
      if let Err(err) = stream.write_all(&resp) {
        tracing::debug!("[ipc] Peer gone, dropping reply: {err}");
      }
    }
  }
  false
}
/// Handle a `SET_ACTIVITY` frame: forward to the event sink and echo the
/// arRPC-shaped confirmation. `raw` is echoed back when the command has no
/// usable body, so lock-step clients never hang.
fn handle_set_activity(
  ipc: &mut dyn IpcFacilitator,
  stream: &mut (impl Read + Write),
  raw: &str,
  activity_cmd: &mut ActivityCmd,
) {
  let args = match activity_cmd.args {
    Some(ref args) => args,
    // No `args` at all is malformed input, not a clear: a genuine clear
    // carries `args: {pid, activity: null}` through the normal path below.
    // Answer the official-shaped error (lock-step clients hang without a
    // reply) and change nothing — clearing tracked pids for garbage would
    // kill live cards on a connection that stays open (Discord/arRPC never
    // mutate presence on invalid input).
    None => {
      tracing::warn!("[ipc] Invalid activity command, skipping");
      let resp = encode(
        PacketType::Frame,
        &commands::rpc_error(
          &activity_cmd.cmd,
          &activity_cmd.nonce,
          4005,
          "Missing activity args",
        ),
      );
      if let Err(err) = stream.write_all(&resp) {
        tracing::debug!("[ipc] Peer gone, dropping reply: {err}");
      }
      return;
    }
  };

  activity_cmd.application_id = Some(ipc.client_id());
  let pid = args.pid.unwrap_or_default();
  ipc.set_pid(pid);
  // Refused pids never reach the sink (echo below still flows, so
  // lock-step clients never hang); recording stays limited to forwarded
  // commands, keeping disconnect cleanup complete by construction.
  if !ipc.admit_published_pid(pid) {
    tracing::debug!("[ipc] Past {MAX_TRACKED_PIDS} tracked pids, refusing pid {pid}");
  } else {
    ipc.note_published_pid(pid);
    ipc.send_event(activity_cmd.clone());
  }
  // Store the unquoted nonce: `on_close` wraps it in `Value::String`, so a
  // JSON `to_string()` here would double-encode the close clear's nonce.
  ipc.set_nonce(match &activity_cmd.nonce {
    Value::String(nonce) => nonce.clone(),
    other => other.to_string(),
  });

  // "IPC will echo back every command you send as a response.
  //  Use this as a lock-step feature to avoid flooding messages.
  //  Can be used to validate messages such as the Presence or Subscribes."
  // Echo the activity back with field fixes applied (arRPC parity): the
  // lock-step guarantee is the reply itself, not a verbatim body.
  activity_cmd.fix();
  let response = commands::set_activity_response(activity_cmd).unwrap_or_else(|| raw.to_string());
  if let Err(err) = stream.write_all(&encode(PacketType::Frame, &response)) {
    tracing::warn!("[ipc] Error sending connection response: {err}");
  }
}
