//! Discord IPC wire framing: packet types, encode/decode, handshake shape.
//!
//! Ported from the legacy `ipc_utils` with two boundary changes:
//! - logging goes through `tracing` (no bespoke macros);
//! - events flow through [`EventSink`] (`try_send`, shed
//!   counted) instead of a blocking `SyncSender`, and the legacy
//!   mid-connection socket `recreate_socket` is gone (see crate docs).
//!
//! The connection pump lives in [`crate::dispatch`].

use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;

use crate::EventSink;

/// Compatibility facade: the pump moved to [`crate::dispatch`], but
/// `frame::handle_stream` and `frame::send_empty` stay valid paths.
pub use crate::dispatch::{handle_stream, send_empty};

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

  /// Record a published pid for disconnect-clear coverage. Called on
  /// every forwarded `SET_ACTIVITY`.
  ///
  /// Default: ignore (preserves single-pid behavior for external
  /// implementors; the current pid is still cleared via [`send_empty`]).
  fn note_published_pid(&mut self, _pid: u64) {}

  /// Check (and record) a published pid before forwarding: unknown pids
  /// past the tracking bound are refused so every forwarded pid stays
  /// covered by disconnect cleanup.
  ///
  /// Default: always admit (external implementors keep today's behavior).
  fn admit_published_pid(&mut self, _pid: u64) -> bool {
    true
  }

  /// Drain tracked pids for disconnect clears, oldest first.
  ///
  /// Default: none (external implementors keep today's exact behavior).
  fn take_published_pids(&mut self) -> Vec<u64> {
    Vec::new()
  }

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

/// Cap on tracked pids per connection: bounds memory against pathological
/// publishers while covering every realistic multiplexer (one connection
/// normally publishes one pid). Beyond the cap the oldest entry drops —
/// same as today's single-pid behavior for it.
///
/// INTENTIONAL DUPLICATION: `rsrpc-transport-ws` carries its own copy of
/// this constant (its `handlers` module). The two transports are siblings
/// with no shared helper crate, and adding a ws→ipc dependency for one
/// constant would couple the wrong crates. Keep both values in sync.
pub(crate) const MAX_TRACKED_PIDS: usize = 16;

/// Check (and record) a published pid for disconnect-clear coverage:
/// known pids refresh as most recent, unknown pids fit only with room.
/// Returns whether the caller may forward — refused pids must not reach
/// the sink, keeping every forwarded pid tracked (and therefore covered
/// by disconnect cleanup).
pub fn admit_pid(history: &mut Vec<u64>, pid: u64) -> bool {
  if let Some(pos) = history.iter().position(|known| *known == pid) {
    history.remove(pos);
    history.push(pid);
    true
  } else if history.len() >= MAX_TRACKED_PIDS {
    false
  } else {
    history.push(pid);
    true
  }
}

/// Record a published pid, refreshing re-published pids as most recent
/// and dropping the oldest beyond the per-connection cap.
///
/// Public so custom [`IpcFacilitator`] implementors share the exact
/// disconnect-clear semantics instead of reimplementing the bound.
/// Legacy evicting behavior, kept for compatibility: prefer [`admit_pid`],
/// which refuses instead of silently dropping tracked history (dropped
/// pids ghost, since disconnect cleanup can no longer clear them).
pub fn track_pid(history: &mut Vec<u64>, pid: u64) {
  if !admit_pid(history, pid) {
    history.remove(0);
    history.push(pid);
  }
}

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
