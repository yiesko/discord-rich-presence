//! Stale-socket probe: is the holder behind a path actually alive?
//!
//! Connect plus a `PING`/`PONG` exchange (any live holder — Discord, arRPC,
//! rsRPC — answers `PONG` per the IPC protocol). Errs toward "alive" on
//! unexpected I/O failures: deleting a live socket's path only orphans it,
//! while failing to reclaim a stale one just moves to the next index.

use std::io::{Read, Write};
use std::time::Duration;

use interprocess::local_socket::{GenericFilePath, Stream, ToFsName, traits::Stream as _};

use crate::frame::{PacketType, encode};

/// How long the probe waits for a `PONG` before declaring the holder wedged
/// (arRPC uses the same 1s budget for socket discovery).
const SOCKET_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Probe whether the process holding `socket_path` answers.
///
/// `false` means nothing answers — a stale file or a wedged holder — and
/// reclaiming the path is safe. Blocking (1s budget); call from
/// `spawn_blocking`, never on an async worker.
#[must_use]
pub fn socket_holder_alive(socket_path: &str) -> bool {
  let name = match socket_path.to_fs_name::<GenericFilePath>() {
    Ok(name) => name,
    Err(_) => return false,
  };
  let mut stream = match Stream::connect(name) {
    Ok(stream) => stream,
    // Nobody listening: stale file (or an unreachable path).
    Err(_) => return false,
  };
  if stream.set_send_timeout(Some(SOCKET_PROBE_TIMEOUT)).is_err()
    || stream.set_recv_timeout(Some(SOCKET_PROBE_TIMEOUT)).is_err()
  {
    return true;
  }
  let ping = encode(PacketType::Ping, "rsrpc-probe");
  if stream.write_all(&ping).is_err() {
    return false;
  }
  let mut header = [0_u8; 8];
  if stream.read_exact(&mut header).is_err() {
    return false;
  }
  u32::from_le_bytes([header[0], header[1], header[2], header[3]]) == PacketType::Pong as u32
}
