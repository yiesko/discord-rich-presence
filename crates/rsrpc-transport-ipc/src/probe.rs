//! Stale-socket probe: is the holder behind a path actually alive?
//!
//! Connect plus a `PING`/`PONG` exchange (any live holder — Discord, arRPC,
//! rsRPC — answers `PONG` per the IPC protocol). Only "nothing accepts the
//! connection" (and a foreign answer) reads as stale: after a successful
//! connect, every I/O failure — including a timeout from a wedged or slow
//! holder — errs toward "alive", because deleting a live socket's path
//! orphans it, while failing to reclaim a stale one just moves to the
//! next index.

use std::io::{Read, Write};
use std::time::Duration;

use interprocess::local_socket::{GenericFilePath, Stream, ToFsName, traits::Stream as _};

use crate::frame::{PacketType, encode};

/// How long the probe waits for a `PONG` before declaring the holder wedged
/// (arRPC uses the same 1s budget for socket discovery).
const SOCKET_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Probe whether the process holding `socket_path` answers.
///
/// `false` means nothing IPC-shaped answers the path — a stale file, a
/// foreign listener, or an unreachable path — and reclaiming it is safe.
/// A live-but-silent holder keeps its path (we bind the next index).
/// Blocking (1s budget); call from `spawn_blocking`, never on an async
/// worker.
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
    // Connect succeeded: a holder accepted us. Err toward "alive".
    return true;
  }
  let mut header = [0_u8; 8];
  if stream.read_exact(&mut header).is_err() {
    // Timeout or reset after acceptance: live-but-slow or mid-crash.
    // Keeping the path only costs the next index, never an orphaned
    // live socket.
    return true;
  }
  u32::from_le_bytes([header[0], header[1], header[2], header[3]]) == PacketType::Pong as u32
}

#[cfg(test)]
mod tests {
  use std::os::unix::net::UnixListener;

  use super::*;

  /// Isolated temp dir per test (tag-suffixed, hermetic).
  fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rsrpc-probe-test-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
  }

  /// Absent socket paths read as not alive.
  #[test]
  fn missing_socket_reads_as_not_alive() {
    let dir = scratch("missing");
    let path = dir.join("discord-ipc-9");
    assert!(!socket_holder_alive(path.to_str().expect("utf8")));
    let _ = std::fs::remove_dir_all(&dir);
  }

  /// A bound-but-silent holder still reads as alive.
  #[test]
  fn silent_holder_reads_as_alive() {
    // A holder that accepts but never answers (wedged or slow) must not
    // be treated as stale: removing its path would orphan a live socket.
    let dir = scratch("silent");
    let path = dir.join("discord-ipc-0");
    let listener = UnixListener::bind(&path).expect("bind");
    std::thread::spawn(move || {
      if let Ok((mut stream, _)) = listener.accept() {
        // Hold the connection open, answer nothing. The probe times out
        // and disconnects; this read then returns and the thread exits.
        let mut byte = [0_u8; 1];
        let _ = stream.read(&mut byte);
      }
    });
    assert!(socket_holder_alive(path.to_str().expect("utf8")));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&dir);
  }
}
