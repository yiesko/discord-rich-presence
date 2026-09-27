//! Stale-socket probe: is the holder behind a path actually alive?
//!
//! Connect plus a `PING` exchange. Only "nothing accepts the connection"
//! reads as stale: after a successful connect, every outcome — a `PONG`,
//! any foreign answer, or an I/O failure including a timeout from a wedged
//! or slow holder — errs toward "alive", because deleting a live socket's
//! path orphans it, while failing to reclaim a stale one just moves to the
//! next index. A foreign service that answers with its own protocol's
//! frames is live, not stale: see `paths.rs` ("a symlink owned by a live
//! foreign service is never ours").

use std::io::{Read, Write};
use std::time::Duration;

use interprocess::local_socket::{GenericFilePath, Stream, ToFsName, traits::Stream as _};

use crate::frame::{PacketType, encode};

/// How long the probe waits per direction (send and recv) before declaring
/// the holder wedged; worst case is 2s total (arRPC uses the same 1s-per-direction
/// budget for socket discovery).
const SOCKET_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Probe whether the process holding `socket_path` answers.
///
/// `false` means nothing answers the path — a stale file or an unreachable
/// path — and reclaiming it is safe. Any answer — `PONG` or a foreign
/// protocol's own frames — reads as live: a live foreign holder keeps its
/// path (we bind the next index), as does a live-but-silent one.
/// Blocking (1s per direction, 2s worst case); call from `spawn_blocking`,
/// never on an async worker.
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
  // Any completed answer — `PONG` or a foreign protocol's own frames —
  // means a live holder behind the path. Reclaiming it would orphan a
  // live service that simply speaks something else (see `paths.rs`).
  tracing::debug!("[ipc] Holder at {socket_path} answered: {header:02x?}");
  true
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

  /// A holder that answers with a non-Pong protocol (foreign service) is
  /// live, not stale: stealing its path would orphan a working foreign
  /// socket.
  #[test]
  fn foreign_answer_reads_as_alive() {
    let dir = scratch("foreign");
    let path = dir.join("discord-ipc-0");
    let listener = UnixListener::bind(&path).expect("bind");
    std::thread::spawn(move || {
      if let Ok((mut stream, _)) = listener.accept() {
        // Read the probe's PING, then answer with something that is not
        // a Pong frame — a foreign protocol's own bytes.
        let mut ping = [0_u8; 8];
        let _ = stream.read_exact(&mut ping);
        let foreign_header = [0xFF; 8];
        let _ = stream.write_all(&foreign_header);
      }
    });
    assert!(socket_holder_alive(path.to_str().expect("utf8")));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&dir);
  }
}
