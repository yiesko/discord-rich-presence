//! Shared integration-test helpers: scratch dirs, frame I/O, sink drain.
//!
//! Import with `#[path = "common/mod.rs"] mod common;` — integration test
//! files are separate crates and cannot otherwise share code. Each target
//! uses a subset, hence the blanket allow below.
//!
//! ```rust,ignore
//! #[path = "common/mod.rs"]
//! mod common;
//! ```
#![allow(dead_code)]

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rsrpc_transport_ipc::frame::{PacketType, encode};
use rsrpc_types::cmd::ActivityCmd;

/// Unique scratch dir, removed on drop.
pub struct Scratch {
  path: PathBuf,
}

impl Scratch {
  /// Fresh unique temp dir for one test (pre-cleaned).
  pub fn new(tag: &str) -> Self {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
      "rsrpc-ipc-test-{}-{}-{}",
      std::process::id(),
      tag,
      COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    // Remove leftovers from a crashed run first.
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("scratch dir");
    Self { path }
  }

  /// Created subdirectory inside the scratch root.
  pub fn sub(&self, name: &str) -> PathBuf {
    let dir = self.path.join(name);
    std::fs::create_dir_all(&dir).expect("scratch subdir");
    dir
  }

  /// Scratch root path.
  pub fn path(&self) -> &Path {
    &self.path
  }
}

impl Drop for Scratch {
  /// Remove the scratch dir (best-effort, panic-safe).
  fn drop(&mut self) {
    let _ = std::fs::remove_dir_all(&self.path);
  }
}

/// Write one IPC frame.
pub fn write_frame(stream: &mut UnixStream, packet: PacketType, body: &str) {
  stream
    .write_all(&encode(packet, body))
    .expect("write frame");
}

/// Read one IPC frame: `(packet_type, body)`.
pub fn read_frame(stream: &mut UnixStream) -> (u32, String) {
  let mut header = [0_u8; 8];
  stream.read_exact(&mut header).expect("read header");
  let packet_type = u32::from_le_bytes(header[0..4].try_into().unwrap());
  let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
  let mut body = vec![0_u8; len];
  stream.read_exact(&mut body).expect("read body");
  (packet_type, String::from_utf8(body).expect("utf8 body"))
}

/// Drain one sink event, failing (not hanging) after 5s.
pub fn recv_cmd(rx: &mut tokio::sync::mpsc::Receiver<ActivityCmd>) -> ActivityCmd {
  let deadline = Instant::now() + Duration::from_secs(5);
  loop {
    match rx.try_recv() {
      Ok(cmd) => return cmd,
      Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
        assert!(
          Instant::now() < deadline,
          "timed out waiting for sink event"
        );
        std::thread::sleep(Duration::from_millis(5));
      }
      Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
        panic!("sink closed unexpectedly")
      }
    }
  }
}
