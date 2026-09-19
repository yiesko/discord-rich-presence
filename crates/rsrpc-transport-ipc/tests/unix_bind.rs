//! Server-level tests (unix): bind + fan-out, stale reclaim, full client
//! flow over the real socket, shutdown cleanup.

#![cfg(unix)]

#[path = "common/mod.rs"]
mod common;

use std::io::Write;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::time::Duration;

use common::{Scratch, read_frame, recv_cmd, write_frame};
use rsrpc_transport_ipc::IpcTransport;
use rsrpc_transport_ipc::frame::{PacketType, encode};
use rsrpc_types::user::RpcUser;

/// Default-identity fixture shared by the bind tests.
fn user() -> std::sync::Arc<std::sync::Mutex<RpcUser>> {
  std::sync::Arc::new(std::sync::Mutex::new(RpcUser::default()))
}

/// Bind creates the socket plus fan-out links; shutdown removes both.
#[tokio::test]
async fn bind_creates_socket_and_fans_out_links() {
  let scratch = Scratch::new("bind");
  let (a, b) = (scratch.sub("a"), scratch.sub("b"));

  let (transport, _rx) = IpcTransport::bind_with_dirs(user(), vec![a.clone(), b.clone()])
    .await
    .unwrap();
  let bound = transport.socket_path().to_string();
  assert!(bound.starts_with(a.to_string_lossy().as_ref()));
  assert!(bound.ends_with("discord-ipc-0"));
  assert!(
    std::fs::symlink_metadata(&bound)
      .expect("socket exists")
      .file_type()
      .is_socket()
  );
  // Fan-out link in the second dir points at the live socket.
  assert_eq!(
    std::fs::read_link(b.join("discord-ipc-0")).expect("fan-out link"),
    std::path::PathBuf::from(&bound)
  );

  transport.shutdown().await;
  // Shutdown cleans the socket file and our links.
  assert!(!std::path::Path::new(&bound).exists());
  assert!(std::fs::read_link(b.join("discord-ipc-0")).is_err());
}

/// Regular files squatting an index are reclaimed for the socket.
#[tokio::test]
async fn stale_regular_file_is_reclaimed() {
  let scratch = Scratch::new("stale");
  let dir = scratch.sub("a");
  // A regular file squatting the first index is stale by definition.
  std::fs::write(dir.join("discord-ipc-0"), b"squat").expect("squat file");

  let (transport, _rx) = IpcTransport::bind_with_dirs(user(), vec![dir.clone()])
    .await
    .unwrap();
  assert!(transport.socket_path().ends_with("discord-ipc-0"));
  assert!(
    std::fs::symlink_metadata(transport.socket_path())
      .expect("socket exists")
      .file_type()
      .is_socket()
  );
  transport.shutdown().await;
}

/// Live sockets are never stolen: bind moves to the next index.
#[tokio::test]
async fn live_holder_blocks_rebind_to_next_index() {
  let scratch = Scratch::new("live");
  let dir = scratch.sub("a");
  // Occupy index 0 with a real listener that answers PONG like a holder.
  let holder = UnixListener::bind(dir.join("discord-ipc-0")).expect("holder socket");
  std::thread::spawn(move || {
    let (mut stream, _) = holder.accept().expect("probe accept");
    let mut ping = vec![0_u8; 8 + "rsrpc-probe".len()];
    use std::io::Read;
    stream.read_exact(&mut ping).expect("read ping");
    let pong = encode(PacketType::Pong, "rsrpc-probe");
    stream.write_all(&pong).expect("write pong");
  });

  let (transport, _rx) = IpcTransport::bind_with_dirs(user(), vec![dir])
    .await
    .unwrap();
  assert!(
    transport.socket_path().ends_with("discord-ipc-1"),
    "live index 0 must be skipped, got {}",
    transport.socket_path()
  );
  transport.shutdown().await;
}

/// NOTE: multi-thread runtime — this test does blocking client I/O on the
/// test thread while the server lives on the runtime. A `current_thread`
/// runtime would deadlock (blocked executor = stalled accept loop).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_client_flow_over_real_socket() {
  let scratch = Scratch::new("flow");
  let dir = scratch.sub("a");

  let (transport, mut rx) = IpcTransport::bind_with_dirs(user(), vec![dir])
    .await
    .unwrap();
  let mut client = UnixStream::connect(transport.socket_path()).expect("connect");
  client
    .set_read_timeout(Some(Duration::from_secs(5)))
    .expect("timeout");

  write_frame(
    &mut client,
    PacketType::Handshake,
    r#"{"v":1,"client_id":"game-1"}"#,
  );
  let (packet_type, body) = read_frame(&mut client);
  assert_eq!(packet_type, 1);
  assert!(body.contains("READY"), "expected READY, got: {body}");

  write_frame(
    &mut client,
    PacketType::Frame,
    r#"{"cmd":"SET_ACTIVITY","args":{"pid":9,"activity":{"name":"G","type":0}},"nonce":"n9"}"#,
  );
  let (_, echo) = read_frame(&mut client);
  assert!(echo.contains("SET_ACTIVITY"));
  let cmd = recv_cmd(&mut rx);
  assert_eq!(cmd.application_id.as_deref(), Some("game-1"));
  assert_eq!(cmd.args.as_ref().and_then(|a| a.pid), Some(9));

  // Ping round-trips without touching the sink.
  write_frame(&mut client, PacketType::Ping, "ping-1");
  let (packet_type, _) = read_frame(&mut client);
  assert_eq!(packet_type, PacketType::Pong as u32);
  assert!(rx.try_recv().is_err());

  write_frame(&mut client, PacketType::Close, "{}");
  let clear = recv_cmd(&mut rx);
  assert_eq!(clear.cmd, "SET_ACTIVITY");
  assert!(
    clear
      .args
      .as_ref()
      .and_then(|a| a.activity.as_ref())
      .is_none()
  );

  transport.shutdown().await;
}

/// A connected-but-silent peer parks its pump in a blocking read: shutdown
/// must still return promptly (closing the socket unblocks the read)
/// instead of stalling out the full drain deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_with_silent_peer_returns_promptly() {
  let scratch = Scratch::new("silent-shutdown");
  let dir = scratch.sub("a");

  let (transport, _rx) = IpcTransport::bind_with_dirs(user(), vec![dir])
    .await
    .unwrap();
  // Connect and say nothing: the pump blocks reading the first header.
  let _silent = UnixStream::connect(transport.socket_path()).expect("connect");
  tokio::time::sleep(Duration::from_millis(200)).await;

  let started = std::time::Instant::now();
  transport.shutdown().await;
  assert!(
    started.elapsed() < Duration::from_secs(4),
    "shutdown must not stall out the 5s drain deadline"
  );
}
