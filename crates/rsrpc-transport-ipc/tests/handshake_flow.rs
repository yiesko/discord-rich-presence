//! End-to-end protocol flow over a real socketpair: handshake → READY,
//! SET_ACTIVITY → echo + sink event, SUBSCRIBE → local ack, Close → clear.
//!
//! No mocks: `handle_stream` runs on a thread against a live peer.

#![cfg(unix)]

#[path = "common/mod.rs"]
mod common;

use std::os::unix::net::UnixStream;
use std::time::Duration;

use common::{read_frame, recv_cmd, write_frame};
use rsrpc_transport_ipc::EventSink;
use rsrpc_transport_ipc::frame::{IpcFacilitator, PacketType, handle_stream};
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;

struct TestFacilitator {
  handshake: bool,
  client_id: String,
  pid: u64,
  nonce: String,
  sink: EventSink,
  published_pids: Vec<u64>,
}

impl IpcFacilitator for TestFacilitator {
  /// Test state: handshake flag.
  fn handshake(&self) -> bool {
    self.handshake
  }
  /// Test state: handshake flag.
  fn set_handshake(&mut self, handshake: bool) {
    self.handshake = handshake;
  }
  /// Test state: handshake application id.
  fn client_id(&self) -> String {
    self.client_id.clone()
  }
  /// Test state: handshake application id.
  fn set_client_id(&mut self, client_id: String) {
    self.client_id = client_id;
  }
  /// Test state: latest activity pid.
  fn pid(&self) -> u64 {
    self.pid
  }
  /// Test state: latest activity pid.
  fn set_pid(&mut self, pid: u64) {
    self.pid = pid;
  }
  /// Test state: latest command nonce.
  fn nonce(&self) -> String {
    self.nonce.clone()
  }
  /// Test state: latest command nonce.
  fn set_nonce(&mut self, nonce: String) {
    self.nonce = nonce;
  }
  /// Default identity payload (no user state in these tests).
  fn user_payload(&self) -> String {
    RpcUser::default().ready_payload()
  }
  /// Default identity (no user state in these tests).
  fn current_user(&self) -> RpcUser {
    RpcUser::default()
  }
  /// Test sink under assertion.
  fn sink(&self) -> &EventSink {
    &self.sink
  }
  /// Test history via the shared bound helper.
  fn note_published_pid(&mut self, pid: u64) {
    rsrpc_transport_ipc::frame::track_pid(&mut self.published_pids, pid);
  }
  /// Test history drain for disconnect-clear assertions.
  fn take_published_pids(&mut self) -> Vec<u64> {
    std::mem::take(&mut self.published_pids)
  }
}

/// Full flow: handshake→READY, SUBSCRIBE ack, SET_ACTIVITY echo+sink, Close→clear.
#[test]
fn handshake_set_activity_close_flow() {
  let (server_stream, mut client) = UnixStream::pair().expect("socketpair");
  client
    .set_read_timeout(Some(Duration::from_secs(5)))
    .expect("timeout");
  let (sink, mut rx) = EventSink::bounded(16);

  let mut facil = TestFacilitator {
    handshake: false,
    client_id: String::new(),
    pid: 0,
    nonce: String::new(),
    sink,
    published_pids: Vec::new(),
  };
  let mut server_stream = server_stream;
  let server = std::thread::spawn(move || handle_stream(&mut facil, &mut server_stream));

  // 1. Handshake → READY frame.
  write_frame(
    &mut client,
    PacketType::Handshake,
    r#"{"v":1,"client_id":"test-app"}"#,
  );
  let (packet_type, body) = read_frame(&mut client);
  assert_eq!(packet_type, 1);
  assert!(body.contains("READY"), "expected READY, got: {body}");

  // 2. SUBSCRIBE → local ack, nothing downstream.
  write_frame(
    &mut client,
    PacketType::Frame,
    r#"{"cmd":"SUBSCRIBE","evt":"VOICE_STATE_UPDATE","nonce":"s1"}"#,
  );
  let (_, ack) = read_frame(&mut client);
  assert!(
    ack.contains("SUBSCRIBE"),
    "expected subscribe ack, got: {ack}"
  );
  assert!(rx.try_recv().is_err(), "subscribe must not reach the sink");

  // 3. SET_ACTIVITY → echo + sink event stamped with the handshake id.
  write_frame(
    &mut client,
    PacketType::Frame,
    r#"{"cmd":"SET_ACTIVITY","args":{"pid":7,"activity":{"name":"G","type":0}},"nonce":"n1"}"#,
  );
  let (_, echo) = read_frame(&mut client);
  assert!(echo.contains("SET_ACTIVITY"), "expected echo, got: {echo}");
  let cmd = recv_cmd(&mut rx);
  assert_eq!(cmd.cmd, "SET_ACTIVITY");
  assert_eq!(cmd.application_id.as_deref(), Some("test-app"));
  assert_eq!(cmd.args.as_ref().and_then(|a| a.pid), Some(7));

  // 4. Close → sink clear carrying the same identity.
  write_frame(&mut client, PacketType::Close, "{}");
  let clear = recv_cmd(&mut rx);
  assert_eq!(clear.cmd, "SET_ACTIVITY");
  assert_eq!(clear.application_id.as_deref(), Some("test-app"));
  assert!(
    clear
      .args
      .as_ref()
      .and_then(|a| a.activity.as_ref())
      .is_none()
  );

  server.join().expect("server thread");
}

/// Garbage bytes kill the pump but must still clear the published pid.
#[test]
fn invalid_utf8_body_clears_published_pid() {
  let (server_stream, mut client) = UnixStream::pair().expect("socketpair");
  client
    .set_read_timeout(Some(Duration::from_secs(5)))
    .expect("timeout");
  let (sink, mut rx) = EventSink::bounded(16);

  let mut facil = TestFacilitator {
    handshake: false,
    client_id: String::new(),
    pid: 0,
    nonce: String::new(),
    sink,
    published_pids: Vec::new(),
  };
  let mut server_stream = server_stream;
  let server = std::thread::spawn(move || handle_stream(&mut facil, &mut server_stream));

  write_frame(
    &mut client,
    PacketType::Handshake,
    r#"{"v":1,"client_id":"test-app"}"#,
  );
  let (_, body) = read_frame(&mut client);
  assert!(body.contains("READY"), "expected READY, got: {body}");

  // Publish presence first: the connection owns a live card now.
  write_frame(
    &mut client,
    PacketType::Frame,
    r#"{"cmd":"SET_ACTIVITY","args":{"pid":7,"activity":{"name":"G","type":0}},"nonce":"n1"}"#,
  );
  let _ = read_frame(&mut client); // echo
  let cmd = recv_cmd(&mut rx);
  assert_eq!(cmd.args.as_ref().and_then(|a| a.pid), Some(7));

  // A frame whose body is not UTF-8 kills the pump: like every other
  // connection loss, the published pid must be cleared, not ghosted.
  use std::io::Write as _;
  let mut header = [0_u8; 8];
  header[0..4].copy_from_slice(&u32::to_le_bytes(PacketType::Frame as u32));
  header[4..8].copy_from_slice(&u32::to_le_bytes(16));
  client.write_all(&header).expect("header");
  client.write_all(&[0xFF_u8; 16]).expect("garbage body");

  let clear = recv_cmd(&mut rx);
  assert_eq!(clear.cmd, "SET_ACTIVITY");
  assert!(
    clear
      .args
      .as_ref()
      .and_then(|a| a.activity.as_ref())
      .is_none(),
    "expected null-activity clear, got: {clear:?}"
  );

  server.join().expect("server thread");
}

/// Handshake + one presence publish, asserting echo and sink event.
fn publish_presence(client: &mut UnixStream, rx: &mut tokio::sync::mpsc::Receiver<ActivityCmd>) {
  write_frame(
    client,
    PacketType::Handshake,
    r#"{"v":1,"client_id":"test-app"}"#,
  );
  let (_, body) = read_frame(client);
  assert!(body.contains("READY"), "expected READY, got: {body}");
  write_frame(
    client,
    PacketType::Frame,
    r#"{"cmd":"SET_ACTIVITY","args":{"pid":9,"activity":{"name":"G","type":0}},"nonce":"n1"}"#,
  );
  let (_, echo) = read_frame(client);
  assert!(echo.contains("SET_ACTIVITY"), "expected echo, got: {echo}");
  let cmd = recv_cmd(rx);
  assert_eq!(cmd.args.as_ref().and_then(|a| a.pid), Some(9));
}

/// Live pump on a socketpair, returning client end, sink and thread.
fn spawn_server() -> (
  UnixStream,
  tokio::sync::mpsc::Receiver<ActivityCmd>,
  std::thread::JoinHandle<()>,
) {
  use std::time::Duration;
  let (server_stream, client) = UnixStream::pair().expect("socketpair");
  client
    .set_read_timeout(Some(Duration::from_secs(5)))
    .expect("timeout");
  let (sink, rx) = EventSink::bounded(16);
  let mut facil = TestFacilitator {
    handshake: false,
    client_id: String::new(),
    pid: 0,
    nonce: String::new(),
    sink,
    published_pids: Vec::new(),
  };
  let mut server_stream = server_stream;
  let server = std::thread::spawn(move || handle_stream(&mut facil, &mut server_stream));
  (client, rx, server)
}

/// Oversize frames close with 1003 and still clear the presence.
#[test]
fn oversize_frame_close_still_clears_presence() {
  use std::io::Write;
  let (mut client, mut rx, server) = spawn_server();
  publish_presence(&mut client, &mut rx);

  // 2 MiB declared, nothing sent: server must refuse with close AND clear.
  let mut header = [0u8; 8];
  header[0..4].copy_from_slice(&1u32.to_le_bytes()); // Frame
  header[4..8].copy_from_slice(&(2 * 1024 * 1024u32).to_le_bytes());
  client.write_all(&header).expect("oversize header");
  let (packet_type, body) = read_frame(&mut client);
  assert_eq!(packet_type, 2, "expected close frame, got: {body}");
  assert!(body.contains("1003"), "expected 1003 close, got: {body}");

  let clear = recv_cmd(&mut rx);
  assert_eq!(clear.cmd, "SET_ACTIVITY");
  assert_eq!(clear.args.as_ref().and_then(|a| a.pid), Some(9));
  assert!(
    clear
      .args
      .as_ref()
      .and_then(|a| a.activity.as_ref())
      .is_none()
  );

  server.join().expect("server thread");
}

/// Unknown packet types close the connection but still clear presence.
#[test]
fn unknown_packet_type_close_still_clears_presence() {
  use std::io::Write;
  let (mut client, mut rx, server) = spawn_server();
  publish_presence(&mut client, &mut rx);

  // Unknown type 99, empty body: close AND clear.
  let mut header = [0u8; 8];
  header[0..4].copy_from_slice(&99u32.to_le_bytes());
  client.write_all(&header).expect("unknown-type header");
  let (packet_type, body) = read_frame(&mut client);
  assert_eq!(packet_type, 2, "expected close frame, got: {body}");

  let clear = recv_cmd(&mut rx);
  assert_eq!(clear.cmd, "SET_ACTIVITY");
  assert_eq!(clear.args.as_ref().and_then(|a| a.pid), Some(9));
  assert!(
    clear
      .args
      .as_ref()
      .and_then(|a| a.activity.as_ref())
      .is_none()
  );

  server.join().expect("server thread");
}

/// Publish one pid, asserting echo and sink event carry it.
fn publish_pid(
  client: &mut UnixStream,
  rx: &mut tokio::sync::mpsc::Receiver<ActivityCmd>,
  pid: u64,
) {
  write_frame(
    client,
    PacketType::Frame,
    &format!(
      r#"{{"cmd":"SET_ACTIVITY","args":{{"pid":{pid},"activity":{{"name":"G{pid}","type":0}}}},"nonce":"n{pid}"}}"#
    ),
  );
  let (_, echo) = read_frame(client);
  assert!(echo.contains("SET_ACTIVITY"), "expected echo, got: {echo}");
  let cmd = recv_cmd(rx);
  assert_eq!(cmd.args.as_ref().and_then(|a| a.pid), Some(pid));
}

/// Next sink clear, returning its pid (panics on non-clears).
fn recv_clear_pid(rx: &mut tokio::sync::mpsc::Receiver<ActivityCmd>) -> u64 {
  let cmd = recv_cmd(rx);
  assert_eq!(cmd.cmd, "SET_ACTIVITY");
  assert!(
    cmd
      .args
      .as_ref()
      .and_then(|a| a.activity.as_ref())
      .is_none(),
    "expected clear"
  );
  cmd
    .args
    .as_ref()
    .and_then(|a| a.pid)
    .expect("clear carries pid")
}

/// Abrupt close (no Close frame) clears every published pid.
#[test]
fn abrupt_close_clears_every_published_pid() {
  let (server_stream, mut client) = UnixStream::pair().expect("socketpair");
  client
    .set_read_timeout(Some(Duration::from_secs(5)))
    .expect("timeout");
  let (sink, mut rx) = EventSink::bounded(16);
  let mut facil = TestFacilitator {
    handshake: false,
    client_id: String::new(),
    pid: 0,
    nonce: String::new(),
    sink,
    published_pids: Vec::new(),
  };
  let mut server_stream = server_stream;
  let server = std::thread::spawn(move || handle_stream(&mut facil, &mut server_stream));

  write_frame(
    &mut client,
    PacketType::Handshake,
    r#"{"v":1,"client_id":"test-app"}"#,
  );
  let (_, body) = read_frame(&mut client);
  assert!(body.contains("READY"));

  // One connection publishes two games, then dies without Close.
  publish_pid(&mut client, &mut rx, 7);
  publish_pid(&mut client, &mut rx, 8);
  drop(client);

  let mut pids = vec![recv_clear_pid(&mut rx), recv_clear_pid(&mut rx)];
  pids.sort_unstable();
  assert_eq!(pids, vec![7, 8]);

  server.join().expect("server thread");
}

/// Beyond 16 pids only the most recent clear on disconnect.
#[test]
fn published_pid_history_is_bounded() {
  let (server_stream, mut client) = UnixStream::pair().expect("socketpair");
  client
    .set_read_timeout(Some(Duration::from_secs(5)))
    .expect("timeout");
  let (sink, mut rx) = EventSink::bounded(64);
  let mut facil = TestFacilitator {
    handshake: false,
    client_id: String::new(),
    pid: 0,
    nonce: String::new(),
    sink,
    published_pids: Vec::new(),
  };
  let mut server_stream = server_stream;
  let server = std::thread::spawn(move || handle_stream(&mut facil, &mut server_stream));

  write_frame(
    &mut client,
    PacketType::Handshake,
    r#"{"v":1,"client_id":"test-app"}"#,
  );
  let (_, body) = read_frame(&mut client);
  assert!(body.contains("READY"));

  // 20 distinct pids: only the 16 most recent may produce clears
  // (MAX_TRACKED_PIDS); older history drops.
  for pid in 1u64..=20 {
    publish_pid(&mut client, &mut rx, pid);
  }
  drop(client);

  let mut pids = Vec::new();
  for _ in 0..16 {
    pids.push(recv_clear_pid(&mut rx));
  }
  pids.sort_unstable();
  assert_eq!(pids, (5u64..=20).collect::<Vec<_>>());

  server.join().expect("server thread");
}

/// Close clears retry through a full sink instead of shedding.
#[test]
fn clean_close_clear_survives_a_full_sink() {
  let (server_stream, mut client) = UnixStream::pair().expect("socketpair");
  client
    .set_read_timeout(Some(Duration::from_secs(5)))
    .expect("timeout");
  let (sink, mut rx) = EventSink::bounded(1);
  // Occupy the only slot: a clean-close clear must wait for space (bounded
  // retry) instead of shedding like an ordinary command, or the bridge
  // keeps the card until the next scan.
  let filler = sink.clone();
  filler.emit(ActivityCmd::empty());

  let mut facil = TestFacilitator {
    handshake: false,
    client_id: String::new(),
    pid: 0,
    nonce: String::new(),
    sink,
    published_pids: Vec::new(),
  };
  let mut server_stream = server_stream;
  let server = std::thread::spawn(move || handle_stream(&mut facil, &mut server_stream));

  write_frame(
    &mut client,
    PacketType::Handshake,
    r#"{"v":1,"client_id":"test-app"}"#,
  );
  let (_, body) = read_frame(&mut client);
  assert!(body.contains("READY"), "expected READY, got: {body}");

  write_frame(&mut client, PacketType::Close, "{}");
  // Give the pump a bounded moment to attempt the clear while the sink is
  // full: there is no observable hook for "clear attempted", and the
  // server thread is parked on this socket read, so it reacts at once.
  // The retry budget (250ms) far exceeds this settle.
  std::thread::sleep(Duration::from_millis(50));

  // Filler first, then the close clear (bounded retry survives the full
  // queue; an ordinary shed would have dropped it already).
  let first = recv_cmd(&mut rx);
  assert_eq!(first.cmd, "");
  let clear = recv_cmd(&mut rx);
  assert_eq!(clear.cmd, "SET_ACTIVITY");
  assert_eq!(
    filler.dropped_total(),
    0,
    "the close clear must not be shed"
  );
  assert_eq!(clear.application_id.as_deref(), Some("test-app"));
  assert!(
    clear
      .args
      .as_ref()
      .and_then(|a| a.activity.as_ref())
      .is_none(),
    "expected null-activity clear"
  );

  server.join().expect("server thread");
}
