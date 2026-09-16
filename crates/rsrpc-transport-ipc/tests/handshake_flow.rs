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
}

impl IpcFacilitator for TestFacilitator {
  fn handshake(&self) -> bool {
    self.handshake
  }
  fn set_handshake(&mut self, handshake: bool) {
    self.handshake = handshake;
  }
  fn client_id(&self) -> String {
    self.client_id.clone()
  }
  fn set_client_id(&mut self, client_id: String) {
    self.client_id = client_id;
  }
  fn pid(&self) -> u64 {
    self.pid
  }
  fn set_pid(&mut self, pid: u64) {
    self.pid = pid;
  }
  fn nonce(&self) -> String {
    self.nonce.clone()
  }
  fn set_nonce(&mut self, nonce: String) {
    self.nonce = nonce;
  }
  fn user_payload(&self) -> String {
    RpcUser::default().ready_payload()
  }
  fn current_user(&self) -> RpcUser {
    RpcUser::default()
  }
  fn sink(&self) -> &EventSink {
    &self.sink
  }
}

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
  };
  let mut server_stream = server_stream;
  let server = std::thread::spawn(move || handle_stream(&mut facil, &mut server_stream));
  (client, rx, server)
}

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
