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
