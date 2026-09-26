#![no_main]

use std::io::{Cursor, Read, Write};

use libfuzzer_sys::fuzz_target;
use rsrpc_transport_ipc::EventSink;
use rsrpc_transport_ipc::frame::{IpcFacilitator, MAX_IPC_PAYLOAD, PacketType, handle_stream};
use rsrpc_types::user::RpcUser;

// Per-connection state with a drain-free sink: the queue is oversized so
// `emit_clear` never parks on a full channel mid-run.
struct FuzzFacilitator {
  handshake: bool,
  pid: u64,
  sink: EventSink,
}

impl IpcFacilitator for FuzzFacilitator {
  fn handshake(&self) -> bool {
    self.handshake
  }
  fn set_handshake(&mut self, handshake: bool) {
    self.handshake = handshake;
  }
  fn client_id(&self) -> String {
    String::new()
  }
  fn set_client_id(&mut self, _client_id: String) {}
  fn pid(&self) -> u64 {
    self.pid
  }
  fn set_pid(&mut self, pid: u64) {
    self.pid = pid;
  }
  fn nonce(&self) -> String {
    String::new()
  }
  fn set_nonce(&mut self, _nonce: String) {}
  fn user_payload(&self) -> String {
    "{}".to_string()
  }
  fn current_user(&self) -> RpcUser {
    RpcUser::default()
  }
  fn sink(&self) -> &EventSink {
    &self.sink
  }
}

// Byte duplex: reads consume the fuzz input, writes accumulate responses.
struct FuzzStream {
  input: Cursor<Vec<u8>>,
  output: Vec<u8>,
}

impl Read for FuzzStream {
  fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    self.input.read(buf)
  }
}

impl Write for FuzzStream {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.output.extend_from_slice(buf);
    Ok(buf.len())
  }
  fn flush(&mut self) -> std::io::Result<()> {
    Ok(())
  }
}

// IPC reader: arbitrary bytes through `handle_stream` with a mock
// facilitator. Must terminate (EOF ends the pump), never panic, and
// only emit well-formed frames (known type, bounded length, full body).
fuzz_target!(|data: &[u8]| {
    let take = data.len().min(64 * 1024);
    let mut stream = FuzzStream {
      input: Cursor::new(data[..take].to_vec()),
      output: Vec::new(),
    };
    let (sink, _rx) = EventSink::bounded(4096);
    let mut ipc = FuzzFacilitator {
      handshake: false,
      pid: 0,
      sink,
    };
    handle_stream(&mut ipc, &mut stream);
    // Every response byte belongs to a complete frame.
    let mut frames = stream.output.as_slice();
    while !frames.is_empty() {
      assert!(frames.len() >= 8, "torn response frame");
      let rtype = u32::from_le_bytes(frames[0..4].try_into().unwrap());
      let length = u32::from_le_bytes(frames[4..8].try_into().unwrap());
      assert!(PacketType::try_from_u32(rtype).is_some(), "unknown response type");
      assert!(length <= MAX_IPC_PAYLOAD, "oversize response frame");
      let total = 8 + length as usize;
      assert!(frames.len() >= total, "truncated response frame");
      frames = &frames[total..];
    }
});
