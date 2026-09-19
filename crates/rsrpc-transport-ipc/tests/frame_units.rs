//! Unit tests for the IPC framing core: packet types, encoding, limits,
//! close frames and the disconnect-clear routing invariant.

use rsrpc_transport_ipc::frame::{MAX_IPC_PAYLOAD, PacketType, close_frame, encode};
use rsrpc_transport_ipc::{EventSink, send_empty};

/// Disconnect clears route as `SET_ACTIVITY`, or the bridge never clears them.
#[test]
fn send_empty_routes_as_set_activity_clear() {
  // Regression: disconnect clears must carry cmd == SET_ACTIVITY, or
  // event_loop misroutes them to broadcast_raw and the presence (and the
  // bridge replay cache) is never cleared — stuck card forever.
  let (sink, mut rx) = EventSink::bounded(1);
  send_empty(&sink, 3);
  let cmd = rx.try_recv().expect("clear queued");
  assert_eq!(cmd.cmd, "SET_ACTIVITY");
  let args = cmd.args.unwrap();
  assert_eq!(args.pid, Some(3));
  assert!(args.activity.is_none());
}

/// Out-of-range packet types map to `None` (refused, never misread).
#[test]
fn unknown_packet_types_are_refused_not_misread_as_frames() {
  assert!(PacketType::try_from_u32(0).is_some());
  assert!(PacketType::try_from_u32(4).is_some());
  assert!(PacketType::try_from_u32(5).is_none());
  assert!(PacketType::try_from_u32(u32::MAX).is_none());
}

/// Payload cap stays at the Discord-compatible 1 MiB.
#[test]
fn payload_limit_matches_discord_1mib() {
  assert_eq!(MAX_IPC_PAYLOAD, 1024 * 1024);
}

/// Close frames carry the Discord-shaped `{code, message}` body.
#[test]
fn close_frame_carries_code_and_message() {
  let frame = close_frame(1003, "Payload too large");
  let r_type = u32::from_le_bytes(frame[0..4].try_into().expect("header"));
  let len = u32::from_le_bytes(frame[4..8].try_into().expect("header")) as usize;
  assert_eq!(r_type, 2);
  assert_eq!(len, frame.len() - 8);
  let body: serde_json::Value = serde_json::from_slice(&frame[8..]).expect("json close body");
  assert_eq!(body["code"], 1003);
  assert_eq!(body["message"], "Payload too large");
}

/// Encoded headers round-trip type tag and length exactly.
#[test]
fn encode_roundtrips_header() {
  let frame = encode(PacketType::Ping, "hello");
  assert_eq!(u32::from_le_bytes(frame[0..4].try_into().unwrap()), 3);
  assert_eq!(u32::from_le_bytes(frame[4..8].try_into().unwrap()), 5);
  assert_eq!(&frame[8..], b"hello");
}

/// Full sinks shed (counted) instead of blocking connection threads.
#[test]
fn full_sink_sheds_and_counts() {
  let (sink, _rx) = EventSink::bounded(1);
  send_empty(&sink, 1);
  // Queue depth 1 is now full: further emits shed instead of blocking.
  send_empty(&sink, 2);
  send_empty(&sink, 3);
  assert_eq!(sink.dropped_total(), 2);
}
