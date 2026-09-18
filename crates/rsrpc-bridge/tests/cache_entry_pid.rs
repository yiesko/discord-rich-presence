//! Unit tests for replay-cache pid resolution (`cache_entry_pid`).
//!
//! Pure function, no runtime: numeric socket ids resolve verbatim,
//! otherwise the pid rides in the JSON body, pid 0 and garbage resolve
//! to `None` (never proven dead, never reaped).

use rsrpc_bridge::cache_entry_pid;
use rsrpc_protocol::commands::CachedActivity;
use rsrpc_types::SocketId;

fn cached_with(pid_json: &str) -> CachedActivity {
  CachedActivity {
    json: tungstenite::Utf8Bytes::from(format!(r#"{{"activity":null,"pid":{pid_json}}}"#)),
    msgpack: bytes::Bytes::new(),
    is_clear: true,
    activity_json: bytes::Bytes::new(),
  }
}

#[test]
fn body_pid_wins_over_numeric_socket_id() {
  // Generic scanner cards are cached under the numeric application id
  // with the real pid in the JSON body: the body is authoritative.
  let payload = cached_with("42");
  assert_eq!(cache_entry_pid(&SocketId::from("4242"), &payload), Some(42));
}

#[test]
fn numeric_socket_id_used_when_body_unusable() {
  let broken = CachedActivity {
    json: tungstenite::Utf8Bytes::from_static("not json"),
    msgpack: bytes::Bytes::new(),
    is_clear: true,
    activity_json: bytes::Bytes::new(),
  };
  assert_eq!(
    cache_entry_pid(&SocketId::from("4242"), &broken),
    Some(4242)
  );
}

#[test]
fn falls_back_to_body() {
  let payload = cached_with("77");
  assert_eq!(
    cache_entry_pid(&SocketId::from("some-app-id"), &payload),
    Some(77)
  );
}

#[test]
fn rejects_zero_and_garbage() {
  let payload = cached_with("0");
  assert_eq!(cache_entry_pid(&SocketId::from("0"), &payload), None);
  // A zero socket id no longer masks a usable body pid: the body is
  // authoritative, so this entry resolves to pid 9.
  let payload = cached_with("9");
  assert_eq!(cache_entry_pid(&SocketId::from("0"), &payload), Some(9));
  let broken = CachedActivity {
    json: tungstenite::Utf8Bytes::from_static("not json"),
    msgpack: bytes::Bytes::new(),
    is_clear: true,
    activity_json: bytes::Bytes::new(),
  };
  assert_eq!(cache_entry_pid(&SocketId::from("app"), &broken), None);
}
