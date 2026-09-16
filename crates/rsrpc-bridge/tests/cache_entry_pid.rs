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
  }
}

#[test]
fn prefers_numeric_socket_id() {
  let payload = cached_with("42");
  assert_eq!(
    cache_entry_pid(&SocketId::from("4242"), &payload),
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
  let payload = cached_with("9");
  assert_eq!(cache_entry_pid(&SocketId::from("0"), &payload), None);
  let broken = CachedActivity {
    json: tungstenite::Utf8Bytes::from_static("not json"),
    msgpack: bytes::Bytes::new(),
    is_clear: true,
  };
  assert_eq!(cache_entry_pid(&SocketId::from("app"), &broken), None);
}
