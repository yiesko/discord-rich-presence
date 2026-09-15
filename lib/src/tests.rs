//! Unit tests for the library, one module per area.
//!
//! Kept in their own folder (instead of inline `#[cfg(test)]` modules) so
//! the implementation files stay focused on implementation.
//! They run with `cargo test --lib`.

mod client_connector;
mod cmd;
mod commands;
mod ipc_utils;
mod logger;
mod overrides;
mod process;
mod rpc_server;
mod state;
mod stats;
mod user;
mod websocket;

/// Serializes every test that mutates process-global env: the variables
/// are process-wide, so exactly one env borrower runs at a time (a
/// concurrent reader during `set_var` is a data race). Poison-proof: a
/// panicking holder must not wedge the rest of the suite.
pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Lock [`ENV_LOCK`], tolerating a poisoned predecessor.
pub(crate) fn lock_env() -> std::sync::MutexGuard<'static, ()> {
  ENV_LOCK
    .lock()
    .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Unique scratch dir under the temp dir, removed on drop — even when
/// the test panics mid-body (plain trailing `remove_dir_all` never runs
/// then, littering `/tmp` and risking cross-run collisions).
pub(crate) struct TempDir(std::path::PathBuf);

impl TempDir {
  pub(crate) fn new(tag: &str) -> Self {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
      "rsrpc-test-{}-{}-{}",
      std::process::id(),
      COUNTER.fetch_add(1, Ordering::Relaxed),
      tag
    ));
    std::fs::create_dir_all(&dir).expect("test scratch dir");
    Self(dir)
  }
}

impl std::ops::Deref for TempDir {
  type Target = std::path::PathBuf;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl Drop for TempDir {
  fn drop(&mut self) {
    let _ = std::fs::remove_dir_all(&self.0);
  }
}

/// Process-global env var set for a test body, restored on drop — even
/// on panic (a trailing restore never runs then, poisoning parallel
/// tests). Hold a [`lock_env`] guard alongside while using it: env
/// mutation is only sound single-threaded, which the lock guarantees.
///
/// # Safety
///
/// Constructing this touches process-global state; the caller must hold
/// [`lock_env`] for the whole lifetime (all current callers do).
pub(crate) struct EnvRestore {
  key: &'static str,
  previous: Option<String>,
}

impl EnvRestore {
  pub(crate) fn set(key: &'static str, value: &str) -> Self {
    let previous = std::env::var(key).ok();
    // SAFETY: by contract the caller holds `lock_env`, so no other
    // thread observes the environment concurrently.
    unsafe { std::env::set_var(key, value) };
    Self { key, previous }
  }
}

impl Drop for EnvRestore {
  fn drop(&mut self) {
    // SAFETY: same contract as construction (drop runs on the same
    // thread while the test's `lock_env` guard is still alive: guards
    // are declared before us, so they drop after us).
    unsafe {
      match &self.previous {
        Some(value) => std::env::set_var(self.key, value),
        None => std::env::remove_var(self.key),
      }
    }
  }
}

/// A live raw-TCP websocket client against a throwaway hub, for tests that
/// need a real server-side `Responder` (kill -9 / dead-tab simulation).
/// Minimal RFC6455 handshake over `TcpStream`: no extra client dependency,
/// only real code — `Responder::send` is exactly what the prune logic
/// relies on.
pub(crate) struct WsTestClient {
  stream: std::net::TcpStream,
  hub: simple_websockets::EventHub,
  pub(crate) responder: simple_websockets::Responder,
}

impl WsTestClient {
  /// Connect and wait (condition-poll, no sleep-guess) for the server-side
  /// `Connect` event carrying the `Responder`.
  pub(crate) fn connect() -> Self {
    use std::io::{Read, Write};
    use std::time::{Duration, Instant};

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind test listener");
    let port = listener.local_addr().expect("test local addr").port();
    let hub = simple_websockets::launch_from_listener(listener).expect("launch test hub");

    let mut stream =
      std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect test client");
    stream
      .set_read_timeout(Some(Duration::from_secs(5)))
      .expect("set read timeout");
    let req = format!(
      "GET /?client_id=test HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).expect("handshake write");
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).expect("handshake read");
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(
      resp.starts_with("HTTP/1.1 101"),
      "expected 101 Switching Protocols, got: {resp}"
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    let responder = loop {
      if let Some(simple_websockets::Event::Connect(_, responder)) = hub.next_event() {
        break responder;
      }
      assert!(
        Instant::now() < deadline,
        "timed out waiting for test Connect event"
      );
      std::thread::sleep(Duration::from_millis(10));
    };
    Self {
      stream,
      hub,
      responder,
    }
  }

  /// kill -9 simulation: drop TCP without a close frame, then wait until the
  /// server task actually ends (`Responder::send` fails). Also drains the
  /// explicit `Disconnect` event, proving both death signals fire.
  /// Deterministic gate: assertions after this never depend on timing.
  pub(crate) fn kill(self) -> simple_websockets::Responder {
    use std::time::{Duration, Instant};

    drop(self.stream);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
      if !self
        .responder
        .send(simple_websockets::Message::Text("probe".to_string()))
      {
        break;
      }
      assert!(
        Instant::now() < deadline,
        "dead test client's send never failed"
      );
      std::thread::sleep(Duration::from_millis(10));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
      if matches!(
        self.hub.next_event(),
        Some(simple_websockets::Event::Disconnect(_))
      ) {
        break;
      }
      assert!(
        Instant::now() < deadline,
        "dead test client never produced Disconnect"
      );
      std::thread::sleep(Duration::from_millis(10));
    }
    self.responder
  }
}
