//! Integration suite for `rsrpc-ws`.
//!
//! Every test is `#[tokio::test]` with explicit `timeout()`s — no `sleep()`.
//! Helpers return on first unexpected event via `panic!` with context.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rsrpc_ws::{DisconnectReason, Event, Message, Server, ServerConfig};
use tokio_tungstenite::tungstenite;

const TIMEOUT: Duration = Duration::from_secs(5);

fn test_config() -> ServerConfig {
  ServerConfig::builder("127.0.0.1:0".parse().unwrap())
    .build()
    .unwrap()
}

async fn next_event(hub: &mut rsrpc_ws::EventHub) -> Event {
  tokio::time::timeout(TIMEOUT, hub.next_event())
    .await
    .expect("timed out waiting for event")
    .expect("hub closed unexpectedly")
}

async fn connect(
  addr: std::net::SocketAddr,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
  let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
    .await
    .expect("client failed to connect");
  ws
}

#[tokio::test]
async fn connect_and_disconnect_roundtrip() {
  let (server, mut hub) = Server::bind(test_config()).await.unwrap();
  let addr = server.local_addr();

  let mut ws = connect(addr).await;
  // Dropping the Connect responder must NOT kill the connection: the
  // server keeps serving incoming messages until close/shutdown.
  let id = match next_event(&mut hub).await {
    Event::Connect(id, _) => id,
    other => panic!("expected Connect, got {other:?}"),
  };

  ws.send(tungstenite::Message::Text("hi".into()))
    .await
    .unwrap();
  match next_event(&mut hub).await {
    Event::Message(got_id, Message::Text(text)) => {
      assert_eq!(got_id, id);
      assert_eq!(text, "hi");
    }
    other => panic!("expected Message, got {other:?}"),
  }

  ws.close(None).await.unwrap();
  match next_event(&mut hub).await {
    Event::Disconnect(got_id, _) => assert_eq!(got_id, id),
    other => panic!("expected Disconnect, got {other:?}"),
  }

  server.shutdown().await;
}

#[tokio::test]
async fn echo_via_responder() {
  let (server, mut hub) = Server::bind(test_config()).await.unwrap();
  let addr = server.local_addr();

  let mut ws = connect(addr).await;
  let responder = match next_event(&mut hub).await {
    Event::Connect(_, responder) => responder,
    other => panic!("expected Connect, got {other:?}"),
  };

  responder.try_send(Message::from("echo-me")).unwrap();
  match tokio::time::timeout(TIMEOUT, ws.next()).await.unwrap() {
    Some(Ok(tungstenite::Message::Text(text))) => assert_eq!(text.as_str(), "echo-me"),
    other => panic!("expected text echo, got {other:?}"),
  }

  responder
    .send_async(Message::Binary(bytes::Bytes::from_static(&[1, 2, 3])))
    .await
    .unwrap();
  match tokio::time::timeout(TIMEOUT, ws.next()).await.unwrap() {
    Some(Ok(tungstenite::Message::Binary(bin))) => assert_eq!(bin.as_ref(), &[1, 2, 3]),
    other => panic!("expected binary echo, got {other:?}"),
  }

  server.shutdown().await;
}

#[tokio::test]
async fn connection_details_carry_peer_and_uri() {
  let (server, mut hub) = Server::bind(test_config()).await.unwrap();
  let addr = server.local_addr();

  let _ws = connect(addr).await;
  match next_event(&mut hub).await {
    Event::Connect(_, responder) => {
      assert!(responder.details().peer.ip().is_loopback());
      assert_eq!(responder.details().uri.as_ref(), "/");
    }
    other => panic!("expected Connect, got {other:?}"),
  }

  server.shutdown().await;
}

#[tokio::test]
async fn over_connection_limit_gets_try_again_later() {
  let config = ServerConfig::builder("127.0.0.1:0".parse().unwrap())
    .max_connections(1)
    .build()
    .unwrap();
  let (server, mut hub) = Server::bind(config).await.unwrap();
  let addr = server.local_addr();

  let _first = connect(addr).await;
  match next_event(&mut hub).await {
    Event::Connect(_, _) => {}
    other => panic!("expected Connect, got {other:?}"),
  }

  // Second connection must be rejected with a 1013 close, not hung.
  let mut second = connect(addr).await;
  match tokio::time::timeout(TIMEOUT, second.next()).await.unwrap() {
    Some(Ok(tungstenite::Message::Close(Some(frame)))) => {
      assert_eq!(
        frame.code,
        tungstenite::protocol::frame::coding::CloseCode::Again
      );
    }
    other => panic!("expected Close(Again), got {other:?}"),
  }

  server.shutdown().await;
}

#[tokio::test]
async fn half_open_conn_dies_on_idle_timeout() {
  use tokio::io::{AsyncReadExt, AsyncWriteExt};

  let config = ServerConfig::builder("127.0.0.1:0".parse().unwrap())
    .keepalive_interval(Duration::from_millis(50))
    .idle_timeout(Duration::from_millis(200))
    .build()
    .unwrap();
  let (server, mut hub) = Server::bind(config).await.unwrap();
  let addr = server.local_addr();

  // Raw TCP client: completes the HTTP upgrade, then goes fully silent —
  // never answers server pings. The server must reap it via idle timeout
  // instead of pinning the task in `incoming.next()` forever (old leak).
  let mut raw = tokio::net::TcpStream::connect(addr).await.unwrap();
  let request = format!(
    "GET / HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
  );
  raw.write_all(request.as_bytes()).await.unwrap();
  let mut response = Vec::new();
  loop {
    let mut chunk = [0u8; 1024];
    let n = raw.read(&mut chunk).await.unwrap();
    assert!(n > 0, "server closed handshake prematurely");
    response.extend_from_slice(&chunk[..n]);
    if response.windows(4).any(|w| w == b"\r\n\r\n") {
      break;
    }
  }
  assert!(
    response.starts_with(b"HTTP/1.1 101"),
    "expected 101 Switching Protocols, got: {}",
    String::from_utf8_lossy(&response)
  );

  let id = match next_event(&mut hub).await {
    Event::Connect(id, _) => id,
    other => panic!("expected Connect, got {other:?}"),
  };
  // Hold the socket open but silent; pings go unanswered.
  let _held = raw;
  match tokio::time::timeout(TIMEOUT, hub.next_event())
    .await
    .unwrap()
  {
    Some(Event::Disconnect(got_id, DisconnectReason::IdleTimeout)) => {
      assert_eq!(got_id, id);
    }
    other => panic!("expected Disconnect(IdleTimeout), got {other:?}"),
  }

  server.shutdown().await;
}

#[tokio::test]
async fn idle_timeout_fires_independently_of_keepalive() {
  use tokio::io::{AsyncReadExt, AsyncWriteExt};

  // Keepalive ticks hourly, idle budget 200ms: a silent peer must still
  // be reaped on the idle deadline, not parked until the next tick.
  let config = ServerConfig::builder("127.0.0.1:0".parse().unwrap())
    .keepalive_interval(Duration::from_secs(3600))
    .idle_timeout(Duration::from_millis(200))
    .build()
    .unwrap();
  let (server, mut hub) = Server::bind(config).await.unwrap();
  let addr = server.local_addr();

  let mut raw = tokio::net::TcpStream::connect(addr).await.unwrap();
  let request = format!(
    "GET / HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
  );
  raw.write_all(request.as_bytes()).await.unwrap();
  let mut response = Vec::new();
  loop {
    let mut chunk = [0u8; 1024];
    let n = raw.read(&mut chunk).await.unwrap();
    assert!(n > 0, "server closed handshake prematurely");
    response.extend_from_slice(&chunk[..n]);
    if response.windows(4).any(|w| w == b"\r\n\r\n") {
      break;
    }
  }
  assert!(response.starts_with(b"HTTP/1.1 101"));

  let id = match next_event(&mut hub).await {
    Event::Connect(id, _) => id,
    other => panic!("expected Connect, got {other:?}"),
  };
  let _held = raw; // open but silent; hourly pings never arrive.
  match tokio::time::timeout(TIMEOUT, hub.next_event())
    .await
    .unwrap()
  {
    Some(Event::Disconnect(got_id, DisconnectReason::IdleTimeout)) => {
      assert_eq!(got_id, id);
    }
    other => panic!("expected Disconnect(IdleTimeout), got {other:?}"),
  }

  server.shutdown().await;
}

#[tokio::test]
async fn shutdown_with_open_conns_completes() {
  let (server, mut hub) = Server::bind(test_config()).await.unwrap();
  let addr = server.local_addr();

  let mut clients = Vec::new();
  for _ in 0..50 {
    clients.push(connect(addr).await);
  }
  for _ in 0..50 {
    match next_event(&mut hub).await {
      Event::Connect(_, _) => {}
      other => panic!("expected Connect, got {other:?}"),
    }
  }

  tokio::time::timeout(TIMEOUT, server.shutdown())
    .await
    .expect("shutdown timed out with 50 open conns");
  // Every connection task emits its Disconnect first; then the hub closes.
  for _ in 0..50 {
    match next_event(&mut hub).await {
      Event::Disconnect(_, DisconnectReason::ServerShutdown) => {}
      other => panic!("expected ServerShutdown disconnect, got {other:?}"),
    }
  }
  assert!(hub.next_event().await.is_none());
}

#[tokio::test]
async fn slow_consumer_try_send_reports_full() {
  let config = ServerConfig::builder("127.0.0.1:0".parse().unwrap())
    .per_client_queue(1)
    .build()
    .unwrap();
  let (server, mut hub) = Server::bind(config).await.unwrap();
  let addr = server.local_addr();

  // Never read from this client: outbox must fill, never grow, never block.
  let _ws = connect(addr).await;
  let responder = match next_event(&mut hub).await {
    Event::Connect(_, responder) => responder,
    other => panic!("expected Connect, got {other:?}"),
  };

  let mut full_seen = false;
  for i in 0..100u32 {
    match responder.try_send(Message::from(format!("flood-{i}"))) {
      Ok(()) => {}
      Err(rsrpc_ws::TrySendError::Full) => {
        full_seen = true;
        break;
      }
      Err(rsrpc_ws::TrySendError::Closed) => panic!("client unexpectedly closed"),
      Err(_) => panic!("unknown TrySendError variant"),
    }
  }
  assert!(full_seen, "queue of 1 must report Full under flood");

  // Prune path: close the slow consumer; client observes a close frame.
  responder.close(rsrpc_ws::CloseCode::Normal).await;
  match tokio::time::timeout(TIMEOUT, hub.next_event())
    .await
    .unwrap()
  {
    Some(Event::Disconnect(_, reason)) => {
      assert_eq!(reason, DisconnectReason::Clean);
    }
    other => panic!("expected Disconnect, got {other:?}"),
  }

  server.shutdown().await;
}
