//! Integration suite for `rsrpc-transport-ws`.
//!
//! Spins a real transport on port `0` (OS-assigned) and speaks the game
//! protocol with `tokio-tungstenite` clients. No `sleep()`: only `timeout()`.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rsrpc_transport_ws::{WsTransport, WsTransportConfig};
use rsrpc_types::cmd::ActivityCmd;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Default-identity fixture shared by the transport tests.
fn user() -> std::sync::Arc<std::sync::Mutex<rsrpc_types::user::RpcUser>> {
  std::sync::Arc::new(std::sync::Mutex::new(rsrpc_types::user::RpcUser::default()))
}

/// OS-assigned port config for hermetic binds.
fn config() -> WsTransportConfig {
  WsTransportConfig::new(0, 0)
}

/// Next sink event, failing (not hanging) after the timeout.
async fn next_cmd(rx: &mut tokio::sync::mpsc::Receiver<ActivityCmd>) -> ActivityCmd {
  tokio::time::timeout(TIMEOUT, rx.recv())
    .await
    .expect("timed out waiting for sink event")
    .expect("sink closed unexpectedly")
}

/// Connect a game client with the given query string.
async fn connect(
  port: u16,
  query: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
  let (ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/{query}"))
    .await
    .expect("client failed to connect");
  ws
}

/// Next text reply, parsed as JSON (panics on anything else).
async fn read_text(
  ws: &mut tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
  >,
) -> serde_json::Value {
  match tokio::time::timeout(TIMEOUT, ws.next()).await.unwrap() {
    Some(Ok(tungstenite::Message::Text(text))) => {
      serde_json::from_str(text.as_str()).expect("reply is JSON")
    }
    other => panic!("expected text reply, got {other:?}"),
  }
}

const SET_ACTIVITY: &str =
  r#"{"cmd":"SET_ACTIVITY","args":{"pid":123,"activity":{"name":"Game","type":0}},"nonce":"1"}"#;

#[tokio::test]
/// `SET_ACTIVITY` reaches the sink and earns its lock-step reply.
async fn set_activity_flows_to_sink_with_reply() {
  let (transport, mut rx) = WsTransport::bind(config(), user()).await.unwrap();
  let port = transport.bound_port();

  let mut ws = connect(port, "?v=1&encoding=json&client_id=test-app").await;
  // READY on connect.
  let ready = read_text(&mut ws).await;
  assert_eq!(ready["evt"], "READY");

  ws.send(tungstenite::Message::Text(SET_ACTIVITY.into()))
    .await
    .unwrap();

  let cmd = next_cmd(&mut rx).await;
  assert_eq!(cmd.cmd, "SET_ACTIVITY");
  // client_id falls back to the connect query when the command omits it.
  assert_eq!(cmd.application_id.as_deref(), Some("test-app"));
  assert_eq!(cmd.args.as_ref().and_then(|a| a.pid), Some(123));

  let reply = read_text(&mut ws).await;
  assert_eq!(reply["cmd"], "SET_ACTIVITY");
  assert_eq!(reply["nonce"], "1");

  transport.shutdown().await;
}

#[tokio::test]
/// Wrong protocol versions are closed before any sink traffic.
async fn invalid_version_is_closed_without_sink_traffic() {
  let (transport, mut rx) = WsTransport::bind(config(), user()).await.unwrap();
  let port = transport.bound_port();

  let mut ws = connect(port, "?v=2&encoding=json&client_id=test-app").await;
  match tokio::time::timeout(TIMEOUT, ws.next()).await.unwrap() {
    Some(Ok(tungstenite::Message::Close(_))) | None => {}
    other => panic!("expected close for v=2, got {other:?}"),
  }
  // No SET_ACTIVITY ever reaches the sink for a rejected client.
  assert!(rx.try_recv().is_err());

  transport.shutdown().await;
}

#[tokio::test]
/// Clean disconnects emit one clear for the last published pid.
async fn disconnect_emits_clear_for_last_activity() {
  let (transport, mut rx) = WsTransport::bind(config(), user()).await.unwrap();
  let port = transport.bound_port();

  let mut ws = connect(port, "?v=1&encoding=json&client_id=test-app").await;
  let _ready = read_text(&mut ws).await;
  ws.send(tungstenite::Message::Text(SET_ACTIVITY.into()))
    .await
    .unwrap();
  let cmd = next_cmd(&mut rx).await;
  assert!(
    cmd
      .args
      .as_ref()
      .and_then(|a| a.activity.as_ref())
      .is_some()
  );
  // Drain the reply so the client read buffer stays clean.
  let _reply = read_text(&mut ws).await;

  ws.close(None).await.unwrap();
  let clear = next_cmd(&mut rx).await;
  assert_eq!(clear.cmd, "SET_ACTIVITY");
  assert_eq!(clear.application_id.as_deref(), Some("test-app"));
  let args = clear.args.expect("clear carries args");
  assert_eq!(args.pid, Some(123));
  assert!(args.activity.is_none());

  transport.shutdown().await;
}

#[tokio::test]
/// Snapshots concurrent with message floods always complete (no lock across await).
async fn snapshots_under_flood_never_stall() {
  let (transport, mut rx) = WsTransport::bind(config(), user()).await.unwrap();
  let port = transport.bound_port();
  let handle = transport.handle();

  // The P1 regression: map snapshots concurrent with message flow must
  // always complete. If any lock were held across `.await`, this times out.
  let snapshots = tokio::spawn(async move {
    for _ in 0..2000 {
      handle.client_count().await;
    }
  });

  let mut ws = connect(port, "?v=1&encoding=json&client_id=flood").await;
  let _ready = read_text(&mut ws).await;
  // 100 publishes back-to-back; replies buffer in the outbox/TCP window.
  for _ in 0..100 {
    ws.send(tungstenite::Message::Text(SET_ACTIVITY.into()))
      .await
      .unwrap();
  }
  // Drain replies (unblocks the pump past the per-client outbox bound),
  // then close, producing the final clear.
  for _ in 0..100 {
    let _ = read_text(&mut ws).await;
  }
  ws.close(None).await.unwrap();

  tokio::time::timeout(Duration::from_secs(15), snapshots)
    .await
    .expect("snapshots stalled: map lock held across await?")
    .unwrap();

  // Every publish + the final clear reaches the sink.
  for _ in 0..101 {
    next_cmd(&mut rx).await;
  }

  transport.shutdown().await;
}

#[tokio::test]
/// Dead sinks shed counted while the pump keeps answering clients.
async fn closed_sink_counts_drops_and_stays_alive() {
  let (transport, rx) = WsTransport::bind(config(), user()).await.unwrap();
  drop(rx);
  let port = transport.bound_port();

  let mut ws = connect(port, "?v=1&encoding=json&client_id=test-app").await;
  let ready = read_text(&mut ws).await;
  assert_eq!(ready["evt"], "READY");

  for _ in 0..5 {
    ws.send(tungstenite::Message::Text(SET_ACTIVITY.into()))
      .await
      .unwrap();
    // Replies still flow: the pump is alive despite the dead sink.
    let reply = read_text(&mut ws).await;
    assert_eq!(reply["cmd"], "SET_ACTIVITY");
  }
  assert!(transport.dropped_total() > 0);

  transport.shutdown().await;
}

/// `SET_ACTIVITY` JSON fixture with caller-chosen pid, app and name.
fn activity_cmd(pid: u64, app: &str, name: &str) -> String {
  format!(
    r#"{{"cmd":"SET_ACTIVITY","application_id":"{app}","args":{{"pid":{pid},"activity":{{"name":"{name}","type":0}}}},"nonce":"n{pid}"}}"#
  )
}

/// Next sink clear as `(application_id, pid)` (panics on non-clears).
async fn recv_clear(
  rx: &mut tokio::sync::mpsc::Receiver<ActivityCmd>,
) -> (Option<String>, Option<u64>) {
  let cmd = next_cmd(rx).await;
  assert_eq!(cmd.cmd, "SET_ACTIVITY");
  assert!(
    cmd
      .args
      .as_ref()
      .and_then(|a| a.activity.as_ref())
      .is_none(),
    "expected clear, got activity"
  );
  (
    cmd.application_id.clone(),
    cmd.args.as_ref().and_then(|a| a.pid),
  )
}

#[tokio::test]
/// Abrupt TCP drops clear every pid published on the connection.
async fn abrupt_close_clears_every_published_pid() {
  let (transport, mut rx) = WsTransport::bind(config(), user()).await.unwrap();
  let port = transport.bound_port();

  let mut ws = connect(port, "?v=1&encoding=json&client_id=test-app").await;
  let _ready = read_text(&mut ws).await;
  // One connection publishes two games under different app ids.
  for (pid, app) in [(11u64, "app-a"), (22u64, "app-b")] {
    ws.send(tungstenite::Message::Text(
      activity_cmd(pid, app, "G").into(),
    ))
    .await
    .unwrap();
    let _echo = read_text(&mut ws).await;
    let cmd = next_cmd(&mut rx).await;
    assert_eq!(cmd.args.as_ref().and_then(|a| a.pid), Some(pid));
  }

  // Abrupt close: raw TCP drop, no close frame.
  drop(ws);
  let mut clears = vec![recv_clear(&mut rx).await, recv_clear(&mut rx).await];
  clears.sort();
  assert_eq!(
    clears,
    vec![
      (Some("app-a".to_string()), Some(11)),
      (Some("app-b".to_string()), Some(22)),
    ]
  );

  transport.shutdown().await;
}

/// Beyond the bound, extras are refused before forwarding: disconnect
/// clears exactly the tracked 1..=16, and nothing ghosts.
#[tokio::test]
async fn published_pid_history_is_bounded() {
  let (transport, mut rx) = WsTransport::bind(config(), user()).await.unwrap();
  let port = transport.bound_port();

  let mut ws = connect(port, "?v=1&encoding=json&client_id=test-app").await;
  let _ready = read_text(&mut ws).await;
  // 20 distinct pids on one connection: the first 16 are tracked, pids
  // 17..=20 are refused before forwarding (lock-step replies still flow,
  // so every echo below proves the pump processed the message).
  for pid in 1u64..=20 {
    ws.send(tungstenite::Message::Text(
      activity_cmd(pid, "app", "G").into(),
    ))
    .await
    .unwrap();
    let _echo = read_text(&mut ws).await;
  }
  // Only 16 publishes may reach the sink (refusals forward nothing).
  let mut published = Vec::new();
  for _ in 0..16 {
    let cmd = next_cmd(&mut rx).await;
    published.push(cmd.args.as_ref().and_then(|a| a.pid).expect("pid"));
  }
  published.sort_unstable();
  assert_eq!(published, (1u64..=16).collect::<Vec<_>>());
  drop(ws);

  let mut pids = std::collections::HashSet::new();
  let deadline = std::time::Instant::now() + TIMEOUT;
  while pids.len() < 16 {
    let cmd = next_cmd(&mut rx).await;
    if cmd.cmd == "SET_ACTIVITY"
      && cmd
        .args
        .as_ref()
        .and_then(|a| a.activity.as_ref())
        .is_none()
      && let Some(pid) = cmd.args.as_ref().and_then(|a| a.pid)
    {
      pids.insert(pid);
    }
    assert!(
      std::time::Instant::now() < deadline,
      "every tracked pid must be cleared, got {pids:?}"
    );
  }
  let mut pids: Vec<u64> = pids.into_iter().collect();
  pids.sort_unstable();
  assert_eq!(pids, (1u64..=16).collect::<Vec<_>>());
  // Refused pids (17..=20) forward nothing and clear nothing: the sink
  // must be drained exactly (an evict-and-clear design would leave four
  // more clears queued here).
  assert!(
    rx.try_recv().is_err(),
    "refused pids must leave no sink traffic behind"
  );

  transport.shutdown().await;
}

/// A reader that stops draining is pruned; the pump keeps serving others.
#[tokio::test]
async fn stalled_reader_does_not_freeze_the_pump() {
  // One client that stops reading must not park the shared pump: replies
  // use a bounded wait and the stalled client is pruned. Big replies fill
  // the socket buffers quickly so the outbox (capacity 1) actually backs
  // up instead of draining into TCP.
  let config = WsTransportConfig::new(0, 0).per_client_queue(1);
  let (transport, mut rx) = WsTransport::bind(config, user()).await.unwrap();
  let port = transport.bound_port();

  // Keep the sink drained so it never applies its own backpressure, and
  // record every command for the ghost-clear assertion below.
  let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::<ActivityCmd>::new()));
  let collector = std::sync::Arc::clone(&events);
  let drain = tokio::spawn(async move {
    while let Some(cmd) = rx.recv().await {
      collector
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(cmd);
    }
  });

  let mut healthy = connect(port, "?v=1&encoding=json&client_id=healthy").await;
  let _ = read_text(&mut healthy).await; // READY

  let mut stalled = connect(port, "?v=1&encoding=json&client_id=stalled").await;
  let _ = read_text(&mut stalled).await; // READY

  let big = format!(
    r#"{{"cmd":"SET_ACTIVITY","args":{{"pid":42,"activity":{{"name":"{}","type":0}}}},"nonce":"1"}}"#,
    "x".repeat(16 * 1024)
  );
  // Flood without ever reading: either the pump has already given up on
  // this client (replies bounded, client pruned) or socket backpressure
  // ends the loop.
  for _ in 0..5000 {
    let sent = tokio::time::timeout(
      Duration::from_millis(50),
      stalled.send(tungstenite::Message::Text(big.clone().into())),
    )
    .await;
    if !matches!(sent, Ok(Ok(()))) {
      break;
    }
  }

  // The healthy client must still be served despite the stalled one.
  healthy
    .send(tungstenite::Message::Text(
      r#"{"cmd":"GET_USER","nonce":"u1"}"#.into(),
    ))
    .await
    .unwrap();
  let reply = read_text(&mut healthy).await;
  assert_eq!(reply["cmd"], "GET_USER");

  // The stalled client published pid 42 (the command reached the sink
  // before the reply timed out): pruning it must clear that pid, or the
  // card ghosts.
  let deadline = std::time::Instant::now() + TIMEOUT;
  loop {
    let cleared = events
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .iter()
      .any(|cmd| {
        cmd.cmd == "SET_ACTIVITY"
          && cmd.args.as_ref().and_then(|args| args.pid) == Some(42)
          && cmd
            .args
            .as_ref()
            .and_then(|args| args.activity.as_ref())
            .is_none()
      });
    if cleared {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "pruning the stalled client must clear its published activity"
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
  }

  transport.shutdown().await;
  drain.abort();
}

/// Recording the publication first means pruning clears it, never ghosts.
#[tokio::test]
async fn close_while_reply_is_undeliverable_clears_publication() {
  // Publication must be recorded even when the reply cannot be delivered
  // (client gone, outbox closed): removal then clears the pid instead of
  // leaving a ghost.
  let (transport, mut rx) = WsTransport::bind(config(), user()).await.unwrap();
  let port = transport.bound_port();

  let mut client = connect(port, "?v=1&encoding=json&client_id=gone").await;
  let _ = read_text(&mut client).await; // READY

  client
    .send(tungstenite::Message::Text(
      r#"{"cmd":"SET_ACTIVITY","args":{"pid":42,"activity":{"name":"Ghost","type":0}},"nonce":"1"}"#
        .into(),
    ))
    .await
    .unwrap();
  drop(client); // no reply read, connection torn down

  let deadline = std::time::Instant::now() + TIMEOUT;
  loop {
    let mut cleared = false;
    while let Ok(cmd) = rx.try_recv() {
      if cmd.cmd == "SET_ACTIVITY"
        && cmd.args.as_ref().and_then(|a| a.pid) == Some(42)
        && cmd
          .args
          .as_ref()
          .and_then(|a| a.activity.as_ref())
          .is_none()
      {
        cleared = true;
      }
    }
    if cleared {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "undeliverable reply must still clear the publication"
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
  }

  transport.shutdown().await;
}

/// A publish shed by a full sink is untracked: disconnect clears only shown cards.
#[tokio::test]
async fn shed_publish_leaves_no_tracking_behind() {
  let (transport, mut rx) = WsTransport::bind(config().event_queue(1), user())
    .await
    .unwrap();
  let port = transport.bound_port();

  let mut ws = connect(port, "?v=1&encoding=json&client_id=test-app").await;
  let _ready = read_text(&mut ws).await;
  // Cap-1 sink, never drained: first publish lands, second sheds.
  for pid in [7u64, 8] {
    ws.send(tungstenite::Message::Text(
      activity_cmd(pid, "app", "G").into(),
    ))
    .await
    .unwrap();
    let _echo = read_text(&mut ws).await;
  }
  let first = next_cmd(&mut rx).await;
  assert_eq!(first.args.as_ref().and_then(|a| a.pid), Some(7));
  drop(ws);

  // Only the shown card clears; the shed pid leaves nothing behind.
  let clear = next_cmd(&mut rx).await;
  assert_eq!(clear.args.as_ref().and_then(|a| a.pid), Some(7));
  assert!(
    tokio::time::timeout(Duration::from_secs(1), rx.recv())
      .await
      .is_err(),
    "shed pid must not produce a disconnect clear"
  );

  transport.shutdown().await;
}

/// A shed re-publish restores the prior slot: the shown card stays clearable.
#[tokio::test]
async fn shed_republish_keeps_prior_tracking() {
  let (transport, mut rx) = WsTransport::bind(config().event_queue(1), user())
    .await
    .unwrap();
  let port = transport.bound_port();

  let mut ws = connect(port, "?v=1&encoding=json&client_id=test-app").await;
  let _ready = read_text(&mut ws).await;
  // Publish lands in the cap-1 sink and stays queued (never drained).
  ws.send(tungstenite::Message::Text(
    activity_cmd(7, "app", "G").into(),
  ))
  .await
  .unwrap();
  let _echo = read_text(&mut ws).await;
  // Re-publish sheds against the full sink: the rollback must restore the
  // prior slot instead of dropping tracking for a shown card.
  ws.send(tungstenite::Message::Text(
    activity_cmd(7, "app", "G2").into(),
  ))
  .await
  .unwrap();
  let _echo = read_text(&mut ws).await;

  let first = next_cmd(&mut rx).await;
  assert_eq!(first.args.as_ref().and_then(|a| a.pid), Some(7));
  drop(ws);

  // Disconnect must still clear pid 7 (shown once, tracked throughout).
  let clear = next_cmd(&mut rx).await;
  assert_eq!(clear.args.as_ref().and_then(|a| a.pid), Some(7));
  assert!(
    clear
      .args
      .as_ref()
      .and_then(|a| a.activity.as_ref())
      .is_none()
  );

  transport.shutdown().await;
}

/// A genuine client clear frees its slot: the next new pid is admitted.
#[tokio::test]
async fn genuine_clear_frees_tracking_capacity() {
  let (transport, mut rx) = WsTransport::bind(config(), user()).await.unwrap();
  let port = transport.bound_port();

  let mut ws = connect(port, "?v=1&encoding=json&client_id=test-app").await;
  let _ready = read_text(&mut ws).await;
  for pid in 1u64..=16 {
    ws.send(tungstenite::Message::Text(
      activity_cmd(pid, "app", "G").into(),
    ))
    .await
    .unwrap();
    let _echo = read_text(&mut ws).await;
  }
  for _ in 0..16 {
    next_cmd(&mut rx).await;
  }
  // Clear pid 1 (null activity), then publish a 17th pid: freed capacity
  // admits it instead of refusing.
  ws.send(tungstenite::Message::Text(
    r#"{"cmd":"SET_ACTIVITY","application_id":"app","args":{"pid":1,"activity":null},"nonce":"c1"}"#.into(),
  ))
  .await
  .unwrap();
  let _echo = read_text(&mut ws).await;
  ws.send(tungstenite::Message::Text(
    activity_cmd(17, "app", "G").into(),
  ))
  .await
  .unwrap();
  let _echo = read_text(&mut ws).await;

  // Drain to the fresh publish (clear + publish, any order past the echo).
  let deadline = std::time::Instant::now() + TIMEOUT;
  loop {
    let cmd = next_cmd(&mut rx).await;
    if cmd.args.as_ref().and_then(|a| a.pid) == Some(17)
      && cmd
        .args
        .as_ref()
        .and_then(|a| a.activity.as_ref())
        .is_some()
    {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "freed capacity must admit pid 17"
    );
  }

  transport.shutdown().await;
}

/// Disallowed origins are closed at connect, before READY or registration.
#[tokio::test]
async fn disallowed_origin_is_refused_before_ready() {
  use tokio_tungstenite::tungstenite::http::Request;

  let (transport, _rx) = WsTransport::bind(config(), user()).await.unwrap();
  let port = transport.bound_port();
  let request = Request::builder()
    .uri(format!(
      "ws://127.0.0.1:{port}/?v=1&encoding=json&client_id=evil"
    ))
    .header("origin", "https://evil.example")
    .header("host", format!("127.0.0.1:{port}"))
    .header("upgrade", "websocket")
    .header("connection", "Upgrade")
    .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
    .header("sec-websocket-version", "13")
    .body(())
    .expect("request builds");
  let (mut ws, _) = tokio_tungstenite::connect_async(request)
    .await
    .expect("connect");
  // No READY may arrive: a disallowed origin is closed at connect, and
  // the client must never be registered.
  match tokio::time::timeout(TIMEOUT, ws.next()).await.unwrap() {
    Some(Ok(tungstenite::Message::Close(_))) => {}
    other => panic!("expected close for disallowed origin, got {other:?}"),
  }
  let deadline = std::time::Instant::now() + TIMEOUT;
  loop {
    if transport
      .client_total()
      .load(std::sync::atomic::Ordering::Relaxed)
      == 0
    {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "disallowed origin must never register a client"
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
  }

  transport.shutdown().await;
}
