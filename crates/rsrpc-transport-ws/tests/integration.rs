//! Integration suite for `rsrpc-transport-ws`.
//!
//! Spins a real transport on port `0` (OS-assigned) and speaks the game
//! protocol with `tokio-tungstenite` clients. No `sleep()`: only `timeout()`.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rsrpc_transport_ws::{WsTransport, WsTransportConfig};
use rsrpc_types::cmd::ActivityCmd;

const TIMEOUT: Duration = Duration::from_secs(5);

fn user() -> std::sync::Arc<std::sync::Mutex<rsrpc_types::user::RpcUser>> {
  std::sync::Arc::new(std::sync::Mutex::new(rsrpc_types::user::RpcUser::default()))
}

fn config() -> WsTransportConfig {
  WsTransportConfig::new(0, 0)
}

async fn next_cmd(rx: &mut tokio::sync::mpsc::Receiver<ActivityCmd>) -> ActivityCmd {
  tokio::time::timeout(TIMEOUT, rx.recv())
    .await
    .expect("timed out waiting for sink event")
    .expect("sink closed unexpectedly")
}

async fn connect(
  port: u16,
  query: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
  let (ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/{query}"))
    .await
    .expect("client failed to connect");
  ws
}

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

fn activity_cmd(pid: u64, app: &str, name: &str) -> String {
  format!(
    r#"{{"cmd":"SET_ACTIVITY","application_id":"{app}","args":{{"pid":{pid},"activity":{{"name":"{name}","type":0}}}},"nonce":"n{pid}"}}"#
  )
}

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

#[tokio::test]
async fn published_pid_history_is_bounded() {
  let (transport, mut rx) = WsTransport::bind(config(), user()).await.unwrap();
  let port = transport.bound_port();

  let mut ws = connect(port, "?v=1&encoding=json&client_id=test-app").await;
  let _ready = read_text(&mut ws).await;
  // 20 distinct pids on one connection: the bounded history (16) evicts
  // the oldest as it goes, and every eviction is cleared at once — so all
  // 20 pids must produce clears, none may ghost. The echo per message
  // proves the pump processed it; sink commands are collected below
  // tolerating any publish/clear interleave.
  for pid in 1u64..=20 {
    ws.send(tungstenite::Message::Text(
      activity_cmd(pid, "app", "G").into(),
    ))
    .await
    .unwrap();
    let _echo = read_text(&mut ws).await;
  }
  drop(ws);

  let mut pids = std::collections::HashSet::new();
  let deadline = std::time::Instant::now() + TIMEOUT;
  while pids.len() < 20 {
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
      "every forwarded pid must be cleared, got {pids:?}"
    );
  }
  let mut pids: Vec<u64> = pids.into_iter().collect();
  pids.sort_unstable();
  assert_eq!(pids, (1u64..=20).collect::<Vec<_>>());

  transport.shutdown().await;
}

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
