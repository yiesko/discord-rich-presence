//! Integration suite for `rsrpc-bridge`.
//!
//! A real bridge on ephemeral ports, driven directly through its input
//! channels plus live `tokio-tungstenite` consumers. No `sleep()`: only
//! `timeout()`. Multi-thread runtime: tests block on sync client I/O while
//! the bridge lives on the runtime.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rsrpc_bridge::{Bridge, BridgeConfig, BridgeInputs, ProcInput, ScannedGame};
use rsrpc_types::user::RpcUser;

const TIMEOUT: Duration = Duration::from_secs(5);

type WsStream =
  tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Fixture {
  bridge: Bridge,
  json_port: u16,
  msgpack_port: u16,
  ipc_tx: tokio::sync::mpsc::Sender<rsrpc_types::cmd::ActivityCmd>,
  game_tx: tokio::sync::mpsc::Sender<rsrpc_types::cmd::ActivityCmd>,
  proc_tx: tokio::sync::mpsc::Sender<ProcInput>,
}

async fn fixture() -> Fixture {
  let (ipc_tx, ipc_rx) = tokio::sync::mpsc::channel(64);
  let (game_tx, game_rx) = tokio::sync::mpsc::channel(1024);
  let (proc_tx, proc_rx) = tokio::sync::mpsc::channel(512);
  let config = BridgeConfig::new(0, 0, 0, 0)
    .app_version("test-bridge")
    .persist_interval(Duration::from_millis(100))
    .refresh_interval(Duration::from_secs(3600));
  let bridge = Bridge::bind(
    config,
    std::sync::Arc::new(std::sync::Mutex::new(RpcUser::default())),
    BridgeInputs {
      ipc_rx,
      game_rx,
      proc_rx,
    },
  )
  .await
  .expect("bridge binds ephemeral ports");
  let (json_port, msgpack_port) = (bridge.json_port(), bridge.msgpack_port());
  Fixture {
    bridge,
    json_port,
    msgpack_port,
    ipc_tx,
    game_tx,
    proc_tx,
  }
}

async fn connect(port: u16, query: &str) -> WsStream {
  let (ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/{query}"))
    .await
    .expect("consumer failed to connect");
  ws
}

async fn read_json(ws: &mut WsStream) -> serde_json::Value {
  match tokio::time::timeout(TIMEOUT, ws.next()).await.unwrap() {
    Some(Ok(tungstenite::Message::Text(text))) => {
      serde_json::from_str(text.as_str()).expect("json frame")
    }
    other => panic!("expected text frame, got {other:?}"),
  }
}

async fn read_msgpack(ws: &mut WsStream) -> serde_json::Value {
  match tokio::time::timeout(TIMEOUT, ws.next()).await.unwrap() {
    Some(Ok(tungstenite::Message::Binary(bytes))) => {
      rmp_serde::from_slice(&bytes).expect("msgpack frame")
    }
    other => panic!("expected binary frame, got {other:?}"),
  }
}

fn set_activity(pid: u64, name: &str) -> rsrpc_types::cmd::ActivityCmd {
  serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app-1",
    "args": { "pid": pid, "activity": { "name": name, "type": 0 } },
    "nonce": "n1",
  }))
  .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_reaches_both_protocols_with_replay() {
  let fx = fixture().await;

  let mut json = connect(fx.json_port, "?format=json").await;
  let ready = read_json(&mut json).await;
  assert_eq!(ready["evt"], "READY");

  let mut pack = connect(fx.msgpack_port, "?format=msgpack").await;
  let ready = read_msgpack(&mut pack).await;
  assert_eq!(ready["evt"], "READY");

  fx.ipc_tx.send(set_activity(42, "Game")).await.unwrap();

  let got = read_json(&mut json).await;
  assert_eq!(got["activity"]["name"], "Game");
  assert_eq!(got["pid"], 42);
  let got_pack: serde_json::Value = read_msgpack(&mut pack).await;
  assert_eq!(got_pack["activity"]["name"], "Game");

  // The game-transport leg feeds the same fan-out.
  fx.game_tx.send(set_activity(43, "GameLeg")).await.unwrap();
  let got = read_json(&mut json).await;
  assert_eq!(got["activity"]["name"], "GameLeg");
  assert_eq!(got["pid"], 43);

  // Late joiner replays the cached presence (both slots, any order).
  let mut late = connect(fx.json_port, "?format=json").await;
  let _ = read_json(&mut late).await; // READY
  let mut names = vec![
    read_json(&mut late).await["activity"]["name"]
      .as_str()
      .unwrap()
      .to_string(),
    read_json(&mut late).await["activity"]["name"]
      .as_str()
      .unwrap()
      .to_string(),
  ];
  names.sort();
  assert_eq!(names, vec!["Game".to_string(), "GameLeg".to_string()]);

  fx.bridge.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_user_fans_out_current_user_update() {
  let fx = fixture().await;
  let mut json = connect(fx.json_port, "?format=json").await;
  let _ = read_json(&mut json).await; // READY

  json
    .send(tungstenite::Message::Text(
      r#"{"type":"SET_USER","nonce":"1","patch":{"username":"web"}}"#.into(),
    ))
    .await
    .unwrap();

  let ack = read_json(&mut json).await;
  assert_eq!(ack["type"], "SET_USER_ACK");
  let update = read_json(&mut json).await;
  assert_eq!(update["evt"], "CURRENT_USER_UPDATE");
  assert_eq!(update["data"]["username"], "web");

  fx.bridge.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn genuine_clear_empties_replay() {
  let fx = fixture().await;
  let mut json = connect(fx.json_port, "?format=json").await;
  let _ = read_json(&mut json).await; // READY

  fx.ipc_tx.send(set_activity(42, "Game")).await.unwrap();
  let got = read_json(&mut json).await;
  assert_eq!(got["activity"]["name"], "Game");

  // Genuine clear: null activity, nonzero pid.
  let clear: rsrpc_types::cmd::ActivityCmd = serde_json::from_value(serde_json::json!({
    "cmd": "SET_ACTIVITY",
    "application_id": "app-1",
    "args": { "pid": 42, "activity": null },
    "nonce": "n2",
  }))
  .unwrap();
  fx.ipc_tx.send(clear).await.unwrap();
  let cleared = read_json(&mut json).await;
  assert!(cleared["activity"].is_null());

  // Late joiner replays nothing (only READY arrives).
  let mut late = connect(fx.json_port, "?format=json").await;
  let _ = read_json(&mut late).await; // READY
  assert!(
    tokio::time::timeout(Duration::from_millis(300), late.next())
      .await
      .is_err(),
    "cleared slot must not replay"
  );

  fx.bridge.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_scan_publishes_generic_and_null_clears() {
  let fx = fixture().await;
  let mut json = connect(fx.json_port, "?format=json").await;
  let _ = read_json(&mut json).await; // READY

  fx.proc_tx
    .send(ProcInput::Detected(ScannedGame {
      id: "game-9".into(),
      name: "Scanned".to_string(),
      pid: 4242,
      start: 1_700_000_000,
    }))
    .await
    .unwrap();
  let got = read_json(&mut json).await;
  assert_eq!(got["activity"]["name"], "Scanned");
  assert_eq!(got["pid"], 4242);

  fx.proc_tx.send(ProcInput::Cleared).await.unwrap();
  let cleared = read_json(&mut json).await;
  assert!(cleared["activity"].is_null());

  fx.bridge.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn null_scan_clears_live_generic_exactly_once() {
  let fx = fixture().await;
  let mut json = connect(fx.json_port, "?format=json").await;
  let _ = read_json(&mut json).await; // READY

  // Generic scanner cards are cached under the numeric application id
  // with the real (live: our own test pid) pid in the JSON body.
  let live = u64::from(std::process::id());
  fx.proc_tx
    .send(ProcInput::Detected(ScannedGame {
      id: "123456789".into(),
      name: "LiveGeneric".to_string(),
      pid: live,
      start: 1_700_000_000,
    }))
    .await
    .unwrap();
  let got = read_json(&mut json).await;
  assert_eq!(got["activity"]["name"], "LiveGeneric");

  fx.proc_tx.send(ProcInput::Cleared).await.unwrap();
  // The empty table legitimately clears the slot once...
  let cleared = read_json(&mut json).await;
  assert!(cleared["activity"].is_null());
  // ...and the ghost sweep must not mistake the numeric app id for a
  // dead pid and clear a second time.
  assert!(
    tokio::time::timeout(Duration::from_millis(300), json.next())
      .await
      .is_err(),
    "null sweep must not re-clear after the table clear"
  );

  fx.bridge.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn format_override_consumer_stays_on_resolved_protocol() {
  let fx = fixture().await;
  // Consumer overrides the JSON port to MessagePack: READY already
  // arrives encoded...
  let mut mp = connect(fx.json_port, "?format=msgpack").await;
  let _ = read_msgpack(&mut mp).await;

  fx.ipc_tx.send(set_activity(42, "Encoded")).await.unwrap();
  // ...and every later broadcast must use the resolved encoding, not
  // the port default: one decoder must suffice for the whole stream.
  let got = read_msgpack(&mut mp).await;
  assert_eq!(got["activity"]["name"], "Encoded");

  fx.bridge.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_removes_state_file() {
  let dir = std::env::temp_dir().join(format!("rsrpc-bridge-state-{}", std::process::id()));
  let _ = std::fs::remove_dir_all(&dir);
  std::fs::create_dir_all(&dir).unwrap();

  let (ipc_tx, ipc_rx) = tokio::sync::mpsc::channel(64);
  let (game_tx, game_rx) = tokio::sync::mpsc::channel(1024);
  let (proc_tx, proc_rx) = tokio::sync::mpsc::channel(512);
  let config = BridgeConfig::new(0, 0, 0, 0)
    .app_version("test-bridge")
    .state_dir(dir.clone())
    .persist_interval(Duration::from_millis(50));
  let bridge = Bridge::bind(
    config,
    std::sync::Arc::new(std::sync::Mutex::new(RpcUser::default())),
    BridgeInputs {
      ipc_rx,
      game_rx,
      proc_rx,
    },
  )
  .await
  .expect("bridge binds");
  let state_path = bridge.state_path().expect("slot selected").to_path_buf();

  ipc_tx.send(set_activity(7, "Stateful")).await.unwrap();
  // Debounced persist lands the file shortly after the publish.
  let deadline = std::time::Instant::now() + Duration::from_secs(5);
  loop {
    if state_path.exists() {
      break;
    }
    assert!(
      std::time::Instant::now() < deadline,
      "snapshot never landed"
    );
    std::thread::sleep(Duration::from_millis(20));
  }
  let body: serde_json::Value =
    serde_json::from_str(&std::fs::read_to_string(&state_path).unwrap()).unwrap();
  assert_eq!(body["appVersion"], "test-bridge");
  drop(game_tx);
  drop(proc_tx);

  bridge.shutdown().await;
  assert!(!state_path.exists(), "shutdown releases the slot");
  let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(target_os = "linux")]
async fn null_scan_reaps_dead_pid_cards_from_replay() {
  let fx = fixture().await;
  let mut json = connect(fx.json_port, "?format=json").await;
  let _ = read_json(&mut json).await; // READY

  // Publish from a pid that is already dead (/proc entry absent): the
  // card broadcasts normally (presence first, questions later)...
  let dead = u32::MAX as u64;
  fx.ipc_tx.send(set_activity(dead, "Ghost")).await.unwrap();
  let got = read_json(&mut json).await;
  assert_eq!(got["activity"]["name"], "Ghost");

  // ...but the scanner's empty table proves nothing with that pid lives:
  // the null scan must broadcast its clear and evict it from replay.
  fx.proc_tx.send(ProcInput::Cleared).await.unwrap();
  let cleared = read_json(&mut json).await;
  assert!(
    cleared["activity"].is_null(),
    "expected clear for dead pid, got: {cleared}"
  );
  assert_eq!(cleared["pid"], dead);

  // Late joiner must not replay the ghost.
  let mut late = connect(fx.json_port, "?format=json").await;
  let _ = read_json(&mut late).await; // READY
  assert!(
    tokio::time::timeout(Duration::from_millis(300), late.next())
      .await
      .is_err(),
    "ghost must not replay after reap"
  );

  fx.bridge.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn null_scan_keeps_live_pid_cards() {
  let fx = fixture().await;
  let mut json = connect(fx.json_port, "?format=json").await;
  let _ = read_json(&mut json).await; // READY

  // Our own test-runner pid is alive: its card must survive null scans.
  let live = std::process::id() as u64;
  fx.ipc_tx.send(set_activity(live, "Live")).await.unwrap();
  let got = read_json(&mut json).await;
  assert_eq!(got["activity"]["name"], "Live");

  fx.proc_tx.send(ProcInput::Cleared).await.unwrap();
  // No clear may arrive for a live pid: only READY-gated silence, then
  // the cached card still replays to a late joiner.
  let mut late = connect(fx.json_port, "?format=json").await;
  let _ = read_json(&mut late).await; // READY
  let replay = read_json(&mut late).await;
  assert_eq!(replay["activity"]["name"], "Live");

  fx.bridge.shutdown().await;
}
