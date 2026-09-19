//! Scanner lifecycle tests: generation swaps are atomic and observable.

use std::sync::Arc;

use rsrpc_detect::db::DetectableActivity;
use rsrpc_detect::refresh::RefreshConfig;
use rsrpc_detect::server::ProcessServer;
use rsrpc_detect::types::{Exec, ProcessEventListeners};

/// One-game custom database fixture for generation-swap tests.
fn custom_db() -> Vec<DetectableActivity> {
  serde_json::from_value(serde_json::json!([
    {
      "id": "424242",
      "name": "Swap Game",
      "hook": true,
      "executables": [{"name": "swapgame.exe", "is_launcher": false, "os": "win32"}],
    }
  ]))
  .expect("fixture parses")
}

/// Empty-database server fixture with a live event sender.
fn server() -> ProcessServer {
  let (tx, _rx) = rsrpc_telemetry::QueueGauge::pair();
  ProcessServer::new_with_custom(
    vec![],
    vec![],
    tx,
    ProcessEventListeners::default(),
    RefreshConfig::default(),
    vec![],
  )
}

/// Appends swap in a new `Arc` generation that classifies immediately.
#[test]
fn append_swaps_in_a_new_classifying_generation() {
  let server = server();
  let before = server.bundle();
  server.append_detectables(custom_db());
  let after = server.bundle();
  // A new Arc: readers holding `before` keep a consistent old snapshot.
  assert!(!Arc::ptr_eq(&before, &after));

  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed = String::with_capacity(256);
  let mut obs_open = false;
  let hit = server
    .match_process(
      &Exec {
        pid: 1,
        path: "/games/swapgame.exe".to_string(),
        arguments: None,
      },
      &after,
      &mut variant_bufs,
      &mut reversed,
      &mut obs_open,
    )
    .expect("fresh generation classifies");
  assert_eq!(&*hit.entry.id, "424242");
}

/// Removed entries stop classifying in the swapped generation.
#[test]
fn remove_by_name_drops_the_generation_entry() {
  let server = server();
  server.append_detectables(custom_db());
  server.remove_detectable_by_name("Swap Game");
  let bundle = server.bundle();
  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed = String::with_capacity(256);
  let mut obs_open = false;
  assert!(
    server
      .match_process(
        &Exec {
          pid: 1,
          path: "/games/swapgame.exe".to_string(),
          arguments: None
        },
        &bundle,
        &mut variant_bufs,
        &mut reversed,
        &mut obs_open,
      )
      .is_none(),
    "removed entry must not classify"
  );
}
