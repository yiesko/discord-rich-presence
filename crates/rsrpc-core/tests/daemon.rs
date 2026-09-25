//! Daemon integration: ephemeral bind, one-shot diagnostics, shutdown.

use std::time::Duration;

use rsrpc_core::{Daemon, RPCConfig};

/// Serializes teardown-observing tests: they assert on process-global
/// thread names, so two of them must never overlap. Async mutex: the
/// guard is held across awaits by design.
static SERIAL_TEARDOWN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Live rsRPC worker threads by `rsrpc-*` comm prefix (Linux only):
/// join accounting observes threads by name (see
/// `worker_threads_carry_names` in rsrpc-detect).
#[cfg(target_os = "linux")]
fn rsrpc_worker_tasks() -> Vec<String> {
  let mut tasks = Vec::new();
  if let Ok(entries) = std::fs::read_dir("/proc/self/task") {
    for entry in entries.flatten() {
      if let Ok(comm) = std::fs::read_to_string(entry.path().join("comm")) {
        let name = comm.trim().to_string();
        if name.starts_with("rsrpc-") {
          tasks.push(name);
        }
      }
    }
  }
  tasks
}

/// Zero-port config: every listener binds ephemeral for hermetic tests.
fn ephemeral_config() -> RPCConfig {
  RPCConfig::builder()
    .port(0)
    .bridge_port_end(0)
    .msgpack_port(0)
    .ws_port_start(0)
    .ws_port_end(0)
    .build()
}

/// Shutdown-test config: scanner, proc-events and IPC off (nothing to
/// observe there), ephemeral TCP listener only — fast and hermetic.
fn shutdown_config() -> RPCConfig {
  RPCConfig::builder()
    .port(0)
    .bridge_port_end(0)
    .msgpack_port(0)
    .ws_port_start(0)
    .ws_port_end(0)
    .enable_process_scanner(false)
    .enable_proc_events(false)
    .enable_ipc_connector(false)
    .build()
}

/// Empty databases detect nothing but stay fully operational.
/// Zero scan intervals are rejected before the automaton build (a zero
/// cadence would hot-spin the scan loop).
#[test]
fn zero_scan_interval_is_rejected() {
  let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap();
  let mut config = ephemeral_config();
  config.scan_interval_secs = 0;
  let daemon = Daemon::from_json_str("[]", config).expect("empty db parses");
  let err = rt
    .block_on(daemon.run_until(async {}))
    .expect_err("zero interval must fail");
  assert!(
    err.to_string().contains("scan_interval_secs"),
    "unexpected error: {err}"
  );
}

/// Empty databases detect nothing but stay fully operational.
#[test]
fn empty_database_detects_nothing_but_stays_ok() {
  let daemon = Daemon::from_json_str("[]", ephemeral_config()).expect("empty db parses");
  let found = daemon.detect_once().expect("scan works");
  assert!(found.is_empty());
  assert!(daemon.database_summary().is_empty());
}

/// The bundled snapshot parses and summarizes non-empty.
#[test]
fn bundled_database_summarizes() {
  let daemon = Daemon::from_bundled(RPCConfig::default()).expect("bundled parses");
  assert!(!daemon.database_summary().is_empty());
}

/// Full boot on ephemeral ports shuts down cleanly inside the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_binds_and_shuts_down_cleanly() {
  let daemon = Daemon::from_json_str("[]", shutdown_config()).expect("empty db parses");
  tokio::time::timeout(Duration::from_secs(30), daemon.run_until(async {}))
    .await
    .expect("run returns after immediate shutdown")
    .expect("clean shutdown");
}

/// Full boot WITH the scanner (proc-events and db update off, so only
/// the scan thread spawns) shuts down cleanly inside the deadline.
/// Teardown joins every scanner thread plus the event pump, so a join
/// deadlock here would hang the test instead of leaking silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_with_scanner_completes_and_tears_down() {
  let _serial = SERIAL_TEARDOWN.lock().await;
  let mut config = shutdown_config();
  config.enable_process_scanner = true;
  config.scan_interval_secs = 1;
  let daemon = Daemon::from_json_str("[]", config).expect("empty db parses");
  tokio::time::timeout(
    Duration::from_secs(30),
    daemon.run_until(async {
      tokio::time::sleep(Duration::from_millis(500)).await;
    }),
  )
  .await
  .expect("run returns after shutdown")
  .expect("clean shutdown with scanner on");
  #[cfg(target_os = "linux")]
  assert!(
    rsrpc_worker_tasks().is_empty(),
    "no rsrpc worker may outlive run_until: {:?}",
    rsrpc_worker_tasks()
  );
}

/// Occupy a contiguous block of free loopback ports, holding the
/// listeners until the caller drops them. Used to force bridge bind
/// failures deterministically: the bridge is pointed at the block, so
/// every port it tries is taken — no dependence on default ranges or
/// whatever else runs on this machine.
fn occupy_contiguous_block(width: u16) -> (Vec<std::net::TcpListener>, u16) {
  for start in (40000u16..65000).step_by(width as usize) {
    let mut held = Vec::new();
    let mut full = true;
    for port in start..start.saturating_add(width) {
      match std::net::TcpListener::bind(("127.0.0.1", port)) {
        Ok(socket) => held.push(socket),
        Err(_) => {
          full = false;
          break;
        }
      }
    }
    if full {
      return (held, start);
    }
  }
  panic!("no {width}-port block free for the bind-failure test");
}

/// Every bridge port occupied forces `Bridge::bind` to fail: teardown
/// must still join every scanner thread (named threads make the leak
/// observable — without error-path teardown this fails with live
/// `rsrpc-*` tasks).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bind_failure_still_tears_down_scanner() {
  let _serial = SERIAL_TEARDOWN.lock().await;
  // Block sized for both bridge ranges (JSON width 11, msgpack width
  // 11, union 12): the bridge is pointed inside it below.
  let (_held, base) = occupy_contiguous_block(12);
  let mut config = shutdown_config();
  config.enable_process_scanner = true;
  config.scan_interval_secs = 1;
  // Fixed bridge range (not ephemeral): with the block above occupied,
  // the bind must fail. (`shutdown_config` uses port 0 = ephemeral,
  // which always succeeds.)
  config.port = base;
  config.bridge_port_end = base + 10;
  config.msgpack_port = base + 1;
  let daemon = Daemon::from_json_str("[]", config).expect("empty db parses");
  let result = tokio::time::timeout(Duration::from_secs(30), daemon.run_until(async {}))
    .await
    .expect("run returns after bind failure");
  assert!(
    result.is_err(),
    "bridge bind into an occupied range must fail"
  );
  #[cfg(target_os = "linux")]
  assert!(
    rsrpc_worker_tasks().is_empty(),
    "no rsrpc worker may outlive a failed run_until: {:?}",
    rsrpc_worker_tasks()
  );
}

/// Dropping the `run_until` future (caller abandonment via abort: the
/// `select!`/timeout case) still stops the scanner: a cancellation
/// guard signals shutdown on drop, so threads exit on their own within
/// one poll interval instead of leaking. Without the guard this fails
/// with live `rsrpc-*` tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_run_stops_scanner_threads() {
  let _serial = SERIAL_TEARDOWN.lock().await;
  let mut config = shutdown_config();
  config.enable_process_scanner = true;
  config.scan_interval_secs = 1;
  let daemon = Daemon::from_json_str("[]", config).expect("empty db parses");
  let handle = tokio::spawn(daemon.run_until(std::future::pending()));
  // Let the scanner threads start (automaton build, binds, first tick).
  tokio::time::sleep(Duration::from_secs(1)).await;
  handle.abort();
  let _ = handle.await;
  // Poll for exit: threads stop on their own, no join involved.
  #[cfg(target_os = "linux")]
  {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
      if rsrpc_worker_tasks().is_empty() {
        break;
      }
      assert!(
        std::time::Instant::now() < deadline,
        "abandoned run leaked threads: {:?}",
        rsrpc_worker_tasks()
      );
      tokio::time::sleep(Duration::from_millis(100)).await;
    }
  }
}
