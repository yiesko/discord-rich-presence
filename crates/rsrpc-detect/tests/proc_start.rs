//! Process start times: real-process round trip plus unknown-pid refusal.

use rsrpc_detect::proc_start::process_start_ms;

fn now_ms() -> u64 {
  std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .ok()
    .and_then(|age| u64::try_from(age.as_millis()).ok())
    .unwrap_or(0)
}

/// Own pid resolves to a plausible start time (not the future, after 2023).
#[test]
fn own_process_start_is_plausible() {
  let started = process_start_ms(u64::from(std::process::id())).expect("own pid resolves");
  assert!(started <= now_ms(), "start {started} is in the future");
  assert!(
    started > 1_700_000_000_000,
    "start {started} is implausibly old"
  );
}

/// Vanished pids refuse instead of fabricating a time.
#[test]
fn unknown_pid_refuses() {
  assert_eq!(process_start_ms(u64::MAX), None);
}
