//! Clear-reason taxonomy: stable log identifiers for journal correlation.

use rsrpc_bridge::handoff::ClearReason;

/// Log strings are stable identifiers for journal correlation.
#[test]
fn clear_reason_log_strings() {
  assert_eq!(ClearReason::SdkClear.as_str(), "sdk-clear");
  assert_eq!(ClearReason::AbruptClose.as_str(), "abrupt-close");
  assert_eq!(ClearReason::ProcessVanished.as_str(), "process-vanished");
  assert_eq!(ClearReason::Yielded.as_str(), "yielded");
}
