//! Typed errors for the former `Message(String)` catch-all sites.
//!
//! Every production call site now names its failure: no stringly-typed
//! errors cross crate boundaries (`type-no-stringly`).

use rsrpc_protocol::error::RsrpcError;

#[test]
fn scan_in_progress_keeps_historical_text() {
  assert_eq!(
    RsrpcError::ScanInProgress.to_string(),
    "Scanning already in progress"
  );
}

#[test]
fn bind_exhaustion_names_range() {
  assert_eq!(
    RsrpcError::WsExhausted {
      start: 6463,
      end: 6472
    }
    .to_string(),
    "websocket bind failed on ports 6463-6472: all in use"
  );
  assert_eq!(
    RsrpcError::BridgeBind {
      name: "json",
      start: 1337,
      end: 1347
    }
    .to_string(),
    "bridge json launch failed on ports 1337-1347: all in use"
  );
}

#[test]
fn invalid_config_names_the_field() {
  assert_eq!(
    RsrpcError::InvalidConfig("event_queue must be non-zero").to_string(),
    "invalid transport config: event_queue must be non-zero"
  );
}
