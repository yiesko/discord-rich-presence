//! Transport tuning: port range, feature flags, queue bounds.

/// Default bound for the transport→bridge event queue.
///
/// Deep enough for legitimate bursts (100+ games publishing at once is
/// unrealistic; floods are shed counted past this).
pub const DEFAULT_EVENT_QUEUE: usize = 1024;

/// Default cap on concurrent game connections.
pub const DEFAULT_MAX_CONNECTIONS: usize = 256;

/// Default bound per game-client outbox (mirrors `rsrpc-ws` default).
pub const DEFAULT_PER_CLIENT_QUEUE: usize = 64;

/// Game WebSocket transport configuration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WsTransportConfig {
  /// First port of the loopback bind range (inclusive).
  pub port_start: u16,
  /// Last port of the loopback bind range (inclusive).
  pub port_end: u16,
  /// Forward `SET_ACTIVITY` to the sink (game presence path).
  pub set_activity: bool,
  /// Forward secondary commands (browser/deep-link/callbacks).
  pub secondary_events: bool,
  /// Bound for the transport→bridge event queue.
  pub event_queue: usize,
  /// Cap on concurrent game connections.
  pub max_connections: usize,
  /// Bound per game-client outbox.
  pub per_client_queue: usize,
}

impl WsTransportConfig {
  /// Bind range `port_start..=port_end` on loopback with full behavior.
  /// Use `(0, 0)` for an OS-assigned ephemeral port (tests).
  #[must_use]
  pub fn new(port_start: u16, port_end: u16) -> Self {
    Self {
      port_start,
      port_end,
      set_activity: true,
      secondary_events: true,
      event_queue: DEFAULT_EVENT_QUEUE,
      max_connections: DEFAULT_MAX_CONNECTIONS,
      per_client_queue: DEFAULT_PER_CLIENT_QUEUE,
    }
  }

  /// Forward `SET_ACTIVITY` to the sink.
  #[must_use]
  pub fn set_activity(mut self, value: bool) -> Self {
    self.set_activity = value;
    self
  }

  /// Forward secondary commands (browser/deep-link/callbacks).
  #[must_use]
  pub fn secondary_events(mut self, value: bool) -> Self {
    self.secondary_events = value;
    self
  }

  /// Bound for the transport→bridge event queue. Must be non-zero.
  #[must_use]
  pub fn event_queue(mut self, n: usize) -> Self {
    self.event_queue = n;
    self
  }

  /// Cap on concurrent game connections. Must be non-zero.
  #[must_use]
  pub fn max_connections(mut self, n: usize) -> Self {
    self.max_connections = n;
    self
  }

  /// Bound per game-client outbox. Must be non-zero.
  #[must_use]
  pub fn per_client_queue(mut self, n: usize) -> Self {
    self.per_client_queue = n;
    self
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Defaults enable activity plus secondary events with bounded queues.
  #[test]
  fn defaults_enable_full_behavior() {
    let config = WsTransportConfig::new(6463, 6472);
    assert!(config.set_activity);
    assert!(config.secondary_events);
    assert_eq!(config.event_queue, DEFAULT_EVENT_QUEUE);
    assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
    assert_eq!(config.per_client_queue, DEFAULT_PER_CLIENT_QUEUE);
  }
}
