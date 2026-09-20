//! Server tuning: every queue bounded, every limit explicit.

use std::{net::SocketAddr, time::Duration};

use crate::Error;

/// Default cap on concurrent connections.
///
/// Each connection owns one Tokio task plus a bounded outbox; 1024 keeps
/// FD and memory use predictable on a loopback daemon.
pub const DEFAULT_MAX_CONNECTIONS: usize = 1024;

/// Default bound for the global event queue (connect/message/disconnect).
pub const DEFAULT_EVENT_QUEUE: usize = 1024;

/// Default bound for each client's outbound queue.
///
/// A full outbox means a slow consumer; the server prunes it instead of
/// growing memory (slow-loris protection).
pub const DEFAULT_PER_CLIENT_QUEUE: usize = 64;

/// Default cap for a single incoming message (1 MiB).
///
/// Well above any Discord RPC payload; bounds attacker-controlled allocation
/// before tungstenite assembles the message.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// Default cap for a single incoming frame (256 KiB).
pub const DEFAULT_MAX_FRAME_SIZE: usize = 256 * 1024;

/// Default server-side ping interval (30 s).
pub const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Default idle timeout (60 s): peers that answer no ping and send nothing
/// are closed so half-open connections never pin a task forever.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Server tuning; all queues bounded (`async-bounded-channel`).
///
/// Built via [`ServerConfig::builder`]; see field docs on the `DEFAULT_*`
/// constants for rationale.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ServerConfig {
  /// Local address to bind.
  pub bind: SocketAddr,
  /// Cap on concurrent connections (accept-side semaphore).
  pub max_connections: usize,
  /// Bound for the global event queue.
  pub event_queue: usize,
  /// Bound for each client's outbound queue.
  pub per_client_queue: usize,
  /// Cap for a single incoming message; `None` disables (not recommended).
  pub max_message_size: Option<usize>,
  /// Cap for a single incoming frame; `None` disables (not recommended).
  pub max_frame_size: Option<usize>,
  /// Interval between server-side pings.
  pub keepalive_interval: Duration,
  /// How long a silent peer may stay connected.
  pub idle_timeout: Duration,
}

impl ServerConfig {
  /// Start building a config for `bind`.
  ///
  /// `#[must_use]` so a dropped builder is a visible lint (`api-builder-must-use`).
  ///
  /// # Examples
  ///
  /// ```
  /// use rsrpc_ws::ServerConfig;
  ///
  /// let config = ServerConfig::builder("127.0.0.1:0".parse().unwrap())
  ///   .max_connections(64)
  ///   .build()
  ///   .unwrap();
  /// assert_eq!(config.max_connections, 64);
  /// ```
  #[must_use]
  pub fn builder(bind: SocketAddr) -> ServerConfigBuilder {
    ServerConfigBuilder {
      bind,
      max_connections: DEFAULT_MAX_CONNECTIONS,
      event_queue: DEFAULT_EVENT_QUEUE,
      per_client_queue: DEFAULT_PER_CLIENT_QUEUE,
      max_message_size: Some(DEFAULT_MAX_MESSAGE_SIZE),
      max_frame_size: Some(DEFAULT_MAX_FRAME_SIZE),
      keepalive_interval: DEFAULT_KEEPALIVE_INTERVAL,
      idle_timeout: DEFAULT_IDLE_TIMEOUT,
    }
  }
}

/// Builder for [`ServerConfig`] (`api-builder-pattern`).
#[derive(Debug, Clone)]
pub struct ServerConfigBuilder {
  bind: SocketAddr,
  max_connections: usize,
  event_queue: usize,
  per_client_queue: usize,
  max_message_size: Option<usize>,
  max_frame_size: Option<usize>,
  keepalive_interval: Duration,
  idle_timeout: Duration,
}

impl ServerConfigBuilder {
  /// Cap on concurrent connections. Must be non-zero.
  #[must_use]
  pub fn max_connections(mut self, n: usize) -> Self {
    self.max_connections = n;
    self
  }

  /// Bound for the global event queue. Must be non-zero.
  #[must_use]
  pub fn event_queue(mut self, n: usize) -> Self {
    self.event_queue = n;
    self
  }

  /// Bound for each client's outbound queue. Must be non-zero.
  #[must_use]
  pub fn per_client_queue(mut self, n: usize) -> Self {
    self.per_client_queue = n;
    self
  }

  /// Cap for a single incoming message.
  #[must_use]
  pub fn max_message_size(mut self, n: Option<usize>) -> Self {
    self.max_message_size = n;
    self
  }

  /// Cap for a single incoming frame.
  #[must_use]
  pub fn max_frame_size(mut self, n: Option<usize>) -> Self {
    self.max_frame_size = n;
    self
  }

  /// Interval between server-side pings. Must be non-zero.
  #[must_use]
  pub fn keepalive_interval(mut self, d: Duration) -> Self {
    self.keepalive_interval = d;
    self
  }

  /// How long a silent peer may stay connected. Must be non-zero.
  #[must_use]
  pub fn idle_timeout(mut self, d: Duration) -> Self {
    self.idle_timeout = d;
    self
  }

  /// Validate and build.
  ///
  /// # Errors
  ///
  /// Returns [`Error::Config`] when any bound is zero.
  pub fn build(self) -> Result<ServerConfig, Error> {
    if self.max_connections == 0 {
      return Err(Error::Config("max_connections must be non-zero"));
    }
    if self.event_queue == 0 {
      return Err(Error::Config("event_queue must be non-zero"));
    }
    if self.per_client_queue == 0 {
      return Err(Error::Config("per_client_queue must be non-zero"));
    }
    if self.keepalive_interval.is_zero() {
      return Err(Error::Config("keepalive_interval must be non-zero"));
    }
    if self.idle_timeout.is_zero() {
      return Err(Error::Config("idle_timeout must be non-zero"));
    }
    Ok(ServerConfig {
      bind: self.bind,
      max_connections: self.max_connections,
      event_queue: self.event_queue,
      per_client_queue: self.per_client_queue,
      max_message_size: self.max_message_size,
      max_frame_size: self.max_frame_size,
      keepalive_interval: self.keepalive_interval,
      idle_timeout: self.idle_timeout,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Ephemeral loopback address for hermetic bind tests.
  fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
  }

  /// Defaults match the documented production bounds.
  #[test]
  fn builder_defaults_are_sane() {
    let cfg = ServerConfig::builder(loopback()).build().unwrap();
    assert_eq!(cfg.max_connections, DEFAULT_MAX_CONNECTIONS);
    assert_eq!(cfg.event_queue, DEFAULT_EVENT_QUEUE);
    assert_eq!(cfg.per_client_queue, DEFAULT_PER_CLIENT_QUEUE);
    assert_eq!(cfg.max_message_size, Some(DEFAULT_MAX_MESSAGE_SIZE));
    assert_eq!(cfg.max_frame_size, Some(DEFAULT_MAX_FRAME_SIZE));
    assert_eq!(cfg.keepalive_interval, DEFAULT_KEEPALIVE_INTERVAL);
    assert_eq!(cfg.idle_timeout, DEFAULT_IDLE_TIMEOUT);
  }

  /// Zero bounds are rejected at build time, never at bind time.
  #[test]
  fn builder_rejects_zero_bounds() {
    assert!(matches!(
      ServerConfig::builder(loopback()).max_connections(0).build(),
      Err(Error::Config(_))
    ));
    assert!(matches!(
      ServerConfig::builder(loopback()).event_queue(0).build(),
      Err(Error::Config(_))
    ));
    assert!(matches!(
      ServerConfig::builder(loopback())
        .per_client_queue(0)
        .build(),
      Err(Error::Config(_))
    ));
    assert!(matches!(
      ServerConfig::builder(loopback())
        .keepalive_interval(Duration::ZERO)
        .build(),
      Err(Error::Config(_))
    ));
    assert!(matches!(
      ServerConfig::builder(loopback())
        .idle_timeout(Duration::ZERO)
        .build(),
      Err(Error::Config(_))
    ));
  }
}
