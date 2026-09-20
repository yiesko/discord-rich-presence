//! Daemon wiring for rsRPC: config, database loading, one-shot
//! diagnostics, and the async run loop.
//!
//! [`Daemon`] holds the parsed database plus staged overrides and either
//! answers diagnostics or consumes itself into the running world
//! (scanner threads, both transports, the bridge) on the caller's Tokio
//! runtime. The crate never installs log subscribers or signal handlers:
//! the binary owns those.

pub mod config;
pub mod daemon;
pub mod database;
pub mod overrides;

pub use config::{RPCConfig, RPCConfigBuilder};
pub use daemon::Daemon;
pub use database::{DetectableSummary, DetectedGame};

/// HTTP agent with a global timeout: without it, a blackholed endpoint
/// hangs fetches — boot, refresh checks, update checks — forever. Callers
/// add their own size caps.
#[must_use]
pub fn http_agent(timeout: std::time::Duration) -> ureq::Agent {
  ureq::Agent::config_builder()
    .timeout_global(Some(timeout))
    .build()
    .into()
}
