//! arRPC-compatible activity bridge: fan-out, replay cache, handoff.
//!
//! Owns the JSON and MessagePack consumer servers plus every pump under
//! one cancellation token. Two deliberate improvements over the legacy
//! connector it replaces:
//!
//! - Snapshots persist dirty-gated on a cadence, never on every publish.
//! - Refresh rebroadcasts share the cached `Arc`, never cloning payloads.
//!
//! Awaits a Tokio runtime from the caller (no hidden runtime).

pub mod bridge;
pub mod config;
pub mod consumer;
pub mod control;
pub mod handoff;
pub mod replay;
pub mod router;
pub mod snapshot;

pub use bridge::{Bridge, BridgeInputs};
pub use config::BridgeConfig;
pub use handoff::{ProcInput, ScannedGame};
pub use replay::cache_entry_pid;
