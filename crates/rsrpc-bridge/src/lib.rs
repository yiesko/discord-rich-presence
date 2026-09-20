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
pub mod handoff;

pub use bridge::{Bridge, BridgeInputs, cache_entry_pid};
pub use config::BridgeConfig;
pub use handoff::{ProcInput, ScannedGame};
