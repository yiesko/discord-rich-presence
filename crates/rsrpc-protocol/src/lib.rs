//! rsRPC wire protocol: errors, bridge payloads, reply builders, flood guard.
//!
//! The crate's only dependency is [`rsrpc_types`]: no I/O, no threads, no
//! HTTP client (fetch failures arrive opaquely wrapped by the caller).
//! Payload builders return `Arc`-shared values so broadcast fan-out clones
//! a pointer, never the payload (`mem-zero-copy`).

pub mod commands;
pub mod error;
pub mod query;
