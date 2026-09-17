//! rsRPC wire protocol: errors, bridge payloads, reply builders, flood guard.
//!
//! The only workspace dependency is [`rsrpc_types`]: no I/O, no threads.
//! Payload builders return `Arc`-shared values so broadcast fan-out clones
//! a pointer, never the payload (`mem-zero-copy`).

pub mod commands;
pub mod error;
pub mod query;
