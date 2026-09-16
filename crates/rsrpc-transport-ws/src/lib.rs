//! Modern async game WebSocket transport.
//!
//! Awaits a Tokio runtime from the caller (no hidden runtime): [`WsTransport::bind`]
//! is `async`. The event pump never holds the client-map lock across `.await`
//! — every map access is a short clone/insert/remove, which is what fixes the
//! legacy `websocket.rs` contention (P1).

pub mod config;
pub mod handlers;
pub mod transport;

pub use config::WsTransportConfig;
pub use transport::{TransportHandle, WsTransport};
