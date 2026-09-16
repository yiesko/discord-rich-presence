//! Discord IPC transport: framed protocol plus platform servers.
//!
//! [`frame`] is platform-independent (any synchronous duplex byte stream);
//! [`unix`] and `windows` own their listeners with a shared JoinSet
//! lifecycle. Transports await a Tokio runtime from the caller.
//!
//! Behavioral differences from the legacy transport:
//! - No mid-connection listener rebind: the bound socket/pipe serves for
//!   the transport lifetime. The legacy index migration on every client
//!   `Close` could flap clients between `discord-ipc-N` paths and stale
//!   the boot-time snapshot path.
//! - Bounded queues shed counted instead of parking connection threads.

pub mod frame;
pub mod sink;

#[cfg(unix)]
pub mod paths;
#[cfg(unix)]
mod probe;
#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

#[cfg(not(any(unix, windows)))]
compile_error!("rsrpc-transport-ipc supports only unix and windows");

#[cfg(unix)]
pub use unix::IpcTransport;
#[cfg(windows)]
pub use windows::IpcTransport;

pub use frame::send_empty;
pub use sink::{DEFAULT_IPC_QUEUE, EventSink};
