pub mod client_connector;
pub mod ipc_utils;
pub mod process;
pub mod steam;
pub mod utils;
pub mod websocket;

#[cfg(target_os = "linux")]
pub(crate) use rsrpc_proc_events as proc_events;

#[cfg(target_os = "windows")]
pub mod ipc_win;

#[cfg(not(target_os = "windows"))]
pub mod ipc_unix;

#[cfg(target_os = "windows")]
mod platform {
  pub use super::ipc_win as ipc;
}

#[cfg(not(target_os = "windows"))]
mod platform {
  pub use super::ipc_unix as ipc;
}

pub use platform::ipc;
