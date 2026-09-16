//! Compatibility shim: the scanner lives in `rsrpc-detect` now.
//!
//! Temporary (removed in Phase 6 when the core drives the scanner
//! directly): glob re-exports keep every `crate::server::process::X`
//! path working untouched.
#![allow(unused_imports)] // re-exports serve #[cfg(test)] consumers; invisible to the lib-only build.
pub use rsrpc_detect::bundle::*;
pub use rsrpc_detect::refresh::*;
pub use rsrpc_detect::scan::*;
pub use rsrpc_detect::server::*;
pub use rsrpc_detect::types::*;
