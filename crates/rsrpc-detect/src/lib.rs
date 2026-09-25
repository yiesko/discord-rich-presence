//! Game detection: database projection, matcher bundle, process scanner.
//!
//! The scanner reads `/proc` (Linux) or `sysinfo` (elsewhere), classifies
//! each process against an Aho-Corasick bundle built from Discord's
//! detectable database plus user overrides, and emits deltas. Reads scale
//! lock-free (`ArcSwap` bundle, `RwLock` exclusions); only genuinely
//! mutable per-tick state sits behind short `Mutex` sections.

pub mod bundle;
pub mod cache;
pub mod database;
pub mod db;
pub mod refresh;
pub mod runtime;
pub mod scan;
pub mod scanner;
pub mod server;
pub mod steam;
pub mod types;

pub use types::ProcessDetectedEvent;
