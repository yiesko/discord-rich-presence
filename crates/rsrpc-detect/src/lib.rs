//! Game detection: database projection, matcher bundle, process scanner.
//!
//! The scanner reads `/proc` (Linux) or `sysinfo` (elsewhere), classifies
//! each process against an Aho-Corasick bundle built from Discord's
//! detectable database plus user overrides, and emits deltas. Reads scale
//! lock-free (`ArcSwap` bundle, `RwLock` exclusions); only genuinely
//! mutable per-tick state sits behind short `Mutex` sections.

pub mod db;
