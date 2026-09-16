//! Shared integration-test helpers: serialized env mutation with
//! restore-on-drop (env is process-global).
//!
//! Import with `#[path = "common/mod.rs"] mod common;`.

use std::sync::{Mutex, MutexGuard};

/// Serializes every test that mutates process-global env: the variables
/// are process-wide, so exactly one env borrower runs at a time (a
/// concurrent reader during `set_var` is a data race). Poison-proof: a
/// panicking holder must not wedge the rest of the suite.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Lock [`ENV_LOCK`], tolerating a poisoned predecessor.
pub fn lock_env() -> MutexGuard<'static, ()> {
  ENV_LOCK
    .lock()
    .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Process-global env var set for a test body, restored on drop — even
/// on panic (a trailing restore never runs then, poisoning parallel
/// tests). Hold a [`lock_env`] guard alongside while using it: env
/// mutation is only sound single-threaded, which the lock guarantees.
///
/// # Safety
///
/// Constructing this touches process-global state; the caller must hold
/// [`lock_env`] for the whole lifetime (all current callers do).
pub struct EnvRestore {
  key: &'static str,
  previous: Option<String>,
}

impl EnvRestore {
  /// Set `key` to `value`, remembering the previous state for restore.
  pub fn set(key: &'static str, value: &str) -> Self {
    let previous = std::env::var(key).ok();
    // SAFETY: by contract the caller holds `lock_env`, so no other
    // thread observes the environment concurrently.
    unsafe { std::env::set_var(key, value) };
    Self { key, previous }
  }
}

impl Drop for EnvRestore {
  fn drop(&mut self) {
    // SAFETY: same contract as construction (drop runs on the same
    // thread while the test's `lock_env` guard is still alive: guards
    // are declared before us, so they drop after us).
    unsafe {
      match &self.previous {
        Some(value) => std::env::set_var(self.key, value),
        None => std::env::remove_var(self.key),
      }
    }
  }
}
