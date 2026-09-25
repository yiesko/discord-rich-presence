//! Replay cache: last payload per socket id, rebroadcast to late
//! joiners and revisited on refresh.
//!
//! Bounded by [`MAX_CACHED_ACTIVITIES`][crate::config::MAX_CACHED_ACTIVITIES]:
//! eviction drops the oldest sequence first. Pids resolve out of payload
//! bodies for ghost reaping (see [`cache_entry_pid`]).

use std::collections::HashMap;
use std::sync::Arc;

use rsrpc_protocol::commands::CachedActivity;
use rsrpc_types::SocketId;

/// Replay cache: socket id → (shared payload, sequence).
pub(crate) type ReplayCache = HashMap<SocketId, (Arc<CachedActivity>, u64)>;

/// Evict the oldest entries while the replay cache exceeds
/// [`MAX_CACHED_ACTIVITIES`][crate::config::MAX_CACHED_ACTIVITIES].
/// Pure map operation (no locks taken here).
pub(crate) fn prune_cache(cache: &mut ReplayCache) {
  while cache.len() > crate::config::MAX_CACHED_ACTIVITIES {
    let oldest = cache
      .iter()
      .min_by_key(|(_, (_, seq))| *seq)
      .map(|(key, _)| key.clone());
    match oldest {
      Some(key) => {
        cache.remove(&key);
      }
      None => break,
    }
  }
}

/// Resolve the owning pid of a replay-cache entry for ghost reaping:
/// the pid rides in the JSON body, falling back to numeric socket ids
/// (SDK connections use the pid verbatim). The body is authoritative
/// because generic scanner cards are cached under the numeric
/// application id: resolving the socket id first would mistake the app
/// id for a dead pid and evict live generic cards on every null sweep.
/// Returns `None` when neither yields a usable pid — pid 0 included:
/// unidentifiable publishers can never be proven dead, and clearing
/// them risks darkening a live-but-broken client (same convention as
/// the private `is_genuine_clear`).
pub fn cache_entry_pid(socket_id: &SocketId, payload: &CachedActivity) -> Option<u64> {
  let body_pid = serde_json::from_str::<serde_json::Value>(&payload.json)
    .ok()
    .and_then(|body| body.get("pid").and_then(serde_json::Value::as_u64))
    .filter(|pid| *pid != 0);
  if let Some(pid) = body_pid {
    return Some(pid);
  }
  if let Ok(pid) = socket_id.as_ref().parse::<u64>() {
    return (pid != 0).then_some(pid);
  }
  None
}
