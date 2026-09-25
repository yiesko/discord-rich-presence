//! Activity routing: game commands and scanner reports in, fan-out
//! decisions out.
//!
//! Decides what each input means (new publish, duplicate flood,
//! genuine clear, handoff between SDK and generic detection) and drives
//! the consumer fan-out. Transport-agnostic: pumps feed it commands,
//! it answers through the [`Shared`] state.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use rsrpc_protocol::commands::{self, CachedActivity};
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::{AppId, SocketId};
use tokio::sync::mpsc;

use super::bridge::Shared;
use super::handoff::{ProcInput, ScannedGame, is_process_alive, track_process_publication};
use super::replay::prune_cache;

/// Session-boundary reason for a process-table event: the table went
/// from empty to non-empty (`game-start`) or back to empty (`game-end`).
/// Repeats within a state stay quiet.
pub(crate) fn table_transition(had_games: bool, input: &ProcInput) -> Option<&'static str> {
  match input {
    ProcInput::Detected(_) if !had_games => Some("game-start"),
    ProcInput::Cleared if had_games => Some("game-end"),
    _ => None,
  }
}

/// Queued depth of a census sender (`max - free`).
pub(crate) fn channel_depth<T>(tx: Option<&mpsc::Sender<T>>) -> usize {
  tx.map(|tx| tx.max_capacity().saturating_sub(tx.capacity()))
    .unwrap_or(0)
}

/// Whether a command is a genuine clear from a real connection (null
/// activity + nonzero pid). Messages that never identified a game (pid 0)
/// are not clears.
pub(crate) fn is_genuine_clear(cmd: &ActivityCmd) -> bool {
  match cmd.args.as_ref().and_then(|args| args.pid) {
    Some(pid) if pid != 0 => cmd
      .args
      .as_ref()
      .is_some_and(|args| args.activity.is_none()),
    _ => false,
  }
}

/// Build the generic process-detection payload for a scanned game.
pub(crate) fn generic_payload(game: &ScannedGame) -> Arc<CachedActivity> {
  let payload_struct = commands::ProcessPayload {
    activity: commands::ProcessActivity {
      application_id: game.id.clone(),
      name: game.name.clone(),
      timestamps: commands::ProcessTimestamps { start: game.start },
      r#type: 0,
      metadata: HashMap::new(),
      flags: 0,
    },
    pid: game.pid,
    socket_id: SocketId::from(&game.id),
  };
  // Same fixed-shape guarantee as `empty_cached` (String/int only):
  // encode failure is a future-field bug — degrade loudly in diagnostics,
  // never panic the broadcast path.
  let activity_json = serde_json::to_vec(&payload_struct.activity).unwrap_or_default();
  Arc::new(commands::CachedActivity {
    json: serde_json::to_string(&payload_struct)
      .map(tungstenite::Utf8Bytes::from)
      .unwrap_or_else(|err| {
        tracing::debug!("[bridge] Generic payload encode failed: {err}");
        tungstenite::Utf8Bytes::from_static("")
      }),
    msgpack: rmp_serde::to_vec_named(&payload_struct)
      .map(bytes::Bytes::from)
      .unwrap_or_else(|err| {
        tracing::debug!("[bridge] Generic payload encode failed: {err}");
        bytes::Bytes::new()
      }),
    // Always built with `activity: Some` above.
    is_clear: false,
    activity_json: bytes::Bytes::from(activity_json),
  })
}

/// Remove one process publication only when the stored pid matches the
/// removal event (pid reuse / EXEC-vs-poll race must not clear a newer
/// detection). Single map access: callers must hold one guard across the
/// check, never compare-then-remove under separate locks.
pub(crate) fn take_matching_process(
  last_process: &mut HashMap<AppId, u64>,
  app_id: &AppId,
  pid: u64,
) -> Option<u64> {
  if last_process.get(app_id).is_some_and(|known| *known == pid) {
    last_process.remove(app_id)
  } else {
    None
  }
}

/// Consume the outstanding process publications for clearing, if any.
/// Returns `(pid, app_id)` pairs, sorted for deterministic clears.
/// Single-shot by construction (`drain`): repeated null scans clear once
/// and then skip.
pub(crate) fn take_process_clear(shared: &Shared) -> Vec<(u64, AppId)> {
  let mut outstanding: Vec<(u64, AppId)> = shared
    .last_process
    .lock()
    .unwrap_or_else(|e| e.into_inner())
    .drain()
    .map(|(app_id, pid)| (pid, app_id))
    .collect();
  outstanding.sort();
  outstanding
}

impl Shared {
  /// IPC-wins handoff: a live SDK presence takes over this app slot
  /// from generic detection (last publisher wins across companions).
  pub(crate) fn note_sdk_publish(&self, cmd: &ActivityCmd) {
    let args = cmd.args.as_ref();
    let pid = args.and_then(|args| args.pid).unwrap_or_default();
    if let Some(app) = args
      .and_then(|args| args.activity.as_ref())
      .and_then(|activity| activity.application_id.clone())
    {
      self
        .handoff
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .note_publish(&app, pid);
    }
  }

  /// Compare the current fingerprint against the cached entry, so first
  /// publishes, real changes and effective clears still log once.
  /// Byte compare: both sides come from the same serializer and the
  /// stored bytes travel with the build — no re-serialize, no re-parse.
  pub(crate) fn activity_changed(&self, pid: u64, fingerprint: Option<&[u8]>) -> bool {
    // Clone the two small shape facts we need (flag + bytes) and let the
    // guard drop at the semicolon: nothing below holds the map lock.
    let cached = self
      .cache
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .get(&SocketId::from(pid.to_string()))
      .map(|(entry, _)| (entry.is_clear, entry.activity_json.clone()));
    match (cached, fingerprint) {
      (None, None) => false,
      (None, Some(_)) => true,
      (Some((is_clear, _)), None) => !is_clear,
      (Some((is_clear, stored)), Some(fp)) => is_clear || stored.as_ref() != fp,
    }
  }

  /// A genuine clear (null activity) from a real connection means the
  /// SDK source went away: hand the slot back to generic detection so
  /// the scanner re-asserts the still-running game. pid == 0 means no
  /// game was ever identified — ignore those, or every fresh
  /// connection would flap the display.
  pub(crate) fn resume_after_clear(&self, cmd: &ActivityCmd, changed: bool) {
    if !is_genuine_clear(cmd) {
      return;
    }
    let pid = cmd
      .args
      .as_ref()
      .and_then(|args| args.pid)
      .unwrap_or_default();
    let resume: Vec<ScannedGame> = {
      let mut handoff = self.handoff.lock().unwrap_or_else(|e| e.into_inner());
      match cmd.application_id.clone() {
        Some(app) if handoff.note_clear(&app, pid) => {
          handoff.resume_for(app.as_ref()).into_iter().collect()
        }
        Some(_) => Vec::new(),
        // No app id: abrupt close (socket died without CLEAR).
        // Release every slot this pid owned, or their generics stay
        // suppressed by a dead owner forever.
        None => handoff
          .note_clear_pid(pid)
          .into_iter()
          .filter_map(|app| handoff.resume_for(app.as_ref()))
          .collect(),
      }
    };
    for game in resume.into_iter().filter(|game| is_process_alive(game.pid)) {
      self.resume_generic(&game);
    }
    if changed {
      tracing::info!("[bridge] Source cleared, resuming process detection");
    } else {
      tracing::debug!("[bridge] Duplicate clear ignored (pid {pid})");
    }
  }

  /// Handle one `SET_ACTIVITY` command: fingerprint, flood-guard,
  /// envelope build, handoff, change detection, genuine-clear resume,
  /// conditional fan-out.
  pub(crate) async fn handle_set_activity(&self, mut cmd: ActivityCmd) {
    // Fingerprint first (runs `fix()`): flood-dropped publishes return
    // before the envelope (JSON + MessagePack) is ever built.
    let fingerprint = commands::activity_fingerprint(&mut cmd);
    let pid = cmd
      .args
      .as_ref()
      .and_then(|args| args.pid)
      .unwrap_or_default();
    if self.flood_dropped(&cmd, fingerprint.as_deref()) {
      let app_key = cmd.application_id.as_deref().unwrap_or("");
      tracing::debug!("[bridge] Dropping duplicate SET_ACTIVITY (app {app_key}, pid {pid})");
      return;
    }
    let Some(payload) = commands::cached_activity(&mut cmd, fingerprint.clone()) else {
      tracing::warn!("[bridge] Invalid activity command, skipping");
      return;
    };
    let app_key = cmd.application_id.as_deref().unwrap_or("");
    let activity = cmd.args.as_ref().and_then(|args| args.activity.as_ref());
    self.note_sdk_publish(&cmd);
    // NOTE: no ignore-list filtering here by design. Forwarded client
    // frames are indistinguishable on this path — filtering would kill
    // the companion this bridge exists to carry.
    let changed = self.activity_changed(pid, fingerprint.as_deref());
    match activity {
      Some(activity) => {
        if changed {
          tracing::info!(
            "[bridge] Published: {} (app {}, pid {})",
            activity.display_name(),
            activity.application_id.as_deref().unwrap_or("?"),
            pid
          );
        } else {
          tracing::debug!(
            "[bridge] Published: {} (app {}, pid {})",
            activity.display_name(),
            activity.application_id.as_deref().unwrap_or("?"),
            pid
          );
        }
      }
      None => {
        if changed {
          tracing::info!("[bridge] Published clear (pid {pid})");
        } else {
          tracing::debug!("[bridge] Published clear (pid {pid})");
        }
      }
    }
    // A genuine clear (null activity) from a real connection means the
    // SDK source went away: hand the slot back to generic detection.
    self.resume_after_clear(&cmd, changed);
    // Identical republishes change nothing observable: the replay cache
    // already holds these exact bytes (late joiners replay them) and the
    // refresh re-asserts them — so skip the fan-out. First publishes,
    // real changes and effective clears always pass.
    if changed {
      self.broadcast_activity(payload, SocketId::from(pid.to_string()));
    } else {
      tracing::debug!(
        "[bridge] Already published identical activity (app {app_key}, pid {pid}), skipping fan-out"
      );
    }
  }

  /// Re-assert generic process presence after its source cleared. The
  /// scanner only emits on *changes*, so without this the slot would stay
  /// dark until the next game switch.
  pub(crate) fn resume_generic(&self, game: &ScannedGame) {
    track_process_publication(
      &mut self.last_process.lock().unwrap_or_else(|e| e.into_inner()),
      game.id.clone(),
      game.pid,
    );
    tracing::debug!(
      "[bridge] Resuming generic presence for {} ({})",
      game.name,
      game.id
    );
    self.broadcast_activity(generic_payload(game), SocketId::from(&game.id));
  }

  /// Broadcast an activity payload, updating the replay cache (clears
  /// evict) and marking the snapshot dirty — never persisting inline.
  pub(crate) fn broadcast_activity(&self, payload: Arc<CachedActivity>, socket_id: SocketId) {
    // Keep the replay cache in sync, pruning cleared activities. The flag
    // travels with the build — no re-parse of our own serialization.
    let is_clear = payload.is_clear;
    {
      let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
      if is_clear {
        cache.remove(&socket_id);
      } else {
        let mut seq = self.activity_seq.lock().unwrap_or_else(|e| e.into_inner());
        *seq = seq.saturating_add(1);
        cache.insert(socket_id, (Arc::clone(&payload), *seq));
        prune_cache(&mut cache);
      }
    }
    self.send_to_all(&payload);
    self.dirty.store(true, Ordering::Relaxed);
  }
}
