use std::{
  collections::VecDeque,
  sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
  },
  time::Duration,
};

use rsrpc_protocol::query::query_params;
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_types::user::RpcUser;
use rsrpc_ws::{ClientId, CloseCode, Event, EventHub, Message, Responder};
use rustc_hash::FxHashMap;
use tokio::sync::RwLock;

use crate::handlers;

use super::server::origin_allowed;
use super::sink::{Admission, ClientSlot, PendingClear, Sink, TryEmit};

/// Cheap per-message snapshot: `responder` is an `Arc` bump and the id is
/// a short string. Publication history is never cloned here — it is only
/// read on disconnect and appended on `SET_ACTIVITY`, both under short
/// write guards.
struct SlotSnapshot {
  responder: Responder,
  query_client_id: Option<String>,
}

/// Pump-owned context (moved into the pump task).
pub(crate) struct PumpCtx {
  pub(crate) hub: EventHub,
  pub(crate) clients: Arc<RwLock<FxHashMap<ClientId, ClientSlot>>>,
  pub(crate) total: Arc<AtomicU64>,
  pub(crate) sink: Sink,
  /// Staged disconnect clears (see `MAX_PENDING_CLEARS`).
  pub(crate) pending_clears: Arc<Mutex<VecDeque<PendingClear>>>,
  /// Live clear-retry tasks (bounded, see `MAX_CLEAR_RETRY_TASKS`).
  pub(crate) clear_retries: Arc<AtomicUsize>,
  pub(crate) user: Arc<Mutex<RpcUser>>,
  pub(crate) set_activity: bool,
  pub(crate) secondary_events: bool,
}

/// Event pump: the only task touching the map, one short guard per event.
pub(crate) async fn pump_loop(
  PumpCtx {
    mut hub,
    clients,
    total,
    sink,
    pending_clears,
    clear_retries,
    user,
    set_activity,
    secondary_events,
  }: PumpCtx,
) {
  while let Some(event) = hub.next_event().await {
    match event {
      Event::Connect(id, responder) => {
        on_connect(id, responder, &clients, &total, &user).await;
      }
      Event::Disconnect(id, _) => {
        remove_and_clear(id, &clients, &total, &sink, &pending_clears, &clear_retries).await;
      }
      Event::Message(id, message) => {
        on_message(
          id,
          message,
          &clients,
          &total,
          &sink,
          &pending_clears,
          &clear_retries,
          &user,
          set_activity,
          secondary_events,
        )
        .await;
      }
      // `Event` is non-exhaustive: future variants must not break the pump.
      _ => {}
    }
  }
}

/// Validate a new game client (version, encoding, origin), send READY and
/// register its slot. Rejected clients are closed before registration.
async fn on_connect(
  id: ClientId,
  responder: Responder,
  clients: &Arc<RwLock<FxHashMap<ClientId, ClientSlot>>>,
  total: &Arc<AtomicU64>,
  user: &Arc<Mutex<RpcUser>>,
) {
  // Parse + validate before any reply or insert.
  let query = query_params(responder.details().uri.as_ref());
  let version = query.get("v").map(String::as_str).unwrap_or("0");
  let encoding = query.get("encoding").map(String::as_str).unwrap_or("json");
  let query_client_id = query.get("client_id").cloned();
  if version != "1" || encoding != "json" {
    tracing::warn!("[transport-ws] Rejecting client {id} (v={version}, encoding={encoding})");
    responder.close(CloseCode::Normal).await;
    return;
  }
  // Origins are per-connection: refuse mismatches here, before any READY
  // or slot registration, instead of per message downstream.
  if !origin_allowed(
    responder
      .details()
      .headers
      .get("origin")
      .and_then(|v| v.to_str().ok()),
  ) {
    tracing::warn!("[transport-ws] Refused origin for client {id}");
    responder.close(CloseCode::Normal).await;
    return;
  }

  // Snapshot the identity under a short lock; the guard never crosses await.
  let ready = user
    .lock()
    .unwrap_or_else(|e| e.into_inner())
    .ready_payload();
  if responder
    .send_async(Message::Text(ready.into()))
    .await
    .is_err()
  {
    // Vanished mid-handshake: nothing to track.
    return;
  }
  clients.write().await.insert(
    id,
    ClientSlot {
      responder,
      published: Vec::new(),
      query_client_id,
    },
  );
  total.fetch_add(1, Ordering::Relaxed);
}

/// Remove the slot and emit one clear per published pid (shared by
/// Disconnect and prune paths). A connection that published several pids
/// (multiplexing companion) clears every card, not just the latest.
/// Complete by construction: extras past the bound are refused before
/// forwarding, so everything forwarded is tracked here. Each clear retries
/// within one shared budget (never one timeout per pid), so a transiently
/// wedged bridge still gets its clears without stalling the pump past a
/// second; past the budget clears shed counted (bridge ghost-reaping
/// bounds any residual staleness).
async fn remove_and_clear(
  id: ClientId,
  clients: &Arc<RwLock<FxHashMap<ClientId, ClientSlot>>>,
  total: &Arc<AtomicU64>,
  sink: &Sink,
  pending_clears: &Arc<Mutex<VecDeque<PendingClear>>>,
  clear_retries: &Arc<AtomicUsize>,
) {
  let slot = clients.write().await.remove(&id);
  let Some(slot) = slot else { return };
  total.fetch_sub(1, Ordering::Relaxed);
  // Older staged clears first (causality), then this slot's own clears.
  // Each stages instead of shedding on a full sink: the next pump event,
  // the retry below, or the shutdown drain retries them until delivered
  // or closed.
  sink.flush_pending(pending_clears);
  let mut staged_any = false;
  for published in &slot.published {
    let cmd = handlers::clear_for_slot(published);
    match sink.try_emit(cmd) {
      TryEmit::Sent => {}
      TryEmit::Full(cmd) => {
        sink.stage_clear(pending_clears, *cmd);
        staged_any = true;
      }
      TryEmit::Closed => {
        sink.count_shed(1);
        tracing::warn!("[transport-ws] Event sink closed, dropping clear");
      }
    }
  }
  // No further traffic may ever come: keep flushing in the background
  // until empty, closed, or budget lapse (see `spawn_clear_retry`).
  if staged_any {
    spawn_clear_retry(
      sink.clone(),
      Arc::clone(pending_clears),
      Arc::clone(clear_retries),
    );
  }
}

/// Cap on concurrent clear-retry tasks: each loops until the shared queue
/// drains, so a sustained bridge wedge with connection churn would otherwise
/// leak one retry task per disconnect forever. Past the cap the queue is
/// still drained by the per-message flush, the shutdown drain, and the
/// capped tasks below.
const MAX_CLEAR_RETRY_TASKS: usize = 4;

/// Retry staged clears without further traffic: polled flush until empty
/// (a closed sink drains counted inside the flush). Expiry is per entry
/// (see `expire_pending`), so overlapping tasks never shed each other's
/// clears, and every task ends on its own; the live-task cap above bounds
/// the churn case where the queue never empties. Complements the
/// message-path and shutdown-drain flushes, which cover traffic and exit.
fn spawn_clear_retry(
  sink: Sink,
  pending_clears: Arc<Mutex<VecDeque<PendingClear>>>,
  active: Arc<AtomicUsize>,
) {
  if active.fetch_add(1, Ordering::AcqRel) >= MAX_CLEAR_RETRY_TASKS {
    active.fetch_sub(1, Ordering::AcqRel);
    return;
  }
  tokio::spawn(async move {
    let _guard = ClearRetryGuard(Arc::clone(&active));
    loop {
      sink.flush_pending(&pending_clears);
      sink.expire_pending(&pending_clears);
      if pending_clears
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_empty()
      {
        break;
      }
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  });
}

/// Returns a retry-task slot to the shared cap when the task ends.
struct ClearRetryGuard(Arc<AtomicUsize>);

impl Drop for ClearRetryGuard {
  fn drop(&mut self) {
    self.0.fetch_sub(1, Ordering::AcqRel);
  }
}

#[allow(clippy::too_many_arguments)]
/// Dispatch one client message; `false` means the client died and its slot
/// must be pruned (disconnect clears follow downstream).
async fn on_message(
  id: ClientId,
  message: Message,
  clients: &Arc<RwLock<FxHashMap<ClientId, ClientSlot>>>,
  total: &Arc<AtomicU64>,
  sink: &Sink,
  pending_clears: &Arc<Mutex<VecDeque<PendingClear>>>,
  clear_retries: &Arc<AtomicUsize>,
  user: &Arc<Mutex<RpcUser>>,
  set_activity: bool,
  secondary_events: bool,
) {
  // Staged disconnect clears ride first (causality: older clears before
  // new publishes). Never waits: a still-full sink simply keeps them
  // staged for the next event.
  sink.flush_pending(pending_clears);
  // Snapshot the cheap fields; the read guard drops before any await
  // below (and the deep `last_cmd` is never cloned here).
  let slot = match clients.read().await.get(&id) {
    Some(slot) => SlotSnapshot {
      responder: slot.responder.clone(),
      query_client_id: slot.query_client_id.clone(),
    },
    None => {
      // Stale message for a removed slot (pruned or rejected client).
      return;
    }
  };
  let Message::Text(text) = message else {
    tracing::warn!("[transport-ws] Ignoring non-text message from client {id}");
    return;
  };
  let event: ActivityCmd = match serde_json::from_str(&text) {
    Ok(event) => event,
    Err(err) => {
      tracing::warn!("[transport-ws] Invalid message from client {id}: {err}");
      return;
    }
  };

  // Every arm reports liveness: `false` means the game client died without
  // a clean `Disconnect` and its slot must be released now.
  let alive = match event.cmd.as_str() {
    "INVITE_BROWSER" | "GUILD_TEMPLATE_BROWSER" | "GIFT_CODE_BROWSER" => {
      if !secondary_events {
        // Disabled: still earn the lock-step reply, forward nothing.
        handlers::ack_without_forward(&event, &slot.responder).await
      } else {
        handlers::handle_browser_command(&event, &slot.responder, sink).await
      }
    }
    "DEEP_LINK" => {
      if !secondary_events {
        handlers::ack_without_forward(&event, &slot.responder).await
      } else {
        handlers::handle_deep_link(&event, &slot.responder).await
      }
    }
    "CONNECTIONS_CALLBACK" => {
      if !secondary_events {
        handlers::ack_without_forward(&event, &slot.responder).await
      } else {
        handlers::handle_connections_callback(&event, &slot.responder).await
      }
    }
    "SUBSCRIBE" | "UNSUBSCRIBE" => handlers::handle_subscribe(&event, &slot.responder).await,
    "GET_USER" => {
      let snapshot = user.lock().unwrap_or_else(|e| e.into_inner()).clone();
      handlers::handle_get_user(&event, &snapshot, &slot.responder).await
    }
    "SET_ACTIVITY" => {
      // Malformed (`args` missing entirely): official error reply like the
      // IPC transport's 4005 — never a clear, never forwarded.
      if event.args.is_none() {
        handlers::handle_malformed_set_activity(&event, &slot.responder).await
      } else if !set_activity {
        // Disabled: still earn the lock-step reply (Forward::Refused
        // replies, never forwards).
        handlers::handle_set_activity(
          &event,
          slot.query_client_id.as_deref(),
          &slot.responder,
          sink,
          pending_clears,
          handlers::Forward::Refused,
        )
        .await
        .0
      } else {
        // Genuine clears are not publications: they never consume history,
        // always forward (the bridge may hold the card), and free the slot
        // once delivered so new pids fit again. Applies the same
        // connect-query client_id fallback the forwarded command carries.
        let app_id = event
          .application_id
          .clone()
          .or_else(|| slot.query_client_id.clone());
        let pid = event.args.as_ref().and_then(|a| a.pid).unwrap_or_default();
        let is_clear = event
          .args
          .as_ref()
          .and_then(|args| args.activity.as_ref())
          .is_none();
        // Record first (one short write, no await inside): every forwarded
        // publication is tracked, so disconnect cleanup stays complete. The
        // pump is the only map mutator, so admission cannot change before
        // forwarding below.
        let admission = if is_clear {
          None
        } else if let Some(entry) = clients.write().await.get_mut(&id) {
          Some(entry.note_published(app_id, pid, event.nonce.clone()))
        } else {
          // Slot pruned mid-flight: forward nothing rather than ghost.
          None
        };
        // Refusals still earn their lock-step reply (clients hang without
        // one) but never reach the sink — recording the publication
        // regardless also keeps the reply-fails prune below clearing the
        // pid instead of ghosting it. Publications behind a still-full
        // staged queue wait their turn instead of overtaking older clears.
        let backlog = !pending_clears
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .is_empty();
        // A still-full staged queue serializes everything behind it —
        // including genuine clears, so a clear for a staged pid lands after
        // its publish instead of jumping ahead (same-pid inversion would
        // show-then-clear out of order downstream).
        let forward = if is_clear && !backlog {
          handlers::Forward::Direct
        } else if is_clear {
          handlers::Forward::Staged
        } else {
          match admission {
            Some(Admission::Admitted { .. }) if backlog => handlers::Forward::Staged,
            Some(Admission::Admitted { .. }) => handlers::Forward::Direct,
            _ => handlers::Forward::Refused,
          }
        };
        let (alive, delivered) = handlers::handle_set_activity(
          &event,
          slot.query_client_id.as_deref(),
          &slot.responder,
          sink,
          pending_clears,
          forward,
        )
        .await;
        if delivered {
          // Landed: genuine clears free their slot for new pids.
          if is_clear && let Some(entry) = clients.write().await.get_mut(&id) {
            entry.forget_published(pid);
          }
        } else {
          match admission {
            // Shed publish: roll back (a refresh restores its prior slot so
            // shown cards stay tracked; first-time publishes leave nothing).
            Some(Admission::Admitted { displaced }) => {
              if let Some(entry) = clients.write().await.get_mut(&id) {
                entry.rollback_publish(pid, displaced);
              }
            }
            Some(Admission::RefusedFull) => {
              tracing::debug!(
                "[transport-ws] Client {id} past {MAX} tracked pids, refusing pid {pid}",
                MAX = handlers::MAX_TRACKED_PIDS,
              );
            }
            None => {}
          }
        }
        alive
      }
    }
    other => handlers::handle_unknown(other, &event, &slot.responder).await,
  };
  if !alive {
    tracing::info!("[transport-ws] Client {id} send failed, pruning");
    remove_and_clear(id, clients, total, sink, pending_clears, clear_retries).await;
  }
}
