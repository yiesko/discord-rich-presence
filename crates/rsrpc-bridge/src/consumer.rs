//! Consumer management: who listens on each encoding, fan-out delivery,
//! dead-client pruning.
//!
//! Consumers connect on the JSON or MessagePack port (overridable per
//! consumer via `format=`); every publish fans out to both tables while
//! pruning dead responders, so a stuck client cannot pin memory (its
//! queued frames) forever.

use std::sync::Mutex;
use std::sync::atomic::Ordering;

use rsrpc_protocol::commands::CachedActivity;
use rsrpc_protocol::query::query_params;
use rsrpc_types::cmd::ActivityCmd;
use rsrpc_ws::{ClientId, Message, Responder};
use rustc_hash::FxHashMap;

use super::bridge::Shared;

/// Which wire encoding a bridge consumer speaks.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum BridgeProtocol {
  /// JSON text frames (port 1337, the arRPC-compatible bridge).
  Json,
  /// MessagePack binary frames (port 1338).
  MsgPack,
}

impl BridgeProtocol {
  /// The protocol requested by the consumer's `format=` query parameter,
  /// falling back to the per-port default when absent.
  pub(crate) fn from_query(uri: &str, default: BridgeProtocol) -> BridgeProtocol {
    match query_params(uri).get("format").map(String::as_str) {
      Some("msgpack") | Some("messagepack") => BridgeProtocol::MsgPack,
      Some("json") => BridgeProtocol::Json,
      _ => default,
    }
  }
}

/// Send a JSON string, encoding it to MessagePack when the consumer speaks
/// MessagePack. Returns whether the frame was queued.
pub(crate) fn send_message(responder: &Responder, data: &str, protocol: BridgeProtocol) -> bool {
  match protocol {
    BridgeProtocol::Json => responder
      .try_send(Message::Text(data.to_string().into()))
      .is_ok(),
    BridgeProtocol::MsgPack => {
      if let Ok(value) = serde_json::from_str::<serde_json::Value>(data)
        && let Ok(bytes) = rmp_serde::to_vec_named(&value)
      {
        responder
          .try_send(Message::Binary(bytes::Bytes::from(bytes)))
          .is_ok()
      } else {
        false
      }
    }
  }
}

/// Send an already dual-encoded activity payload to a consumer.
pub(crate) fn send_cached(
  responder: &Responder,
  payload: &CachedActivity,
  protocol: BridgeProtocol,
) {
  match protocol {
    BridgeProtocol::Json => {
      let _ = responder.try_send(Message::Text(payload.json.clone()));
    }
    BridgeProtocol::MsgPack => {
      let _ = responder.try_send(Message::Binary(payload.msgpack.clone()));
    }
  }
}

impl Shared {
  /// Client map for one bridge encoding.
  pub(crate) fn clients_for(
    &self,
    protocol: BridgeProtocol,
  ) -> &Mutex<FxHashMap<ClientId, Responder>> {
    match protocol {
      BridgeProtocol::Json => &self.json_clients,
      BridgeProtocol::MsgPack => &self.msgpack_clients,
    }
  }

  /// Encoding a consumer speaks: the resolved `format=` override recorded
  /// at connect, falling back to the port default for unknown ids (which
  /// cannot happen: every insert is paired with a table entry).
  pub(crate) fn protocol_for(&self, id: ClientId, default: BridgeProtocol) -> BridgeProtocol {
    self
      .consumer_protocol
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .get(&id)
      .copied()
      .unwrap_or(default)
  }

  /// Flood guard: byte-identical republishes inside the window are
  /// dropped — before any broadcast, cache write or log line.
  /// Takes the precomputed fingerprint so dropped publishes never pay
  /// for the envelope build.
  pub(crate) fn flood_dropped(&self, cmd: &ActivityCmd, fingerprint: Option<&[u8]>) -> bool {
    let args = cmd.args.as_ref();
    let pid = args.and_then(|args| args.pid).unwrap_or_default();
    self
      .recent
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .should_drop(
        cmd.application_id.as_deref().unwrap_or(""),
        pid,
        fingerprint,
        std::time::Instant::now(),
      )
  }

  /// Send one payload to every connected consumer, pruning dead ones so a
  /// stuck client cannot pin memory (its queued frames) forever.
  pub(crate) fn send_to_all(&self, payload: &CachedActivity) {
    let mut json_clients = self.json_clients.lock().unwrap_or_else(|e| e.into_inner());
    let mut msgpack_clients = self
      .msgpack_clients
      .lock()
      .unwrap_or_else(|e| e.into_inner());
    if json_clients.is_empty() && msgpack_clients.is_empty() {
      tracing::debug!("[bridge] No consumers connected, skipping");
      return;
    }
    for (clients, payload) in [
      (&mut json_clients, Message::Text(payload.json.clone())),
      (
        &mut msgpack_clients,
        Message::Binary(payload.msgpack.clone()),
      ),
    ] {
      // try_send never blocks: a full outbox reports Full here and the
      // slot is released now, not pinned until a lagging Disconnect.
      let dead: Vec<ClientId> = clients
        .iter()
        .filter_map(|(id, responder)| responder.try_send(payload.clone()).err().map(|_| *id))
        .collect();
      self
        .dropped_broadcasts
        .fetch_add(dead.len() as u64, Ordering::Relaxed);
      for id in dead {
        tracing::warn!("[bridge] Pruning dead consumer {id}");
        clients.remove(&id);
        // Paired table: every insert writes both, so every prune clears
        // both — otherwise dead ids pin protocol entries forever.
        self
          .consumer_protocol
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .remove(&id);
      }
    }
  }

  /// Broadcast a non-activity event as-is, serializing once per encoding.
  /// A frame that cannot encode is dropped loudly (not silently).
  pub(crate) fn broadcast_raw(&self, cmd: &ActivityCmd) {
    let mut json_clients = self.json_clients.lock().unwrap_or_else(|e| e.into_inner());
    let mut msgpack_clients = self
      .msgpack_clients
      .lock()
      .unwrap_or_else(|e| e.into_inner());
    if json_clients.is_empty() && msgpack_clients.is_empty() {
      tracing::debug!("[bridge] No consumers connected, skipping");
      return;
    }
    let json_payload = if json_clients.is_empty() {
      None
    } else {
      match serde_json::to_string(cmd) {
        Ok(payload) => Some(payload),
        Err(err) => {
          tracing::debug!("[bridge] Dropping unserializable fan-out frame: {err}");
          None
        }
      }
    };
    let msgpack_payload = if msgpack_clients.is_empty() {
      None
    } else {
      match rmp_serde::to_vec_named(cmd) {
        Ok(payload) => Some(payload),
        Err(err) => {
          tracing::debug!("[bridge] Dropping unserializable fan-out frame: {err}");
          None
        }
      }
    };
    if let Some(payload) = json_payload {
      let dead: Vec<ClientId> = json_clients
        .iter()
        .filter_map(|(id, responder)| {
          responder
            .try_send(Message::Text(payload.clone().into()))
            .err()
            .map(|_| *id)
        })
        .collect();
      self
        .dropped_broadcasts
        .fetch_add(dead.len() as u64, Ordering::Relaxed);
      for id in dead {
        tracing::warn!("[bridge] Pruning dead consumer {id}");
        json_clients.remove(&id);
        // Paired table (see `send_to_all`): prune both or dead ids pin
        // protocol entries forever.
        self
          .consumer_protocol
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .remove(&id);
      }
    }
    if let Some(payload) = msgpack_payload {
      let dead: Vec<ClientId> = msgpack_clients
        .iter()
        .filter_map(|(id, responder)| {
          responder
            .try_send(Message::Binary(bytes::Bytes::from(payload.clone())))
            .err()
            .map(|_| *id)
        })
        .collect();
      self
        .dropped_broadcasts
        .fetch_add(dead.len() as u64, Ordering::Relaxed);
      for id in dead {
        tracing::warn!("[bridge] Pruning dead consumer {id}");
        msgpack_clients.remove(&id);
        // Paired table (see `send_to_all`): prune both or dead ids pin
        // protocol entries forever.
        self
          .consumer_protocol
          .lock()
          .unwrap_or_else(|e| e.into_inner())
          .remove(&id);
      }
    }
  }
}
