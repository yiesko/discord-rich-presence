use std::{
  collections::HashMap,
  sync::{Arc, Mutex},
};

use super::utils::GaugeSender;

use serde_json::Value;
use simple_websockets::{Event, EventHub, Message, Responder};

use crate::{
  cmd::{ActivityCmd, ActivityCmdArgs},
  commands, debug, error, log,
  url_params::get_url_params,
  user::RpcUser,
  warn,
};

// (last activity, client_id from the connect query, responder)
pub(crate) type ActivityResponder = (Option<ActivityCmd>, Option<String>, Responder);

#[derive(Clone)]
pub(crate) struct WebsocketConnector {
  server: Arc<Mutex<Option<EventHub>>>,
  pub clients: Arc<Mutex<HashMap<u64, ActivityResponder>>>,
  /// Actual bound port (`None` when no port in the range was free and the
  /// process exited — kept for the state snapshot).
  pub bound_port: Option<u16>,
  user: Arc<Mutex<RpcUser>>,

  event_sender: GaugeSender<ActivityCmd>,
}

impl WebsocketConnector {
  /// Bind the WebSocket bridge on the first free port in
  /// `ws_port_start..=ws_port_end` (loopback only).
  ///
  /// # Errors
  ///
  /// Returns [`RsrpcError::WsBind`](crate::error::RsrpcError::WsBind)
  /// (keeping the last `io::Error` as source) when no port in the range
  /// could be bound.
  pub(crate) fn new(
    event_sender: GaugeSender<ActivityCmd>,
    ws_port_start: u16,
    ws_port_end: u16,
    user: Arc<Mutex<RpcUser>>,
  ) -> crate::error::Result<Self> {
    // Try starting websocket server on the configured range, bound to
    // loopback only (games always connect to 127.0.0.1).
    let mut last_err: Option<std::io::Error> = None;
    for port in ws_port_start..=ws_port_end {
      let listener = match std::net::TcpListener::bind(("127.0.0.1", port)) {
        Ok(listener) => listener,
        Err(err) => {
          // Only port conflicts are routine (another RPC server owns the
          // port); surface other failures distinctly in the log.
          if err.kind() == std::io::ErrorKind::AddrInUse {
            warn!("[Websocket] Port {} in use, trying next", port);
          } else {
            warn!(
              "[Websocket] Cannot bind port {} ({}), trying next",
              port, err
            );
          }
          last_err = Some(err);
          continue;
        }
      };

      match simple_websockets::launch_from_listener(listener) {
        Ok(server) => {
          log!("[Websocket] Server started on port {}", port);
          return Ok(Self {
            server: Arc::new(Mutex::new(Some(server))),
            clients: Arc::new(Mutex::new(HashMap::new())),
            bound_port: Some(port),
            user,
            event_sender,
          });
        }
        Err(_) => {
          warn!(
            "[Websocket] Failed to start server on port {}, trying next",
            port
          );
        }
      }
    }

    error!("[Websocket] Failed to start server on any port");
    match last_err {
      Some(source) => Err(crate::error::RsrpcError::WsBind {
        start: ws_port_start,
        end: ws_port_end,
        source,
      }),
      // Empty range (or only launch_from_listener failures): no bind
      // error to keep — same text as before, lowercase, no log prefix.
      None => Err(crate::error::RsrpcError::WsExhausted {
        start: ws_port_start,
        end: ws_port_end,
      }),
    }
  }

  pub(crate) fn start(&mut self, set_activity: bool, secondary_events: bool) {
    // Double-start is a caller bug: the taken server below marks it.
    // Ignore fail-safe instead of panicking on the take.
    if self
      .server
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .is_none()
    {
      warn!("[Websocket] Already started, ignoring duplicate start");
      return;
    }
    let server = self
      .server
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .take()
      .expect("[bug] server checked above");
    let clients = self.clients.clone();
    let event_sender = self.event_sender.clone();
    let user = self.user.clone();

    std::thread::spawn(move || {
      let mut clients = clients.lock().unwrap_or_else(|e| e.into_inner());

      loop {
        debug!("[Websocket] Polling for events...");

        match server.poll_event() {
          Event::Connect(client_id, responder) => {
            let connection = responder.connection_details();
            let url_params = get_url_params(connection.uri.clone());
            let version = url_params.get("v").unwrap_or(&"0".to_string()).clone();
            let encoding = url_params
              .get("encoding")
              .unwrap_or(&"json".to_string())
              .clone();
            let ws_client_id = url_params.get("client_id").cloned();

            log!("[Websocket] Client {} connected", client_id);

            if version != "1" || encoding != "json" {
              warn!(
                "[Websocket] Invalid connection from client {} (v={}, encoding={}), closing",
                client_id, version, encoding
              );
              responder.close();
              continue;
            }

            responder.send(Message::Text(
              user
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .ready_payload(),
            ));

            clients.insert(client_id, (None, ws_client_id, responder));
          }
          Event::Disconnect(client_id) => {
            log!("[Websocket] Client {} disconnected", client_id);
            let responder = match clients.remove(&client_id) {
              Some(responder) => responder,
              // A client that was never inserted (invalid handshake) was
              // closed directly, nothing to clean up.
              None => continue,
            };

            handle_disconnect(client_id, &event_sender, &responder);
          }
          Event::Message(client_id, message) => {
            debug!(
              "[Websocket] Received message from client {}: {:?}",
              client_id, message
            );

            let responder = match clients.get_mut(&client_id) {
              Some(responder) => responder,
              None => continue,
            };
            let message = match message {
              Message::Text(text) => text,
              _ => "".to_string(),
            };

            // If not ActivityCmd, ignore
            let event: ActivityCmd = match serde_json::from_str(&message) {
              Ok(event) => event,
              Err(e) => {
                warn!("[Websocket] Invalid message from client {}", client_id);
                warn!("[Websocket] Error: {}", e);
                continue;
              }
            };

            // If origin isn't a Discord URL, ignore
            let origin = responder.2.connection_details().headers.get("origin");

            if let Some(origin) = origin {
              let value = origin.to_str().unwrap_or_default();
              let valid = [
                "https://discord.com",
                "https://canary.discord.com",
                "https://ptb.discord.com",
              ];

              if !valid.contains(&value) {
                warn!("[Websocket] Invalid origin from client {}", client_id);
                continue;
              }
            }

            // Every arm reports whether the reply send succeeded: a `false`
            // means the game client died without a clean `Disconnect`
            // (kill -9, crashed game) and its slot — last activity plus any
            // queued frames — must be released now, not pinned until a
            // `Disconnect` that may lag behind under flood.
            let alive = match event.cmd.as_str() {
              "INVITE_BROWSER" | "GUILD_TEMPLATE_BROWSER" | "GIFT_CODE_BROWSER" => {
                if !secondary_events {
                  continue;
                }

                handle_browser_command(&event, &event_sender, &responder.2)
              }
              "DEEP_LINK" => handle_deep_link(&event, &responder.2),
              "CONNECTIONS_CALLBACK" => handle_connections_callback(&event, &responder.2),
              "SUBSCRIBE" | "UNSUBSCRIBE" => {
                // Blind ACK like arRPC: no voice/guild backend exists, but
                // clients wait for the lock-step reply.
                responder
                  .2
                  .send(Message::Text(commands::subscribe_ack(&event)))
              }
              "GET_USER" => {
                let wanted = event.args.as_ref().and_then(|args| args.user_id.as_ref());
                let user = user.lock().unwrap_or_else(|e| e.into_inner()).clone();
                let matched = wanted.is_none_or(|id| *id == user.id);
                responder.2.send(Message::Text(commands::user_response(
                  &event,
                  matched.then_some(&user),
                )))
              }
              "SET_ACTIVITY" => {
                if !set_activity {
                  continue;
                }

                handle_set_activity(&event, &event_sender, responder)
              }
              other => {
                let unsupported = commands::unsupported_command(other);
                if unsupported.is_none() {
                  warn!("[Websocket] Unknown command: {}", other);
                }
                let (code, message) = unsupported.unwrap_or((1000, "Unknown command"));
                responder.2.send(Message::Text(commands::rpc_error(
                  &event.cmd,
                  &event.nonce,
                  code,
                  message,
                )))
              }
            };
            if !alive {
              // Same prompt release as the `Disconnect` arm below: the
              // borrow of `responder` ended with the match, so removal is
              // safe here.
              log!("[Websocket] Client {} send failed, pruning", client_id);
              if let Some(dead) = clients.remove(&client_id) {
                handle_disconnect(client_id, &event_sender, &dead);
              }
            }
          }
        }
      }
    });
  }
}

fn event_args_as_hashmap(args: Option<ActivityCmdArgs>) -> HashMap<String, Value> {
  // Serde serialize the args
  let args = match args {
    Some(args) => serde_json::to_value(&args).unwrap_or(Value::Null),
    None => Value::Null,
  };

  // Re-deserialize the args as a hashmap, preserving value types
  match args {
    Value::Object(map) => map.into_iter().collect(),
    _ => HashMap::new(),
  }
}

pub(crate) fn handle_browser_command(
  event: &ActivityCmd,
  event_sender: &GaugeSender<ActivityCmd>,
  responder: &Responder,
) -> bool {
  // Discord error codes for unusable invite/template/gift ids.
  let (code, message) = if event.cmd == "GUILD_TEMPLATE_BROWSER" {
    (4017_u16, "Invalid guild template id")
  } else if event.cmd == "GIFT_CODE_BROWSER" {
    (4016_u16, "Invalid gift code")
  } else {
    (4011_u16, "Invalid invite id")
  };
  let has_code = event
    .args
    .as_ref()
    .and_then(|args| args.code.as_ref())
    .is_some_and(|code| !code.trim().is_empty());
  if !has_code {
    warn!("[Websocket] {} without code from client", event.cmd);
    return responder.send(Message::Text(commands::rpc_error(
      &event.cmd,
      &event.nonce,
      code,
      message,
    )));
  }

  // Let's just assume this went well I don't care
  let response = ActivityCmd {
    application_id: event.application_id.clone(),
    cmd: event.cmd.clone(),
    args: None,
    data: Some(event_args_as_hashmap(event.args.clone())),
    evt: None,
    nonce: event.nonce.clone(),
  };

  // Send the event away!
  if event_sender.send(event.clone()).is_err() {
    warn!("[Websocket] Event receiver gone, dropping message");
    return true;
  }

  // Respond (client-supplied `data` may hold non-finite floats, which
  // JSON cannot encode: drop loudly instead of panicking the poll loop).
  // Nothing was sent, so the client is still considered alive.
  let Ok(response) = serde_json::to_string(&response) else {
    warn!(
      "[Websocket] Dropping unserializable response for {}",
      event.cmd
    );
    return true;
  };
  responder.send(Message::Text(response))
}

pub(crate) fn handle_deep_link(event: &ActivityCmd, responder: &Responder) -> bool {
  let response = ActivityCmd {
    application_id: event.application_id.clone(),
    cmd: event.cmd.clone(),
    args: None,
    data: None,
    evt: None,
    nonce: event.nonce.clone(),
  };

  let Ok(response) = serde_json::to_string(&response) else {
    warn!("[Websocket] Dropping unserializable deep-link response");
    return true;
  };
  responder.send(Message::Text(response))
}

pub(crate) fn handle_connections_callback(event: &ActivityCmd, responder: &Responder) -> bool {
  let mut data = HashMap::new();
  data.insert("code".to_string(), Value::Number(1000.into()));

  let response = ActivityCmd {
    application_id: event.application_id.clone(),
    cmd: event.cmd.clone(),
    args: None,
    data: Some(data),
    evt: Some("ERROR".to_string()),
    nonce: event.nonce.clone(),
  };

  // `data` here is locally built (no client floats): encode failure
  // would be a coding bug, but the poll loop must not die on it.
  let Ok(response) = serde_json::to_string(&response) else {
    warn!("[Websocket] Dropping unserializable connections response");
    return true;
  };
  responder.send(Message::Text(response))
}

pub(crate) fn handle_set_activity(
  event: &ActivityCmd,
  event_sender: &GaugeSender<ActivityCmd>,
  responder: &mut ActivityResponder,
) -> bool {
  // Fall back to the client_id provided on connect (query param) when the
  // command itself does not carry an application_id.
  let mut event = event.clone();
  if event.application_id.is_none() {
    event.application_id = responder.1.clone();
  }

  // Apply field fixes so the confirmation reply carries labels/urls (fix is
  // idempotent, so the event_loop applying it again is harmless).
  event.fix();

  // Set the last activity for the client
  responder.0 = Some(event.clone());

  if event_sender.send(event.clone()).is_err() {
    warn!("[Websocket] Event receiver gone, dropping message");
    return true;
  }

  // Confirm to the game client (arRPC-shaped reply); some RPC libraries
  // wait for this before considering the presence set. No confirm to send
  // means the client is still considered alive.
  if let Some(response) = commands::set_activity_response(&event) {
    responder.2.send(Message::Text(response))
  } else {
    true
  }
}

fn handle_disconnect(
  _client_id: u64,
  event_sender: &GaugeSender<ActivityCmd>,
  responder: &ActivityResponder,
) {
  if let Some(ref activity_cmd) = responder.0 {
    // Send empty activity. pid defaults to 0 when the last command had
    // no args (malformed SET_ACTIVITY): pid 0 is never a genuine clear,
    // so it is safely ignored downstream — and this never panics, which
    // would kill the whole bridge poll loop (stuck presence for everyone).
    let activity_cmd = ActivityCmd {
      application_id: activity_cmd.application_id.clone(),
      cmd: "SET_ACTIVITY".to_string(),
      data: None,
      evt: None,
      args: Some(ActivityCmdArgs {
        pid: Some(
          activity_cmd
            .args
            .as_ref()
            .and_then(|args| args.pid)
            .unwrap_or_default(),
        ),
        activity: None,
        code: None,
        user_id: None,
      }),
      nonce: activity_cmd.nonce.clone(),
    };

    if event_sender.send(activity_cmd).is_err() {
      warn!("[Websocket] Event receiver gone, dropping clear");
    }
  }
}
