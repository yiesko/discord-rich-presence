use crate::cmd::{ActivityCmd, ActivityCmdArgs};
use crate::server::utils::QueueGauge;
use crate::server::websocket::{
  handle_browser_command, handle_connections_callback, handle_deep_link, handle_set_activity,
};

use super::WsTestClient;

fn event(cmd: &str) -> ActivityCmd {
  ActivityCmd {
    cmd: cmd.to_string(),
    nonce: serde_json::Value::String("n".to_string()),
    ..ActivityCmd::empty()
  }
}

fn live_sender() -> (
  crate::server::utils::GaugeSender<ActivityCmd>,
  crate::server::utils::GaugeReceiver<ActivityCmd>,
) {
  QueueGauge::pair()
}

#[test]
fn deep_link_reports_dead_client() {
  let dead = WsTestClient::connect().kill();
  assert!(
    !handle_deep_link(&event("DEEP_LINK"), &dead),
    "send to a dead game client must report failure so the poll loop prunes it"
  );
}

#[test]
fn deep_link_reports_live_client() {
  let client = WsTestClient::connect();
  assert!(handle_deep_link(&event("DEEP_LINK"), &client.responder));
}

#[test]
fn connections_callback_reports_dead_client() {
  let dead = WsTestClient::connect().kill();
  assert!(
    !handle_connections_callback(&event("CONNECTIONS_CALLBACK"), &dead),
    "send to a dead game client must report failure so the poll loop prunes it"
  );
}

#[test]
fn browser_command_reports_dead_client() {
  // No `code` arg: error reply path (still a responder send).
  let dead = WsTestClient::connect().kill();
  let (tx, _rx) = live_sender();
  assert!(
    !handle_browser_command(&event("INVITE_BROWSER"), &tx, &dead),
    "send to a dead game client must report failure so the poll loop prunes it"
  );
}

#[test]
fn set_activity_reports_dead_client() {
  let dead = WsTestClient::connect().kill();
  let (tx, _rx) = live_sender();
  let mut event = event("SET_ACTIVITY");
  event.args = Some(ActivityCmdArgs {
    pid: Some(99),
    activity: None,
    code: None,
    user_id: None,
  });
  let mut responder = (None, None, dead);
  assert!(
    !handle_set_activity(&event, &tx, &mut responder),
    "send to a dead game client must report failure so the poll loop prunes it"
  );
}
