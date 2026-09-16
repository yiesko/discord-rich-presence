use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;
use std::collections::HashMap;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ActivityPayload {
  pub activity: Option<Activity>,
  pub pid: Option<u64>,
  #[serde(rename = "socketId")]
  pub socket_id: Option<String>,
}

#[skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ActivityCmd {
  pub application_id: Option<String>,
  pub cmd: String,
  pub args: Option<ActivityCmdArgs>,
  pub data: Option<HashMap<String, Value>>,
  pub evt: Option<String>,
  pub nonce: Value,
}

impl ActivityCmd {
  #[must_use]
  pub fn empty() -> Self {
    Self {
      application_id: None,
      cmd: "".to_string(),
      args: None,
      data: None,
      evt: None,
      nonce: Value::String("".to_string()),
    }
  }

  pub fn fix(&mut self) {
    self.fix_timestamps();
    self.fix_buttons();
    self.fix_flags();
  }

  pub fn fix_timestamps(&mut self) {
    if let Some(timestamps) = self
      .args
      .as_mut()
      .and_then(|args| args.activity.as_mut())
      .and_then(|activity| activity.timestamps.as_mut())
    {
      let cur = chrono::Utc::now().timestamp() + (100 * 365 * 24 * 3600);

      // convert starting timestamp
      if let Some(start) = timestamps.start.as_mut() {
        *start = TimeoutValue(normalize_timestamp(start.0, cur));
      }

      // convert ending timestamp
      if let Some(end) = timestamps.end.as_mut() {
        *end = TimeoutValue(normalize_timestamp(end.0, cur));
      }
    }
  }

  pub fn fix_buttons(&mut self) {
    // If `buttons` are an array of objects, we need to map the labels to `buttons` (as a string array) and the urls to `metadata.button_urls` (as an array of strings)
    if let Some(activity) = self.args.as_mut().and_then(|args| args.activity.as_mut())
      && let Some(buttons) = activity.buttons.as_mut()
    {
      let mut button_urls: Vec<String> = vec![];
      let mut button_labels: Vec<Value> = vec![];

      for b in buttons.iter() {
        match b {
          // Already a plain label, keep it as-is
          Value::String(_) => button_labels.push(b.clone()),
          Value::Object(map) => {
            if let Some(label) = map.get("label") {
              button_labels.push(label.clone());
            }
            if let Some(url) = map.get("url").and_then(|url| url.as_str()) {
              button_urls.push(url.to_string());
            }
          }
          other => button_labels.push(other.clone()),
        }
      }

      // Only attach metadata when there are actual urls to attach
      if !button_urls.is_empty() {
        activity.metadata = Some(Metadata {
          button_urls: Some(button_urls),
          ..activity.metadata.clone().unwrap_or_default()
        });
      }

      activity.buttons = Some(button_labels);
    }
  }

  pub fn fix_flags(&mut self) {
    if let Some(activity) = self.args.as_mut().and_then(|args| args.activity.as_mut())
      && activity.instance.unwrap_or(false)
      && activity.flags.is_none()
    {
      activity.flags = Some(1);
    }
  }
}

#[skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ActivityCmdArgs {
  pub pid: Option<u64>,
  pub activity: Option<Activity>,
  // For INVITE_BROWSER / GUILD_TEMPLATE_BROWSER / GIFT_CODE_BROWSER
  pub code: Option<String>,
  // For GET_USER
  pub user_id: Option<String>,
}

#[skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Party {
  pub id: Option<String>,
  pub size: Option<Vec<u32>>,
  /// 0 = private, 1 = public (official party privacy).
  pub privacy: Option<u32>,
}

#[skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Assets {
  pub large_image: Option<String>,
  pub large_text: Option<String>,
  pub small_image: Option<String>,
  pub small_text: Option<String>,
  /// URLs opened when clicking the images (official activity-assets fields).
  pub large_url: Option<String>,
  pub small_url: Option<String>,
}

#[skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Secrets {
  pub join: Option<String>,
  pub spectate: Option<String>,
  #[serde(rename = "match")]
  pub match_secret: Option<String>,
}

#[skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct Emoji {
  pub name: Option<String>,
  pub id: Option<String>,
  pub animated: Option<bool>,
}

// Important: https://docs.discord.sex/resources/presence#activity-metadata-object
#[skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub struct Metadata {
  pub button_urls: Option<Vec<String>>,
  pub artist_ids: Option<Vec<String>>,
  pub album_id: Option<String>,
  pub context_uri: Option<String>,
}

#[skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Activity {
  pub id: Option<String>,
  pub name: Option<String>,
  pub buttons: Option<Vec<Value>>,
  #[serde(default)]
  pub r#type: u32,
  pub url: Option<String>,
  pub created_at: Option<u64>,
  pub session_id: Option<String>,
  pub platform: Option<String>,
  pub supported_platforms: Option<Vec<String>>,
  pub timestamps: Option<Timestamps>,
  pub application_id: Option<String>,
  pub details: Option<String>,
  /// URL opened when clicking the details text (official, max 256 chars).
  pub details_url: Option<String>,
  pub state: Option<String>,
  /// URL opened when clicking the state text (official, max 256 chars).
  pub state_url: Option<String>,
  pub sync_id: Option<String>,
  pub instance: Option<bool>,
  /// Which field the member list shows (0 = name, 1 = state, 2 = details).
  pub status_display_type: Option<u32>,
  pub flags: Option<u32>,
  pub emoji: Option<Emoji>,
  pub party: Option<Party>,
  pub assets: Option<Assets>,
  pub secrets: Option<Secrets>,
  pub metadata: Option<Metadata>,
  /// Forward-compatibility: fields this bridge does not model yet (new
  /// official keys, game-specific extras) round-trip untouched instead
  /// of being dropped on parse — a strict client must see what the game
  /// sent, byte-equivalent modulo key order.
  #[serde(flatten, default)]
  pub extra: HashMap<String, Value>,
}

impl Activity {
  /// Human-readable label for logs: name first, then details/state, so
  /// publishers without a name (music apps send song/artist instead)
  /// still identify themselves instead of showing `?`.
  #[must_use]
  pub fn display_name(&self) -> &str {
    [&self.name, &self.details, &self.state]
      .into_iter()
      .flatten()
      .map(|text| text.trim())
      .find(|text| !text.is_empty())
      .unwrap_or("?")
  }
}

/// Normalize a client-supplied timestamp to milliseconds (what Discord
/// renders), mirroring arRPC's precision sniffing:
///
/// - nanoseconds (`>= 1e17`, ~1.8e18 now) are divided by 1e6,
/// - microseconds (`>= 1e14`, ~1.8e15 now) are divided by 1e3,
/// - milliseconds (above `now + 100y`, ~1.7e12 now) pass through,
/// - anything smaller is seconds and is multiplied by 1e3.
///
/// The µs/ns branches sit above the legacy s/ms heuristic so existing
/// second/millisecond inputs behave exactly as before.
fn normalize_timestamp(value: i64, millis_threshold: i64) -> i64 {
  const MICROS_THRESHOLD: i64 = 100_000_000_000_000; // 1e14
  const NANOS_THRESHOLD: i64 = 100_000_000_000_000_000; // 1e17

  if value >= NANOS_THRESHOLD {
    value / 1_000_000
  } else if value >= MICROS_THRESHOLD {
    value / 1_000
  } else if value > millis_threshold {
    value
  } else {
    value * 1000
  }
}

#[skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct TimeoutValue(pub(crate) i64);

impl TimeoutValue {
  /// Raw timestamp value: caller unit before [`ActivityCmd::fix`], millis after.
  #[must_use]
  pub fn value(&self) -> i64 {
    self.0
  }
}
#[skip_serializing_none]
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Timestamps {
  #[serde(default)]
  pub start: Option<TimeoutValue>,
  #[serde(default)]
  pub end: Option<TimeoutValue>,
}
