//! Bridge origin policy: which web origins may drive presence.
//!
//! WebSocket has no CORS gate of its own, so any page could otherwise
//! open `ws://127.0.0.1:1337` and read local activities or send bridge
//! commands. Mirrors the game transport: absent `Origin` (native
//! clients) always passes — only a mismatched origin is refused.

/// Browser origins allowed to drive bridge commands without extra
/// configuration (Discord's own pages).
pub const ALLOWED_ORIGINS: [&str; 3] = [
  "https://discord.com",
  "https://canary.discord.com",
  "https://ptb.discord.com",
];

/// Whether a bridge `Origin` may connect: absent passes (native
/// clients), Discord's pages pass, anything else needs an explicit
/// `RSRPC_BRIDGE_ALLOWED_ORIGINS` entry (exact match).
pub(crate) fn origin_allowed(origin: Option<&str>, extra: &[String]) -> bool {
  match origin {
    None => true,
    Some(value) => ALLOWED_ORIGINS.contains(&value) || extra.iter().any(|allowed| allowed == value),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Absent origin (native clients) passes; Discord's pages pass; an
  /// explicit extra origin passes; anything else is refused.
  #[test]
  fn origin_policy() {
    let extra = vec!["https://my-client.example".to_string()];
    assert!(origin_allowed(None, &[]));
    assert!(origin_allowed(None, &extra));
    assert!(origin_allowed(Some("https://discord.com"), &[]));
    assert!(origin_allowed(Some("https://canary.discord.com"), &[]));
    assert!(origin_allowed(Some("https://ptb.discord.com"), &[]));
    assert!(origin_allowed(Some("https://my-client.example"), &extra));
    assert!(!origin_allowed(Some("https://evil.example"), &[]));
    assert!(!origin_allowed(Some("https://evil.example"), &extra));
    assert!(!origin_allowed(Some("http://discord.com"), &[]));
  }
}
