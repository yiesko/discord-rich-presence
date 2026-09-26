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
/// `RSRPC_BRIDGE_ALLOWED_ORIGINS` entry (normalized once at
/// configuration, then exact match).
pub(crate) fn origin_allowed(origin: Option<&str>, extra: &[String]) -> bool {
  match origin {
    None => true,
    Some(value) => ALLOWED_ORIGINS.contains(&value) || extra.iter().any(|allowed| allowed == value),
  }
}

/// Canonicalize a configured extra origin to browser serialization:
/// lowercase scheme and host, no trailing slash, no default port.
/// Browsers always send `Origin` in this form, so normalizing once when
/// the config is built keeps the per-connection check an exact
/// comparison — and a value like `https://client/` or
/// `https://CLIENT` matches instead of failing silently.
pub(crate) fn normalize_origin(origin: &str) -> String {
  let canonical = origin.trim().trim_end_matches('/').to_ascii_lowercase();
  for (scheme, default_port) in [("http://", ":80"), ("https://", ":443")] {
    if let Some(host) = canonical.strip_prefix(scheme)
      && let Some(bare) = host.strip_suffix(default_port)
    {
      return format!("{scheme}{bare}");
    }
  }
  canonical
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

  /// Configured entries canonicalize to browser serialization:
  /// trailing slashes, casing and default ports stop mattering, while
  /// non-default ports are preserved.
  #[test]
  fn extra_origins_normalize() {
    assert_eq!(
      normalize_origin("https://my-client.example/"),
      "https://my-client.example"
    );
    assert_eq!(
      normalize_origin("HTTPS://My-Client.Example"),
      "https://my-client.example"
    );
    assert_eq!(
      normalize_origin("https://my-client.example:443"),
      "https://my-client.example"
    );
    assert_eq!(
      normalize_origin("http://my-client.example:80/"),
      "http://my-client.example"
    );
    assert_eq!(
      normalize_origin("https://my-client.example:8443"),
      "https://my-client.example:8443"
    );
    assert_eq!(
      normalize_origin("  https://my-client.example  "),
      "https://my-client.example"
    );
  }

  /// A mixed-case configured entry with a trailing slash matches the
  /// serialized origin the browser actually sends.
  #[test]
  fn normalized_extras_match_browser_origins() {
    let extra = vec![normalize_origin("HTTPS://My-Client.Example/")];
    assert!(origin_allowed(Some("https://my-client.example"), &extra));
    assert!(!origin_allowed(Some("https://evil.example"), &extra));
  }
}
