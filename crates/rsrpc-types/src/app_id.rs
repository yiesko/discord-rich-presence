//! Newtyped ids: [`AppId`] (game/activity slot) and [`SocketId`] (client slot).
//!
//! `Arc<str>` inside (`own-arc-shared`): clones share the allocation and
//! `Borrow<str>` map lookups never allocate on the hot path. Same JSON wire
//! format as a plain string.

use std::sync::Arc;

/// Discord application id: identifies a game/activity slot. Newtyped so
/// socket ids, pids and raw strings can never mix at compile time; same
/// wire format as the inner string.
#[derive(
  Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct AppId(pub Arc<str>);

/// Bridge socket id: identifies one client connection slot. See [`AppId`].
#[derive(
  Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct SocketId(pub Arc<str>);

impl std::fmt::Display for AppId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.0.fmt(f)
  }
}

impl std::fmt::Display for SocketId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.0.fmt(f)
  }
}

impl AsRef<str> for AppId {
  fn as_ref(&self) -> &str {
    &self.0
  }
}

/// Borrow as `str` so `HashMap<AppId, _>` lookups accept `&str` without
/// allocating an owned key on the hot path.
impl std::borrow::Borrow<str> for AppId {
  fn borrow(&self) -> &str {
    &self.0
  }
}

/// Same as [`AppId`]: socket maps accept `&str` lookups directly.
impl std::borrow::Borrow<str> for SocketId {
  fn borrow(&self) -> &str {
    &self.0
  }
}

impl AsRef<str> for SocketId {
  fn as_ref(&self) -> &str {
    &self.0
  }
}

impl From<String> for AppId {
  fn from(id: String) -> Self {
    Self(id.into())
  }
}

impl From<Box<str>> for AppId {
  fn from(id: Box<str>) -> Self {
    Self(Arc::from(id))
  }
}

impl From<&str> for AppId {
  fn from(id: &str) -> Self {
    Self(Arc::from(id))
  }
}

impl From<String> for SocketId {
  fn from(id: String) -> Self {
    Self(id.into())
  }
}

impl From<Box<str>> for SocketId {
  fn from(id: Box<str>) -> Self {
    Self(Arc::from(id))
  }
}

impl From<&str> for SocketId {
  fn from(id: &str) -> Self {
    Self(Arc::from(id))
  }
}

/// An app slot doubles as its own socket on the generic (scanner-driven)
/// path: move the id across instead of cloning it.
impl From<AppId> for SocketId {
  fn from(id: AppId) -> Self {
    Self(id.0)
  }
}

/// Borrow an app id as its socket without touching the inner string.
impl From<&AppId> for SocketId {
  fn from(id: &AppId) -> Self {
    Self(id.0.clone())
  }
}

/// Unwrap back to the wire string (Display also works for formatting).
impl From<AppId> for String {
  fn from(id: AppId) -> Self {
    id.0.to_string()
  }
}

/// Unwrap back to the wire string (Display also works for formatting).
impl From<SocketId> for String {
  fn from(id: SocketId) -> Self {
    id.0.to_string()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::collections::HashMap;

  #[test]
  fn ids_share_arc_allocation() {
    assert_eq!(
      std::mem::size_of::<AppId>(),
      std::mem::size_of::<Arc<str>>()
    );
    assert_eq!(
      std::mem::size_of::<SocketId>(),
      std::mem::size_of::<Arc<str>>()
    );
  }

  #[test]
  fn clone_shares_instead_of_copying() {
    let id = AppId::from("123456789012345678");
    let cloned = id.clone();
    assert!(std::ptr::eq(id.as_ref().as_ptr(), cloned.as_ref().as_ptr()));
    let sock = SocketId::from("sock");
    let sock_cloned = sock.clone();
    assert!(std::ptr::eq(
      sock.as_ref().as_ptr(),
      sock_cloned.as_ref().as_ptr()
    ));
  }

  #[test]
  fn str_lookup_needs_no_owned_key() {
    let mut map: HashMap<AppId, u64> = HashMap::new();
    map.insert(AppId::from("game-1"), 42);
    assert_eq!(map.get("game-1"), Some(&42));
    let mut sockets: HashMap<SocketId, u64> = HashMap::new();
    sockets.insert(SocketId::from("sock-1"), 7);
    assert_eq!(sockets.get("sock-1"), Some(&7));
  }

  #[test]
  fn conversions_roundtrip() {
    let app = AppId::from("abc");
    assert_eq!(app.to_string(), "abc");
    assert_eq!(String::from(app), "abc");
    let sock = SocketId::from(&AppId::from("abc"));
    assert_eq!(sock.as_ref(), "abc");
    let moved = SocketId::from(AppId::from("abc"));
    assert_eq!(moved.as_ref(), "abc");
  }

  #[test]
  fn boxed_str_converts() {
    // One copy into the refcounted layout (Arc stores counters inline, so
    // in-place adoption is impossible); every later clone is free.
    let id = AppId::from(Box::<str>::from("boxed-id"));
    assert_eq!(id.as_ref(), "boxed-id");
    let cloned = id.clone();
    assert!(std::ptr::eq(id.as_ref().as_ptr(), cloned.as_ref().as_ptr()));
    let sock = SocketId::from(Box::<str>::from("sock-id"));
    assert_eq!(sock.as_ref(), "sock-id");
  }

  #[test]
  fn wire_format_is_plain_string() {
    let json = serde_json::to_string(&AppId::from("123")).unwrap();
    assert_eq!(json, r#""123""#);
    let back: AppId = serde_json::from_str(&json).unwrap();
    assert_eq!(back, AppId::from("123"));
  }
}
