//! Wire message with cheap clone for binary payloads.

use bytes::Bytes;

/// An incoming/outgoing WebSocket message.
///
/// `Binary` clones are O(1): [`Bytes`] is refcounted, so broadcast fan-out
/// shares the allocation instead of copying per client (`mem-zero-copy`).
/// `Text` is an owned [`String`]; tungstenite's `Utf8Bytes` stays internal.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Message {
  /// UTF-8 text message.
  Text(String),
  /// Binary message (zero-copy clone).
  Binary(Bytes),
}

impl Message {
  /// Length in bytes of the payload.
  #[must_use]
  pub fn len(&self) -> usize {
    match self {
      Self::Text(s) => s.len(),
      Self::Binary(b) => b.len(),
    }
  }

  /// True when the payload is empty.
  #[must_use]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }
}

impl From<String> for Message {
  fn from(s: String) -> Self {
    Self::Text(s)
  }
}

impl From<&str> for Message {
  fn from(s: &str) -> Self {
    Self::Text(s.to_owned())
  }
}

impl From<Bytes> for Message {
  fn from(b: Bytes) -> Self {
    Self::Binary(b)
  }
}

impl From<Vec<u8>> for Message {
  fn from(v: Vec<u8>) -> Self {
    Self::Binary(Bytes::from(v))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn binary_clone_shares_allocation() {
    let msg = Message::Binary(Bytes::from(vec![1u8; 1024]));
    let cloned = msg.clone();
    let (Message::Binary(a), Message::Binary(b)) = (msg, cloned) else {
      panic!("expected Binary variants");
    };
    assert!(!a.is_empty());
    assert_eq!(a.as_ptr(), b.as_ptr());
  }

  #[test]
  fn conversions_cover_common_inputs() {
    assert_eq!(Message::from("hi"), Message::Text("hi".to_owned()));
    assert_eq!(
      Message::from(String::from("hi")),
      Message::Text("hi".to_owned())
    );
    assert_eq!(
      Message::from(vec![1u8, 2]),
      Message::Binary(Bytes::from_static(&[1, 2]))
    );
    assert!(Message::from("").is_empty());
    assert_eq!(Message::from("hi").len(), 2);
  }
}
