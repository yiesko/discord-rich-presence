//! Wire message with cheap clone for both payload kinds.

use bytes::Bytes;
use tungstenite::Utf8Bytes;

/// Re-exported so consumers can name the text backing without depending
/// on `tungstenite` directly.
pub use tungstenite::Utf8Bytes as TextBytes;

/// An incoming/outgoing WebSocket message.
///
/// Both variants clone in O(1): [`Bytes`] and [`Utf8Bytes`] are refcounted,
/// so broadcast fan-out shares the allocation instead of copying per
/// client (`mem-zero-copy`). Builders that already own a `String`/`Vec<u8>`
/// hand the allocation over with no copy (`From` impls below).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Message {
  /// UTF-8 text message.
  Text(Utf8Bytes),
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
  /// Adopt an owned string as text (no copy).
  fn from(s: String) -> Self {
    Self::Text(s.into())
  }
}

impl From<&str> for Message {
  /// Copy a borrowed string as text.
  fn from(s: &str) -> Self {
    Self::Text(s.into())
  }
}

impl From<Utf8Bytes> for Message {
  /// Adopt refcounted text without copying.
  fn from(s: Utf8Bytes) -> Self {
    Self::Text(s)
  }
}

impl From<Bytes> for Message {
  /// Adopt refcounted bytes without copying.
  fn from(b: Bytes) -> Self {
    Self::Binary(b)
  }
}

impl From<Vec<u8>> for Message {
  /// Adopt an owned byte vector as binary (no copy).
  fn from(v: Vec<u8>) -> Self {
    Self::Binary(Bytes::from(v))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Binary clones share the backing (pointer-equal), never copy bytes.
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

  /// Text clones share the backing (pointer-equal), never copy bytes.
  #[test]
  fn text_clone_shares_allocation() {
    let msg = Message::from(String::from("hello"));
    let cloned = msg.clone();
    let (Message::Text(a), Message::Text(b)) = (msg, cloned) else {
      panic!("expected Text variants");
    };
    assert_eq!(a.as_str(), "hello");
    assert!(std::ptr::eq(a.as_str().as_ptr(), b.as_str().as_ptr()));
  }

  /// Every `From` direction builds the expected variant with equal bytes.
  #[test]
  fn conversions_cover_common_inputs() {
    assert_eq!(
      Message::from("hi"),
      Message::from(Utf8Bytes::from_static("hi"))
    );
    assert_eq!(
      Message::from(String::from("hi")),
      Message::from(Utf8Bytes::from_static("hi"))
    );
    assert_eq!(
      Message::from(vec![1u8, 2]),
      Message::Binary(Bytes::from_static(&[1, 2]))
    );
    assert!(Message::from("").is_empty());
    assert_eq!(Message::from("hi").len(), 2);
  }
}
