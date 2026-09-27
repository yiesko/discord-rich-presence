use rsrpc_protocol::query::query_params;

/// `key=value` pairs parse; bare flags and empty inputs yield nothing.
#[test]
fn parses_pairs_and_ignores_bare_flags() {
  let params = query_params("/?v=1&encoding=json&client_id=abc");
  assert_eq!(params.get("v").map(String::as_str), Some("1"));
  assert_eq!(params.get("encoding").map(String::as_str), Some("json"));
  assert_eq!(params.get("client_id").map(String::as_str), Some("abc"));
  assert!(query_params("/").is_empty());
  assert!(query_params("/?flag").is_empty());
  assert!(query_params("").is_empty());
}

/// Duplicate keys: last write wins (legacy `get_url_params` semantics —
/// the map insert overwrites).
#[test]
fn duplicate_keys_last_wins() {
  let params = query_params("/?a=1&a=2");
  assert_eq!(params.get("a").map(String::as_str), Some("2"));
}

/// `key=` parses to an empty value — kept, and distinct from a bare
/// flag, which is ignored entirely.
#[test]
fn empty_value_is_kept() {
  let params = query_params("/?flag=&x=1");
  assert_eq!(params.get("flag").map(String::as_str), Some(""));
  assert_eq!(params.get("x").map(String::as_str), Some("1"));
}

/// Only the first `?` starts the query; a later one stays part of the
/// value (no percent-decoding, no re-splitting).
#[test]
fn multiple_question_marks_keep_legacy_semantics() {
  let params = query_params("/?a=1?b=2");
  assert_eq!(params.get("a").map(String::as_str), Some("1?b=2"));
  assert!(!params.contains_key("b"));
}
