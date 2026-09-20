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
