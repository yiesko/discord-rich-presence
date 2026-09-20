//! Query-string parsing shared by the game and bridge transports.
//!
//! Exact legacy `get_url_params` semantics: no `?` yields nothing, pairs
//! without `=` are ignored, no percent-decoding (Discord clients send
//! plain ASCII here).

use std::collections::HashMap;

/// Split the query of `uri` (`/path?a=1&b=2`) into key/value pairs.
#[must_use]
pub fn query_params(uri: &str) -> HashMap<String, String> {
  let mut params = HashMap::new();
  let Some((_, query)) = uri.split_once('?') else {
    return params;
  };
  for pair in query.split('&') {
    if let Some((key, value)) = pair.split_once('=') {
      params.insert(key.to_string(), value.to_string());
    }
  }
  params
}
