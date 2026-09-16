//! Database loading and one-shot diagnostics.
//!
//! Parsing prefers the direct struct decode (zero DOM); bodies whose
//! entries miss required fields fall back through the trimmed projection.
//! Callers that already hold a parsed `Vec` skip both via
//! [`Daemon::from_parsed`][crate::daemon::Daemon::from_parsed].

use std::sync::Arc;

use rsrpc_detect::db::{self, DetectableActivity};
use rsrpc_protocol::error::{Result, RsrpcError};

/// Minimal game info returned by one-shot scans.
#[derive(Clone, Debug)]
pub struct DetectedGame {
  /// Discord application id.
  pub id: String,
  /// Human-readable game name.
  pub name: String,
  /// OS pid, when the scan observed one.
  pub pid: Option<u64>,
}

/// Minimal database entry for inventory diagnostics.
#[derive(Clone, Debug)]
pub struct DetectableSummary {
  /// Discord application id.
  pub id: String,
  /// Human-readable game name.
  pub name: String,
  /// Number of executable patterns.
  pub executables: usize,
}

/// Parse a database body, direct first then trimmed fallback.
///
/// Returns the entries plus a source label for the boot inventory line
/// (`direct` or `trimmed`).
///
/// # Errors
///
/// Returns [`RsrpcError::InvalidJson`] when neither shape parses (a
/// fetched-but-garbage body: callers fall back to the bundled snapshot).
pub fn parse_body(body: &str) -> Result<(Vec<DetectableActivity>, &'static str)> {
  if let Ok(parsed) = serde_json::from_str::<Vec<DetectableActivity>>(body) {
    return Ok((parsed, "direct"));
  }
  let trimmed = db::trim_detectable_value(body)?;
  let parsed: Vec<DetectableActivity> =
    serde_json::from_value(trimmed).map_err(|err| RsrpcError::InvalidJson { source: err })?;
  Ok((parsed, "trimmed"))
}

/// Read a database file, then [`parse_body`].
///
/// # Errors
///
/// [`RsrpcError::UnreadableFile`] when the file is missing or unreadable,
/// `InvalidJson` when it does not parse.
pub fn load_file(path: &std::path::Path) -> Result<Vec<DetectableActivity>> {
  let body = std::fs::read_to_string(path).map_err(|source| RsrpcError::UnreadableFile {
    path: path.to_path_buf(),
    source,
  })?;
  Ok(parse_body(&body)?.0)
}

/// The bundled offline snapshot. Essentially infallible (validated at
/// release time); bubbles `InvalidJson` only if the embedded data is
/// corrupt.
///
/// # Errors
///
/// See [`parse_body`].
pub fn load_bundled() -> Result<Vec<DetectableActivity>> {
  Ok(parse_body(db::BUNDLED_DETECTABLE)?.0)
}

/// Summarize a held database (entry count, executable counts, names).
#[must_use]
pub fn summarize(detectable: &[Arc<DetectableActivity>]) -> Vec<DetectableSummary> {
  detectable
    .iter()
    .map(|entry| DetectableSummary {
      id: entry.id.clone(),
      name: entry.name.clone(),
      executables: entry.executables.as_ref().map(Vec::len).unwrap_or(0),
    })
    .collect()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parse_body_prefers_direct_and_labels() {
    let (parsed, label) = parse_body(r#"[{"id":"1","name":"G","hook":false}]"#).unwrap();
    assert_eq!(label, "direct");
    assert_eq!(parsed.len(), 1);
  }

  #[test]
  fn parse_body_falls_back_through_trim() {
    // Missing `hook` (required without default): direct parse fails, the
    // trimmed projection defaults it.
    let (parsed, label) = parse_body(r#"[{"id":"1","name":"G"}]"#).unwrap();
    assert_eq!(label, "trimmed");
    assert_eq!(parsed[0].name, "G");
  }

  #[test]
  fn parse_body_rejects_garbage() {
    assert!(parse_body("not json{{").is_err());
  }

  #[test]
  fn bundled_loads() {
    assert!(!load_bundled().expect("bundled snapshot parses").is_empty());
  }
}
