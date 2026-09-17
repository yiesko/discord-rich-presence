//! Hourly database/exclusions refresh: conditional fetches (ETag plus
//! raw and canonical content hashes) so an unchanged hour costs ~nothing.

use std::time::Duration;

use crate::db::{DetectableActivity, Exclusions, body_hash, parse_exclusions};
use crate::server::ProcessServer;

/// HTTP agent with a global timeout: without it, a blackholed endpoint
/// hangs the hourly refresh thread forever.
fn http_agent(timeout: Duration) -> ureq::Agent {
  ureq::Agent::config_builder()
    .timeout_global(Some(timeout))
    .build()
    .into()
}

/// Hourly database-refresh inputs, threaded from startup into the
/// refresh loop as one value (see [`ProcessServer::start`]).
#[derive(Clone, Debug, Default)]
pub struct RefreshConfig {
  pub db_url: Option<String>,
  pub enable: bool,
  pub etag: Option<String>,
  pub content_hash: Option<(u64, u64)>,
  pub exclusions_url: Option<String>,
}

/// Outcome of one conditional refresh: either the database changed (new
/// ETag + parsed activities), its bytes are identical (new ETag, same
/// bytes: CDN etags flap without content changes), its trimmed content is
/// identical (new ETag, different bytes, same games: volatile CDN
/// whitespace/ordering/metadata), or the server said 304 (keep everything).
/// Both hashes always travel with the outcome, so the next check's
/// fast paths stay armed no matter which arm produced them.
pub enum FetchOutcome {
  Unchanged,
  SameContent {
    etag: Option<String>,
    content_hash: u64,
    trimmed_hash: u64,
  },
  Updated {
    etag: Option<String>,
    content_hash: u64,
    trimmed_hash: u64,
    detectable: Vec<DetectableActivity>,
  },
}

/// Fetch Discord's detection exclusions (installer/crash-reporter names +
/// regex patterns). Tiny payload (a few KB): plain GET with a 1 MiB cap, no
/// ETag dance — the hourly cadence dominates the cost, and parsing is
/// `tolerant by design` (see [`parse_exclusions`]).
pub fn fetch_exclusions(url: &str) -> rsrpc_protocol::error::Result<Exclusions> {
  let body = http_agent(std::time::Duration::from_secs(30))
    .get(url)
    .call()?
    .into_body()
    .with_config()
    .limit(1024 * 1024)
    .read_to_string()?;
  Ok(parse_exclusions(&body))
}

/// Fetch the detectable games database, skipping the download when it has
/// not changed since `etag` (Discord answers `304`, `ETag` + `max-age=3600`
/// line up with the hourly cadence). A 304 costs one header round trip and
/// zero parsing, so idle hours leave RSS untouched.
pub fn fetch_detectable_etag(
  url: &str,
  etag: Option<&str>,
  known_raw: Option<u64>,
  known_trimmed: Option<u64>,
) -> rsrpc_protocol::error::Result<FetchOutcome> {
  let mut request = http_agent(std::time::Duration::from_secs(30)).get(url);
  if let Some(tag) = etag {
    request = request.header("If-None-Match", tag);
  }
  let response = request.call()?;
  if response.status().as_u16() == 304 {
    return Ok(FetchOutcome::Unchanged);
  }
  let etag = response
    .headers()
    .get("etag")
    .and_then(|value| value.to_str().ok())
    .map(str::to_string);
  let body = response
    .into_body()
    .with_config()
    .limit(64 * 1024 * 1024)
    .read_to_string()?;

  // Same bytes under a new tag (CDN etag flaps): skip everything below —
  // no parse, no rebuild. This is where the retained memory comes from,
  // not the download.
  let content_hash = body_hash(&body);
  if known_raw.is_some_and(|known| known == content_hash) {
    return Ok(FetchOutcome::SameContent {
      etag,
      content_hash,
      // Same bytes hash to the same canonical form: the trimmed value
      // is whatever it was when these bytes were last seen.
      trimmed_hash: known_trimmed.unwrap_or(content_hash),
    });
  }

  // Direct parse first: serde skips unknown fields, so the full body
  // parses with zero DOM overhead (~5x less transient memory than the
  // trimmed-Value pass). The trimming pass stays as fallback for entries
  // missing required fields (it defaults them). The first error is
  // reused below: re-parsing the same body a third time just to produce
  // an identical error would double the failure cost for nothing.
  let parsed = match serde_json::from_str::<Vec<DetectableActivity>>(&body) {
    Ok(parsed) => parsed,
    Err(first_err) => {
      if let Ok(trimmed) = crate::db::trim_detectable_value(&body)
        && let Ok(parsed) = serde_json::from_value::<Vec<DetectableActivity>>(trimmed)
      {
        parsed
      } else {
        return Err(first_err.into());
      }
    }
  };
  // The raw body has served its purposes (raw hash above, parse input
  // here): drop its ~13MB BEFORE the canonical hash and the automata
  // build below, instead of letting it ride along to the end of this
  // function and inflating the rebuild peak.
  drop(body);
  finish_fetch(etag, content_hash, known_trimmed, parsed)
}

/// Hash the canonical scanner projection and decide whether the parsed
/// games actually changed: volatile CDN bytes (whitespace, metadata the
/// scanner never reads) hash identically, so those hours skip the
/// automaton rebuild entirely. Entry order is preserved (first-wins
/// index ties depend on it), so a pure reorder rebuilds.
fn finish_fetch(
  etag: Option<String>,
  content_hash: u64,
  known_trimmed: Option<u64>,
  detectable: Vec<DetectableActivity>,
) -> rsrpc_protocol::error::Result<FetchOutcome> {
  let trimmed_hash = crate::db::canonical_content_hash(&detectable);
  if known_trimmed.is_some_and(|known| known == trimmed_hash) {
    return Ok(FetchOutcome::SameContent {
      etag,
      content_hash,
      trimmed_hash,
    });
  }
  Ok(FetchOutcome::Updated {
    etag,
    content_hash,
    trimmed_hash,
    detectable,
  })
}

/// Whether a fetched exclusions set may replace the active one: an empty
/// fetch over a non-empty active set means a broken upstream (error page
/// served with HTTP 200 parses to nothing), never a legitimate wipe.
fn may_replace_exclusions(current: &Exclusions, fetched: &Exclusions) -> bool {
  !fetched.is_empty() || current.is_empty()
}

impl ProcessServer {
  /// Refresh the exclusions list once, best-effort: failures keep the
  /// previous set (empty at first boot = current behavior).
  pub(crate) fn refresh_exclusions(&self) {
    let Some(url) = self.refresh.exclusions_url.clone() else {
      return;
    };
    match fetch_exclusions(&url) {
      Ok(exclusions) => {
        let current = self.exclusions.read().unwrap_or_else(|e| e.into_inner());
        if !may_replace_exclusions(&current, &exclusions) {
          tracing::warn!(
            "[Process Scanner] Exclusions fetch came back empty, keeping previous set"
          );
          return;
        }
        drop(current);
        tracing::info!(
          "[Process Scanner] Exclusions updated: {} names, {} patterns",
          exclusions.executables.len(),
          exclusions.patterns.len()
        );
        self.set_exclusions(exclusions);
      }
      Err(err) => {
        tracing::warn!("[Process Scanner] Error updating exclusions, retrying in 1h: {err}");
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use std::collections::HashSet;

  use super::*;

  fn full_exclusions() -> Exclusions {
    Exclusions {
      executables: HashSet::from(["setup.exe".to_string()]),
      patterns: regex::RegexSet::empty(),
    }
  }

  fn empty_exclusions() -> Exclusions {
    Exclusions {
      executables: HashSet::new(),
      patterns: regex::RegexSet::empty(),
    }
  }

  #[test]
  fn empty_fetch_never_wipes_active_set() {
    // A malformed upstream (error page served with HTTP 200) parses to
    // an empty set: keep serving the previous one instead of unblocking
    // installers and crash reporters for an hour.
    assert!(!may_replace_exclusions(
      &full_exclusions(),
      &empty_exclusions()
    ));
    assert!(may_replace_exclusions(
      &full_exclusions(),
      &full_exclusions()
    ));
    assert!(may_replace_exclusions(
      &empty_exclusions(),
      &empty_exclusions()
    ));
  }
}
