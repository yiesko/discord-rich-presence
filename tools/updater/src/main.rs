//! Fetches Discord's detectable games database and writes a trimmed offline
//! snapshot for rsrpc.
//!
//! Usage (from the workspace root):
//!
//! ```bash
//! cargo run --manifest-path tools/updater/Cargo.toml
//! ```
//!
//! This writes `crates/rsrpc-detect/resources/detectable.json`, which is
//! embedded into the detection crate at build time via `include_str!`
//! (see `rsrpc_detect::db::BUNDLED_DETECTABLE`).

use std::path::PathBuf;

const DETECTABLE_URL: &str = "https://discord.com/api/v9/applications/detectable";
const USER_AGENT: &str = "rsrpc-updater/0.37.0";

fn output_path() -> PathBuf {
  PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .join("..")
    .join("..")
    .join("crates")
    .join("rsrpc-detect")
    .join("resources")
    .join("detectable.json")
}

/// Local HTTP agent (mirrors `rsrpc::http_agent`): a global timeout so a
/// blackholed endpoint cannot hang the fetch forever.
fn http_agent(timeout: std::time::Duration) -> ureq::Agent {
  ureq::Agent::config_builder().timeout_global(Some(timeout)).build().into()
}

/// Three attempts with a 1s delay between: any transient error from
/// Discord's CDN fails the tool, which fails every CI job that invokes it.
fn fetch_with_retry(agent: &ureq::Agent, url: &str) -> Result<String, Box<dyn std::error::Error>> {
  let mut last_error: Option<Box<dyn std::error::Error>> = None;
  for attempt in 1..=3 {
    match agent.get(url).header("User-Agent", USER_AGENT).call() {
      Ok(response) => {
        return response
          .into_body()
          .with_config()
          .limit(64 * 1024 * 1024)
          .read_to_string()
          .map_err(|e| e.into());
      }
      Err(e) => {
        eprintln!("attempt {attempt}/3 failed: {e}");
        last_error = Some(Box::new(e));
        if attempt < 3 {
          std::thread::sleep(std::time::Duration::from_secs(1));
        }
      }
    }
  }
  Err(last_error.expect("the loop always runs at least once"))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
  println!("Fetching detectable.json from {DETECTABLE_URL}...");

  let body = fetch_with_retry(
    &http_agent(std::time::Duration::from_secs(60)),
    DETECTABLE_URL,
  )?;

  // Single source of truth: the same trim the scanner, the CLI fallback
  // and the hourly refresh use (see `rsrpc_detect::db::trim_detectable`).
  // A hand-rolled copy here drifted before (aliases were silently dropped
  // from the bundled snapshot); never duplicate it again.
  let output = rsrpc_detect::db::trim_detectable(&body)?;
  // Count from the trimmed (small) output, not the full body: one small
  // transient DOM instead of two (the full-body DOM just for a log line).
  let games: usize = serde_json::from_str::<Vec<serde_json::Value>>(&output)
    .map(|games| games.len())
    .unwrap_or(0);
  println!("Trimmed to {games} games, writing snapshot...");
  let path = output_path();
  if let Some(parent) = path.parent() {
    std::fs::create_dir_all(parent)?;
  }
  // Write to a temp file first, then rename: a crash mid-write leaves the
  // previous snapshot intact instead of a truncated detectable.json.
  let tmp = path.with_extension("tmp");
  std::fs::write(&tmp, &output)?;
  std::fs::rename(&tmp, &path)?;
  println!("Wrote {} bytes to {}", output.len(), path.display());

  Ok(())
}
