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

fn main() -> Result<(), Box<dyn std::error::Error>> {
  println!("Fetching detectable.json from {DETECTABLE_URL}...");

  let body = http_agent(std::time::Duration::from_secs(60))
    .get(DETECTABLE_URL)
    .call()?
    .into_body()
    .with_config()
    .limit(64 * 1024 * 1024)
    .read_to_string()?;

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
  std::fs::write(&path, &output)?;
  println!("Wrote {} bytes to {}", output.len(), path.display());

  Ok(())
}
