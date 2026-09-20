//! User override loading: custom detectable games merged over the main
//! database (matched without the OS filter, so Proton/Wine and launcher
//! entries work on any platform).
//!
//! Two sources, both optional and additive:
//! - a single file (`overrides.json`): one JSON array — or one bare
//!   object — of [`DetectableActivity`];
//! - a directory (`overrides.d`): every `*.json` file inside, sorted by
//!   file name, each an array or a bare object.
//!
//! A corrupt file never fails the load: it is skipped with a warning and
//! the rest still applies. Missing paths mean "no overrides", silently.

use std::path::{Path, PathBuf};

use rsrpc_detect::db::DetectableActivity;

/// Parse override content: a JSON array of entries, or a single entry
/// object (convenience for one-game files).
pub fn parse_overrides(content: &str) -> Result<Vec<DetectableActivity>, String> {
  if let Ok(entries) = serde_json::from_str::<Vec<DetectableActivity>>(content) {
    return Ok(entries);
  }
  serde_json::from_str::<DetectableActivity>(content)
    .map(|entry| vec![entry])
    .map_err(|err| format!("not an override array or object: {err}"))
}

/// Load one override file. Missing file = empty (not an error).
pub fn load_file(path: &Path) -> Result<Vec<DetectableActivity>, String> {
  if !path.exists() {
    return Ok(Vec::new());
  }
  let content = std::fs::read_to_string(path)
    .map_err(|err| format!("cannot read {}: {err}", path.display()))?;
  parse_overrides(&content).map_err(|err| format!("{}: {err}", path.display()))
}

/// Load every `*.json` file in `dir`, sorted by file name. Missing dir =
/// empty; corrupt files are skipped with a warning, never fatal. A
/// mid-iteration IO error is warned (not silent); extensions match
/// case-insensitively (`.JSON` counts).
pub fn load_dir(dir: &Path) -> Vec<DetectableActivity> {
  let Ok(entries) = std::fs::read_dir(dir) else {
    return Vec::new();
  };
  let mut files: Vec<PathBuf> = Vec::new();
  for entry in entries {
    let entry = match entry {
      Ok(entry) => entry,
      Err(err) => {
        tracing::warn!(
          "[overrides] Skipping unreadable entry in {}: {}",
          dir.display(),
          err
        );
        continue;
      }
    };
    let path = entry.path();
    let is_json = path
      .extension()
      .and_then(|ext| ext.to_str())
      .is_some_and(|ext| ext.eq_ignore_ascii_case("json"));
    if is_json {
      files.push(path);
    }
  }
  files.sort();
  let mut overrides = Vec::new();
  for file in files {
    match load_file(&file) {
      Ok(mut entries) => overrides.append(&mut entries),
      Err(err) => tracing::warn!("[overrides] Skipping {}: {}", file.display(), err),
    }
  }
  overrides
}

fn config_dir_with(env: &dyn Fn(&str) -> Option<String>) -> PathBuf {
  env("XDG_CONFIG_HOME")
    .map(PathBuf::from)
    .unwrap_or_else(|| {
      let home = env("HOME").unwrap_or_else(|| ".".to_string());
      PathBuf::from(home).join(".config")
    })
    .join("rsrpc")
}

/// Default single-file location, compatible with rsrpc-wrapper:
/// `$RSRPC_OVERRIDES_FILE`, else `$XDG_CONFIG_HOME/rsrpc/overrides.json`,
/// else `~/.config/rsrpc/overrides.json`.
#[must_use]
pub fn default_file_path() -> PathBuf {
  default_file_path_with(&|key| std::env::var(key).ok())
}

/// Same as [`default_file_path`], with an explicit environment source
/// instead of the process environment (hermetic tests, embedders that
/// virtualize configuration).
#[must_use]
pub fn default_file_path_with(env: &dyn Fn(&str) -> Option<String>) -> PathBuf {
  if let Some(custom) = env("RSRPC_OVERRIDES_FILE") {
    return PathBuf::from(custom);
  }
  config_dir_with(env).join("overrides.json")
}

/// Default directory location: `$RSRPC_OVERRIDES_DIR`, else
/// `$XDG_CONFIG_HOME/rsrpc/overrides.d`, else `~/.config/rsrpc/overrides.d`.
#[must_use]
pub fn default_dir_path() -> PathBuf {
  default_dir_path_with(&|key| std::env::var(key).ok())
}

/// Same as [`default_dir_path`], with an explicit environment source
/// instead of the process environment (hermetic tests, embedders that
/// virtualize configuration).
#[must_use]
pub fn default_dir_path_with(env: &dyn Fn(&str) -> Option<String>) -> PathBuf {
  if let Some(custom) = env("RSRPC_OVERRIDES_DIR") {
    return PathBuf::from(custom);
  }
  config_dir_with(env).join("overrides.d")
}
