use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::library::{Fingerprint, SteamLibraries, library_owns_prefix};
use crate::vdf::{MAX_FOLDERS_BYTES, read_limited};

/// Cache filename under `$XDG_CACHE_HOME` (else `~/.cache`).
const CACHE_FILE: &str = "rsrpc/steam-libraries.json";

/// Cache schema version: bumped to 2 when prefix keys became canonical
/// (`/`-separated), so v1 caches holding Windows backslash keys are
/// ignored once and rescanned instead of reused empty.
const STEAM_CACHE_VERSION: u64 = 2;

/// On-disk cache: per-library prefix maps with fingerprints. Best-effort
/// accelerator — corrupt/missing cache means a full parse, never an error.
#[derive(Clone, Debug)]
pub(crate) struct CachedLibrary {
  pub(crate) fingerprint: Fingerprint,
  pub(crate) dirs: HashMap<String, String>,
}

/// Cache file location under the platform cache dir, if one is known.
fn cache_path() -> Option<PathBuf> {
  let base = std::env::var_os("XDG_CACHE_HOME")
    .map(PathBuf::from)
    .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))?;
  Some(base.join(CACHE_FILE))
}

fn fingerprint_to_json(fingerprint: &Fingerprint) -> serde_json::Value {
  serde_json::json!({
    "dir_mtime_ms": fingerprint.dir_mtime_ms,
    "manifests": fingerprint.manifests,
    "newest_manifest_ms": fingerprint.newest_manifest_ms,
    "compat_mtime_ms": fingerprint.compat_mtime_ms,
  })
}

fn fingerprint_from_json(value: &serde_json::Value) -> Option<Fingerprint> {
  Some(Fingerprint {
    dir_mtime_ms: value.get("dir_mtime_ms")?.as_u64()?,
    manifests: usize::try_from(value.get("manifests")?.as_u64()?).ok()?,
    newest_manifest_ms: value.get("newest_manifest_ms")?.as_u64()?,
    // Pre-compat caches lack the field: default 0 forces exactly one
    // rescan for libraries that actually have compatdata (self-healing;
    // libraries without it compare 0 == 0 and reuse).
    compat_mtime_ms: value
      .get("compat_mtime_ms")
      .and_then(|v| v.as_u64())
      .unwrap_or(0),
  })
}

/// Read the on-disk library cache, tolerating absence and corruption as
/// an empty cache (rediscovery covers the gap).
pub(crate) fn load_cache() -> HashMap<String, CachedLibrary> {
  let Some(path) = cache_path() else {
    return HashMap::new();
  };
  load_cache_from(&path)
}

/// Cache entries from one file (split for hermetic tests: production
/// reads the platform cache dir, tests pass a scratch file).
fn load_cache_from(path: &Path) -> HashMap<String, CachedLibrary> {
  let mut cached = HashMap::new();
  let Ok(body) = read_limited(path, MAX_FOLDERS_BYTES) else {
    return cached;
  };
  let Ok(doc) = serde_json::from_str::<serde_json::Value>(&body) else {
    return cached;
  };
  if doc.get("version").and_then(|v| v.as_u64()) != Some(STEAM_CACHE_VERSION) {
    return cached;
  }
  if let Some(libraries) = doc.get("libraries").and_then(|v| v.as_object()) {
    for (lib_path, entry) in libraries {
      let (Some(fingerprint), Some(dirs)) = (
        entry.get("fingerprint").and_then(fingerprint_from_json),
        entry.get("dirs").and_then(|v| v.as_object()),
      ) else {
        continue;
      };
      // Re-validate ownership on load: `save_cache` only writes
      // well-formed prefixes, but a hand-edited cache could inject e.g.
      // `"/"` and match every process until the fingerprint moves
      // (a dirs-only poison never triggers a rescan by itself).
      let library = Path::new(lib_path.as_str());
      let dirs = dirs
        .iter()
        .filter_map(|(prefix, appid)| {
          let appid = appid.as_str()?;
          if appid.is_empty() || !library_owns_prefix(library, prefix) {
            return None;
          }
          Some((prefix.clone(), appid.to_string()))
        })
        .collect();
      cached.insert(lib_path.clone(), CachedLibrary { fingerprint, dirs });
    }
  }
  cached
}

pub(crate) fn save_cache(libraries: &SteamLibraries) {
  let Some(path) = libraries.cache_override.clone().or_else(cache_path) else {
    return;
  };
  if let Some(parent) = path.parent()
    && std::fs::create_dir_all(parent).is_err()
  {
    return;
  }
  // Invert dirs by owning library (see `library_owns_prefix`): one cache
  // entry per library keeps validation per-library too. The key set is
  // snapshotted once: re-cloning it per prefix would be quadratic.
  let mut by_library: HashMap<String, HashMap<String, String>> = HashMap::new();
  for lib_path in libraries.fingerprints.keys() {
    by_library.insert(lib_path.clone(), HashMap::new());
  }
  let lib_paths: Vec<String> = by_library.keys().cloned().collect();
  for (prefix, appid) in &libraries.dirs {
    for lib_path in &lib_paths {
      if library_owns_prefix(Path::new(lib_path), prefix) {
        if let Some(entry) = by_library.get_mut(lib_path) {
          entry.insert(prefix.clone(), appid.clone());
        }
        break;
      }
    }
  }
  let mut doc = serde_json::Map::new();
  doc.insert(
    "version".to_string(),
    serde_json::Value::from(STEAM_CACHE_VERSION),
  );
  let mut libs = serde_json::Map::new();
  for (lib_path, fingerprint) in &libraries.fingerprints {
    let mut entry = serde_json::Map::new();
    entry.insert("fingerprint".to_string(), fingerprint_to_json(fingerprint));
    let dirs = by_library
      .remove(lib_path)
      .unwrap_or_default()
      .into_iter()
      .map(|(prefix, appid)| (prefix, serde_json::Value::String(appid)))
      .collect();
    entry.insert("dirs".to_string(), serde_json::Value::Object(dirs));
    libs.insert(lib_path.clone(), serde_json::Value::Object(entry));
  }
  doc.insert("libraries".to_string(), serde_json::Value::Object(libs));
  let body = serde_json::Value::Object(doc).to_string();
  // Atomic: tmp + rename, so a crash mid-write never corrupts the cache.
  let tmp = path.with_extension("json.tmp");
  if std::fs::write(&tmp, body).is_ok() {
    let _ = std::fs::rename(&tmp, &path);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// v1 caches (backslash prefix keys on Windows) are ignored once and
  /// rescanned instead of reused empty.
  #[test]
  fn v1_cache_is_ignored() {
    let dir = std::env::temp_dir().join(format!("rsrpc-steam-test-cachev1-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let file = dir.join("cache.json");
    std::fs::write(
        &file,
        r#"{"version":1,"libraries":{"/lib":{"fingerprint":{"dir_mtime_ms":1,"manifests":1,"newest_manifest_ms":1,"compat_mtime_ms":0},"dirs":{"/lib/steamapps/common/game/":"1"}}}}"#,
      )
      .expect("v1 cache");
    assert!(
      load_cache_from(&file).is_empty(),
      "v1 cache must be ignored"
    );
    let _ = std::fs::remove_dir_all(&dir);
  }

  /// The current schema version still loads through the same path.
  #[test]
  fn current_cache_version_loads() {
    let dir = std::env::temp_dir().join(format!("rsrpc-steam-test-cachev2-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let file = dir.join("cache.json");
    let body = serde_json::json!({
      "version": STEAM_CACHE_VERSION,
      "libraries": {
        "/lib": {
          "fingerprint": {"dir_mtime_ms": 1, "manifests": 1, "newest_manifest_ms": 1, "compat_mtime_ms": 0},
          "dirs": {"/lib/steamapps/common/game/": "1"}
        }
      }
    });
    std::fs::write(&file, body.to_string()).expect("v2 cache");
    let cached = load_cache_from(&file);
    assert_eq!(
      cached["/lib"].dirs.get("/lib/steamapps/common/game/"),
      Some(&"1".to_string())
    );
    let _ = std::fs::remove_dir_all(&dir);
  }
}
