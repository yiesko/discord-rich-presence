use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache::{load_cache, save_cache};
use crate::mounts::mount_library_roots;
use crate::vdf::{
  MAX_FOLDERS_BYTES, MAX_MANIFEST_BYTES, library_paths, manifest_ids, parse_vdf_str, read_limited,
};

/// Override for non-standard installs (and hermetic tests): when set to an
/// existing directory, only it is used as the Steam root.
const STEAM_ROOT_ENV: &str = "RSRPC_STEAM_ROOT";

/// Extra library roots defined by the user as a platform-native path
/// list (`:`-separated on Unix, `;`-separated on Windows), merged with
/// everything discovered automatically.
const STEAM_LIBRARIES_ENV: &str = "RSRPC_STEAM_LIBRARIES";

/// Lowercased `.../steamapps/common/<installdir>/` (or
/// `.../steamapps/compatdata/<id>/`) for prefix matching against the
/// scanner's process paths.
fn common_prefix(library: &str, installdir: &str) -> String {
  let mut prefix = canonical_steam_path(&format!("{library}/steamapps/common/{installdir}"));
  if !prefix.ends_with('/') {
    prefix.push('/');
  }
  prefix
}

/// Canonical form for prefix matching: lowercase, `/` separators,
/// leading `/`. Windows contributes `\`-separated library and process
/// paths; without this, cached keys and queries never share a form
/// there and no prefix ever matches.
fn canonical_steam_path(raw: &str) -> String {
  let mut out = raw.replace('\\', "/").to_lowercase();
  if !out.starts_with('/') {
    out.insert(0, '/');
  }
  out
}

/// Change marker for one library: steamapps dir mtime, manifest count,
/// newest manifest file mtime, and compatdata dir mtime. Installs,
/// uninstalls, moves and content edits each change at least one of the
/// four (count covers add/remove, dir mtime covers rename churn,
/// newest-file covers in-place rewrites, compatdata covers Proton-prefix
/// appearances with no manifest change). In-place edits *inside* an
/// existing pfx without any mtime movement stay invisible until an
/// unrelated change forces a rescan (fail-closed: miss, never
/// mis-attribute).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Fingerprint {
  pub(crate) dir_mtime_ms: u64,
  pub(crate) manifests: usize,
  pub(crate) newest_manifest_ms: u64,
  pub(crate) compat_mtime_ms: u64,
}

fn file_mtime_ms(path: &Path) -> Option<u64> {
  std::fs::metadata(path)
    .and_then(|meta| meta.modified())
    .ok()?
    .duration_since(UNIX_EPOCH)
    .ok()
    .and_then(|age| u64::try_from(age.as_millis()).ok())
}

fn dir_fingerprint(apps_dir: &Path) -> Option<Fingerprint> {
  let dir_mtime_ms = file_mtime_ms(apps_dir)?;
  let mut manifests = 0;
  let mut newest_manifest_ms = 0;
  let mut compat_mtime_ms = 0;
  if let Ok(entries) = std::fs::read_dir(apps_dir) {
    for entry in entries.flatten() {
      let name = entry.file_name();
      let name = name.to_string_lossy();
      if name.starts_with("appmanifest_") && name.ends_with(".acf") {
        manifests += 1;
        if let Some(mtime) = file_mtime_ms(&entry.path()) {
          newest_manifest_ms = newest_manifest_ms.max(mtime);
        }
      } else if name == "compatdata"
        && let Some(mtime) = file_mtime_ms(&entry.path())
      {
        // Proton prefixes appearing with no manifest change (the
        // never-written-acf case) still move this marker.
        compat_mtime_ms = compat_mtime_ms.max(mtime);
      }
    }
  }
  Some(Fingerprint {
    dir_mtime_ms,
    manifests,
    newest_manifest_ms,
    compat_mtime_ms,
  })
}

/// Install-dir -> AppId over every known Steam library.
#[derive(Clone, Debug, Default)]
pub struct SteamLibraries {
  /// `libraryfolders.vdf` files whose mtime gates a refresh.
  watched: Vec<PathBuf>,
  /// Last-seen mtimes of the watched files.
  mtimes: HashMap<PathBuf, SystemTime>,
  /// Lowercased install-dir prefix -> AppId.
  pub(crate) dirs: HashMap<String, String>,
  /// Library path (string form) -> fingerprint at last scan.
  pub(crate) fingerprints: HashMap<String, Fingerprint>,
  /// Roots this map was built from. Refresh re-resolves libraries from
  /// THESE roots — never a fresh environment sweep — so an exclusive
  /// source (`RSRPC_STEAM_ROOT`, injected layouts) is never diluted by
  /// re-discovery. Fresh mounts are picked up by re-resolving (mounts
  /// are probed per root via their folders files... see below).
  roots: Vec<PathBuf>,
  /// True when discovery was exclusive (explicit root override): refresh
  /// re-resolves the stored roots only, never the full source list.
  exclusive: bool,
  /// Ticks since last full root re-collection (mounts are re-probed
  /// every 120th tick — not every tick, the `/proc` exe sweep is the most
  /// expensive discovery source; installs still surface immediately via
  /// the folders-file marker).
  ticks: u64,
  /// Override for the on-disk cache file (hermetic-test seam: refresh
  /// persistence lands here instead of the real user cache dir; `None`
  /// keeps production behavior). Loading (`discover`) always uses the
  /// real cache — the override scopes the write path tests exercise.
  pub(crate) cache_override: Option<PathBuf>,
}

impl SteamLibraries {
  /// Build from one explicit root, parsing unconditionally (custom
  /// installs, tooling, hermetic tests). Empty when the root holds no
  /// Steam layout. Prefer [`SteamLibraries::discover`] in production: it
  /// consults the on-disk cache and every discovery source.
  pub fn from_root(root: &Path) -> Self {
    let mut libraries = Self {
      roots: vec![root.to_path_buf()],
      exclusive: true,
      ..Default::default()
    };
    let (libs, folders_file) = root_libraries(root);
    if let Some(file) = folders_file {
      if let Ok(mtime) = std::fs::metadata(&file).and_then(|meta| meta.modified()) {
        libraries.mtimes.insert(file.clone(), mtime);
      }
      libraries.watched.push(file);
    }
    for lib in libs {
      libraries.scan_library(&lib);
    }
    libraries
  }

  /// Full discovery: collect roots from every source, resolve them to
  /// libraries, reuse the on-disk cache wherever fingerprints still
  /// match, parse only what is new or changed.
  pub fn discover() -> Self {
    let mut libraries = Self::default();
    let cached = load_cache();
    let mut scanned = 0;
    let mut reused = 0;
    let roots = collect_roots();
    libraries.roots = roots.clone();
    libraries.exclusive = custom_root().is_some();
    for root in roots {
      let (libs, folders_file) = root_libraries(&root);
      if let Some(file) = folders_file {
        if let Ok(mtime) = std::fs::metadata(&file).and_then(|meta| meta.modified()) {
          libraries.mtimes.insert(file.clone(), mtime);
        }
        if !libraries.watched.contains(&file) {
          libraries.watched.push(file);
        }
      }
      for lib in libs {
        let key = lib.to_string_lossy().to_string();
        let live = dir_fingerprint(&lib.join("steamapps"));
        let cached_dirs = live.as_ref().and_then(|live| {
          cached
            .get(&key)
            .and_then(|entry| (entry.fingerprint == *live).then(|| entry.dirs.clone()))
        });
        match cached_dirs {
          Some(dirs) => {
            if libraries.fingerprints.contains_key(&key) {
              continue; // same library via another root: already merged
            }
            reused += 1;
            for (prefix, appid) in dirs {
              libraries.dirs.entry(prefix).or_insert(appid);
            }
            // `cached_dirs` is `Some` only when `live` is (see above):
            // keep the fingerprint without unwrapping the proof apart.
            if let Some(live) = live {
              libraries.fingerprints.insert(key, live);
            }
          }
          None => {
            if libraries.fingerprints.contains_key(&key) {
              continue; // same library via another root: already parsed
            }
            scanned += 1;
            libraries.scan_library(&lib);
          }
        }
      }
    }
    tracing::info!(
      "[Process Scanner] Steam libraries: {} install dirs ({} parsed, {} from cache)",
      libraries.dirs.len(),
      scanned,
      reused
    );
    save_cache(&libraries);
    libraries
  }

  /// Revalidate once per scan tick: one stat per watched file plus one
  /// fingerprint per known library; only new or changed libraries pay
  /// for manifest parsing. Vanished libraries are dropped. Every 120th
  /// tick the roots themselves are re-collected (fresh mounts): the
  /// `/proc` exe sweep costs ~5ms, so not every tick — and installs
  /// already trigger re-collection via the folders-file marker.
  #[hotpath::measure]
  pub fn refresh_if_stale(&mut self) {
    self.ticks = self.ticks.saturating_add(1);
    let folders_changed = self.watched.iter().any(|file| {
      std::fs::metadata(file)
        .and_then(|meta| meta.modified())
        .ok()
        != self.mtimes.get(file).cloned()
    });
    // New libraries appear via a folders-file change (installs register
    // there) or a fresh mount: re-collect roots (stats only) when the
    // marker moved or the mount-probe tick hit, else re-fingerprint the
    // known libraries.
    let mut libs: Vec<PathBuf>;
    if folders_changed || self.ticks.is_multiple_of(120) {
      if folders_changed {
        tracing::debug!("[Process Scanner] Steam folders changed, re-resolving libraries");
      }
      // Re-resolve from the SAME source discovery used: an exclusive map
      // (explicit override, injected layouts) re-resolves its stored
      // roots only and is never diluted by re-discovery. Otherwise
      // re-collect everything (new disks, new mounts) and remember it.
      let roots = if self.exclusive {
        self.roots.clone()
      } else {
        collect_roots()
      };
      libs = Vec::new();
      for root in &roots {
        let (root_libs, folders_file) = root_libraries(root);
        if let Some(file) = folders_file {
          if let Ok(mtime) = std::fs::metadata(&file).and_then(|meta| meta.modified()) {
            self.mtimes.insert(file.clone(), mtime);
          }
          if !self.watched.contains(&file) {
            self.watched.push(file);
          }
        }
        libs.extend(root_libs);
      }
      if !self.exclusive {
        self.roots = roots;
      }
      libs.sort();
      libs.dedup();
      // Prune watch markers whose root no longer resolves a live Steam
      // layout (unmounted/deleted steamapps): markers for live layouts
      // stay exactly as before, so transiently-missing files keep their
      // slots — but a root that remains a plain directory must not pin
      // full re-resolution on every tick.
      let live_files: std::collections::HashSet<PathBuf> = self
        .roots
        .iter()
        .map(|root| root.join("steamapps"))
        .filter(|apps| apps.is_dir())
        .map(|apps| apps.join("libraryfolders.vdf"))
        .collect();
      self.watched.retain(|file| live_files.contains(file));
      self.mtimes.retain(|file, _| live_files.contains(file));
    } else {
      libs = self.fingerprints.keys().map(PathBuf::from).collect();
    }
    // Fast path (steady state: every tick): every library fingerprints
    // identically and the set is unchanged — the cached prefixes are
    // current, so return without the O(libs x prefixes) recopy below
    // and without touching the on-disk cache. Fingerprinting itself is
    // a handful of stats.
    if libs.len() == self.fingerprints.len()
      && libs.iter().all(|lib| {
        let key = lib.to_string_lossy();
        match (
          dir_fingerprint(&lib.join("steamapps")),
          self.fingerprints.get(key.as_ref()),
        ) {
          (Some(live), Some(known)) => live == *known,
          _ => false,
        }
      })
    {
      return;
    }
    let mut changed = false;
    let mut fresh_dirs: HashMap<String, String> = HashMap::new();
    let mut fresh_fingerprints: HashMap<String, Fingerprint> = HashMap::new();
    for lib in &libs {
      let key = lib.to_string_lossy().to_string();
      match dir_fingerprint(&lib.join("steamapps")) {
        // Vanished (unmounted, deleted): drop it.
        None => changed = true,
        Some(live) => {
          if self.fingerprints.get(&key) == Some(&live) {
            // Unchanged: keep this library's current prefixes. The
            // owning-prefix form is hoisted per library (not rebuilt
            // per cached prefix) — this whole branch only runs when
            // some *other* library changed.
            let owned = library_prefix(lib);
            for (prefix, appid) in &self.dirs {
              if prefix.starts_with(&owned) {
                fresh_dirs.insert(prefix.clone(), appid.clone());
              }
            }
            fresh_fingerprints.insert(key, live);
          } else {
            changed = true;
            let before = fresh_dirs.len();
            self.scan_library_into(lib, &mut fresh_dirs);
            tracing::debug!(
              "[Process Scanner] Steam library rescanned: {} ({} prefixes)",
              lib.display(),
              fresh_dirs.len().saturating_sub(before)
            );
            fresh_fingerprints.insert(key, live);
          }
        }
      }
    }
    self.fingerprints = fresh_fingerprints;
    if changed || fresh_dirs.len() != self.dirs.len() {
      self.dirs = fresh_dirs;
      save_cache(self);
    }
  }

  /// AppId whose install dir is the longest prefix of `path`. The
  /// query is canonicalized first (lowercase, `/` separators, leading
  /// `/`), so native Windows paths match the cached keys.
  #[inline]
  pub fn match_prefix(&self, path: &str) -> Option<&str> {
    let normalized_path = canonical_steam_path(path);
    let mut best: Option<&str> = None;
    let mut best_len = 0;
    for (prefix, appid) in &self.dirs {
      if prefix.len() > best_len && normalized_path.starts_with(prefix.as_str()) {
        best = Some(appid.as_str());
        best_len = prefix.len();
      }
    }
    best
  }

  /// Parse one library's manifests into the map. Unconditional: callers
  /// decide freshness via fingerprints first.
  fn scan_library(&mut self, library: &Path) {
    let mut dirs = std::mem::take(&mut self.dirs);
    self.scan_library_into(library, &mut dirs);
    self.dirs = dirs;
    let key = library.to_string_lossy().to_string();
    if let Some(fingerprint) = dir_fingerprint(&library.join("steamapps")) {
      self.fingerprints.insert(key, fingerprint);
    }
  }

  fn scan_library_into(&self, library: &Path, dirs: &mut HashMap<String, String>) {
    let apps_dir = library.join("steamapps");
    let Ok(entries) = std::fs::read_dir(&apps_dir) else {
      return;
    };
    // Entry-count cap: a stuffed `steamapps/` (junk numeric dirs) must
    // not turn one rescan into millions of stats. Real libraries hold
    // dozens of entries; the rest is skipped, never fatal.
    const MAX_SCAN_ENTRIES: usize = 4096;
    let mut manifests = 0;
    let mut seen = 0;
    for entry in entries.flatten() {
      seen += 1;
      if seen > MAX_SCAN_ENTRIES {
        tracing::debug!(
          "[Process Scanner] Steam library {} over entry cap, skipping tail",
          library.display()
        );
        break;
      }
      let name = entry.file_name();
      let name = name.to_string_lossy();
      // Proton prefix without a manifest (deleted/never-written acf):
      // `steamapps/compatdata/<id>/` still names its owner.
      if name.chars().all(|c| c.is_ascii_digit()) && entry.path().join("pfx").is_dir() {
        let prefix = canonical_steam_path(&format!(
          "{}/steamapps/compatdata/{}/",
          library.to_string_lossy(),
          name
        ));
        dirs.entry(prefix).or_insert(name.to_string());
        continue;
      }
      if !name.starts_with("appmanifest_") || !name.ends_with(".acf") {
        continue;
      }
      if let Ok(body) = read_limited(&entry.path(), MAX_MANIFEST_BYTES)
        && let Some((appid, installdir)) = manifest_ids(&parse_vdf_str(&body))
      {
        dirs
          .entry(common_prefix(&library.to_string_lossy(), &installdir))
          .or_insert(appid);
        manifests += 1;
      }
    }
    tracing::debug!(
      "[Process Scanner] Steam library {}: {} manifests",
      library.display(),
      manifests
    );
  }
}

/// Whether `prefix` (a cached install-dir key) belongs to `library`.
/// Keys are built canonical (`canonical_steam_path` + `/steamapps/...`),
/// so a string-prefix test on the canonical library path is exact.
pub(crate) fn library_owns_prefix(library: &Path, prefix: &str) -> bool {
  prefix.starts_with(&library_prefix(library))
}

/// Normalized owning-prefix form of one library (`<lib-lower>/steamapps/`),
/// hoisted so hot loops build it once per library instead of per prefix.
pub(crate) fn library_prefix(library: &Path) -> String {
  let mut lib = canonical_steam_path(&library.to_string_lossy());
  if !lib.ends_with('/') {
    lib.push('/');
  }
  lib.push_str("steamapps/");
  lib
}

/// One root's libraries: itself plus every path its `libraryfolders.vdf`
/// lists (secondary disks, custom mounts). Missing file means the root
/// alone is the only library.
fn root_libraries(root: &Path) -> (Vec<PathBuf>, Option<PathBuf>) {
  let steamapps = root.join("steamapps");
  if !steamapps.is_dir() {
    return (Vec::new(), None);
  }
  let folders_file = steamapps.join("libraryfolders.vdf");
  let mut libraries = vec![root.to_path_buf()];
  if let Ok(body) = read_limited(&folders_file, MAX_FOLDERS_BYTES) {
    libraries.extend(
      library_paths(&parse_vdf_str(&body))
        .iter()
        .map(PathBuf::from),
    );
  } else {
    tracing::debug!(
      "[Process Scanner] No libraryfolders.vdf under {}",
      root.display()
    );
  }
  libraries.sort();
  libraries.dedup();
  (libraries, Some(folders_file))
}

/// Explicit root override (`RSRPC_STEAM_ROOT` pointing at an existing
/// directory): exclusive — nothing else is consulted.
fn custom_root() -> Option<PathBuf> {
  let custom = PathBuf::from(std::env::var(STEAM_ROOT_ENV).ok()?);
  custom.is_dir().then_some(custom)
}

/// Split `$RSRPC_STEAM_LIBRARIES` into roots with platform-native
/// separators (`:` Unix, `;` Windows — so `C:\` drive letters survive),
/// trimming surrounding whitespace and dropping empties.
fn split_library_list(var: &std::ffi::OsStr) -> Vec<PathBuf> {
  std::env::split_paths(var)
    .map(|path| PathBuf::from(path.to_string_lossy().trim().to_owned()))
    .filter(|path| !path.as_os_str().is_empty())
    .collect()
}

/// Every candidate root, duplicates removed: explicit override (exclusive),
/// user list, running processes, PATH, mounted partitions, home fallbacks.
fn collect_roots() -> Vec<PathBuf> {
  if let Some(custom) = custom_root() {
    return vec![custom];
  }
  let mut candidates = Vec::new();
  if let Some(extra) = std::env::var_os(STEAM_LIBRARIES_ENV) {
    candidates.extend(split_library_list(&extra));
  }
  candidates.extend(running_steam_roots());
  candidates.extend(path_steam_roots());
  candidates.extend(mount_library_roots());
  // Conventional locations always join: a few cheap stats, and a found
  // secondary library must never shadow the primary root (real-world
  // case: a mounted SteamLibrary hid ~/.local/share/Steam).
  candidates.extend(fallback_steam_roots());
  let mut roots = Vec::new();
  for root in candidates {
    push_valid_root(&mut roots, root);
  }
  if roots.is_empty() {
    // Nothing discovered anywhere: conventional locations or nothing.
    for root in fallback_steam_roots() {
      push_valid_root(&mut roots, root);
    }
  }
  roots
}

/// Keep `root` when it actually holds a Steam layout, deduplicated.
/// Symlinks are resolved first (`~/.steam/steam` classically points at
/// `~/.local/share/Steam`): without this the same library is scanned
/// once per alias on every change.
fn push_valid_root(roots: &mut Vec<PathBuf>, root: PathBuf) {
  let canonical = std::fs::canonicalize(&root).unwrap_or(root);
  if canonical.join("steamapps").is_dir() && !roots.contains(&canonical) {
    roots.push(canonical);
  }
}

/// Walk `start` upward (plus itself, up to 5 levels) for the ancestor
/// holding a `steamapps` directory. Turns any known-inside-Steam path
/// (client exe, resolved symlink) into the root, wherever it is mounted.
fn walk_up_for_steamapps(start: &Path) -> Option<PathBuf> {
  let mut current = Some(start);
  for _ in 0..6 {
    let dir = current?;
    if dir.join("steamapps").is_dir() {
      return Some(dir.to_path_buf());
    }
    current = dir.parent();
  }
  None
}

/// Roots from a running `steam`/`steamcmd` process: read `/proc/<pid>/exe`,
/// walk upward. Linux-only; empty elsewhere.
#[cfg(target_os = "linux")]
fn running_steam_roots() -> Vec<PathBuf> {
  let mut roots = Vec::new();
  let Ok(proc_dir) = std::fs::read_dir("/proc") else {
    return roots;
  };
  for entry in proc_dir.flatten() {
    if !entry
      .file_name()
      .to_string_lossy()
      .chars()
      .all(|c| c.is_ascii_digit())
    {
      continue;
    }
    let Ok(exe) = std::fs::read_link(entry.path().join("exe")) else {
      continue;
    };
    let name = exe.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    if name != "steam" && name != "steamcmd" {
      continue;
    }
    if let Some(parent) = exe.parent()
      && let Some(root) = walk_up_for_steamapps(parent)
      && !roots.contains(&root)
    {
      tracing::debug!(
        "[Process Scanner] Steam root from pid {}: {}",
        entry.file_name().to_string_lossy(),
        root.display()
      );
      roots.push(root);
    }
  }
  roots
}

#[cfg(not(target_os = "linux"))]
fn running_steam_roots() -> Vec<PathBuf> {
  Vec::new()
}

/// Roots from `steam`/`steamcmd` on `PATH`, symlinks resolved
/// (`/usr/bin/steam` classically links into the real install).
fn path_steam_roots() -> Vec<PathBuf> {
  let mut roots = Vec::new();
  let paths: Vec<PathBuf> = std::env::var_os("PATH")
    .map(|paths| std::env::split_paths(&paths).collect())
    .unwrap_or_default();
  for dir in paths {
    for binary in ["steam", "steamcmd"] {
      let candidate = dir.join(binary);
      if !candidate.is_file() {
        continue;
      }
      let resolved = std::fs::canonicalize(&candidate).unwrap_or(candidate);
      if let Some(parent) = resolved.parent()
        && let Some(root) = walk_up_for_steamapps(parent)
        && !roots.contains(&root)
      {
        roots.push(root);
      }
    }
  }
  roots
}

/// Conventional home locations: a few cheap stats, always consulted.
/// Kept because Steam's own `~/.steam/steam` symlink contract is stable —
/// a discovered secondary library must never shadow the primary root.
fn fallback_steam_roots() -> Vec<PathBuf> {
  let mut roots = Vec::new();
  if let Ok(home) = std::env::var("HOME") {
    for candidate in [
      format!("{home}/.steam/steam"),
      format!("{home}/.local/share/Steam"),
      format!("{home}/.steam/root"),
      format!("{home}/.var/app/com.valvesoftware.Steam/data/Steam"),
    ] {
      let path = PathBuf::from(candidate);
      if path.join("steamapps").is_dir() && !roots.contains(&path) {
        roots.push(path);
      }
    }
  }
  roots
}

/// Persist the library cache best-effort; failures only cost a rescan.
impl SteamLibraries {
  /// Point refresh persistence at a test-local file (hermetic tests).
  /// Production leaves this unset and uses the platform cache dir.
  pub fn set_cache_file(&mut self, path: PathBuf) {
    self.cache_override = Some(path);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Canonical form folds separators and case: Windows library roots
  /// and native process paths meet the cached `/`-separated keys.
  #[test]
  fn canonical_path_folds_separators_and_case() {
    assert_eq!(
      canonical_steam_path("C:\\Steam\\library"),
      "/c:/steam/library"
    );
    assert_eq!(
      canonical_steam_path("c:/Steam/Library/"),
      "/c:/steam/library/"
    );
    assert_eq!(
      canonical_steam_path("/home/u/.local/share/Steam"),
      "/home/u/.local/share/steam"
    );
    assert_eq!(canonical_steam_path("relative\\dir"), "/relative/dir");
  }

  /// Platform list splitting: native separators, surrounding spaces
  /// trimmed, empty segments dropped.
  #[test]
  fn library_list_splits_and_cleans() {
    use std::ffi::OsStr;
    if cfg!(target_os = "windows") {
      assert_eq!(
        split_library_list(OsStr::new("C:\\A; D:\\B;;")),
        vec![PathBuf::from("C:\\A"), PathBuf::from("D:\\B")]
      );
    } else {
      assert_eq!(
        split_library_list(OsStr::new("/a: /b::")),
        vec![PathBuf::from("/a"), PathBuf::from("/b")]
      );
      // Backslashes and semicolons are ordinary characters on Unix:
      // only `:` separates there.
      assert_eq!(
        split_library_list(OsStr::new("C\\A;D\\B")),
        vec![PathBuf::from("C\\A;D\\B")]
      );
    }
  }
}
