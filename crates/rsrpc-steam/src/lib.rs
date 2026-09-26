//! Steam library provider: install-dir -> AppId from Steam's own files.
//!
//! Covers games whose per-process store id is unreadable (no environ, no
//! `AppId=` on the command line): Steam knows exactly what it installed
//! where, so a process running under `<library>/steamapps/common/<dir>/`
//! inherits that library entry's AppId. Read-only by design: we never
//! write Steam's files, only parse them.
//!
//! Sources: `<root>/steamapps/libraryfolders.vdf` (library list, new
//! `"path"` format and legacy numeric format) plus each library's
//! `steamapps/appmanifest_<id>.acf` (`appid` + `installdir`).
//!
//! Roots are discovered dynamically, most reliable first:
//! `$RSRPC_STEAM_ROOT` (exclusive override), `$RSRPC_STEAM_LIBRARIES`
//! (user-defined, additive), a running `steam`/`steamcmd` process
//! (`/proc` exe walk-up), `PATH` lookup, mounted partitions probed for
//! library layouts (`/proc/mounts`), plus conventional home locations
//! (cheap stats — a found secondary library never shadows the primary
//! root). Only ONE valid root already gives full coverage: its
//! `libraryfolders.vdf` lists every other library, wherever mounted.
//!
//! Light by design: a JSON cache (`$XDG_CACHE_HOME/rsrpc/steam-libraries.json`)
//! stores each library's prefix map with a fingerprint (steamapps dir
//! mtime + manifest count + newest manifest mtime). Restarts and scan
//! ticks revalidate with stats only and reparse solely what changed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/* logging via tracing (see below) */

/// Override for non-standard installs (and hermetic tests): when set to an
/// existing directory, only it is used as the Steam root.
const STEAM_ROOT_ENV: &str = "RSRPC_STEAM_ROOT";
/// Extra library roots defined by the user, `:`-separated, merged with
/// everything discovered automatically.
const STEAM_LIBRARIES_ENV: &str = "RSRPC_STEAM_LIBRARIES";
/// Cache filename under `$XDG_CACHE_HOME` (else `~/.cache`).
const CACHE_FILE: &str = "rsrpc/steam-libraries.json";

#[derive(Clone, Debug)]
pub enum Vdf {
  Str(String),
  Map(HashMap<String, Vdf>),
}

/// Tokenize Valve VDF: quoted strings plus braces. Everything else
/// (whitespace, stray bytes) is skipped — manifests are machine-written,
/// no need for error recovery beyond "unparseable".
///
/// Note: quoted braces (`"{"`) are indistinguishable from structural
/// ones downstream — a manifest *value* containing a bare brace would
/// misparse its entry into a skipped map (fail-closed: the library is
/// dropped, never mis-attributed). Real manifests never contain braces
/// in values, so a sentinel token type is not worth the churn.
fn tokenize(input: &str) -> Vec<String> {
  /// Longest single value kept (real paths/manifest fields are bytes):
  /// longer quoted runs are consumed but dropped, so a many-MB quoted
  /// blob cannot balloon one `String`.
  const MAX_TOKEN_LEN: usize = 64 * 1024;
  let mut tokens = Vec::new();
  let mut chars = input.chars();
  while let Some(char) = chars.next() {
    // Token-count cap enforced mid-stream (see `MAX_VDF_TOKENS`): stop
    // early instead of building millions of tokens just to discard them.
    if tokens.len() >= MAX_VDF_TOKENS {
      break;
    }
    match char {
      '"' => {
        let mut string = String::new();
        let mut closed = false;
        let mut overlong = false;
        loop {
          match chars.next() {
            None => break,
            Some('"') => {
              closed = true;
              break;
            }
            // VDF escapes (`\"`, `\\`): keep the escaped char literally.
            Some('\\') => {
              if let Some(escaped) = chars.next()
                && !overlong
              {
                string.push(escaped);
              }
            }
            Some(char) => {
              if overlong {
                continue;
              }
              if string.len() >= MAX_TOKEN_LEN {
                overlong = true;
                continue;
              }
              string.push(char);
            }
          }
        }
        // Unterminated quote (truncated/corrupt file): drop the partial
        // token instead of merging the rest of the file into it.
        // Overlong tokens are pushed truncated (not dropped): dropping
        // would shift key/value pairing and risk mis-attribution, while
        // a truncated path/id simply fails closed downstream (`is_dir`
        // misses, AppId lookups miss).
        if closed {
          tokens.push(string);
        }
      }
      '{' => tokens.push("{".to_string()),
      '}' => tokens.push("}".to_string()),
      _ => {}
    }
  }
  tokens
}

/// Parse a token stream into nested maps: `"key" "value"` or
/// `"key" { ... }`. Returns the top-level map; trailing garbage after a
/// complete document is ignored.
///
/// Iterative (explicit stack, depth-capped): the recursive version
/// overflowed the stack on crafted nesting (`"a"{"a"...`), killing the
/// scanning thread from a plain data file.
fn parse_vdf(tokens: &[String]) -> HashMap<String, Vdf> {
  /// Maximum nesting depth (real manifests nest ~3 deep).
  const MAX_DEPTH: usize = 64;
  let mut current = HashMap::new();
  let mut stack: Vec<(HashMap<String, Vdf>, String)> = Vec::new();
  let mut pending: Option<String> = None;
  for token in tokens {
    if token == "}" {
      // Close current frame into its parent; stray `}` ends the parse.
      let Some((mut parent, key)) = stack.pop() else {
        break;
      };
      parent.insert(key, Vdf::Map(current));
      current = parent;
      continue;
    }
    if token == "{" {
      // A `{` needs a pending key; otherwise malformed, skip it.
      let Some(key) = pending.take() else {
        continue;
      };
      if stack.len() >= MAX_DEPTH {
        break; // nesting attack: stop, keep what parsed.
      }
      stack.push((std::mem::take(&mut current), key));
      continue;
    }
    if let Some(key) = pending.take() {
      current.insert(key, Vdf::Str(token.clone()));
    } else {
      pending = Some(token.clone());
    }
  }
  // Unclosed frames (truncated tail): fold back up so the parsed prefix
  // still yields data instead of nothing. A dangling final key without a
  // value is dropped, like before.
  while let Some((mut parent, key)) = stack.pop() {
    parent.insert(key, Vdf::Map(current));
    current = parent;
  }
  current
}

/// Parse a whole VDF document into nested maps.
#[must_use]
pub fn parse_vdf_str(input: &str) -> HashMap<String, Vdf> {
  let tokens = tokenize(input);
  // Token-count cap: a 4MB file of quote pairs could otherwise build a
  // million-entry map. Corrupt/oversized input parses to nothing (the
  // library is skipped, never the daemon).
  if tokens.len() > MAX_VDF_TOKENS {
    return HashMap::new();
  }
  parse_vdf(&tokens)
}

/// Bounds for Steam's own files (see [`read_limited`]): real manifests
/// are tens of KB; anything bigger is corrupt or hostile.
const MAX_FOLDERS_BYTES: u64 = 4 * 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_VDF_TOKENS: usize = 200_000;

/// Read a small text file with a byte cap. Over-cap or non-UTF8 content
/// is an error the callers already treat as "skip this file". Both the
/// pre-read stat AND the post-read length are checked: a concurrent
/// grow/replace (Steam rewriting manifests, symlink swap) between the
/// two must not bypass the cap.
fn read_limited(path: &Path, limit: u64) -> Result<String, std::io::Error> {
  fn too_large() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::FileTooLarge, "VDF file over size cap")
  }
  let meta = std::fs::metadata(path)?;
  if meta.len() > limit {
    return Err(too_large());
  }
  let bytes = std::fs::read(path)?;
  if bytes.len() as u64 > limit {
    return Err(too_large());
  }
  String::from_utf8(bytes)
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "VDF file not UTF-8"))
}

/// Library paths from a parsed `libraryfolders.vdf`: new format nests them
/// under `"path"`, legacy format stores the path directly as the value.
#[must_use]
pub fn library_paths(doc: &HashMap<String, Vdf>) -> Vec<String> {
  let folders = doc
    .get("libraryfolders")
    .or_else(|| doc.get("LibraryFolders"));
  let inner = match folders {
    Some(Vdf::Map(map)) => map,
    _ => return Vec::new(),
  };
  let mut paths = Vec::new();
  for value in inner.values() {
    match value {
      Vdf::Str(path) => paths.push(path.clone()),
      Vdf::Map(map) => {
        if let Some(Vdf::Str(path)) = map.get("path") {
          paths.push(path.clone());
        }
      }
    }
  }
  paths
}

/// `(appid, installdir)` from a parsed `appmanifest_<id>.acf`.
pub fn manifest_ids(doc: &HashMap<String, Vdf>) -> Option<(String, String)> {
  let state = match doc.get("AppState") {
    Some(Vdf::Map(map)) => map,
    _ => return None,
  };
  let appid = match state.get("appid") {
    Some(Vdf::Str(id)) if !id.trim().is_empty() => id.trim().to_string(),
    _ => return None,
  };
  let installdir = match state.get("installdir") {
    Some(Vdf::Str(dir)) if !dir.trim().is_empty() => dir.trim().to_string(),
    _ => return None,
  };
  Some((appid, installdir))
}

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
struct Fingerprint {
  dir_mtime_ms: u64,
  manifests: usize,
  newest_manifest_ms: u64,
  compat_mtime_ms: u64,
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
  dirs: HashMap<String, String>,
  /// Library path (string form) -> fingerprint at last scan.
  fingerprints: HashMap<String, Fingerprint>,
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
  cache_override: Option<PathBuf>,
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
/// Keys are built as `<library-lower>/steamapps/...`, so a string-prefix
/// test on the lowercased library path is exact.
fn library_owns_prefix(library: &Path, prefix: &str) -> bool {
  prefix.starts_with(&library_prefix(library))
}

/// Normalized owning-prefix form of one library (`<lib-lower>/steamapps/`),
/// hoisted so hot loops build it once per library instead of per prefix.
fn library_prefix(library: &Path) -> String {
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

/// Every candidate root, duplicates removed: explicit override (exclusive),
/// user list, running processes, PATH, mounted partitions, home fallbacks.
fn collect_roots() -> Vec<PathBuf> {
  if let Some(custom) = custom_root() {
    return vec![custom];
  }
  let mut candidates = Vec::new();
  if let Ok(extra) = std::env::var(STEAM_LIBRARIES_ENV) {
    candidates.extend(
      extra
        .split(':')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(PathBuf::from),
    );
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

/// Decode `/proc/mounts` octal escapes (`\040` space, `\012` newline,
/// `\011` tab, `\134` backslash) in one pass: chained `replace` calls
/// would corrupt an encoded backslash followed by digits (`\134040` is
/// a literal `\040`, not a space).
pub fn unescape_mount(field: &str) -> String {
  // Decode into bytes first, then UTF-8 once: multi-byte characters
  // arrive as consecutive octal escapes (é = \303\251), and decoding
  // each byte as a char would mangle them (Ã©).
  let mut out: Vec<u8> = Vec::with_capacity(field.len());
  let mut chars = field.chars().peekable();
  while let Some(char) = chars.next() {
    if char != '\\' {
      let mut encoded = [0u8; 4];
      out.extend_from_slice(char.encode_utf8(&mut encoded).as_bytes());
      continue;
    }
    // Collect up to 3 octal digits without consuming the terminator.
    let mut code = String::new();
    while code.len() < 3 {
      match chars.peek() {
        Some('0'..='7') => {
          if let Some(digit) = chars.next() {
            code.push(digit);
          }
        }
        _ => break,
      }
    }
    if code.len() == 3
      && let Ok(byte) = u8::from_str_radix(&code, 8)
    {
      out.push(byte);
      continue;
    }
    // Not an escape (short run, non-octal, or >0xFF): emit literally;
    // the peeked terminator is still queued for normal handling.
    out.push(b'\\');
    out.extend_from_slice(code.as_bytes());
  }
  String::from_utf8_lossy(&out).into_owned()
}

/// Partition-aware probing (Linux): every locally-mounted filesystem is
/// checked for a handful of conventional library layouts
/// (`<mnt>/SteamLibrary`, `<mnt>/Steam`, ...). Stats only, startup and
/// folders-change ticks — never a walk. Catches libraries on disks Steam
/// itself no longer lists (moved drives, copied folders, other users).
/// Pure over a mounts-table string for testability; the live table comes
/// from `/proc/mounts`.
#[must_use]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn mount_library_roots_for(mounts: &str) -> Vec<PathBuf> {
  const SKIP_TYPES: &[&str] = &[
    "proc",
    "sysfs",
    "cgroup",
    "cgroup2",
    "tmpfs",
    "devtmpfs",
    "devpts",
    "overlay",
    "squashfs",
    "nsfs",
    "tracefs",
    "debugfs",
    "securityfs",
    "configfs",
    "fusectl",
    "selinuxfs",
    "overlayfs",
    "fuse.portal",
  ];
  const LAYOUTS: &[&str] = &["", "SteamLibrary", "Steam", "steam", "Games/SteamLibrary"];
  let mut roots = Vec::new();
  for line in mounts.lines() {
    let mut parts = line.split_whitespace();
    let (Some(_device), Some(mount), Some(fstype)) = (parts.next(), parts.next(), parts.next())
    else {
      continue;
    };
    if SKIP_TYPES.contains(&fstype) {
      continue;
    }
    // Pseudo trees even on real fstypes: never libraries there.
    // (Checked pre-unescape: escapes never introduce a leading `/`.)
    if mount.starts_with("/proc/") || mount.starts_with("/sys/") || mount.starts_with("/dev/") {
      continue;
    }
    let mount = unescape_mount(mount);
    for layout in LAYOUTS {
      let candidate = if layout.is_empty() {
        PathBuf::from(&mount)
      } else {
        PathBuf::from(&mount).join(layout)
      };
      if candidate.join("steamapps").is_dir() && !roots.contains(&candidate) {
        tracing::debug!(
          "[Process Scanner] Steam library on mount: {}",
          candidate.display()
        );
        roots.push(candidate);
      }
    }
  }
  roots
}

#[cfg(target_os = "linux")]
fn mount_library_roots() -> Vec<PathBuf> {
  // Kernel-generated and small in practice; still capped like the rest.
  read_limited(Path::new("/proc/mounts"), MAX_FOLDERS_BYTES)
    .map(|mounts| mount_library_roots_for(&mounts))
    .unwrap_or_default()
}

#[cfg(not(target_os = "linux"))]
fn mount_library_roots() -> Vec<PathBuf> {
  Vec::new()
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

/// On-disk cache: per-library prefix maps with fingerprints. Best-effort
/// accelerator — corrupt/missing cache means a full parse, never an error.
#[derive(Clone, Debug)]
struct CachedLibrary {
  fingerprint: Fingerprint,
  dirs: HashMap<String, String>,
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
fn load_cache() -> HashMap<String, CachedLibrary> {
  let mut cached = HashMap::new();
  let Some(path) = cache_path() else {
    return cached;
  };
  let Ok(body) = read_limited(&path, MAX_FOLDERS_BYTES) else {
    return cached;
  };
  let Ok(doc) = serde_json::from_str::<serde_json::Value>(&body) else {
    return cached;
  };
  if doc.get("version").and_then(|v| v.as_u64()) != Some(1) {
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

/// Persist the library cache best-effort; failures only cost a rescan.
impl SteamLibraries {
  /// Point refresh persistence at a test-local file (hermetic tests).
  /// Production leaves this unset and uses the platform cache dir.
  pub fn set_cache_file(&mut self, path: PathBuf) {
    self.cache_override = Some(path);
  }
}

fn save_cache(libraries: &SteamLibraries) {
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
  doc.insert("version".to_string(), serde_json::Value::from(1));
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
}
