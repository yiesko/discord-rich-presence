//! Steam library roots: install-dir to AppId mapping (VDF provider).
//!
//! Refreshed once per scan tick when a `libraryfolders.vdf` changed and
//! consulted on path misses, so Steam games resolve without repeated
//! filesystem walks.

use rsrpc_steam::SteamLibraries;

use super::server::ProcessServer;

impl ProcessServer {
  /// Replace the Steam libraries map (hermetic construction seam for
  /// tests and tooling; production discovers at construction and refreshes
  /// per tick).
  pub fn set_steam_libraries(&self, libraries: SteamLibraries) {
    *self
      .steam_libraries
      .write()
      .unwrap_or_else(|e| e.into_inner()) = libraries;
  }

  /// AppId whose Steam install dir prefixes `normalized_path` (already
  /// lowercased `/`-separated). Cloned out of the lock; tiny strings.
  pub fn steam_prefix_app_id(&self, normalized_path: &str) -> Option<String> {
    self
      .steam_libraries
      .read()
      .unwrap_or_else(|e| e.into_inner())
      .match_prefix(normalized_path)
      .map(str::to_string)
  }

  /// Revalidate the Steam libraries (stats only unless something changed).
  /// Called once per scan tick; the scan loop goes through here so tests
  /// can drive the same path.
  pub fn refresh_steam_libraries(&self) {
    self
      .steam_libraries
      .write()
      .unwrap_or_else(|e| e.into_inner())
      .refresh_if_stale();
  }
}
