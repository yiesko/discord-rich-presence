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

mod cache;
mod library;
mod mounts;
mod vdf;

pub use library::SteamLibraries;
pub use mounts::{mount_library_roots_for, unescape_mount};
pub use vdf::{Vdf, library_paths, manifest_ids, parse_vdf_str};
