//! Unix socket placement: official dir order plus fan-out symlinks.
//!
//! Games probe every dir × index `0..=9`, so the bound socket is symlinked
//! into each candidate dir. All helpers are best-effort and never touch
//! foreign files.

use std::path::{Path, PathBuf};

/// Candidate IPC directories in official resolution order
/// (`XDG_RUNTIME_DIR` → `TMPDIR` → `TMP` → `TEMP` → `/tmp`).
pub fn socket_dir_candidates() -> Vec<PathBuf> {
  candidate_dirs_from([
    ("XDG_RUNTIME_DIR", std::env::var("XDG_RUNTIME_DIR").ok()),
    ("TMPDIR", std::env::var("TMPDIR").ok()),
    ("TMP", std::env::var("TMP").ok()),
    ("TEMP", std::env::var("TEMP").ok()),
  ])
}

/// Pure core of [`socket_dir_candidates`]: first-seen order, blanks and
/// duplicates dropped, `/tmp` always last. Separated for unit tests
/// (environment mutation is process-global).
#[must_use]
pub fn candidate_dirs_from(vars: [(&str, Option<String>); 4]) -> Vec<PathBuf> {
  let mut dirs = Vec::new();
  for (_, value) in vars {
    if let Some(dir) = value {
      let dir = dir.trim_end_matches('/');
      if !dir.is_empty() {
        let path = PathBuf::from(dir);
        if !dirs.contains(&path) {
          dirs.push(path);
        }
      }
    }
  }
  let fallback = PathBuf::from("/tmp");
  if !dirs.contains(&fallback) {
    dirs.push(fallback);
  }
  dirs
}

/// Symlink the bound `discord-ipc-{index}` socket into every other
/// candidate dir. Stale ours-shaped symlinks (`discord-ipc-*`) are
/// replaced; anything else (live sockets, foreign files) is left alone;
/// missing/unwritable dirs are skipped.
pub fn fanout_socket_link(dirs: &[PathBuf], bound_path: &str, file_name: &str) {
  let bound = Path::new(bound_path);
  for dir in dirs {
    let link = dir.join(file_name);
    if link == bound {
      continue;
    }
    match std::fs::symlink_metadata(&link) {
      Err(_) => {
        // Absent: create unless the dir itself is missing/unwritable.
        if let Err(err) = std::os::unix::fs::symlink(bound, &link) {
          tracing::debug!("[ipc] Skipping socket link {}: {err}", link.display());
        }
      }
      Ok(meta) => {
        if !meta.file_type().is_symlink() {
          continue; // foreign file/socket: never touch.
        }
        // Ours by shape (or stale): repoint at the live socket.
        let ours = std::fs::read_link(&link)
          .ok()
          .and_then(|target| {
            target
              .file_name()
              .and_then(|name| name.to_str())
              .map(|name| name.starts_with("discord-ipc-"))
          })
          .unwrap_or(false);
        if !ours {
          continue;
        }
        let _ = std::fs::remove_file(&link);
        if let Err(err) = std::os::unix::fs::symlink(bound, &link) {
          tracing::debug!("[ipc] Skipping socket link {}: {err}", link.display());
        }
      }
    }
  }
}

/// Remove our fan-out symlinks for `bound_path`: keeps `/tmp` et al. clean
/// across restarts. Best-effort; foreign files are never touched.
pub fn remove_socket_links(dirs: &[PathBuf], bound_path: &str) {
  let bound = Path::new(bound_path);
  let file_name = match bound.file_name().and_then(|n| n.to_str()) {
    Some(name) => name.to_string(),
    None => return,
  };
  for dir in dirs {
    let link = dir.join(&file_name);
    if link == bound {
      continue;
    }
    let ours = std::fs::read_link(&link).ok().and_then(|target| {
      target
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.starts_with("discord-ipc-"))
    });
    if ours.unwrap_or(false) {
      let _ = std::fs::remove_file(&link);
    }
  }
}

/// `discord-ipc-{n}` file name of a bound socket path (for fan-out links).
#[must_use]
pub fn socket_file_name(bound_path: &str) -> String {
  Path::new(bound_path)
    .file_name()
    .and_then(|name| name.to_str())
    .unwrap_or("discord-ipc-0")
    .to_string()
}
