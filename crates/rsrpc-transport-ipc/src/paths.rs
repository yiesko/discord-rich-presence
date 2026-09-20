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

/// Whether `link` may be replaced by our live socket: it already points
/// at it, or it is a stale ours-shaped symlink (`discord-ipc-*`) whose
/// target is gone (previous crashed run). A symlink owned by a live
/// foreign service is never ours — stealing it would redirect that
/// service's clients to us.
fn link_reclaimable(link: &Path, bound: &Path) -> bool {
  let target = match std::fs::read_link(link) {
    Ok(target) => target,
    Err(_) => return false, // not a symlink: never touch.
  };
  // Resolve relative targets against the link's parent for the checks.
  let resolved = if target.is_absolute() {
    target.clone()
  } else {
    link
      .parent()
      .map(|parent| parent.join(&target))
      .unwrap_or(target.clone())
  };
  if resolved == bound {
    return true;
  }
  let ours_shaped = target
    .file_name()
    .and_then(|name| name.to_str())
    .is_some_and(|name| name.starts_with("discord-ipc-"));
  ours_shaped && std::fs::symlink_metadata(&resolved).is_err()
}

/// Symlink the bound `discord-ipc-{index}` socket into every other
/// candidate dir. Stale ours-shaped symlinks (`discord-ipc-*`) are
/// replaced; anything else (live sockets, foreign files, live foreign
/// links) is left alone; missing/unwritable dirs are skipped.
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
        if !link_reclaimable(&link, bound) {
          continue; // live foreign link: never steal.
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
/// across restarts. Best-effort; only links pointing at our socket (or
/// stale ours-shaped ones) are removed — foreign files are never touched.
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
    if link_reclaimable(&link, bound) {
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

#[cfg(test)]
mod tests {
  use super::*;

  /// Isolated temp dir per test (tag-suffixed, hermetic).
  fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rsrpc-paths-test-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
  }

  /// Fanout skips links whose socket answers (live foreign owner).
  #[test]
  fn fanout_never_steals_live_foreign_link() {
    let ours = scratch("ours");
    let foreign = scratch("foreign");
    // A live foreign service owns discord-ipc-0 here (real socket file
    // behind a symlink): fan-out must leave it alone, never repoint it
    // at our socket.
    let foreign_socket = foreign.join("discord-ipc-9");
    std::fs::write(&foreign_socket, b"socket").expect("foreign socket");
    let foreign_link = foreign.join("discord-ipc-0");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&foreign_socket, &foreign_link).expect("foreign link");

    let bound = ours.join("discord-ipc-0");
    std::fs::write(&bound, b"socket").expect("bound socket");
    fanout_socket_link(&[ours.clone(), foreign.clone()], "unused", "discord-ipc-0");

    assert_eq!(
      std::fs::read_link(&foreign_link).expect("link still there"),
      foreign_socket,
      "live foreign link must not be repointed"
    );
    let _ = std::fs::remove_dir_all(&ours);
    let _ = std::fs::remove_dir_all(&foreign);
  }

  /// Dangling links to dead sockets are reclaimed for our bind.
  #[test]
  fn fanout_reclaims_stale_dangling_link() {
    let dir = scratch("stale");
    // Our previous run crashed and left a dangling ours-shaped link:
    // reclaim it for the live socket.
    let link = dir.join("discord-ipc-0");
    #[cfg(unix)]
    std::os::unix::fs::symlink(dir.join("discord-ipc-7"), &link).expect("stale link");
    let bound = dir.join("discord-ipc-1");
    std::fs::write(&bound, b"socket").expect("bound socket");

    fanout_socket_link(
      std::slice::from_ref(&dir),
      bound.to_str().expect("utf8"),
      "discord-ipc-0",
    );
    assert_eq!(std::fs::read_link(&link).expect("link still there"), bound);
    let _ = std::fs::remove_dir_all(&dir);
  }
}
