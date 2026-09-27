use std::path::PathBuf;

#[cfg(target_os = "linux")]
use crate::vdf::{MAX_FOLDERS_BYTES, read_limited};
#[cfg(target_os = "linux")]
use std::path::Path;

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
pub(crate) fn mount_library_roots() -> Vec<PathBuf> {
  // Kernel-generated and small in practice; still capped like the rest.
  read_limited(Path::new("/proc/mounts"), MAX_FOLDERS_BYTES)
    .map(|mounts| mount_library_roots_for(&mounts))
    .unwrap_or_default()
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn mount_library_roots() -> Vec<PathBuf> {
  Vec::new()
}
