//! Steam provider tests: VDF parsing, mount decoding, fixture libraries.
//!
//! Hermetic by construction: every filesystem test builds a fake Steam root
//! under a unique temp dir (removed on drop). The process-global env tests
//! (cache roundtrip) stay in `lib` with the env-serialization harness.

use std::path::PathBuf;

use rsrpc_steam::{SteamLibraries, Vdf, library_paths, manifest_ids, parse_vdf_str};

/// Unique scratch dir, removed on drop, panic or not.
struct Scratch {
  path: PathBuf,
}

impl Scratch {
  /// Fresh unique temp dir for one test (pre-cleaned).
  fn new(tag: &str) -> Self {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
      "rsrpc-steam-test-{}-{}-{}",
      std::process::id(),
      tag,
      COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("scratch dir");
    Self { path }
  }

  /// Path inside the scratch root.
  fn join(&self, name: &str) -> PathBuf {
    self.path.join(name)
  }
}

impl Drop for Scratch {
  /// Remove the scratch dir (best-effort, panic-safe).
  fn drop(&mut self) {
    let _ = std::fs::remove_dir_all(&self.path);
  }
}

/// Hermetic fake Steam root with `libraryfolders.vdf` + manifests.
/// Fake Steam root with libraryfolders.vdf plus the given manifests.
fn fake_steam_root(tag: &str, manifests: &[(&str, &str)]) -> Scratch {
  let root = Scratch::new(tag);
  let apps = root.join("steamapps");
  std::fs::create_dir_all(&apps).unwrap();
  let folders = format!(
    "\"libraryfolders\"\n{{\n\"0\"\n{{\n\"path\"\t\t\"{}\"\n}}\n}}",
    root.path.to_string_lossy().replace('\\', "\\\\")
  );
  std::fs::write(apps.join("libraryfolders.vdf"), folders).unwrap();
  for (appid, dir) in manifests {
    let manifest =
      format!("\"AppState\"\n{{\n\"appid\"\t\t\"{appid}\"\n\"installdir\"\t\t\"{dir}\"\n}}");
    std::fs::write(apps.join(format!("appmanifest_{appid}.acf")), manifest).unwrap();
  }
  root
}

/// Deep nesting, truncation and stray braces degrade without corrupting.
#[test]
fn vdf_rejects_nesting_attacks_and_truncation() {
  // 10k-deep nesting: iterative parser survives, depth cap stops it.
  // The shallow prefix still parses (fail-open for data, fail-closed
  // for the stack).
  let mut hostile = String::from("\"root\"\n{\n\"ok\" \"yes\"\n");
  for _ in 0..10_000 {
    hostile.push_str("\"a\"\n{\n");
  }
  let doc = parse_vdf_str(&hostile);
  assert!(doc.contains_key("root"));

  // Untterminated quote: partial token dropped, prior data kept.
  let doc = parse_vdf_str("\"a\"\n{\n\"k\" \"v\"\n\"open");
  let inner = doc.get("a").and_then(|v| match v {
    Vdf::Map(map) => Some(map),
    _ => None,
  });
  // `k/v` parsed before the break; the unterminated tail is gone.
  assert!(inner.is_some_and(|map| map.get("k").is_some()));

  // Stray closing brace ends the parse instead of corrupting it.
  let doc = parse_vdf_str("\"a\" \"1\"\n}\n\"b\" \"2\"");
  assert!(!doc.contains_key("b"));
}

/// New + legacy libraryfolders shapes and manifest id/dir extraction.
#[test]
fn vdf_parses_libraryfolders_and_manifest() {
  // New format: paths nested under "path".
  let doc = parse_vdf_str(
    r#""libraryfolders"
{
  "0"
  {
    "path"  "/home/u/.local/share/Steam"
    "label"  ""
    "apps"
    {
      "3513350"  "89181523617"
    }
  }
  "1"
  {
    "path"  "/mnt/games/Steam"
  }
}"#,
  );
  let mut paths = library_paths(&doc);
  paths.sort();
  assert_eq!(
    paths,
    vec!["/home/u/.local/share/Steam", "/mnt/games/Steam"]
  );

  // Legacy format: path directly as the value.
  let doc = parse_vdf_str("\"libraryfolders\"\n{\n\"0\"\t\t\"/old/steam\"\n}");
  assert_eq!(library_paths(&doc), vec!["/old/steam"]);

  // Manifest excerpt (real NTE shape): appid + installdir.
  let doc = parse_vdf_str(
    "\"AppState\"\n{\n\"appid\"\t\t\"4508340\"\n\"Universe\"\t\t\"1\"\n\"name\"\t\t\"NTE: Neverness to Everness\"\n\"installdir\"\t\t\"Neverness to Everness\"\n}",
  );
  assert_eq!(
    manifest_ids(&doc),
    Some(("4508340".to_string(), "Neverness to Everness".to_string()))
  );
  // Missing halves degrade to None, never panic.
  assert!(manifest_ids(&parse_vdf_str("\"AppState\"\n{\n\"appid\"\t\t\"1\"\n}")).is_none());
  assert!(manifest_ids(&parse_vdf_str("")).is_none());
}

/// Install-prefix matching is case-insensitive; outside matches nothing.
#[test]
fn from_root_matches_install_prefix() {
  let root = fake_steam_root("prefix", &[("12345", "Vdf Game")]);
  let libraries = SteamLibraries::from_root(&root.path);
  // Case-insensitive prefix hit (query arrives lowercased from scanner).
  let prefix = format!(
    "{}/steamapps/common/vdf game/",
    root.path.to_string_lossy().to_lowercase()
  );
  assert_eq!(
    libraries.match_prefix(&format!("{prefix}game.exe")),
    Some("12345")
  );
  // Outside every install dir: no match.
  assert!(libraries.match_prefix("/usr/bin/fish").is_none());
}

/// Vanished roots drop out on refresh instead of serving stale dirs.
#[test]
fn refresh_drops_libraries_of_vanished_roots() {
  let root = fake_steam_root("vanish", &[("12345", "Vdf Game")]);
  let query = format!(
    "{}/steamapps/common/vdf game/game.exe",
    root.path.to_string_lossy().to_lowercase()
  );
  let mut libraries = SteamLibraries::from_root(&root.path);
  assert_eq!(libraries.match_prefix(&query), Some("12345"));

  // The whole disk goes away: re-resolution must drop the install dir
  // instead of serving it forever (mount churn).
  let path = root.path.clone();
  drop(root);
  let _ = std::fs::remove_dir_all(&path);
  libraries.refresh_if_stale();
  assert_eq!(libraries.match_prefix(&query), None);
}

/// Off-root libraries surface via the mount table; pseudo-fs never do.
#[test]
fn mount_roots_detect_partition_layouts() {
  // A second disk carrying a library outside every Steam root: the mount
  // table alone must surface it (pseudo filesystems never do).
  let disk = Scratch::new("disk");
  let lib = disk.join("SteamLibrary");
  std::fs::create_dir_all(lib.join("steamapps")).unwrap();
  let mounts = format!(
    "proc /proc proc rw 0 0\n\
     sysfs /sys sysfs rw 0 0\n\
     /dev/sda1 / ext4 rw 0 0\n\
     /dev/sdb1 /mnt/data ext4 rw 0 0\n\
     /dev/sdc1 {} ext4 rw 0 0\n",
    disk.path.to_string_lossy()
  );
  let roots = rsrpc_steam::mount_library_roots_for(&mounts);
  assert!(roots.contains(&lib), "partition library missing: {roots:?}");
  // Pseudo mounts contribute nothing.
  assert!(
    !roots
      .iter()
      .any(|r| r.starts_with("/proc") || r.starts_with("/sys"))
  );
}

/// Octal escapes decode (incl. multi-byte UTF-8); bad ones pass through.
#[test]
fn mount_escapes_decode_octal() {
  use rsrpc_steam::unescape_mount;

  assert_eq!(unescape_mount("/mnt/data"), "/mnt/data");
  assert_eq!(unescape_mount("/mnt/my\\040disk"), "/mnt/my disk");
  assert_eq!(unescape_mount("/mnt/a\\012b"), "/mnt/a\nb");
  assert_eq!(unescape_mount("/mnt/back\\134slash"), "/mnt/back\\slash");
  // Encoded backslash followed by digits is literal, not a space.
  assert_eq!(unescape_mount("/mnt/x\\134040"), "/mnt/x\\040");
  // Truncated/invalid escapes pass through untouched.
  assert_eq!(unescape_mount("/mnt/tail\\"), "/mnt/tail\\");
  assert_eq!(unescape_mount("/mnt/x\\4y"), "/mnt/x\\4y");
  // Multi-byte UTF-8 arrives as consecutive octal escapes: decode the
  // bytes first, then the string (é, not Ã©).
  assert_eq!(unescape_mount("/mnt/caf\\303\\251"), "/mnt/café");
}
