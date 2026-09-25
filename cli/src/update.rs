//! Self-update (OTA) for the `rsrpc-cli` binary.
//!
//! Flow: [`cmd_check`] reports, [`cmd_stage`] downloads + verifies + stages,
//! and [`apply_pending_on_boot`] — called on every startup — atomically
//! swaps a staged binary into place and re-executes, so the new version
//! takes over on the next (re)start. [`cmd_rollback`] restores the kept
//! `.prev` image. [`spawn_watcher`] runs the daily background check inside
//! the daemon.
//!
//! Trust model (MVP): release metadata and binaries come from GitHub
//! Releases over HTTPS; every staged byte is SHA256-verified against the
//! `SHA256SUMS.txt` manifest published by CI before it is ever executed.
//! Automatic applying is opt-in (`--auto-update`); by default the daemon
//! only logs that an update exists.

use std::collections::HashMap;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

/// GitHub API endpoint for the newest stable release.
const LATEST_RELEASE_URL: &str = "https://api.github.com/repos/yiesko/discord-rich-presence/releases/latest";
/// Checksum manifest asset name inside a release.
const CHECKSUMS_ASSET: &str = "SHA256SUMS.txt";
/// Detached minisign signature of [`CHECKSUMS_ASSET`].
const CHECKSUMS_SIG_ASSET: &str = "SHA256SUMS.txt.minisig";
/// Embedded release-signing public key (minisign format, key id
/// 5F3AB8C92376EEFD). The matching secret key lives only in GitHub
/// Secrets (`MINISIGN_SECRET_KEY`) plus the holder's offline backup —
/// never in this repo — so a compromised GitHub account/token alone
/// cannot ship a trusted binary: staged payloads only execute after this
/// key verifies the manifest signature.
const UPDATE_PUBKEY: &str = "RWT97nYjybg6X/Q35LBD/thrjkAmYmEHbRm8TQjvpJeLO2kNONgb4ibw";
/// Staged (downloaded, verified, not yet applied) binary file name.
const STAGED_FILE: &str = "rsrpc-cli.staged";
/// OTA state file name (JSON).
const STATE_FILE: &str = "ota.json";
/// Suffix for the kept previous image (manual rollback source).
const PREV_SUFFIX: &str = ".prev";
/// Set on the re-executed process so a staged apply happens at most once
/// per boot chain (anti-loop guard).
const APPLIED_ENV: &str = "RSRPC_OTA_APPLIED";
/// Env override for the OTA directory (tests, portable setups).
const OTA_DIR_ENV: &str = "RSRPC_OTA_DIR";
/// Cap for release metadata + checksum downloads.
const META_DOWNLOAD_LIMIT: u64 = 1024 * 1024;
/// Cap for binary downloads (release binaries are ~10 MiB).
const BINARY_DOWNLOAD_LIMIT: u64 = 128 * 1024 * 1024;
/// HTTP timeout for update traffic.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
/// Daily background-check cadence inside the daemon.
const WATCH_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// Settle delay before the watcher's first check after boot.
const WATCH_BOOT_DELAY: Duration = Duration::from_secs(5 * 60);

/// Compiled crate version (the "current" side of every comparison).
fn current_version() -> Version {
  // Invariant: Cargo.toml versions are valid semver (enforced by cargo).
  Version::parse(env!("CARGO_PKG_VERSION")).expect("crate version is valid semver")
}

/// Release target triple for this build, when the OTA publishes one.
///
/// MVP covers Linux x86_64 + ARM64 only; anything else reports
/// [`CheckOutcome::UnsupportedTarget`] instead of erroring.
#[must_use]
pub fn target_triple() -> Option<&'static str> {
  match (std::env::consts::ARCH, std::env::consts::OS) {
    ("x86_64", "linux") => Some("x86_64-unknown-linux-gnu"),
    ("aarch64", "linux") => Some("aarch64-unknown-linux-gnu"),
    _ => None,
  }
}

/// Parse a release `tag_name` (`v1.2.3` or `1.2.3`) into semver.
#[must_use]
pub fn parse_version(tag: &str) -> Option<Version> {
  Version::parse(tag.trim().strip_prefix('v').unwrap_or(tag.trim())).ok()
}

/// One downloadable file inside a release.
#[derive(Clone, Debug, PartialEq)]
pub struct Asset {
  /// File name (e.g. `rsrpc-cli-x86_64-unknown-linux-gnu`).
  pub name: String,
  /// Direct download URL.
  pub url: String,
}

/// Parsed `releases/latest` payload (only the fields the updater needs).
#[derive(Clone, Debug, PartialEq)]
pub struct ReleaseInfo {
  /// Release version from `tag_name`.
  pub version: Version,
  /// Raw tag (for display).
  pub tag: String,
  /// Listed assets.
  pub assets: Vec<Asset>,
}

/// Parse the `releases/latest` JSON body. Unknown fields are ignored;
/// a body missing `tag_name`/usable assets yields `None`.
#[must_use]
pub fn parse_release(body: &serde_json::Value) -> Option<ReleaseInfo> {
  let tag = body.get("tag_name")?.as_str()?;
  let version = parse_version(tag)?;
  let assets = body
    .get("assets")?
    .as_array()?
    .iter()
    .filter_map(|asset| {
      Some(Asset {
        name: asset.get("name")?.as_str()?.to_string(),
        url: asset.get("browser_download_url")?.as_str()?.to_string(),
      })
    })
    .collect();
  Some(ReleaseInfo {
    version,
    tag: tag.to_string(),
    assets,
  })
}

/// Pick the asset for `file_name` (exact match, e.g. the target binary or
/// [`CHECKSUMS_ASSET`]).
#[must_use]
pub fn select_asset<'a>(release: &'a ReleaseInfo, file_name: &str) -> Option<&'a Asset> {
  release.assets.iter().find(|asset| asset.name == file_name)
}

/// Parse a `sha256sum`-style manifest (`<hex>[ *]<file>` per line) into
/// `file name -> lowercase hex digest`. Malformed lines are skipped.
#[must_use]
pub fn parse_checksums(text: &str) -> HashMap<String, String> {
  text
    .lines()
    .filter_map(|line| {
      let mut parts = line.split_whitespace();
      let hex = parts.next()?;
      let name = parts.next()?.trim_start_matches('*');
      if hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()) && !name.is_empty() {
        Some((name.to_string(), hex.to_ascii_lowercase()))
      } else {
        None
      }
    })
    .collect()
}

/// SHA256 of `bytes` as lowercase hex.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
  format!("{:x}", Sha256::digest(bytes))
}

/// Verify a detached minisign signature over `payload` with `pubkey_b64`
/// (strict: legacy non-prehashed signatures are rejected).
///
/// # Errors
///
/// Returns the error when the key/signature does not parse or the
/// signature is invalid.
pub fn verify_signature(
  pubkey_b64: &str,
  payload: &[u8],
  sig_text: &str,
) -> Result<(), Box<dyn std::error::Error>> {
  let key = minisign_verify::PublicKey::from_base64(pubkey_b64)
    .map_err(|err| format!("bad update signing key: {err}"))?;
  let signature = minisign_verify::Signature::decode(sig_text)
    .map_err(|err| format!("bad update signature: {err}"))?;
  key
    .verify(payload, &signature, false)
    .map_err(|err| format!("update signature invalid: {err}"))?;
  Ok(())
}

/// Why an update was refused before any network/download happened.
#[derive(Clone, Debug, PartialEq)]
pub enum Refusal {
  /// Running from a cargo build tree (`target/debug|release` in path).
  DevBuild(PathBuf),
  /// Installed via `cargo install` (`~/.cargo/bin`): cargo owns it.
  CargoManaged(PathBuf),
  /// The running file is not named `rsrpc-cli*`: refuse to overwrite
  /// something unrecognized.
  UnexpectedName(String),
  /// The executable's directory is not writable.
  NotWritable(PathBuf),
}

impl fmt::Display for Refusal {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::DevBuild(path) => write!(
        f,
        "dev build ({}): rebuild or `cargo install` instead of self-updating",
        path.display()
      ),
      Self::CargoManaged(path) => write!(
        f,
        "managed by cargo ({}): run `cargo install` to update",
        path.display()
      ),
      Self::UnexpectedName(name) => write!(
        f,
        "unexpected binary name ({name}): refusing to overwrite unrecognized files"
      ),
      Self::NotWritable(dir) => {
        write!(f, "no write permission for {}", dir.display())
      }
    }
  }
}

/// Check the running executable is one this updater may replace.
///
/// Takes the exe path explicitly so tests can pass fixtures; production
/// passes [`std::env::current_exe`].
///
/// # Errors
///
/// Returns the [`Refusal`] when the binary is a dev build, cargo-managed,
/// oddly named, or lives in a read-only directory.
pub fn check_eligibility(exe: &Path) -> Result<(), Refusal> {
  if exe.components().any(|part| {
    part.as_os_str() == "target"
      && exe
        .parent()
        .is_some_and(|parent| parent.ends_with("debug") || parent.ends_with("release"))
  }) || exe
    .ancestors()
    .any(|ancestor| ancestor.ends_with("target/debug") || ancestor.ends_with("target/release"))
  {
    return Err(Refusal::DevBuild(exe.to_path_buf()));
  }
  if exe
    .ancestors()
    .any(|ancestor| ancestor.ends_with(".cargo/bin"))
  {
    return Err(Refusal::CargoManaged(exe.to_path_buf()));
  }
  let stem = exe.file_stem().and_then(|stem| stem.to_str()).unwrap_or("");
  if stem != "rsrpc-cli" {
    return Err(Refusal::UnexpectedName(
      exe
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("?")
        .to_string(),
    ));
  }
  if let Some(dir) = exe.parent()
    && std::fs::metadata(dir).is_ok_and(|meta| meta.permissions().readonly())
  {
    return Err(Refusal::NotWritable(dir.to_path_buf()));
  }
  Ok(())
}

/// OTA working directory (`ota.json` state + staged binary).
#[derive(Clone, Debug)]
pub struct OtaPaths {
  dir: PathBuf,
}

impl OtaPaths {
  /// Resolve from `RSRPC_OTA_DIR` (used verbatim), then `XDG_CACHE_HOME`,
  /// then `~/.cache`, then the temp dir (last resort, still functional).
  #[must_use]
  pub fn from_env() -> Self {
    if let Some(dir) = std::env::var_os(OTA_DIR_ENV)
      .map(PathBuf::from)
      .filter(|path| path.is_absolute())
    {
      return Self::with_dir(dir);
    }
    let dir = std::env::var_os("XDG_CACHE_HOME")
      .map(PathBuf::from)
      .filter(|path| path.is_absolute())
      .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
      .unwrap_or_else(std::env::temp_dir)
      .join("rsrpc")
      .join("ota");
    Self::with_dir(dir)
  }

  /// Explicit directory (tests, portable setups).
  #[must_use]
  pub fn with_dir(dir: PathBuf) -> Self {
    Self { dir }
  }

  fn state_file(&self) -> PathBuf {
    self.dir.join(STATE_FILE)
  }

  fn staged_file(&self) -> PathBuf {
    self.dir.join(STAGED_FILE)
  }

  /// Create the directory (idempotent).
  ///
  /// # Errors
  ///
  /// Returns the I/O error when the directory cannot be created.
  pub fn ensure_dir(&self) -> std::io::Result<()> {
    std::fs::create_dir_all(&self.dir)
  }
}

/// Persisted OTA state: what is staged (if anything) and what the last
/// check saw.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct OtaState {
  #[serde(default)]
  staged_version: Option<String>,
  #[serde(default)]
  staged_sha256: Option<String>,
  #[serde(default)]
  latest_seen: Option<String>,
}

fn load_state(paths: &OtaPaths) -> OtaState {
  std::fs::read_to_string(paths.state_file())
    .ok()
    .and_then(|body| serde_json::from_str(&body).ok())
    .unwrap_or_default()
}

fn save_state(paths: &OtaPaths, state: &OtaState) {
  if paths.ensure_dir().is_err() {
    return;
  }
  if let Ok(body) = serde_json::to_string_pretty(state) {
    let _ = std::fs::write(paths.state_file(), body);
  }
}

/// Outcome of [`check`].
#[derive(Clone, Debug, PartialEq)]
pub enum CheckOutcome {
  /// Running the newest known version.
  UpToDate { current: Version },
  /// A newer release with a usable asset for this target.
  Available {
    current: Version,
    latest: Version,
    tag: String,
    asset_name: String,
    asset_url: String,
    /// `SHA256SUMS.txt` download URL, when the release publishes one.
    /// [`stage`] refuses to proceed without it (unverified binaries
    /// never execute).
    checksums_url: Option<String>,
    /// `SHA256SUMS.txt.minisig` download URL: the manifest signature
    /// verified against [`UPDATE_PUBKEY`] before any hash is trusted.
    /// [`stage`] refuses to proceed without it.
    checksums_sig_url: Option<String>,
  },
  /// Newer releases may exist, but none ships this target triple.
  UnsupportedTarget { current: Version, triple: String },
}

/// Fetch + parse the latest-release metadata (thin ureq wrapper).
fn fetch_release() -> Result<ReleaseInfo, Box<dyn std::error::Error>> {
  let body = rsrpc_core::http_agent(HTTP_TIMEOUT)
    .get(LATEST_RELEASE_URL)
    .header("Accept", "application/vnd.github+json")
    .call()
    .map_err(|err| format!("update check failed: {err}"))?
    .into_body()
    .with_config()
    .limit(META_DOWNLOAD_LIMIT)
    .read_to_string()
    .map_err(|err| format!("update check failed: {err}"))?;
  let json: serde_json::Value = serde_json::from_str(&body)
    .map_err(|err| format!("update check failed: bad release JSON: {err}"))?;
  parse_release(&json).ok_or_else(|| "update check failed: release has no usable version".into())
}

/// Check for a newer release.
///
/// # Errors
///
/// Returns the error when the network/metadata fetch or parse fails.
/// A missing asset for this target is *not* an error (see
/// [`CheckOutcome::UnsupportedTarget`]).
pub fn check() -> Result<CheckOutcome, Box<dyn std::error::Error>> {
  let current = current_version();
  let Some(triple) = target_triple() else {
    return Ok(CheckOutcome::UnsupportedTarget {
      current,
      triple: format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
    });
  };
  let release = fetch_release()?;
  if release.version <= current {
    return Ok(CheckOutcome::UpToDate { current });
  }
  let asset_name = format!("rsrpc-cli-{triple}");
  let asset = select_asset(&release, &asset_name).cloned();
  let checksums_url = select_asset(&release, CHECKSUMS_ASSET).map(|asset| asset.url.clone());
  let checksums_sig_url =
    select_asset(&release, CHECKSUMS_SIG_ASSET).map(|asset| asset.url.clone());
  match asset {
    Some(asset) => Ok(CheckOutcome::Available {
      current,
      latest: release.version,
      tag: release.tag,
      asset_name: asset.name,
      asset_url: asset.url,
      checksums_url,
      checksums_sig_url,
    }),
    None => Ok(CheckOutcome::UnsupportedTarget {
      current,
      triple: format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
    }),
  }
}

fn download(url: &str, limit: u64) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
  rsrpc_core::http_agent(HTTP_TIMEOUT)
    .get(url)
    .header("Accept", "application/octet-stream")
    .call()
    .map_err(|err| -> Box<dyn std::error::Error> { format!("download failed: {err}").into() })?
    .into_body()
    .with_config()
    .limit(limit)
    .read_to_vec()
    .map_err(|err| -> Box<dyn std::error::Error> { format!("download failed: {err}").into() })
}

/// Stream a download straight to `path` while hashing it incrementally:
/// the ~10MB release binary never sits whole in RAM (the old
/// `read_to_vec` peak). Returns the hex SHA256. The configured `limit`
/// is enforced by the reader itself, so a hostile server cannot fill
/// the disk either. A partial file is left for the caller to remove.
fn download_to_file(
  url: &str,
  path: &Path,
  limit: u64,
) -> Result<String, Box<dyn std::error::Error>> {
  use std::io::Read;

  // Bound in two steps: the config (and its reader) borrows the body,
  // so the response must live in a local — not a temporary.
  let response = rsrpc_core::http_agent(HTTP_TIMEOUT)
    .get(url)
    .header("Accept", "application/octet-stream")
    .call()
    .map_err(|err| -> Box<dyn std::error::Error> { format!("download failed: {err}").into() })?;
  let mut body = response.into_body();
  let mut body = body.with_config().limit(limit).reader();
  let file = std::fs::File::create(path).map_err(|err| -> Box<dyn std::error::Error> {
    format!("cannot stage update: {err}").into()
  })?;
  let mut file = std::io::BufWriter::new(file);
  let mut hasher = Sha256::new();
  let mut buf = [0u8; 64 * 1024];
  loop {
    let n = body
      .read(&mut buf)
      .map_err(|err| -> Box<dyn std::error::Error> { format!("download failed: {err}").into() })?;
    if n == 0 {
      break;
    }
    hasher.update(&buf[..n]);
    std::io::Write::write_all(&mut file, &buf[..n]).map_err(
      |err| -> Box<dyn std::error::Error> { format!("cannot stage update: {err}").into() },
    )?;
  }
  std::io::Write::flush(&mut file).map_err(|err| -> Box<dyn std::error::Error> {
    format!("cannot stage update: {err}").into()
  })?;
  Ok(format!("{:x}", hasher.finalize()))
}

/// Download, verify (SHA256 vs the release manifest) and stage an
/// available update. Records it in the OTA state for
/// [`apply_pending_on_boot`].
///
/// # Errors
///
/// Returns the error on refusal, network failure, checksum mismatch, or
/// any filesystem failure (a partial staged file is removed).
pub fn stage(
  available: &CheckOutcome,
  paths: &OtaPaths,
  exe: &Path,
) -> Result<Version, Box<dyn std::error::Error>> {
  let CheckOutcome::Available {
    latest,
    asset_name,
    asset_url,
    checksums_url,
    checksums_sig_url,
    ..
  } = available
  else {
    return Err("nothing to stage: no update available".into());
  };
  check_eligibility(exe).map_err(|refusal| format!("cannot self-update: {refusal}"))?;
  paths
    .ensure_dir()
    .map_err(|err| format!("cannot stage update: {err}"))?;

  let staged = paths.staged_file();
  // Manifest first (tiny, in RAM): signature before anything else, so a
  // bad release fails before the binary even starts downloading.
  let checksums_url = checksums_url
    .as_deref()
    .ok_or_else(|| -> Box<dyn std::error::Error> {
      "release has no SHA256SUMS.txt: refusing unverified binary".into()
    })?;
  let checksums_sig_url =
    checksums_sig_url
      .as_deref()
      .ok_or_else(|| -> Box<dyn std::error::Error> {
        "release has no SHA256SUMS.txt.minisig: refusing unsigned binary".into()
      })?;
  let manifest = download(checksums_url, META_DOWNLOAD_LIMIT)?;
  let manifest_sig = download(checksums_sig_url, META_DOWNLOAD_LIMIT)?;
  // Signature first: no hash from this manifest is trusted until the
  // embedded release key vouches for it.
  let manifest_sig =
    String::from_utf8(manifest_sig).map_err(|err| format!("bad SHA256SUMS.txt.minisig: {err}"))?;
  verify_signature(UPDATE_PUBKEY, &manifest, &manifest_sig)?;
  let manifest = String::from_utf8(manifest).map_err(|err| format!("bad SHA256SUMS.txt: {err}"))?;
  let expected = parse_checksums(&manifest)
    .remove(asset_name.as_str())
    .ok_or_else(|| -> Box<dyn std::error::Error> {
      format!("SHA256SUMS.txt has no entry for {asset_name}: refusing unverified binary").into()
    })?;
  // Stream the binary to a sibling temp file while hashing it: the
  // ~10MB image never sits whole in RAM, and a failed download,
  // checksum mismatch or crash never touches a previously verified
  // staged image (nor its ota.json entry) — the partial file is simply
  // removed. Same directory, so the final rename is atomic.
  let tmp = staged.with_extension("part");
  let actual = download_to_file(asset_url, &tmp, BINARY_DOWNLOAD_LIMIT).inspect_err(|_| {
    let _ = std::fs::remove_file(&tmp);
  })?;
  if actual != expected {
    let _ = std::fs::remove_file(&tmp);
    return Err(format!("checksum mismatch for {asset_name}: refusing unverified binary").into());
  }
  #[cfg(unix)]
  if let Err(err) = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)) {
    let _ = std::fs::remove_file(&tmp);
    return Err(format!("cannot stage update: {err}").into());
  }
  if let Err(err) = std::fs::rename(&tmp, &staged) {
    let _ = std::fs::remove_file(&tmp);
    return Err(format!("cannot stage update: {err}").into());
  }
  save_state(
    paths,
    &OtaState {
      staged_version: Some(latest.to_string()),
      staged_sha256: Some(actual),
      latest_seen: Some(latest.to_string()),
    },
  );
  Ok(latest.clone())
}

/// Previous-image sidecar next to the executable (`rsrpc-cli` →
/// `rsrpc-cli.prev`): plain suffix append, so extensionless and `.exe`
/// binaries alike keep one predictable name from a single place.
fn prev_path(exe: &Path) -> PathBuf {
  let mut prev = exe.as_os_str().to_owned();
  prev.push(PREV_SUFFIX);
  PathBuf::from(prev)
}

/// Swap `new_image` over `exe`, keeping the previous image at
/// [`prev_path`] as the rollback source. Falls back to copy when the OTA
/// dir lives on another filesystem (`EXDEV`).
fn swap_binary(exe: &Path, new_image: &Path) -> std::io::Result<()> {
  let prev = prev_path(exe);
  let _ = std::fs::remove_file(&prev);
  // Renaming the running image aside is allowed on Linux/Windows; only
  // overwriting it in place is not.
  std::fs::rename(exe, &prev)?;
  match std::fs::rename(new_image, exe) {
    Ok(()) => Ok(()),
    Err(err) if err.kind() == std::io::ErrorKind::CrossesDevices => {
      let tmp = exe.with_extension("ota-tmp");
      std::fs::copy(new_image, &tmp)?;
      #[cfg(unix)]
      std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
      std::fs::rename(&tmp, exe)?;
      let _ = std::fs::remove_file(new_image);
      Ok(())
    }
    Err(err) => {
      // Best effort: put the previous image back so the install still
      // boots.
      let _ = std::fs::rename(&prev, exe);
      Err(err)
    }
  }
}

/// Exchange `exe` with its [`prev_path`] image so a rollback is itself
/// reversible (rolling back twice restores the starting state). Same
/// directory on both sides, so plain renames suffice — no copy fallback
/// needed, and a failed middle rename restores the original layout.
fn swap_back(exe: &Path, prev: &Path) -> std::io::Result<()> {
  let tmp = exe.with_extension("ota-swap");
  std::fs::rename(exe, &tmp)?;
  if let Err(err) = std::fs::rename(prev, exe) {
    let _ = std::fs::rename(&tmp, exe);
    return Err(err);
  }
  // exe already holds the restored image; prev is missing, but the
  // install boots — report, do not unwind a working state.
  std::fs::rename(&tmp, prev)?;
  Ok(())
}

/// Re-execute this binary with the same arguments (Unix: `exec`, same
/// PID — invisible to systemd; Windows: spawn + exit).
fn reexec(exe: &Path) -> ! {
  #[cfg(unix)]
  {
    use std::os::unix::process::CommandExt;
    let err = std::process::Command::new(exe)
      .args(std::env::args_os().skip(1))
      .env(APPLIED_ENV, "1")
      .exec();
    eprintln!("[rsrpc] re-exec failed after update ({err}); restart to run the new version");
    std::process::exit(1);
  }
  #[cfg(windows)]
  {
    match std::process::Command::new(exe)
      .args(std::env::args_os().skip(1))
      .env(APPLIED_ENV, "1")
      .spawn()
    {
      Ok(_) => std::process::exit(0),
      Err(err) => {
        eprintln!("[rsrpc] re-spawn failed after update ({err}); restart to run the new version");
        std::process::exit(1);
      }
    }
  }
  #[cfg(not(any(unix, windows)))]
  {
    let _ = exe;
    eprintln!("[rsrpc] update applied; restart to run the new version");
    std::process::exit(0);
  }
}

/// Apply a staged update at boot: re-verify, swap, clear state, re-exec.
///
/// Runs on every startup; no-ops (fast: one small JSON read) when nothing
/// is staged or the anti-loop guard is set. Never fails boot: any problem
/// discards the staged file and continues with the running version.
pub fn apply_pending_on_boot() {
  if std::env::var_os(APPLIED_ENV).is_some() {
    return;
  }
  let Ok(exe) = std::env::current_exe().and_then(|path| path.canonicalize()) else {
    return;
  };
  let paths = OtaPaths::from_env();
  let state = load_state(&paths);
  let (Some(staged_version), Some(staged_sha256)) = (state.staged_version, state.staged_sha256)
  else {
    return;
  };
  let discard = |why: &str| {
    eprintln!("[rsrpc] discarding staged update: {why}");
    let _ = std::fs::remove_file(paths.staged_file());
    save_state(&paths, &OtaState::default());
  };
  let Ok(staged_version) = Version::parse(&staged_version) else {
    discard("bad staged version");
    return;
  };
  if staged_version <= current_version() {
    discard("staged version is not newer");
    return;
  }
  if let Err(refusal) = check_eligibility(&exe) {
    discard(&refusal.to_string());
    return;
  }
  let bytes = match std::fs::read(paths.staged_file()) {
    Ok(bytes) => bytes,
    Err(err) => {
      discard(&format!("cannot read staged file: {err}"));
      return;
    }
  };
  if sha256_hex(&bytes) != staged_sha256 {
    discard("staged checksum mismatch");
    return;
  }
  match swap_binary(&exe, &paths.staged_file()) {
    Ok(()) => {
      save_state(
        &paths,
        &OtaState {
          latest_seen: Some(staged_version.to_string()),
          ..OtaState::default()
        },
      );
      println!("[rsrpc] updated to v{staged_version}, restarting");
      reexec(&exe);
    }
    Err(err) => discard(&format!("cannot replace binary: {err}")),
  }
}

/// Restore the kept `.prev` image and clear any staged update, then
/// re-execute.
///
/// # Errors
///
/// Returns the error when no previous image exists or the swap fails.
pub fn cmd_rollback() -> Result<(), Box<dyn std::error::Error>> {
  let exe = std::env::current_exe()
    .and_then(|path| path.canonicalize())
    .map_err(|err| format!("cannot locate running binary: {err}"))?;
  check_eligibility(&exe).map_err(|refusal| format!("cannot roll back: {refusal}"))?;
  let prev = prev_path(&exe);
  if !prev.is_file() {
    return Err("no previous version kept (nothing to roll back to)".into());
  }
  // A staged update would re-apply on the next boot and undo this
  // rollback: clear it first.
  let paths = OtaPaths::from_env();
  let _ = std::fs::remove_file(paths.staged_file());
  save_state(&paths, &OtaState::default());
  swap_back(&exe, &prev).map_err(|err| format!("rollback failed: {err}"))?;
  println!("[rsrpc] rolled back; restarting with the previous version");
  reexec(&exe);
}

/// `--check-update`: print status; exit 2 when an update is available.
pub fn cmd_check() -> Result<(), Box<dyn std::error::Error>> {
  match check()? {
    CheckOutcome::UpToDate { current } => {
      println!("[rsrpc] up to date (v{current})");
      Ok(())
    }
    CheckOutcome::Available { latest, tag, .. } => {
      println!(
        "[rsrpc] update available: v{latest} (release {tag}); run with --update to stage it"
      );
      std::process::exit(2);
    }
    CheckOutcome::UnsupportedTarget { current, triple } => {
      println!("[rsrpc] no published builds for {triple} (running v{current})");
      Ok(())
    }
  }
}

/// `--update`: check, confirm (unless `yes`), download + stage.
pub fn cmd_stage(yes: bool) -> Result<(), Box<dyn std::error::Error>> {
  let outcome = check()?;
  let available = match &outcome {
    CheckOutcome::UpToDate { current } => {
      println!("[rsrpc] up to date (v{current})");
      return Ok(());
    }
    CheckOutcome::UnsupportedTarget { current, triple } => {
      println!("[rsrpc] no published builds for {triple} (running v{current})");
      return Ok(());
    }
    available @ CheckOutcome::Available { .. } => available,
  };
  let CheckOutcome::Available { latest, .. } = available else {
    return Ok(());
  };
  if !yes {
    print!("[rsrpc] download and stage v{latest}? applies on next start [y/N] ");
    std::io::stdout().flush().ok();
    let mut answer = String::new();
    if std::io::stdin().read_line(&mut answer).is_err()
      || !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes")
    {
      println!("[rsrpc] cancelled");
      return Ok(());
    }
  }
  let exe = std::env::current_exe()
    .and_then(|path| path.canonicalize())
    .map_err(|err| format!("cannot locate running binary: {err}"))?;
  let staged = stage(available, &OtaPaths::from_env(), &exe)?;
  println!("[rsrpc] staged v{staged}; applies on next start");
  Ok(())
}

/// Daily background check inside the daemon: logs availability, and stages
/// when `auto` (opt-in) — never restarts anything by itself.
pub fn spawn_watcher(auto: bool) {
  if std::thread::Builder::new()
    .name("rsrpc-ota".to_string())
    .spawn(move || {
      // Let boot settle (sockets, first scan) before any network.
      std::thread::sleep(WATCH_BOOT_DELAY);
      let mut notified: Option<Version> = None;
      loop {
        match check() {
          Err(err) => eprintln!("[rsrpc] background update check failed: {err}"),
          Ok(outcome) => {
            if let CheckOutcome::Available { ref latest, .. } = outcome
              && notified.as_ref() != Some(latest)
            {
              if auto {
                let paths = OtaPaths::from_env();
                let staged = paths
                  .ensure_dir()
                  .map_err(|err| format!("auto-update failed: {err}"))
                  .and(
                    std::env::current_exe()
                      .and_then(|path| path.canonicalize())
                      .map_err(|err| format!("auto-update failed: cannot locate binary: {err}")),
                  )
                  .and_then(|exe| {
                    stage(&outcome, &paths, &exe)
                      .map_err(|err| format!("auto-update failed: {err}"))
                  });
                match staged {
                  Ok(version) => {
                    println!("[rsrpc] staged update v{version}; applies on next start");
                    notified = Some(latest.clone());
                  }
                  Err(err) => eprintln!("{err}"),
                }
              } else {
                println!("[rsrpc] update available: v{latest}; run with --update to stage it");
                notified = Some(latest.clone());
              }
            }
          }
        }
        // Jitter (pid-derived, no extra deps) so fleets do not hammer
        // the API in lockstep.
        let jitter = Duration::from_secs(u64::from(std::process::id() % 3600));
        std::thread::sleep(WATCH_INTERVAL + jitter);
      }
    })
    .is_err()
  {
    eprintln!("[rsrpc] could not spawn update watcher; continuing without background checks");
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// RAII temp dir (std-only): unique per test, removed on drop.
  struct TempDir {
    path: PathBuf,
  }

  impl TempDir {
    fn new(tag: &str) -> Self {
      let path = std::env::temp_dir().join(format!(
        "rsrpc-ota-test-{}-{}-{}",
        std::process::id(),
        tag,
        std::time::SystemTime::now()
          .duration_since(std::time::UNIX_EPOCH)
          .map(|elapsed| elapsed.as_nanos())
          .unwrap_or(0)
      ));
      std::fs::create_dir_all(&path).expect("test tempdir");
      Self { path }
    }
  }

  impl Drop for TempDir {
    fn drop(&mut self) {
      let _ = std::fs::remove_dir_all(&self.path);
    }
  }

  #[test]
  fn parse_version_accepts_bare_and_v_prefixed_tags() {
    assert_eq!(parse_version("v1.2.3"), Some(Version::new(1, 2, 3)));
    assert_eq!(parse_version("1.2.3"), Some(Version::new(1, 2, 3)));
    assert_eq!(parse_version("  v0.32.2\n"), Some(Version::new(0, 32, 2)));
    assert!(parse_version("not-a-version").is_none());
    assert!(parse_version("").is_none());
  }

  #[test]
  fn parse_release_extracts_version_and_assets() {
    let body: serde_json::Value = serde_json::from_str(
      r#"{"tag_name":"v0.33.0","assets":[
        {"name":"rsrpc-cli-x86_64-unknown-linux-gnu","browser_download_url":"https://example.com/a"},
        {"name":"SHA256SUMS.txt","browser_download_url":"https://example.com/s"},
        {"name":"notes.txt"}
      ]}"#,
    )
    .expect("fixture");
    let release = parse_release(&body).expect("release");
    assert_eq!(release.version, Version::new(0, 33, 0));
    assert_eq!(release.tag, "v0.33.0");
    // The asset without URL is skipped, the rest survive.
    assert_eq!(release.assets.len(), 2);
    let binary = select_asset(&release, "rsrpc-cli-x86_64-unknown-linux-gnu").expect("binary");
    assert_eq!(binary.url, "https://example.com/a");
    assert!(select_asset(&release, "rsrpc-cli-mips-unknown-linux-gnu").is_none());
  }

  #[test]
  fn parse_release_rejects_bodies_without_version() {
    assert!(parse_release(&serde_json::json!({"assets": []})).is_none());
    assert!(parse_release(&serde_json::json!({"tag_name": "nope"})).is_none());
    assert!(parse_release(&serde_json::json!({})).is_none());
  }

  #[test]
  fn parse_checksums_handles_both_sum_styles_and_skips_garbage() {
    let manifest = "abc123\n\
      d4e5f6  rsrpc-cli-x86_64-unknown-linux-gnu\n\
      00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff *rsrpc-cli-aarch64-unknown-linux-gnu\n\
      nothex  some-file\n";
    let sums = parse_checksums(manifest);
    assert_eq!(sums.len(), 1);
    assert_eq!(
      sums["rsrpc-cli-aarch64-unknown-linux-gnu"],
      "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
    );
  }

  #[test]
  fn sha256_hex_matches_known_vector() {
    assert_eq!(
      sha256_hex(b"abc"),
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
  }

  /// Throwaway test keypair (secret destroyed after signing the fixture
  /// below with the official minisign CLI — modern prehashed format, the
  /// same flags CI uses).
  const TEST_PUBKEY: &str = "RWTTN7gN0OW7+aDqRGjfeCOvh0eG6CRWOV+28hf/0KrAhStOWvstnZyj";
  const TEST_MANIFEST: &str = "deadbeef  rsrpc-cli-x86_64-unknown-linux-gnu\n";
  const TEST_MANIFEST_SIG: &str = "untrusted comment: signature from minisign secret key\n\
    RUTTN7gN0OW7+RWfZYlU0Na0KzxWp8NCOB1jICVjohNcwSOq8FRVGi8gkRg/aOCJh0ZadUWiNobX3FZvJLzPNHDDOmM4+MXWfwQ=\n\
    trusted comment: rsrpc test fixture\n\
    FXtne+7W3eoXDYvCw2T5QfBz8WB51H2aY97qU8Atm208bRF77hdMmwilf/2xnAACD1cZCmbvQQ+tr5YX6Lg8Cg==\n";

  #[test]
  fn embedded_release_key_parses() {
    // Guards against transcription typos in UPDATE_PUBKEY: if the
    // embedded key does not even parse, every future stage() fails.
    assert!(minisign_verify::PublicKey::from_base64(UPDATE_PUBKEY).is_ok());
  }

  #[test]
  fn valid_manifest_signature_verifies() {
    assert!(verify_signature(TEST_PUBKEY, TEST_MANIFEST.as_bytes(), TEST_MANIFEST_SIG).is_ok());
  }

  #[test]
  fn tampered_manifest_fails_signature() {
    let tampered = TEST_MANIFEST.replace("deadbeef", "badcafe0");
    assert!(verify_signature(TEST_PUBKEY, tampered.as_bytes(), TEST_MANIFEST_SIG).is_err());
  }

  #[test]
  fn signature_from_another_key_fails() {
    // The production key must reject the test fixture (key-id mismatch):
    // signatures are bound to their key, not just well-formed.
    assert!(verify_signature(UPDATE_PUBKEY, TEST_MANIFEST.as_bytes(), TEST_MANIFEST_SIG).is_err());
  }

  #[test]
  fn garbage_signature_fails() {
    assert!(verify_signature(TEST_PUBKEY, TEST_MANIFEST.as_bytes(), "nope").is_err());
    assert!(
      verify_signature(
        TEST_PUBKEY,
        TEST_MANIFEST.as_bytes(),
        "untrusted comment: x\n!!!!\ntrusted comment: y\n!!!!\n"
      )
      .is_err()
    );
  }

  #[test]
  fn eligibility_refuses_dev_cargo_and_foreign_binaries() {
    let tmp = TempDir::new("eligibility");
    // Dev build layout.
    let dev = tmp.path.join("target").join("debug").join("rsrpc-cli");
    std::fs::create_dir_all(dev.parent().expect("parent")).expect("mkdir");
    std::fs::write(&dev, b"x").expect("write");
    assert!(matches!(check_eligibility(&dev), Err(Refusal::DevBuild(_))));
    // Cargo-managed install.
    let cargo = tmp.path.join(".cargo").join("bin").join("rsrpc-cli");
    std::fs::create_dir_all(cargo.parent().expect("parent")).expect("mkdir");
    std::fs::write(&cargo, b"x").expect("write");
    assert!(matches!(
      check_eligibility(&cargo),
      Err(Refusal::CargoManaged(_))
    ));
    // Foreign binary name.
    let foreign = tmp.path.join("bin").join("something-else");
    std::fs::create_dir_all(foreign.parent().expect("parent")).expect("mkdir");
    std::fs::write(&foreign, b"x").expect("write");
    assert!(matches!(
      check_eligibility(&foreign),
      Err(Refusal::UnexpectedName(_))
    ));
    // A proper user-local layout passes.
    let good = tmp.path.join("local").join("bin").join("rsrpc-cli");
    std::fs::create_dir_all(good.parent().expect("parent")).expect("mkdir");
    std::fs::write(&good, b"x").expect("write");
    assert!(check_eligibility(&good).is_ok());
  }

  #[test]
  fn state_roundtrips_and_tolerates_garbage() {
    let tmp = TempDir::new("state");
    let paths = OtaPaths::with_dir(tmp.path.join("ota"));
    // Missing file -> default state.
    assert_eq!(load_state(&paths), OtaState::default());
    let state = OtaState {
      staged_version: Some("0.33.0".to_string()),
      staged_sha256: Some("abc".to_string()),
      latest_seen: Some("0.33.0".to_string()),
    };
    save_state(&paths, &state);
    assert_eq!(load_state(&paths), state);
    // Garbage file -> default state, never fatal.
    std::fs::write(paths.state_file(), "{nope").expect("write");
    assert_eq!(load_state(&paths), OtaState::default());
  }

  #[test]
  fn env_override_points_verbatim_at_the_dir() {
    // Regression: RSRPC_OTA_DIR used to gain an extra rsrpc/ota suffix,
    // so staged state written there was never found on boot.
    let tmp = TempDir::new("envdir");
    let dir = tmp.path.join("custom");
    let previous = std::env::var_os(OTA_DIR_ENV);
    // SAFETY: single-threaded test process section touching only this
    // variable (no other test reads RSRPC_OTA_DIR); restored below.
    unsafe {
      std::env::set_var(OTA_DIR_ENV, &dir);
    }
    let resolved = OtaPaths::from_env();
    match previous {
      Some(value) => unsafe {
        std::env::set_var(OTA_DIR_ENV, value);
      },
      None => unsafe {
        std::env::remove_var(OTA_DIR_ENV);
      },
    }
    assert_eq!(resolved.state_file(), dir.join(STATE_FILE));
  }

  #[test]
  fn prev_path_appends_a_single_suffix() {
    assert_eq!(
      prev_path(Path::new("/bin/rsrpc-cli")),
      PathBuf::from("/bin/rsrpc-cli.prev")
    );
    assert_eq!(
      prev_path(Path::new("C:/x/rsrpc-cli.exe")),
      PathBuf::from("C:/x/rsrpc-cli.exe.prev")
    );
  }

  #[test]
  fn swap_binary_rotates_staged_over_exe() {
    let tmp = TempDir::new("swap");
    let exe = tmp.path.join("rsrpc-cli");
    let staged = tmp.path.join("rsrpc-cli.staged");
    std::fs::write(&exe, b"v1").expect("write");
    std::fs::write(&staged, b"v2").expect("write");

    swap_binary(&exe, &staged).expect("swap");

    assert_eq!(std::fs::read(&exe).expect("read"), b"v2");
    assert_eq!(std::fs::read(prev_path(&exe)).expect("read"), b"v1");
    assert!(!staged.exists());
  }

  #[test]
  fn rollback_toggles_without_destroying_either_side() {
    // Regression: the old rollback deleted the prev file before moving
    // it back, so it printed success, changed nothing, and left no prev
    // behind. Distinct bytes on each side catch any content loss.
    let tmp = TempDir::new("rollback");
    let exe = tmp.path.join("rsrpc-cli");
    let prev = prev_path(&exe);
    std::fs::write(&exe, b"v2-running").expect("write");
    std::fs::write(&prev, b"v1-kept").expect("write");

    swap_back(&exe, &prev).expect("first rollback");
    assert_eq!(std::fs::read(&exe).expect("read"), b"v1-kept");
    assert_eq!(std::fs::read(&prev).expect("read"), b"v2-running");

    swap_back(&exe, &prev).expect("second rollback");
    assert_eq!(std::fs::read(&exe).expect("read"), b"v2-running");
    assert_eq!(std::fs::read(&prev).expect("read"), b"v1-kept");
  }

  #[test]
  fn rollback_without_prev_fails() {
    let tmp = TempDir::new("rollback-missing");
    let exe = tmp.path.join("rsrpc-cli");
    std::fs::write(&exe, b"v1").expect("write");

    assert!(swap_back(&exe, &prev_path(&exe)).is_err());
    // The running image is untouched by the failed attempt.
    assert_eq!(std::fs::read(&exe).expect("read"), b"v1");
  }
}
