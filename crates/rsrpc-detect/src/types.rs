//! Scanner data model: slim match inputs, hits and scan state.
//!
//! The slim forms (`ScannedEntry`, `ScannedExe`) carry only what matchers
//! read: storing full [`DetectableActivity`]s
//! would pin ~3x the memory per database generation.

use std::sync::{Arc, Mutex};

use crate::db::DetectableActivity;

#[derive(Default, Clone)]
pub struct ProcessScanState {
  pub obs_open: bool,
}

#[derive(Default)]
pub struct ProcessEventListeners {
  /// Optional scan-tick callback, invoked with the post-tick state.
  pub on_process_scan_complete: Option<Arc<Mutex<ProcessCallback>>>,
}

#[derive(Clone)]
pub struct Exec {
  pub pid: u64,
  pub path: String,
  pub arguments: Option<String>,
}

/// Scanner event: one game appeared, the whole table went empty, or one
/// previously-announced slot vanished while others remain.
///
/// Enum (not `Option`) because the states are mutually exclusive: a
/// per-slot clear must not be mistaken for a full-table clear downstream.
#[derive(Clone, Debug)]
pub enum ProcessDetectedEvent {
  /// One classified game (re-emits are deduped downstream).
  Detected(ScannedHit),
  /// No games detected: clear every outstanding generic publication.
  Cleared,
  /// A slot present in the previous non-empty snapshot but absent now.
  /// Carries the last-known app id + pid so the bridge can clear exactly
  /// that card without flapping co-running games.
  Removed {
    /// Application id of the vanished slot.
    id: Box<str>,
    /// Last-known pid (for the empty payload + logs).
    pid: u64,
  },
}

impl ProcessDetectedEvent {
  /// One classified game.
  #[must_use]
  pub fn detected(hit: ScannedHit) -> Self {
    Self::Detected(hit)
  }

  /// The empty-table clear event (the old `id == "null"` convention, now
  /// explicit).
  #[must_use]
  pub fn cleared() -> Self {
    Self::Cleared
  }

  /// A per-slot clear for a vanished snapshot entry.
  #[must_use]
  pub fn removed(id: Box<str>, pid: u64) -> Self {
    Self::Removed { id, pid }
  }
}

/// One executable as the scanner needs it: matcher inputs only. Slimmer
/// than [`DetectableActivity`]'s full form — same field names, so match
/// code reads unchanged. Text rides as `Box<str>` (16B, no capacity
/// field) instead of `String` (24B): ~11k executables × 2 string fields
/// is real money, and these never grow after the build.
#[derive(Clone, Debug)]
pub struct ScannedExe {
  pub name: Box<str>,
  pub os: OsName,
  pub is_launcher: bool,
  pub arguments: Option<String>,
}

/// Executable OS: the database only ever carries three values (`win32`
/// × 11k, `linux`/`darwin` handful), so hot values are unit variants
/// (zero allocation) and anything exotic rides generation-owned inside
/// `Other` — freed with the bundle on the next swap. This replaced a
/// `&'static str` interning that leaked every unknown value for the
/// process lifetime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OsName {
  Win32,
  Linux,
  Darwin,
  Empty,
  Other(Box<str>),
}

impl OsName {
  pub fn from_os(os: &str) -> Self {
    match os {
      "win32" => Self::Win32,
      "linux" => Self::Linux,
      "darwin" => Self::Darwin,
      "" => Self::Empty,
      other => Self::Other(other.to_owned().into_boxed_str()),
    }
  }

  /// Zero-copy twin of [`from_os`](OsName::from_os) for owned inputs:
  /// exotic values move instead of cloning.
  pub fn from_string(os: String) -> Self {
    match os.as_str() {
      "win32" => Self::Win32,
      "linux" => Self::Linux,
      "darwin" => Self::Darwin,
      "" => Self::Empty,
      _ => Self::Other(os.into_boxed_str()),
    }
  }

  pub fn as_str(&self) -> &str {
    match self {
      Self::Win32 => "win32",
      Self::Linux => "linux",
      Self::Darwin => "darwin",
      Self::Empty => "",
      Self::Other(os) => os,
    }
  }

  pub fn is_empty(&self) -> bool {
    matches!(self, Self::Empty)
  }
}

/// One database game as the scanner needs it: matching identity plus
/// publish labels, nothing else. A full [`DetectableActivity`] carries
/// ~30 mostly-`None` fields (`Option<String>` costs 24B even when empty),
/// so storing 24k of them pins ~13MB; the slim form keeps under a third
/// of that. Converted once per generation at build time from the full
/// public struct (which stays the API and transport shape) — only
/// Steam-distributor SKUs survive the trip, since no matcher reads any
/// other distributor.
#[derive(Clone, Debug)]
pub struct ScannedEntry {
  pub id: Box<str>,
  pub name: Box<str>,
  pub executables: Vec<ScannedExe>,
  pub steam_ids: Vec<Box<str>>,
  pub aliases: Vec<Box<str>>,
}

impl ScannedEntry {
  /// Convert a full public entry to the slim scanner form (borrowing):
  /// used where the input is shared (`Arc` main list). See
  /// [`from_owned`](ScannedEntry::from_owned) for owned batches.
  pub fn from_activity(activity: &DetectableActivity) -> Self {
    Self {
      id: activity.id.clone().into_boxed_str(),
      name: activity.name.clone().into_boxed_str(),
      executables: activity
        .executables
        .as_ref()
        .map(|exes| {
          exes
            .iter()
            .map(|exe| ScannedExe {
              name: exe.name.clone().into_boxed_str(),
              os: OsName::from_os(&exe.os),
              is_launcher: exe.is_launcher,
              arguments: exe.arguments.clone(),
            })
            .collect()
        })
        .unwrap_or_default(),
      steam_ids: activity
        .third_party_skus
        .as_ref()
        .map(|skus| {
          skus
            .iter()
            .filter(|sku| sku.distributor == "steam")
            .filter_map(|sku| sku.id.clone())
            .filter(|id| !id.is_empty())
            .map(|id| id.into_boxed_str())
            .collect()
        })
        .unwrap_or_default(),
      aliases: activity
        .aliases
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|alias| alias.into_boxed_str())
        .collect(),
    }
  }

  /// Same conversion for an owned entry: moves strings instead of cloning
  /// them, so batch conversions (hourly refresh, override appends) skip
  /// ~130k duplicate allocations. Same output as [`from_activity`](ScannedEntry::from_activity).
  pub fn from_owned(activity: DetectableActivity) -> Self {
    Self {
      id: activity.id.into_boxed_str(),
      name: activity.name.into_boxed_str(),
      executables: activity
        .executables
        .unwrap_or_default()
        .into_iter()
        .map(|exe| ScannedExe {
          name: exe.name.into_boxed_str(),
          os: OsName::from_string(exe.os),
          is_launcher: exe.is_launcher,
          arguments: exe.arguments,
        })
        .collect(),
      steam_ids: activity
        .third_party_skus
        .unwrap_or_default()
        .into_iter()
        .filter(|sku| sku.distributor == "steam")
        .filter_map(|sku| sku.id)
        .filter(|id| !id.is_empty())
        .map(|id| id.into_boxed_str())
        .collect(),
      aliases: activity
        .aliases
        .unwrap_or_default()
        .into_iter()
        .map(|alias| alias.into_boxed_str())
        .collect(),
    }
  }
}

/// One classified game: the shared slim entry plus the observation (pid +
/// epoch-millis start). Replaces stamping pid/timestamp onto a full-struct
/// clone per hit per tick — zero per-hit allocation beyond the timestamp
/// read itself.
#[derive(Clone, Debug)]
pub struct ScannedHit {
  pub entry: Arc<ScannedEntry>,
  pub pid: u64,
  pub start: u64,
}

impl ScannedHit {
  /// Attach the observation (pid + now as epoch millis) to a shared
  /// entry: no clone, unlike the old full-struct stamp.
  pub fn stamp(entry: Arc<ScannedEntry>, pid: u64) -> Self {
    // Epoch millis as a NUMBER: Discord's schema (and strict clients)
    // want an integer here — a stringified timestamp is silently
    // dropped downstream.
    let start = std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .ok()
      .and_then(|age| u64::try_from(age.as_millis()).ok())
      .unwrap_or(0);
    Self { entry, pid, start }
  }
}

const _: () = assert!(std::mem::size_of::<ScannedEntry>() <= 104);

const _: () = assert!(std::mem::size_of::<ScannedExe>() <= 72);

/// Callback invoked with the scan state after every scan tick (e.g. the
/// OBS-open flag for streamer-mode tooling).
pub type ProcessCallback = dyn FnMut(ProcessScanState) + Send + Sync;
