use aho_corasick::AhoCorasick;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

#[cfg(not(target_os = "linux"))]
use sysinfo::System;

use crate::ProcessCallback;
use crate::{debug, log, warn};

use super::super::DetectableActivity;
use super::super::detection::{Exclusions, parse_exclusions};
use super::steam::SteamLibraries;

#[derive(Default, Clone)]
pub struct ProcessScanState {
  pub obs_open: bool,
}

#[derive(Default)]
pub(crate) struct ProcessEventListeners {
  pub on_process_scan_complete: Option<Arc<Mutex<ProcessCallback>>>,
}

#[derive(Clone)]
pub(crate) struct Exec {
  pub(crate) pid: u64,
  pub(crate) path: String,
  pub(crate) arguments: Option<String>,
}

#[derive(Clone)]
pub(crate) struct ProcessDetectedEvent {
  /// Classified game, or `None` for the empty-table clear event (the old
  /// `id == "null"` convention, now explicit).
  pub hit: Option<ScannedHit>,
}

/// One executable as the scanner needs it: matcher inputs only. Slimmer
/// than [`DetectableActivity`]'s full form — same field names, so match
/// code reads unchanged. Text rides as `Box<str>` (16B, no capacity
/// field) instead of `String` (24B): ~11k executables × 2 string fields
/// is real money, and these never grow after the build.
#[derive(Clone, Debug)]
pub(crate) struct ScannedExe {
  pub(crate) name: Box<str>,
  pub(crate) os: OsName,
  pub(crate) is_launcher: bool,
  pub(crate) arguments: Option<String>,
}

/// Executable OS: the database only ever carries three values (`win32`
/// × 11k, `linux`/`darwin` handful), so hot values are unit variants
/// (zero allocation) and anything exotic rides generation-owned inside
/// `Other` — freed with the bundle on the next swap. This replaced a
/// `&'static str` interning that leaked every unknown value for the
/// process lifetime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OsName {
  Win32,
  Linux,
  Darwin,
  Empty,
  Other(Box<str>),
}

impl OsName {
  pub(crate) fn from_str(os: &str) -> Self {
    match os {
      "win32" => Self::Win32,
      "linux" => Self::Linux,
      "darwin" => Self::Darwin,
      "" => Self::Empty,
      other => Self::Other(other.to_owned().into_boxed_str()),
    }
  }

  /// Zero-copy twin of [`from_str`](OsName::from_str) for owned inputs:
  /// exotic values move instead of cloning.
  pub(crate) fn from_string(os: String) -> Self {
    match os.as_str() {
      "win32" => Self::Win32,
      "linux" => Self::Linux,
      "darwin" => Self::Darwin,
      "" => Self::Empty,
      _ => Self::Other(os.into_boxed_str()),
    }
  }

  pub(crate) fn as_str(&self) -> &str {
    match self {
      Self::Win32 => "win32",
      Self::Linux => "linux",
      Self::Darwin => "darwin",
      Self::Empty => "",
      Self::Other(os) => os,
    }
  }

  pub(crate) fn is_empty(&self) -> bool {
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
pub(crate) struct ScannedEntry {
  pub(crate) id: Box<str>,
  pub(crate) name: Box<str>,
  pub(crate) executables: Vec<ScannedExe>,
  pub(crate) steam_ids: Vec<Box<str>>,
  pub(crate) aliases: Vec<Box<str>>,
}

impl ScannedEntry {
  /// Convert a full public entry to the slim scanner form (borrowing):
  /// used where the input is shared (`Arc` main list). See
  /// [`from_owned`](ScannedEntry::from_owned) for owned batches.
  pub(crate) fn from_activity(activity: &DetectableActivity) -> Self {
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
              os: OsName::from_str(&exe.os),
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
  pub(crate) fn from_owned(activity: DetectableActivity) -> Self {
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

// Layout guard (see `mem-assert-type-size`): the slim entry must stay
// lean — `Box<str>` (16B) over `String` (24B) is the whole point. The
// `OsName` enum costs one tag word over a bare pointer (72B vs 64B) to
// own exotic values per generation instead of leaking them.
const _: () = assert!(std::mem::size_of::<ScannedEntry>() <= 104);
const _: () = assert!(std::mem::size_of::<ScannedExe>() <= 72);

/// One classified game: the shared slim entry plus the observation (pid +
/// epoch-millis start). Replaces stamping pid/timestamp onto a full-struct
/// clone per hit per tick — zero per-hit allocation beyond the timestamp
/// read itself.
#[derive(Clone, Debug)]
pub(crate) struct ScannedHit {
  pub(crate) entry: Arc<ScannedEntry>,
  pub(crate) pid: u64,
  pub(crate) start: u64,
}

impl ScannedHit {
  /// Attach the observation (pid + now as epoch millis) to a shared
  /// entry: no clone, unlike the old full-struct stamp.
  pub(crate) fn stamp(entry: Arc<ScannedEntry>, pid: u64) -> Self {
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

/// Sorted key→index table replacing a `HashMap` for the aux lookups: no
/// buckets (~33B each in SwissTable), no hashing, no per-key `String`
/// capacity field. Lookups happen only on match misses (rare per tick),
/// where O(log n) binary search over ~20k short keys is noise next to a
/// single `/proc` read. First-inserted index wins ties — identical to the
/// `or_insert` it replaces (callers insert canonical entries before
/// aliases, preserving canonical-first ties).
#[derive(Clone, Debug, Default)]
pub(crate) struct SortedIndex {
  entries: Vec<(Box<str>, usize)>,
}

impl SortedIndex {
  /// Build from `(key, index)` pairs: stable-sorts by key and keeps the
  /// first index per duplicate key (canonical-first ties, like the
  /// `or_insert` this replaces), then shrinks to fit.
  pub(crate) fn build(mut entries: Vec<(Box<str>, usize)>) -> Self {
    // Stable sort: equal keys keep insertion order, so `dedup_by` below
    // (which keeps the FIRST of each run) preserves first-wins.
    entries.sort_by(|a, b| {
      let (ka, kb): (&str, &str) = (&a.0, &b.0);
      ka.cmp(kb)
    });
    entries.dedup_by(|curr, prev| curr.0 == prev.0);
    entries.shrink_to_fit();
    Self { entries }
  }

  /// Look up one key by binary search: O(log n), only used on match
  /// misses (rare per tick).
  pub(crate) fn get(&self, key: &str) -> Option<&usize> {
    self
      .entries
      .binary_search_by(|(k, _)| {
        let ka: &str = k;
        ka.cmp(key)
      })
      .ok()
      .map(|idx| &self.entries[idx].1)
  }

  /// Number of indexed keys (boot diagnostics).
  pub(crate) fn len(&self) -> usize {
    self.entries.len()
  }

  /// Membership probe (test-only; production only needs [`get`](SortedIndex::get)).
  #[cfg(test)]
  pub(crate) fn contains_key(&self, key: &str) -> bool {
    self.get(key).is_some()
  }

  /// Iterate all entries (used to derive the de-dotted twin table).
  pub(crate) fn iter(&self) -> impl Iterator<Item = &(Box<str>, usize)> + '_ {
    self.entries.iter()
  }
}

/// Everything derived from one database generation, swapped atomically.
///
/// The atomic unit is automata, index tables, lists and aux maps. The
/// independent exclusions filter and Steam roots refresh alongside (two
/// swaps, not one): a tick may briefly mix generations. Harmless by
/// construction — exclusions only subtract, roots only add candidates —
/// so folding them into the bundle would buy nothing.
///
/// Rationale: the scanner used to hold the automaton, the index table,
/// the activity list and the aux maps under SEPARATE locks, replaced one
/// at a time by the hourly refresh. A scan landing mid-swap then probed a
/// NEW automaton against OLD indexes: index-out-of-bounds panic (dead
/// scan thread) or, worse, a silently mis-attributed game. With the whole
/// generation behind ONE Arc, readers clone it once per tick (or per
/// event) and can only ever observe a self-consistent snapshot; the
/// writer builds the next generation off-thread and swaps the pointer.
pub(crate) struct DetectablesBundle {
  list: Vec<Arc<ScannedEntry>>,
  ac: AhoCorasick,
  indexes: Vec<[usize; 2]>,
  /// Proton fallback automaton (Linux only): `win32` executables from the
  /// main DB, probed after the native patterns and the authoritative Steam
  /// AppId, but before the stem/folder heuristics. Empty on other platforms.
  proton_ac: Option<AhoCorasick>,
  proton_indexes: Vec<[usize; 2]>,
  /// Steam AppId (steam-distributor SKUs only) -> activity index.
  /// Lets us detect store games whose DB entry ships empty `executables`.
  /// Sorted vec, not a map: lookups only happen on match misses.
  steam_map: SortedIndex,
  /// Normalized game name -> activity index, for the conservative exe-stem
  /// fallback (exact, multi-word names only, e.g. `how to fish`).
  /// Sorted vec, not a map: lookups only happen on match misses.
  name_map: SortedIndex,
  /// De-dotted twin of `name_map` above (only keys containing dots):
  /// last-tier fallback for dotted title folders (`R.E.P.O.`,
  /// `Q.U.B.E.`), which the exact walk skips as versions. Built by
  /// [`undotted_names`], same canonical-first ties.
  name_map_nodot: SortedIndex,
  custom: Vec<Arc<ScannedEntry>>,
  custom_ac: Option<AhoCorasick>,
  custom_indexes: Vec<[usize; 2]>,
}

impl DetectablesBundle {
  /// Blind-but-valid generation for catastrophic automaton build
  /// failures (network-supplied DB exceeds builder limits): detection
  /// misses everything instead of panicking the daemon.
  fn empty() -> Self {
    Self {
      list: Vec::new(),
      ac: build_ac_automaton(&[]).expect("[bug] empty automaton always builds"),
      indexes: Vec::new(),
      proton_ac: None,
      proton_indexes: Vec::new(),
      steam_map: SortedIndex::default(),
      name_map: SortedIndex::default(),
      name_map_nodot: SortedIndex::default(),
      custom: Vec::new(),
      custom_ac: None,
      custom_indexes: Vec::new(),
    }
  }
}

/// Memoized SteamAppId plus its invalidation sequence (see
/// [`ProcessServer::cached_app_id`]). Verified outcomes (`Present` /
/// `Absent`) cost zero I/O until EXEC invalidation or pid death; only
/// `Stale` reads environ. Distinguishing `Absent` from `Stale` is the
/// whole point: nearly every desktop process has no SteamAppId, and
/// re-reading kilobytes of environ for all of them every tick was the
/// biggest per-process I/O cost.
#[derive(Clone, Debug, PartialEq, Eq)]
enum AppIdMemo {
  /// Verified: this pid has no SteamAppId. Valid until invalidated.
  Absent(u64),
  /// Verified SteamAppId, valid under the same rules.
  Present(u64, String),
  /// Never read, or invalidated by EXEC: read environ, then store the
  /// outcome — unless the sequence moved under us (racing EXEC), in
  /// which case the stale read is discarded, never stored.
  /// (Constructed only by the Linux-only `drop_appid`; other platforms
  /// would flag the variant as dead without the allow.)
  #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
  Stale(u64),
}

#[derive(Clone)]
pub(crate) struct ProcessServer {
  /// Current detection generation (see [`DetectablesBundle`]): cloned by
  /// readers, swapped whole by writers. Custom overrides live in the same
  /// bundle, so user appends can never tear against the main patterns
  /// either.
  detectables: Arc<Mutex<Arc<DetectablesBundle>>>,
  scanning: Arc<AtomicBool>,
  /// Double-`start` guard: a second scan generation would orphan the
  /// first loop's wake handle and double-emit EXEC hits.
  started: Arc<AtomicBool>,
  /// Set by the EXEC fast path whenever it publishes a hit, consumed by
  /// the scan loop: an EXEC-published game that dies before any poll
  /// observes it would otherwise never emit its clear (the delta would
  /// see two identical empty snapshots). Forcing one full emission per
  /// EXEC publication closes that hole; repeats dedup downstream.
  scan_dirty: Arc<AtomicBool>,

  pub event_sender: super::utils::GaugeSender<ProcessDetectedEvent>,

  event_listeners: Arc<Mutex<ProcessEventListeners>>,

  /// Hourly database-refresh inputs (source URL, toggle, startup seeds).
  /// Read once when the refresh thread spawns; immutable afterwards.
  refresh: RefreshConfig,
  /// Pids of the currently detected games (refreshed every scan tick,
  /// plus event-driven EXEC hits). Lets the proc-events watcher wake the
  /// scan loop the moment a TRACKED game exits — untracked exits never
  /// cause a scan.
  pub(crate) detected_pids: Arc<Mutex<HashSet<u64>>>,
  /// Memoized SteamAppId per pid: environ never changes after exec, so
  /// one read per process lifetime suffices (environ is kilobytes — the
  /// biggest per-process cost in the profiler). Invalidated by EXEC (the
  /// watcher drops the entry before reclassifying) and by death (swept
  /// every tick against the live pid set). Bounded by live process count.
  /// Each entry carries a sequence number: a `drop_appid` racing an
  /// in-flight read bumps it, and the stale read is discarded instead of
  /// pinning a pre-exec environ for the pid lifetime.
  appid_cache: Arc<Mutex<HashMap<u64, AppIdMemo>>>,
  /// Scan thread handle for early wakeups (proc-events EXIT of a tracked
  /// game). Registered by the scan thread itself on startup.
  scan_wake: Arc<Mutex<Option<std::thread::Thread>>>,
  /// Last scan-loop iteration start. EXIT wakes are debounced against it:
  /// Proton games spawn/die short-lived helpers constantly, and every one
  /// of them matches the game — without this, tracked exits unpark the
  /// loop several times per second (measured 0.76s effective cadence
  /// instead of 5s during NFS). Minimum 1s between early scans.
  pub(crate) last_scan: Arc<Mutex<std::time::Instant>>,
  /// Discord detection exclusions (installer/crash-reporter basenames +
  /// regexes): excluded processes are dropped before any matching.
  /// Empty until [`ProcessServer::set_exclusions`] (startup fetch) or the
  /// hourly refresh fills it; empty behaves exactly like no exclusions.
  exclusions: Arc<Mutex<Exclusions>>,
  /// Steam install-dir -> AppId (VDF provider): refreshed once per scan
  /// tick when a `libraryfolders.vdf` changed, consulted on path misses.
  steam_libraries: Arc<Mutex<SteamLibraries>>,
  /// Application IDs never published by the scan thread (coexistence with
  /// a richer publisher elsewhere). Filtered right after the scan, so an
  /// ignored-only result behaves exactly like no game: null event, clear.
  /// Hash set (built once): consulted per detected game per tick.
  ignored_ids: HashSet<String>,
  /// Event-driven proc-events watcher (netlink `cn_proc` fast path)
  /// on/off. Plain bool (no lock needed: written once via
  /// [`ProcessServer::set_proc_events`] before [`ProcessServer::start`],
  /// read once there). `true` unless `--no-proc-events` opted out.
  /// Linux-only read (other platforms have no watcher); the allow keeps
  /// cross-platform builds warning-free.
  #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
  pub(crate) enable_proc_events: bool,

  #[cfg(not(target_os = "linux"))]
  sysinfo: Arc<Mutex<System>>,
}

/// Re-entrancy guard for [`ProcessServer::scan_for_processes`]: acquired
/// atomically, released on drop (all exit paths, panics included).
pub(crate) struct ScanGuard {
  flag: Arc<AtomicBool>,
}

impl ScanGuard {
  pub(crate) fn try_acquire(flag: &Arc<AtomicBool>) -> Option<Self> {
    flag
      .compare_exchange(
        false,
        true,
        std::sync::atomic::Ordering::Acquire,
        std::sync::atomic::Ordering::Relaxed,
      )
      .ok()?;
    Some(Self {
      flag: Arc::clone(flag),
    })
  }
}

impl Drop for ScanGuard {
  fn drop(&mut self) {
    self.flag.store(false, std::sync::atomic::Ordering::Release);
  }
}

/// Hourly database-refresh inputs, threaded from startup into the
/// refresh loop as one value (see [`ProcessServer::start`]).
#[derive(Clone, Debug, Default)]
pub(crate) struct RefreshConfig {
  pub(crate) db_url: Option<String>,
  pub(crate) enable: bool,
  pub(crate) etag: Option<String>,
  pub(crate) content_hash: Option<(u64, u64)>,
  pub(crate) exclusions_url: Option<String>,
}

/// Build the initial detection generation, folding `custom` overrides
/// in: one automaton construction per boot instead of build-then-rebuild.
fn initial_bundle(
  detectable: Vec<Arc<DetectableActivity>>,
  custom: Vec<DetectableActivity>,
) -> Arc<DetectablesBundle> {
  log!("[Process Scanner] Building Aho-Corasick patterns for main detectable activities...");
  // Convert both sides straight to the slim scanner form: no
  // intermediate `Vec<Arc>` of full entries (24k Arcs built just to be
  // re-walked and dropped).
  let slim = |list: Vec<Arc<DetectableActivity>>| {
    list
      .iter()
      .map(|entry| Arc::new(ScannedEntry::from_activity(entry)))
      .collect::<Vec<_>>()
  };
  let custom: Vec<Arc<ScannedEntry>> = custom
    .into_iter()
    .map(|entry| Arc::new(ScannedEntry::from_owned(entry)))
    .collect();
  let bundle = Arc::new(build_bundle(slim(detectable), custom).unwrap_or_else(|e| {
    warn!(
      "[Process Scanner] Bundled database failed to build ({}), starting blind",
      e
    );
    DetectablesBundle::empty()
  }));
  log!("[Process Scanner] Done!");
  bundle
}

impl ProcessServer {
  /// Folds `custom` overrides into the initial build: one automaton
  /// construction instead of build-then-rebuild. Startup (and one-shot
  /// diagnostics) use this so staged overrides never cost a second full
  /// build per boot.
  pub(crate) fn new_with_custom(
    detectable: Vec<Arc<DetectableActivity>>,
    custom: Vec<DetectableActivity>,
    event_sender: super::utils::GaugeSender<ProcessDetectedEvent>,
    event_listeners: ProcessEventListeners,
    refresh: RefreshConfig,
    ignored_ids: Vec<String>,
  ) -> Self {
    let bundle = initial_bundle(detectable, custom);

    let server = ProcessServer {
      scanning: Arc::new(AtomicBool::new(false)),
      started: Arc::new(AtomicBool::new(false)),
      scan_dirty: Arc::new(AtomicBool::new(false)),
      detectables: Arc::new(Mutex::new(bundle)),
      event_sender,

      // Event listeners
      event_listeners: Arc::new(Mutex::new(event_listeners)),

      // Detectable database auto-refresh
      refresh,
      ignored_ids: ignored_ids.into_iter().collect(),
      enable_proc_events: true,
      exclusions: Arc::new(Mutex::new(Exclusions::default())),
      steam_libraries: Arc::new(Mutex::new(SteamLibraries::discover())),
      detected_pids: Arc::new(Mutex::new(HashSet::new())),
      appid_cache: Arc::new(Mutex::new(HashMap::new())),
      scan_wake: Arc::new(Mutex::new(None)),
      last_scan: Arc::new(Mutex::new(std::time::Instant::now())),

      // sysinfo System
      #[cfg(not(target_os = "linux"))]
      sysinfo: Arc::new(Mutex::new(System::new())),
    };

    // One-time parse arenas are now garbage: steady state is the lean
    // structures just built.
    release_parse_arenas();

    server
  }

  /// Rebuild the bundle with a new custom list, swapping the pointer in
  /// one write: no scan can ever observe a half-rebuilt custom automaton.
  fn rebuild_custom(&self, custom: Vec<Arc<ScannedEntry>>) {
    log!("[Process Scanner] Updating Aho-Corasick patterns for custom detectable activities...");
    let current = self
      .detectables
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .clone();
    let next = match build_bundle(current.list.clone(), custom) {
      Ok(next) => Arc::new(next),
      Err(e) => {
        warn!(
          "[Process Scanner] Refusing custom rebuild ({}), keeping current",
          e
        );
        return;
      }
    };
    *self.detectables.lock().unwrap_or_else(|e| e.into_inner()) = next;
    log!("[Process Scanner] Done!");
    // Rebuilt automaton state is lean; the transient scratch is garbage.
    release_parse_arenas();
  }

  /// Replace the main detectable games database at runtime (used by the
  /// periodic refresh), rebuilding the whole bundle and swapping it in
  /// one pointer write.
  fn update_main_detectables(&self, detectable: Vec<DetectableActivity>) {
    // Never swap in an empty database (outage returning `[]`, corrupt
    // fetch): it would build a failing automaton and blind detection.
    // Keep serving the current data instead.
    if detectable.is_empty() {
      warn!("[Process Scanner] Refusing empty detectable database update, keeping current");
      return;
    }
    log!("[Process Scanner] Rebuilding Aho-Corasick patterns for main detectable activities...");
    // Move (not clone) into the slim form, then drop the input before
    // the automata build below (~tens of MB of scratch): the fat structs
    // must not ride along to the end of this function and double the
    // rebuild peak.
    let slim: Vec<Arc<ScannedEntry>> = detectable
      .into_iter()
      .map(|entry| Arc::new(ScannedEntry::from_owned(entry)))
      .collect();
    let detectable = slim;
    let custom = self
      .detectables
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .custom
      .clone();
    let next = match build_bundle(detectable, custom) {
      Ok(next) => Arc::new(next),
      Err(e) => {
        warn!(
          "[Process Scanner] Refusing detectable database update ({}), keeping current",
          e
        );
        return;
      }
    };
    let old_count = Arc::strong_count(&self.detectables.lock().unwrap_or_else(|e| e.into_inner()));
    let rss_before = crate::server::utils::rss_bytes();
    *self.detectables.lock().unwrap_or_else(|e| e.into_inner()) = next;
    let rss_after = crate::server::utils::rss_bytes();
    log!(
      "[Process Scanner] Done! (bundle swap old_refs={} rss_before={} rss_after={})",
      old_count,
      rss_before
        .map(|b| format!("{:.1}MB", b as f64 / 1_048_576.0))
        .unwrap_or_else(|| "n/a".to_string()),
      rss_after
        .map(|b| format!("{:.1}MB", b as f64 / 1_048_576.0))
        .unwrap_or_else(|| "n/a".to_string())
    );
    // Fetch string, JSON DOM and trimmed copy are now garbage: hand the
    // hourly spike back (refresh cadence itself is unchanged).
    release_parse_arenas();
    if let Some(rss) = crate::server::utils::rss_bytes() {
      log!(
        "[Process Scanner] post-trim rss={:.1}MB",
        rss as f64 / 1_048_576.0
      );
    }
  }

  pub(crate) fn append_detectables(&self, detectable: Vec<DetectableActivity>) {
    // Append to the custom list, since that's what is actually scanned.
    // Full public entries convert once to the slim scanner form here,
    // moving (not cloning) their strings.
    let mut custom = self
      .detectables
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .custom
      .clone();
    custom.extend(
      detectable
        .into_iter()
        .map(|entry| Arc::new(ScannedEntry::from_owned(entry))),
    );
    self.rebuild_custom(custom);
  }

  pub(crate) fn remove_detectable_by_name(&self, name: &str) {
    let mut custom = self
      .detectables
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .custom
      .clone();
    custom.retain(|x| {
      let current: &str = &x.name;
      current != name
    });
    self.rebuild_custom(custom);
  }

  /// Replace the exclusions set (startup fetch, tests). The hourly refresh
  /// thread overwrites it on the same cadence when `exclusions_url` is set.
  pub(crate) fn set_exclusions(&self, exclusions: Exclusions) {
    *self.exclusions.lock().unwrap_or_else(|e| e.into_inner()) = exclusions;
  }

  /// Replace the Steam libraries map. Test-only for now (hence the
  /// gate): production builds it via discovery in
  /// [`ProcessServer::new_with_custom`]
  /// and refreshes it per scan tick.
  #[cfg(test)]
  pub(crate) fn set_steam_libraries(&self, libraries: SteamLibraries) {
    *self
      .steam_libraries
      .lock()
      .unwrap_or_else(|e| e.into_inner()) = libraries;
  }

  /// AppId whose Steam install dir prefixes `normalized_path` (already
  /// lowercased `/`-separated). Cloned out of the lock; tiny strings.
  pub(crate) fn steam_prefix_app_id(&self, normalized_path: &str) -> Option<String> {
    self
      .steam_libraries
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .match_prefix(normalized_path)
      .map(str::to_string)
  }

  /// SteamAppId for one pid, memoized: environ is kilobytes and never
  /// changes after exec, so re-reading it every 5s per process was pure
  /// waste (profiler: ~5KB of the ~5KB per-process cost). IO happens
  /// outside the lock; EXEC invalidates via [`ProcessServer::drop_appid`],
  /// whose sequence bump discards a racing stale read below.
  /// (`pub(crate)` for the memo-semantics regression test.)
  pub(crate) fn cached_app_id(&self, pid: u64) -> Option<String> {
    // Fast paths: verified memos, cloned once straight into the return.
    // `Absent` is the common case (non-Steam processes) and performs no
    // I/O at all; only `Stale` falls through to the environ read below.
    let memo = self
      .appid_cache
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .get(&pid)
      .cloned();
    let seq0 = match memo {
      Some(AppIdMemo::Present(_, id)) => return Some(id),
      Some(AppIdMemo::Absent(_)) => return None,
      Some(AppIdMemo::Stale(seq)) => seq,
      None => 0,
    };
    let id = read_steam_app_id(pid);
    let mut cache = self.appid_cache.lock().unwrap_or_else(|e| e.into_inner());
    // A racing EXEC invalidated this pid mid-read (sequence bumped) or
    // the tick swept it: discard the stale environ instead of pinning
    // it for the pid lifetime.
    let fresh = match cache.get(&pid) {
      Some(AppIdMemo::Stale(seq)) if *seq == seq0 => true,
      None if seq0 == 0 => true,
      _ => false,
    };
    if fresh {
      let memo = match id.clone() {
        Some(id) => AppIdMemo::Present(seq0, id),
        None => AppIdMemo::Absent(seq0),
      };
      cache.insert(pid, memo);
    }
    id
  }

  /// Drop memoized AppIds of pids that died since the last tick: pid
  /// reuse must never serve a stale id. One set build + retain per tick.
  fn sweep_dead_appids(&self, processes: &[Exec]) -> crate::error::Result<()> {
    let mut live = HashSet::with_capacity(processes.len());
    live.extend(processes.iter().map(|process| process.pid));
    self
      .appid_cache
      .lock()
      .map_err(|e| crate::error::RsrpcError::Poisoned("appid_cache", e.to_string()))?
      .retain(|pid, _| live.contains(pid));
    Ok(())
  }

  /// Drop one pid's memoized AppId (EXEC: same pid, new image, possibly
  /// new environ). Called by the proc-events watcher before reclassifying.
  /// Bumps the sequence so an in-flight [`ProcessServer::cached_app_id`]
  /// read for the old image is discarded, never stored.
  #[cfg(target_os = "linux")]
  pub(crate) fn drop_appid(&self, pid: u64) {
    self
      .appid_cache
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .entry(pid)
      .and_modify(|entry| {
        let seq = match entry {
          AppIdMemo::Absent(seq) | AppIdMemo::Present(seq, _) | AppIdMemo::Stale(seq) => *seq,
        };
        *entry = AppIdMemo::Stale(seq.wrapping_add(1));
      })
      .or_insert(AppIdMemo::Stale(1));
  }

  /// Whether a tracked game's EXIT should wake the scan loop early.
  /// Untracked exits never wake (one HashSet lookup, consumed either
  /// way — dead is dead). Tracked ones wake at most once per second:
  /// a recent EXIT stays tracked so a follow-up EXIT can still wake —
  /// only an actual wake consumes the pid. Proton helpers die
  /// constantly, and every one of them matches the game, so unwedged
  /// wakes would unpark the loop several times per second (measured
  /// 0.76s effective cadence instead of 5s during NFS).
  #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
  pub(crate) fn should_wake_on_exit(&self, pid: u64) -> bool {
    if !self
      .detected_pids
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .contains(&pid)
    {
      return false;
    }
    let due = self
      .last_scan
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .elapsed()
      .as_secs()
      >= 1;
    if due {
      self
        .detected_pids
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&pid);
    }
    due
  }

  /// Revalidate the Steam libraries (stats only unless something changed).
  /// Called once per scan tick; the scan loop goes through here so tests
  /// can drive the same path.
  pub(crate) fn refresh_steam_libraries(&self) {
    self
      .steam_libraries
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .refresh_if_stale();
  }

  /// Refresh the exclusions list once, best-effort: failures keep the
  /// previous set (empty at first boot = current behavior).
  fn refresh_exclusions(&self) {
    let Some(url) = self.refresh.exclusions_url.clone() else {
      return;
    };
    match fetch_exclusions(&url) {
      Ok(exclusions) => {
        log!(
          "[Process Scanner] Exclusions updated: {} names, {} patterns",
          exclusions.executables.len(),
          exclusions.patterns.len()
        );
        self.set_exclusions(exclusions);
      }
      Err(err) => {
        warn!("[Process Scanner] Error updating exclusions, retrying in 1h: {err}");
      }
    }
  }

  /// Start scan/refresh/watch threads. `watch_slot` receives the watch
  /// backlog gauge once the watcher spawns (stays detached when the
  /// watcher is disabled/unsupported, or on duplicate start).
  pub(crate) fn start(
    &self,
    scan_interval: Duration,
    watch_slot: &Arc<Mutex<super::utils::QueueGauge>>,
  ) {
    // Double-start is a caller bug: ignore fail-safe instead of leaking
    // a second scan/dispatch/watch generation.
    if self.started.swap(true, std::sync::atomic::Ordering::AcqRel) {
      warn!("[Process Scanner] Already started, ignoring duplicate start");
      return;
    }
    let wait_time = scan_interval;
    let clone = self.clone();

    // No custom rebuild needed here: new() already built the bundle with
    // its (empty) custom side, and appends rebuild on their own path.

    // Periodically refresh the detectable games database (like pog5-rsrpc).
    // Sleep first: startup already fetched synchronously, so an immediate
    // refetch would parse the whole DB twice for the same data (double
    // transient memory + startup time for zero new information). Refreshes
    // are conditional (ETag): an unchanged database costs one header round
    // trip and zero parsing, so steady-state RSS never ratchets.
    if clone.refresh.enable && clone.refresh.db_url.is_some() {
      let db_clone = clone.clone();
      // Hoisted once: `db_url` is immutable after `new()`, so the hourly
      // thread never unwraps an `Option` per iteration.
      let db_url = db_clone
        .refresh
        .db_url
        .clone()
        .expect("[bug] db_url checked above");
      std::thread::spawn(move || {
        // Seeded from the startup fetch when available: the first check
        // is conditional like every other, instead of one guaranteed
        // redundant full rebuild per daemon lifetime.
        let mut etag = db_clone.refresh.etag.clone();
        // Content hashes of the last built database: the raw hash guards
        // against byte-identical bodies (no parse at all), the trimmed
        // hash against volatile CDN bytes around identical games (parse,
        // but no rebuild). Either way an unchanged hour costs ~nothing.
        // Seeded from the startup fetch when available (see
        // `initial_db_content_hash`): without a seed the first check
        // always rebuilds once.
        let mut content_hash: Option<u64> = db_clone.refresh.content_hash.map(|(raw, _)| raw);
        let mut trimmed_hash: Option<u64> =
          db_clone.refresh.content_hash.map(|(_, trimmed)| trimmed);
        // Unlike the DB, exclusions are NOT fetched synchronously at
        // startup (tiny payload, empty = current behavior), so prime them
        // here instead of waiting an hour for the first set.
        db_clone.refresh_exclusions();
        loop {
          std::thread::sleep(Duration::from_secs(3600));
          db_clone.refresh_exclusions();
          match fetch_detectable_etag(&db_url, etag.as_deref(), content_hash, trimmed_hash) {
            Ok(FetchOutcome::Unchanged) => {
              log!(
                "[Process Scanner] DB check: unchanged (etag {})",
                etag.as_deref().unwrap_or("none")
              );
            }
            Ok(FetchOutcome::SameContent {
              etag: new_tag,
              content_hash: new_hash,
              trimmed_hash: new_trimmed,
            }) => {
              log!(
                "[Process Scanner] DB check: same content, new tag (etag {} -> {})",
                etag.as_deref().unwrap_or("none"),
                new_tag.as_deref().unwrap_or("none")
              );
              etag = new_tag;
              content_hash = Some(new_hash);
              trimmed_hash = Some(new_trimmed);
            }
            Ok(FetchOutcome::Updated {
              etag: new_tag,
              content_hash: new_hash,
              trimmed_hash: new_trimmed,
              detectable,
            }) => {
              log!(
                "[Process Scanner] DB updated: {} entries (etag {} -> {})",
                detectable.len(),
                etag.as_deref().unwrap_or("none"),
                new_tag.as_deref().unwrap_or("none")
              );
              etag = new_tag;
              content_hash = Some(new_hash);
              trimmed_hash = Some(new_trimmed);
              db_clone.update_main_detectables(detectable);
            }
            Err(err) => {
              warn!(
                "[Process Scanner] Error updating detectable database, retrying in 1h: {}",
                err
              );
            }
          }
        }
      });
    }

    std::thread::spawn(move || {
      // Register for early wakeups: the proc-events watcher unparks us
      // the moment a tracked game exits (Linux only; elsewhere None and
      // the cadence below is a plain sleep).
      *clone.scan_wake.lock().unwrap_or_else(|e| e.into_inner()) = Some(std::thread::current());
      // Idle backoff state: consecutive ticks with no games detected.
      let mut idle_ticks: u32 = 0;
      // Game ids already announced this boot (first-sighting INFO below).
      let mut seen_ids: HashSet<String> = HashSet::new();
      // Last detection snapshot forwarded to the bridge, as sorted
      // (app id, pid) pairs. The bridge dedups repeats internally, so
      // re-sending an identical table every tick only costs wakeups,
      // channel traffic and locks for zero effect — forward deltas only.
      // Transitions (including pid changes on restart) still send the
      // full current table, exactly like before.
      let mut last_emitted: Vec<(String, u64)> = Vec::new();
      // First-tick liveness proof (INFO, once): a scan thread that never
      // completes tick one is otherwise indistinguishable from an idle
      // one without a debug build.
      let mut first_tick = true;
      // Run the process scan repeatedly (base cadence, stretched while idle)
      loop {
        *clone.last_scan.lock().unwrap_or_else(|e| e.into_inner()) = std::time::Instant::now();
        let mut detected = match clone.scan_for_processes() {
          Ok(detected) => detected,
          Err(err) => {
            warn!(
              "[Process Scanner] Error while scanning processes, retrying: {}",
              err
            );
            wait_scan(wait_time);
            continue;
          }
        };
        // Coexistence filter: ignored app IDs behave as absent, so a richer
        // publisher elsewhere owns the slot (clears flow normally).
        let before = detected.len();
        detected = apply_ignore_list(detected, &clone.ignored_ids);
        if detected.len() != before {
          debug!(
            "[Process Scanner] Ignored {} detected game(s)",
            before - detected.len()
          );
        }
        // First-tick liveness proof (INFO, once per boot): proves the
        // loop enumerated and classified, whatever it found. A boot
        // with games running that reports 0 here is a wedged scan,
        // not an idle one — distinguishable without debug builds.
        if first_tick {
          first_tick = false;
          log!(
            "[Process Scanner] First tick complete: {} game(s)",
            detected.len()
          );
        }
        // First sightings this boot, at INFO: without this, a daemon
        // whose bridge path goes quiet is indistinguishable from a
        // blind scanner except with a debug build. Bounded: one line
        // per game id per boot, same cadence as bridge publishes.
        for game in first_sightings(&mut seen_ids, &detected) {
          log!(
            "[Process Scanner] Detected: {} ({})",
            game.entry.name,
            game.entry.id
          );
        }
        // Track live game pids for the proc-events watcher: only THEIR
        // exits wake us early (a build storm's exits never cause a scan).
        // Wholesale replace (not merge): a watcher insert racing this
        // write can be dropped, but the next tick re-derives it from the
        // live table while the game still runs — and a lost EXIT entry
        // heals the same way. Bounded staleness (≤1 tick), no growth,
        // no liveness probe per entry.
        *clone
          .detected_pids
          .lock()
          .unwrap_or_else(|e| e.into_inner()) = detected.iter().map(|game| game.pid).collect();
        // Forward on change only (see `last_emitted`): identical tables
        // are already fully represented downstream (`note_scan` entries
        // persist, `last_process` dedups), so skipping them changes
        // nothing observable — it just stops waking the bridge thread.
        // The EXEC fast path can publish a game no poll ever observes
        // (sub-tick lifetime + missed EXIT): `scan_dirty` forces one full
        // emission after any EXEC publication, so its clear still flows.
        let mut snapshot: Vec<(String, u64)> = detected
          .iter()
          .map(|game| (game.entry.id.to_string(), game.pid))
          .collect();
        snapshot.sort();
        let changed = snapshot != last_emitted;
        let forced = clone
          .scan_dirty
          .swap(false, std::sync::atomic::Ordering::AcqRel);
        let emit = changed || forced;
        if emit {
          last_emitted = snapshot;
        }
        // Forward the changed table, one event per slot. Downstream
        // publishes per app id and dedups repeats, so co-running games
        // each own their card instead of only the first.
        if emit && !detected.is_empty() {
          for game in &detected {
            if clone
              .event_sender
              .send(ProcessDetectedEvent {
                hit: Some(game.clone()),
              })
              .is_err()
            {
              warn!("[Process Scanner] Event receiver gone, retrying scan");
              wait_scan(wait_time);
              continue;
            }
          }
        }

        // If there are no detected processes, send an empty message —
        // but only on the transition into emptiness (the bridge clears
        // once and then ignores further nulls the same way). A forced
        // emission of the empty table carries the EXEC-hole clear.
        if emit && detected.is_empty() {
          let cleared = clone.event_sender.send(ProcessDetectedEvent { hit: None });
          if cleared.is_err() {
            warn!("[Process Scanner] Event receiver gone, retrying scan");
            wait_scan(wait_time);
            continue;
          }
        }

        // Idle backoff: consecutive empty ticks stretch the cadence
        // (base → 30s cap). Safe because game START arrives via EXEC
        // events instantly and EXITs of tracked games unpark us early —
        // polling only backstops what the watcher cannot (untracked
        // exits, DB refreshes). Any detection or early wake resets.
        if detected.is_empty() {
          idle_ticks = idle_ticks.saturating_add(1);
        } else {
          idle_ticks = 0;
        }
        let cadence = idle_wait(wait_time, idle_ticks);
        let wait_start = std::time::Instant::now();
        wait_scan(cadence);
        if wait_start.elapsed() < cadence.mul_f32(0.9) {
          idle_ticks = 0;
        }
      }
    });

    // Event-driven fast path (Linux): EXEC classifies one process at once,
    // EXIT of a tracked game wakes the scan above. Best-effort — setup
    // failure keeps pure polling, silently.
    #[cfg(target_os = "linux")]
    if self.enable_proc_events {
      let gauge = spawn_proc_watcher(self);
      *watch_slot
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = gauge;
    } else {
      log!("[Process Scanner] proc-events watcher disabled by configuration, polling only");
    }
  }

  /// Opt out of the event-driven proc-events watcher (called once,
  /// before [`ProcessServer::start`]). Polling continues either way.
  pub(crate) fn set_proc_events(&mut self, enable: bool) {
    self.enable_proc_events = enable;
  }

  #[cfg(not(target_os = "linux"))]
  pub(crate) fn process_list(&self) -> crate::error::Result<Vec<Exec>> {
    use std::path::Path;
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, UpdateKind};

    let mut processes = Vec::new();
    let mut sys = self.sysinfo.lock().unwrap_or_else(|e| e.into_inner());
    sys.refresh_processes_specifics(
      ProcessesToUpdate::All,
      true,
      ProcessRefreshKind::nothing()
        .with_exe(UpdateKind::OnlyIfNotSet)
        .with_cmd(UpdateKind::OnlyIfNotSet),
    );

    for proc in sys.processes() {
      let mut cmd = proc.1.cmd().iter();
      processes.push(Exec {
        pid: u64::from(proc.0.as_u32()),
        path: proc.1.exe().unwrap_or(Path::new("")).display().to_string(),
        arguments: cmd.next().map(|_| {
          cmd
            .map(|x| x.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
        }),
      });
    }

    Ok(processes)
  }

  #[cfg(target_os = "linux")]
  pub(crate) fn process_list() -> crate::error::Result<Vec<Exec>> {
    use std::fs;

    let proc_list = fs::read_dir("/proc")?.filter(|e| {
      if let Ok(entry) = e {
        // Only if we can parse this as a number (lossy: /proc names are
        // always ASCII pids; anything else is skipped, never fatal).
        return entry.file_name().to_string_lossy().parse::<u64>().is_ok();
      }

      false
    });
    let mut processes = Vec::new();

    for entry in proc_list {
      let entry = entry?;
      let path = entry.path();

      let Ok(pid) = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .parse::<u64>()
      else {
        continue;
      };
      // Same single-pid reader as the EXEC fast path: unreadable pids
      // (kernel threads, zombies, races) are skipped, never fatal.
      if let Some(exec) = read_exec(pid) {
        processes.push(exec);
      }
    }

    Ok(processes)
  }

  /// Current detection generation, shared lock-free after the clone.
  /// Scan ticks and EXEC events each hold one Arc for their whole
  /// classification, so a concurrent refresh can only swap in the NEXT
  /// fully-built generation — never a torn mix.
  #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
  pub(crate) fn bundle(&self) -> Arc<DetectablesBundle> {
    self
      .detectables
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .clone()
  }

  /// Single reversed-path AC probe (main DB, then custom overrides).
  /// Shared lookup half of the scan: direct-path and cwd-joined probes
  /// match identically through here.
  ///
  /// Test-only seam: production classifies through [`probe_variants`]
  /// (see below), which probes every path variant against main before
  /// any variant reaches custom, for strict precedence. Gated out of
  /// normal builds so the single-path order cannot drift from it.
  #[cfg(test)]
  pub(crate) fn ac_probe(
    &self,
    reversed_path: &str,
    bundle: &DetectablesBundle,
  ) -> Option<(Arc<ScannedEntry>, usize)> {
    // Same-bundle automaton + indexes: the ids this find() returns can
    // only index the table they were built with. No locks, no tearing.
    self
      .main_probe(reversed_path, bundle)
      .or_else(|| self.custom_probe(reversed_path, bundle))
  }

  /// Main-DB half of [`ProcessServer::ac_probe`]: every path variant is
  /// probed here before any variant reaches custom overrides, so a main
  /// pattern always beats a custom one regardless of 64-bit stripping.
  fn main_probe(
    &self,
    reversed_path: &str,
    bundle: &DetectablesBundle,
  ) -> Option<(Arc<ScannedEntry>, usize)> {
    let mat = bundle.ac.find(reversed_path)?;
    let exe_index = bundle.indexes[mat.pattern().as_usize()];
    Some((bundle.list[exe_index[0]].clone(), exe_index[1]))
  }

  /// Custom-overrides half of [`ProcessServer::ac_probe`].
  fn custom_probe(
    &self,
    reversed_path: &str,
    bundle: &DetectablesBundle,
  ) -> Option<(Arc<ScannedEntry>, usize)> {
    let custom_ac = bundle.custom_ac.as_ref()?;
    let mat = custom_ac.find(reversed_path)?;
    let exe_index = bundle.custom_indexes[mat.pattern().as_usize()];
    Some((bundle.custom[exe_index[0]].clone(), exe_index[1]))
  }

  /// Proton fallback probe (main DB `win32` entries on Linux): same shape
  /// as [`ProcessServer::ac_probe`], consulted only after the native
  /// patterns, user overrides and the authoritative Steam AppId all miss.
  /// Empty automaton off-Linux, so this is a cheap `None` there.
  pub(crate) fn proton_probe(
    &self,
    reversed_path: &str,
    bundle: &DetectablesBundle,
  ) -> Option<(Arc<ScannedEntry>, usize)> {
    let automaton = bundle.proton_ac.as_ref()?;
    let mat = automaton.find(reversed_path)?;
    let exe_index = bundle.proton_indexes[mat.pattern().as_usize()];
    Some((bundle.list[exe_index[0]].clone(), exe_index[1]))
  }

  /// Shared variant loop: try `path` plus its 64-bit-stripped variants
  /// against the native (`proton = false`) or Proton (`proton = true`)
  /// automaton. Precedence is per automaton, not per variant: every
  /// variant is probed against main first, then every variant against
  /// custom — so a main pattern always beats a custom one even when
  /// only a stripped variant collides (e.g. main `wow.exe` vs custom
  /// `wow64.exe`).
  fn probe_variants(
    &self,
    path: &str,
    variant_bufs: &mut [String; 5],
    reversed_path: &mut String,
    bundle: &DetectablesBundle,
    proton: bool,
  ) -> Option<(Arc<ScannedEntry>, usize)> {
    let variant_count = path_variants_into(path, variant_bufs);
    // Automaton passes outer, variants inner (strict precedence). Without
    // user overrides there is no custom automaton: run a single pass
    // instead of probing a guaranteed-`None` second automaton per variant.
    let passes: usize = if proton || bundle.custom_ac.is_none() {
      1
    } else {
      2
    };
    for pass in 0..passes {
      for variant in &variant_bufs[..variant_count] {
        reversed_path.clear();
        reversed_path.extend(variant.chars().rev());
        let found = if proton {
          self.proton_probe(reversed_path, bundle)
        } else if pass == 0 {
          self.main_probe(reversed_path, bundle)
        } else {
          self.custom_probe(reversed_path, bundle)
        };
        if found.is_some() {
          return found;
        }
      }
    }
    None
  }

  /// Classify one enumerated process, cheapest source first:
  /// native AC, bare-exe+cwd, authoritative Steam AppId, Proton (`win32`)
  /// AC, Steam install-dir, then the stem/folder heuristics — except when
  /// the launcher supplied a shortcut-range AppId (Steam-assigned
  /// non-Steam id): then stem/folder run before the install-dir. Lazy `/proc` reads (cwd,
  /// environ, stat) happen only on misses/hits respectively — never for
  /// the whole table. Extracted from the scan loop for reuse and testing;
  /// the loop itself just maps over it.
  #[hotpath::measure]
  pub(crate) fn match_process(
    &self,
    process: &Exec,
    bundle: &DetectablesBundle,
    variant_bufs: &mut [String; 5],
    reversed_path: &mut String,
    obs_open: &mut bool,
  ) -> Option<ScannedHit> {
    // Process path with consistent slashes (original case: the
    // automata match ASCII case-insensitively). Borrowed until a
    // rewrite is actually needed — the common Linux case (no
    // backslashes, absolute path) allocates nothing at all.
    let mut process_path: std::borrow::Cow<str> = std::borrow::Cow::Borrowed(&process.path);

    if process_path.contains('\\') {
      process_path = std::borrow::Cow::Owned(process_path.replace('\\', "/"));
    }

    if !process_path.starts_with('/') {
      process_path = std::borrow::Cow::Owned(format!("/{process_path}"));
    }

    // Discord exclusions first: installers, crash reporters and friends
    // are invisible before any matching (one basename lookup instead of
    // the full probe chain, and they can never shadow a real game).
    // Before the OBS flag too: an excluded process is absent, period.
    // Only the tiny basename is lowercased (the exclusion list is) — and
    // only when it actually contains uppercase ASCII, so the common
    // already-lowercase path allocates nothing at all.
    let raw_basename = process_path.rsplit('/').next().unwrap_or(&process_path);
    let basename: std::borrow::Cow<str> = if raw_basename.bytes().any(|b| b.is_ascii_uppercase()) {
      std::borrow::Cow::Owned(raw_basename.to_ascii_lowercase())
    } else {
      std::borrow::Cow::Borrowed(raw_basename)
    };
    if self
      .exclusions
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .is_excluded(&basename)
    {
      debug!("[Process Scanner] Excluded process, skipping: {basename}");
      return None;
    }

    // OBS binaries ship lowercase; the flag only feeds an unconsumed
    // observer callback, so original-case matching is exact enough.
    if !*obs_open && (process_path.contains("obs64") || process_path.contains("streamlabs")) {
      *obs_open = true;
    }

    // Aho-Corasick matching against the path and its 64-bit-stripped
    // variants (so `wow64.exe` also matches a `wow.exe` pattern, like
    // arrpc/pog5-rsrpc). First hit in variant order wins.
    let mut found = self.probe_variants(&process_path, variant_bufs, reversed_path, bundle, false);

    // Proton bare-exe probe (DOOM Eternal case): argv[0] without
    // directories plus the process cwd often reconstructs the install
    // path the DB knows. Only for bare exes (paths with directories
    // already had their full match above); one readlink per miss.
    if found.is_none()
      && let Some(exe) = bare_exe(&process_path)
      && let Some(cwd) = read_cwd(process.pid)
    {
      let candidate = format!("{cwd}/{exe}");
      debug!(
        "[Process Scanner] Bare exe, probing cwd-joined path for pid {}",
        process.pid
      );
      found = self.probe_variants(&candidate, variant_bufs, reversed_path, bundle, false);
      if found.is_some() {
        debug!("[Process Scanner] Cwd match for pid {}", process.pid);
      }
    }

    let (obj, exe_index) = match found {
      Some(found) => found,
      None => {
        // Lowercase copy for the case-sensitive tail below (store-id
        // maps, stem/folder heuristics). Paid only on misses — the hot
        // AC path above never allocates it.
        let lowered = process_path.to_ascii_lowercase();
        // The AppId comes memoized (one environ read per process
        // lifetime); cmdline fallback when environ is unreadable
        // (sandboxed Proton runtimes hide it from service contexts).
        let app_id = self.cached_app_id(process.pid);
        let (app_id, via_cmdline) = match app_id {
          Some(id) => (Some(id), false),
          // Owned copy: only paid when environ is unreadable (sandboxed
          // Proton), so the common paths stay borrow-only.
          None => (
            app_id_from_args(process.arguments.as_deref()).map(|id| id.to_string()),
            true,
          ),
        };
        if via_cmdline && let Some(id) = app_id.as_deref() {
          debug!(
            "[Process Scanner] AppId {} for pid {} from command line (environ unreadable)",
            id, process.pid
          );
        }
        // Authoritative Steam AppId first: a store id beats every fuzzy
        // path heuristic below.
        if let Some(hit) = match_steam_id(
          app_id.as_deref(),
          process.pid,
          &bundle.steam_map,
          &bundle.list,
          &bundle.custom,
        ) {
          return Some(hit);
        }
        // An AppId the launcher gave us but the database doesn't know needs
        // a second look: Steam itself assigns high-bit-set ids
        // (e.g. 2532755798) to non-Steam shortcuts, while real store ids
        // are small. A shortcut-range id is a self-identified non-Steam
        // game, so its own exe/folder name beats install location; a
        // small unknown id is a real game missing from the DB, where
        // location (Steam's ground truth) still beats name guessing.
        let non_steam = app_id.as_deref().is_some_and(is_shortcut_id);
        if non_steam
          && let Some(hit) = match_name_or_folder(
            &lowered,
            process.pid,
            &bundle.name_map,
            &bundle.name_map_nodot,
            &bundle.list,
          )
        {
          return Some(hit);
        }
        // Proton fallback: `win32` executables from the main DB. Catches
        // Wine/Proton games whose store id is unreadable and whose exe is
        // too generic for the stem/folder heuristics.
        if let Some((obj, exe_index)) =
          self.probe_variants(&process_path, variant_bufs, reversed_path, bundle, true)
        {
          return finish_direct_hit(&obj, exe_index, process);
        }
        // Steam's own word: the process runs under a known install dir,
        // so it inherits that entry's AppId. Beats name guessing below,
        // loses to a DB-declared path above.
        if let Some(library_appid) = self.steam_prefix_app_id(&lowered)
          && let Some(hit) = match_steam_id(
            Some(&library_appid),
            process.pid,
            &bundle.steam_map,
            &bundle.list,
            &bundle.custom,
          )
        {
          debug!(
            "[Process Scanner] Steam library match: {} (appid {})",
            hit.entry.name, library_appid
          );
          return Some(hit);
        }
        // Last resort: exe-stem == game name, then install-folder walk
        // (already consulted above for shortcut-range AppIds).
        if non_steam {
          return None;
        }
        return match_name_or_folder(
          &lowered,
          process.pid,
          &bundle.name_map,
          &bundle.name_map_nodot,
          &bundle.list,
        );
      }
    };

    finish_direct_hit(&obj, exe_index, process)
  }

  #[hotpath::measure]
  pub(crate) fn scan_for_processes(&self) -> crate::error::Result<Vec<ScannedHit>> {
    #[cfg(not(target_os = "linux"))]
    let processes = self.process_list()?;
    #[cfg(target_os = "linux")]
    let processes = ProcessServer::process_list()?;

    debug!("[Process Scanner] Process scan triggered");

    // Re-entrancy guard: a manual `scan_for_processes` racing the scan
    // thread (or two manual triggers) must not interleave. RAII so every
    // exit path — including `?` and panics — releases it.
    let _scan_guard = ScanGuard::try_acquire(&self.scanning).ok_or_else(|| {
      debug!("[Process Scanner] Scanning already in progress");
      crate::error::RsrpcError::Message("Scanning already in progress".to_string())
    })?;

    let mut obs_open = false;

    // One generation for the whole tick: clone the Arc once, classify
    // every process against it. A refresh landing mid-tick only swaps in
    // the next bundle, which this tick simply won't see — no torn reads.
    let bundle = self
      .detectables
      .lock()
      .map_err(|e| crate::error::RsrpcError::Poisoned("detectables", e.to_string()))?
      .clone();

    // Steam generation marker: one stat per watched libraryfolders.vdf;
    // the parse itself runs only when something actually changed.
    self.refresh_steam_libraries();

    // Drop memoized AppIds of dead pids (pid reuse must never serve a
    // stale id): one set build + retain per tick, replacing hundreds of
    // kilobyte environ re-reads.
    self.sweep_dead_appids(&processes)?;

    let mut reversed_path = String::with_capacity(256);
    // Variant scratch space, reused for every process: the scan allocates
    // nothing per process at steady state (see path_variants_into).
    let mut variant_bufs: [String; 5] = Default::default();

    let mut detected_list: Vec<ScannedHit> = processes
      .iter()
      .filter_map(|process| {
        self.match_process(
          process,
          &bundle,
          &mut variant_bufs,
          &mut reversed_path,
          &mut obs_open,
        )
      })
      .collect();

    let callback = self
      .event_listeners
      .lock()
      .map_err(|e| crate::error::RsrpcError::Poisoned("event_listeners", e.to_string()))?
      .on_process_scan_complete
      .clone();

    if let Some(callback) = callback.as_ref() {
      callback
        .lock()
        .map_err(|e| crate::error::RsrpcError::Poisoned("process callback", e.to_string()))?(
        ProcessScanState { obs_open },
      );
    }

    detected_list.shrink_to_fit();

    debug!("[Process Scanner] Process scan complete");

    Ok(detected_list)
  }
}

/// Sleep the scan cadence. `park_timeout` (not `sleep`) so the proc-events
/// watcher can wake the loop early when a tracked game exits; a permit
/// stored by an unpark during scan work just causes one early rescan,
/// which downstream dedups harmlessly.
fn wait_scan(wait_time: Duration) {
  std::thread::park_timeout(wait_time);
}

/// Idle-stretched cadence: base × 2^idle_ticks, capped at 30s
/// (5s → 10s → 20s → 30s at the default base). Overflow-safe.
pub(crate) fn idle_wait(base: Duration, idle_ticks: u32) -> Duration {
  const MAX_BACKOFF: Duration = Duration::from_secs(30);
  let stretched = base
    .checked_mul(1 << idle_ticks.min(4))
    .unwrap_or(MAX_BACKOFF);
  stretched.min(MAX_BACKOFF)
}

/// Spawn the netlink dispatch (Linux): EXEC classifies one process and
/// emits hits at once; EXIT of a tracked game unparks the scan loop for
/// an immediate natural clear. Setup failure (or a dead receiver on
/// shutdown) ends the thread quietly — polling carries on.
/// Spawn the netlink watcher + dispatch threads. Returns the shared watch
/// backlog gauge so the resource census can read it (the pair itself stays
/// inside: `start` hands the gauge up to the daemon).
#[cfg(target_os = "linux")]
fn spawn_proc_watcher(server: &ProcessServer) -> super::utils::QueueGauge {
  use super::proc_events::{ProcEvent, watch};
  use super::utils::QueueGauge;

  let (tx, rx) = QueueGauge::pair();
  let gauge = tx.gauge();
  let dispatch = server.clone();
  std::thread::spawn(move || {
    let mut variant_bufs: [String; 5] = Default::default();
    let mut reversed_path = String::with_capacity(256);
    while let Ok(event) = rx.recv() {
      match event {
        ProcEvent::Exec(pid) => {
          let Some(exec) = read_exec(pid) else {
            debug!("[Process Scanner] exec event: pid {pid} unreadable, skipping");
            continue;
          };
          // Same pid, new image: the memoized AppId may be stale.
          dispatch.drop_appid(pid);
          // One generation for the whole classification: a refresh
          // landing mid-probe can only swap in the next bundle, which
          // this event simply won't see.
          let bundle = dispatch.bundle();
          let mut obs_open = false;
          if let Some(hit) = dispatch.match_process(
            &exec,
            &bundle,
            &mut variant_bufs,
            &mut reversed_path,
            &mut obs_open,
          ) {
            let game_pid = hit.pid;
            debug!(
              "[Process Scanner] exec event: pid {pid} matched {}",
              hit.entry.name
            );
            dispatch
              .detected_pids
              .lock()
              .unwrap_or_else(|e| e.into_inner())
              .insert(game_pid);
            // Mark the scan dirty: this publication bypasses the polling
            // snapshot, so the next tick must emit its full table even if
            // unchanged — otherwise a game that dies before any poll
            // observes it would never emit its clear (see `scan_dirty`).
            dispatch
              .scan_dirty
              .store(true, std::sync::atomic::Ordering::Release);
            // Receiver gone means shutdown: end the thread, polling dies
            // with the daemon anyway.
            if dispatch
              .event_sender
              .send(ProcessDetectedEvent { hit: Some(hit) })
              .is_err()
            {
              break;
            }
          }
        }
        ProcEvent::Exit(pid) => {
          // Only tracked games MAY wake the scan — decided in one place
          // so the debounce is unit-testable (see below).
          if dispatch.should_wake_on_exit(pid)
            && let Some(thread) = dispatch
              .scan_wake
              .lock()
              .unwrap_or_else(|e| e.into_inner())
              .as_ref()
          {
            thread.unpark();
          }
        }
      }
    }
  });
  // Best-effort watcher with periodic resubscribe: a failed self-test
  // (or a mid-run socket death) falls back to polling, but a transient
  // kernel stall must not pin polling until the next daemon restart —
  // delivery has been observed to resume on its own. First failure warns
  // (the documented sandbox diagnosis), later ones stay in debug so
  // genuinely unsupported systems do not log every 5 minutes forever.
  // A sleeping retry never delays shutdown: process exit does not wait
  // for this thread.
  std::thread::spawn(move || {
    let mut attempts = 0u32;
    loop {
      match watch(tx.clone()) {
        // Receiver gone: daemon shutting down.
        Ok(()) => break,
        Err(err) => {
          attempts = attempts.saturating_add(1);
          if attempts == 1 {
            warn!("[Process Scanner] proc-events unavailable ({err}), polling only; retrying");
          } else {
            debug!("[Process Scanner] proc-events still unavailable ({err}), polling only");
          }
          std::thread::sleep(std::time::Duration::from_secs(5 * 60));
        }
      }
    }
  });
  gauge
}

/// Read one process's cmdline into an `Exec` (Linux). `None` for kernel
/// threads, zombies, vanished or unreadable pids — the caller just skips
/// them; the periodic scan is the backstop.
///
/// Bounded: at most 64 KiB are read (argv[0] lives at the start, so
/// truncation only ever cuts late arguments, never the path). Adversarial
/// megabyte-cmdlines would otherwise multiply per process per tick.
#[cfg(target_os = "linux")]
pub(crate) fn read_exec(pid: u64) -> Option<Exec> {
  const MAX_CMDLINE_BYTES: u64 = 64 * 1024;
  let path = format!("/proc/{pid}/cmdline");
  // NOTE: no metadata size check here — /proc files report st_size 0
  // despite having content; an early `len() == 0` return would skip
  // EVERY process (total detection blindness).
  let file = std::fs::File::open(&path).ok()?;
  let mut cmdline = Vec::new();
  use std::io::Read;
  file
    .take(MAX_CMDLINE_BYTES + 1)
    .read_to_end(&mut cmdline)
    .ok()?;
  if cmdline.is_empty() {
    return None;
  }
  cmdline.truncate(usize::try_from(MAX_CMDLINE_BYTES).unwrap_or(usize::MAX));
  // Truncation may split a multibyte char: back off to the boundary
  // in one step (never rescan: `valid_up_to` is the split point).
  if let Err(err) = std::str::from_utf8(&cmdline) {
    cmdline.truncate(err.valid_up_to());
  }
  let cmdline = String::from_utf8(cmdline).ok()?;
  let mut cmd_iter = cmdline.split('\0');
  let (cmd_path, cmd_args) = (
    cmd_iter.next().unwrap_or("").to_string(),
    cmd_iter.collect::<Vec<_>>().join(" "),
  );
  Some(Exec {
    pid,
    path: cmd_path,
    arguments: if cmd_args.is_empty() {
      None
    } else {
      Some(cmd_args)
    },
  })
}

fn os_matches(os: &str) -> bool {
  match std::env::consts::OS {
    "windows" => os == "win32",
    "macos" => os == "darwin",
    "linux" => os == "linux",
    _ => true,
  }
}

/// Return one-time parse arenas (fetch body, JSON DOM, trimmed copy,
/// automaton build scratch) to the OS. Steady state is the lean
/// structures only; without this, the allocator holds the startup spike
/// as RSS indefinitely. Called after the initial build and every hourly
/// rebuild (refresh cadence itself is unchanged).
fn release_parse_arenas() {
  release_platform_arenas();
}

/// Linux (glibc/musl).
#[cfg(target_os = "linux")]
fn release_platform_arenas() {
  // SAFETY: malloc_trim only advises the allocator to release free pages;
  // it cannot invalidate live allocations, so it is always safe to call.
  unsafe {
    libc::malloc_trim(0);
  }
}

/// macOS: drain purgeable memory in all zones.
#[cfg(target_os = "macos")]
fn release_platform_arenas() {
  // Declared locally: libc 0.2 exposes only the zone-struct field, not
  // this stable Darwin function (malloc/malloc.h, present since 10.6).
  unsafe extern "C" {
    fn malloc_zone_pressure_relief(zone: *mut libc::c_void, goal: libc::size_t) -> libc::size_t;
  }
  // SAFETY: (NULL, 0) means "all zones, no goal" and is advisory-only;
  // it cannot invalidate live allocations.
  unsafe {
    malloc_zone_pressure_relief(std::ptr::null_mut(), 0);
  }
}

/// Windows: compact our own process heap.
#[cfg(target_os = "windows")]
fn release_platform_arenas() {
  // SAFETY: HeapCompact with flags=0 only coalesces free blocks of the
  // given heap; it cannot invalidate live allocations.
  unsafe {
    let heap = winapi::um::heapapi::GetProcessHeap();
    if !heap.is_null() {
      winapi::um::heapapi::HeapCompact(heap, 0);
    }
  }
}

/// Other platforms: nothing to release through a stable API.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn release_platform_arenas() {}

/// Read `SteamAppId` from `/proc/<pid>/environ` (set by Steam/Proton for
/// every game process it launches, including non-Steam shortcuts).
/// Callers read this lazily: environ is the most expensive read of the
/// scan, so only misses pay for it.
#[cfg(target_os = "linux")]
fn read_steam_app_id(pid: u64) -> Option<String> {
  // Bounded: 256 KiB covers any legitimate environ (SteamAppId lives
  // near the front); larger means hostile or broken, and reading it
  // whole per new pid would multiply per tick.
  const MAX_ENVIRON_BYTES: u64 = 256 * 1024;
  let path = format!("/proc/{pid}/environ");
  // NOTE: no metadata size check — /proc files report st_size 0 (see above).
  let file = std::fs::File::open(&path).ok()?;
  let mut env = Vec::new();
  use std::io::Read;
  file
    .take(MAX_ENVIRON_BYTES + 1)
    .read_to_end(&mut env)
    .ok()?;
  env.truncate(usize::try_from(MAX_ENVIRON_BYTES).unwrap_or(usize::MAX));
  for entry in env.split(|b| *b == 0) {
    if let Some(id) = entry.strip_prefix(b"SteamAppId=")
      && let Ok(id) = std::str::from_utf8(id)
    {
      let id = id.trim();
      if !id.is_empty() {
        return Some(id.to_string());
      }
    }
  }
  None
}

/// No environ AppIds outside Linux (Steam matching there was already
/// limited to the exe/name paths).
#[cfg(not(target_os = "linux"))]
fn read_steam_app_id(_pid: u64) -> Option<String> {
  None
}

/// Steam AppId from a process command line (`reaper SteamLaunch
/// AppId=4508340 ...`): fallback when `/proc/<pid>/environ` is
/// unreadable. Sandboxed Proton runtimes (pressure-vessel/bwrap) hide
/// environ from service contexts while cmdline stays readable — without
/// this, every such game is invisible to automatic detection. Same trust
/// as environ (both launcher-provided, display-only use): the token must
/// stand alone (`AppId=` at a word boundary, followed by digits).
pub(crate) fn app_id_from_args(arguments: Option<&str>) -> Option<&str> {
  const TOKEN: &str = "AppId=";
  let args = arguments?;
  let mut rest = args;
  while let Some(pos) = rest.find(TOKEN) {
    let boundary = pos == 0
      || rest[..pos]
        .chars()
        .next_back()
        .is_none_or(|c| !c.is_ascii_alphanumeric());
    rest = &rest[pos + TOKEN.len()..];
    if !boundary {
      continue;
    }
    // Borrow the digit run instead of collecting it: ASCII digits are
    // single-byte, so the byte count is always a char boundary.
    let len = rest.bytes().take_while(u8::is_ascii_digit).count();
    if len > 0 {
      return Some(&rest[..len]);
    }
  }
  None
}

/// Whether a process is currently suspended (`T` state: SIGSTOP'd, e.g. a
/// paused game). Suspended games show a frozen frame (or nothing) — they
/// are not being played, so matches on them are discarded and the scan
/// treats them as absent (clears). Checked lazily, only for matched
/// processes: one tiny `stat` read per hit, never per scan.
#[cfg(target_os = "linux")]
pub(crate) fn is_suspended(pid: u64) -> bool {
  // Capped like every other `/proc` read (`stat` is tiny, but the cap
  // documents the rule has no exceptions).
  use std::io::Read;
  let mut stat = String::new();
  std::fs::File::open(format!("/proc/{pid}/stat"))
    .and_then(|file| file.take(8 * 1024).read_to_string(&mut stat).map(|_| stat))
    .ok()
    .and_then(|stat| parse_stat_state(&stat))
    .is_some_and(|state| state == 'T' || state == 't')
}

/// Stubbed on non-Linux (Steam matching there is already limited).
#[cfg(not(target_os = "linux"))]
fn is_suspended(_pid: u64) -> bool {
  false
}

/// Process state from `/proc/<pid>/stat`: the field right after the last
/// `)` (comm may itself contain spaces and parens). `None` when unreadable
/// or malformed — never counted as suspended. Linux-only like its sole
/// caller: without `/proc` there is nothing to parse.
#[cfg(target_os = "linux")]
#[inline]
pub(crate) fn parse_stat_state(stat: &str) -> Option<char> {
  stat
    .rfind(')')
    .and_then(|end| stat[end + 1..].split_whitespace().next())
    .and_then(|state| state.chars().next())
}

/// Lowercase-trim a name for map keys and lookups. ASCII-only by
/// design: every lookup side lowercases ASCII too, and the AC automaton
/// matches `ascii_case_insensitive` — one consistent rule for a
/// Windows-centric database, instead of half the paths folding Unicode
/// and the other half not.
///
/// Punctuation forbidden in Windows filenames (`: ? " < > | * / \`)
/// is dropped (via a space, runs collapsed): no real folder or exe stem
/// can ever contain those characters, so a DB title like `Name:
/// Subtitle` still matches its on-disk `Name Subtitle` folder — while
/// the multi-word/length gate keeps generic collisions out exactly as
/// before.
#[inline]
pub(crate) fn normalize_name(name: &str) -> String {
  const FORBIDDEN: [char; 9] = ['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
  name
    .trim()
    .to_ascii_lowercase()
    .replace(&FORBIDDEN[..], " ")
    .split_whitespace()
    .collect::<Vec<_>>()
    .join(" ")
}

/// Conservative gate for the exe-stem fallback: exact, multi-word names with
/// a minimum length. Keeps generic stems (`fish`, `steam`, `game`, `reaper`)
/// from ever matching same-named DB entries.
#[inline]
pub(crate) fn name_matchable(normalized: &str) -> bool {
  normalized.contains(' ') && normalized.chars().count() >= 6
}

/// Executable stem of an already-normalized (`/`-separated, lowercase) path,
/// without extension: `/games/how to fish.exe` -> `how to fish`. Borrowed:
/// callers only compare it against the map.
#[inline]
pub(crate) fn exe_stem(normalized_path: &str) -> &str {
  let base = normalized_path
    .rsplit('/')
    .next()
    .unwrap_or(normalized_path);
  match base.rfind('.') {
    Some(dot) if dot > 0 => &base[..dot],
    _ => base,
  }
}

/// Drop matches on suspended processes (see [`is_suspended`]): a SIGSTOP'd
/// game shows a frozen frame at best — it is not being played. Single
/// choke point for every aux hit (appid/stem/folder), mirroring the scan
/// loop's post-match check for AC hits. Stamps the observation onto the
/// shared slim entry (no clone).
fn live_or_none(obj: &Arc<ScannedEntry>, pid: u64) -> Option<ScannedHit> {
  if is_suspended(pid) {
    debug!("[Process Scanner] Ignoring suspended process (pid {pid})");
    return None;
  }
  Some(ScannedHit::stamp(obj.clone(), pid))
}

/// First sightings this boot: entries of `detected` not yet in `seen`
/// (which is updated in place). The scan loop logs these at INFO so a
/// silent daemon is distinguishable from a blind one without a debug
/// build; the bridge still owns publish/dedup logging downstream.
pub(crate) fn first_sightings<'a>(
  seen: &mut HashSet<String>,
  detected: &'a [ScannedHit],
) -> Vec<&'a ScannedHit> {
  detected
    .iter()
    .filter(|game| seen.insert(game.entry.id.to_string()))
    .collect()
}

/// Auxiliary lookup maps over the main DB:
/// Steam store id -> activity index, normalized game name -> activity index.
/// Alternative titles (`aliases`) join the name map under the same
/// conservative gate — exact, multi-word only — so a generic alias can
/// never collide; canonical names are inserted first and win ties.
pub(crate) fn build_aux_maps(detectables: &[Arc<ScannedEntry>]) -> (SortedIndex, SortedIndex) {
  let mut steam: Vec<(Box<str>, usize)> = Vec::new();
  let mut names: Vec<(Box<str>, usize)> = Vec::new();

  for (index, activity) in detectables.iter().enumerate() {
    for id in &activity.steam_ids {
      if !id.is_empty() {
        steam.push((id.clone(), index));
      }
    }

    // Only matchable names participate in the exe-stem fallback, so generic
    // stems (`fish`, `steam`, `game`) can never collide with `Fish`/`Steam`.
    let normalized = normalize_name(&activity.name);
    if name_matchable(&normalized) {
      names.push((normalized.into_boxed_str(), index));
    }
    for alias in &activity.aliases {
      let normalized = normalize_name(alias);
      if name_matchable(&normalized) {
        names.push((normalized.into_boxed_str(), index));
      }
    }
  }

  let (steam_map, name_map) = (SortedIndex::build(steam), SortedIndex::build(names));
  log!(
    "[Process Scanner] Aux maps: {} steam ids, {} matchable names",
    steam_map.len(),
    name_map.len()
  );
  (steam_map, name_map)
}

/// De-dotted form for the folder-walk fallback tier: dots become
/// spaces (runs collapsed), so `R.E.P.O. Ghost Haul` compares equal on
/// the map-key side and the on-disk folder side alike.
fn dedot(name: &str) -> String {
  name
    .replace('.', " ")
    .split_whitespace()
    .collect::<Vec<_>>()
    .join(" ")
}

/// De-dotted twin of the name map, for the folder-walk fallback (see
/// [`match_name_or_folder`]): only keys that actually contain dots, so
/// the extra table stays tiny. Built in canonical order (lowest index
/// wins ties), exactly like the main map.
pub(crate) fn undotted_names(name_map: &SortedIndex) -> SortedIndex {
  let mut dotted: Vec<(usize, Box<str>)> = name_map
    .iter()
    .filter(|(key, _)| key.contains('.'))
    .map(|(key, index)| (*index, key.clone()))
    .collect();
  // Lowest index first: `SortedIndex::build` keeps the first of each
  // duplicate key, so canonical entries win ties exactly like before.
  dotted.sort_by_key(|(index, _)| *index);
  SortedIndex::build(
    dotted
      .into_iter()
      .map(|(index, key)| (dedot(&key).into_boxed_str(), index))
      .collect(),
  )
}

/// Drop scan results whose application id is ignored, preserving order.
/// Pure: an ignored-only scan yields an empty vec, which the scan thread
/// already turns into the normal null event (clear). Tested below.
pub(crate) fn apply_ignore_list(
  detected: Vec<ScannedHit>,
  ignored_ids: &HashSet<String>,
) -> Vec<ScannedHit> {
  if ignored_ids.is_empty() {
    return detected;
  }
  detected
    .into_iter()
    .filter(|game| {
      let id: &str = &game.entry.id;
      !ignored_ids.contains(id)
    })
    .collect()
}

/// Shared tail of every direct path hit (native or Proton): when the
/// database declares `arguments` for an executable, the process command
/// line must contain them (parity with arrpc/pog5-rsrpc, e.g. TF2
/// `-game tf`); then the suspended check + timestamp stamp via
/// `live_or_none`.
fn finish_direct_hit(
  obj: &Arc<ScannedEntry>,
  exe_index: usize,
  process: &Exec,
) -> Option<ScannedHit> {
  // A hit without executables (or a stale index) is corrupt input, not a
  // game: skip the process instead of panicking the scan.
  let executable = obj.executables.get(exe_index)?;

  if let Some(exec_args) = &executable.arguments {
    let has_args = process
      .arguments
      .as_ref()
      .is_some_and(|args| args.contains(exec_args));
    if !has_args {
      debug!(
        "[Process Scanner] Argument mismatch for pid {}, skipping",
        process.pid
      );
      return None;
    }
  }

  live_or_none(obj, process.pid)
}

/// Steam's non-Steam shortcut range: ids Steam itself assigns when a
/// shortcut is added carry the high bit (`crc32(exe + name) | 0x80000000`,
/// e.g. 2532755798 for "How to Fish" — verified against a live
/// `shortcuts.vdf`); real store ids are small. Unknown to the DB +
/// shortcut-range means a self-identified non-Steam game.
#[inline]
pub(crate) fn is_shortcut_id(app_id: &str) -> bool {
  app_id.parse::<u32>().is_ok_and(|id| id & 0x8000_0000 != 0)
}

/// Authoritative aux lookup: `SteamAppId` (store games with empty
/// `executables`, legit Steam). Runs before every fuzzy heuristic — a
/// store id beats path guessing. Custom overrides participate as a
/// linear fallback (they are user-sized, not DB-sized): a custom entry
/// carrying only a steam distributor id matches here, since the map
/// only indexes the main list. Canonical map hits always win.
pub(crate) fn match_steam_id(
  steam_app_id: Option<&str>,
  pid: u64,
  steam_map: &SortedIndex,
  detectable_list: &[Arc<ScannedEntry>],
  custom: &[Arc<ScannedEntry>],
) -> Option<ScannedHit> {
  let appid = steam_app_id?;
  if let Some(&idx) = steam_map.get(appid) {
    let obj = detectable_list.get(idx)?;
    debug!(
      "[Process Scanner] Steam match: {} (appid {})",
      obj.name, appid
    );
    return live_or_none(obj, pid);
  }
  // Custom override with a bare steam SKU (no executables to index).
  for obj in custom {
    let sku_match = obj.steam_ids.iter().any(|id| {
      let sid: &str = id;
      sid == appid
    });
    if sku_match {
      debug!(
        "[Process Scanner] Steam match (custom): {} (appid {})",
        obj.name, appid
      );
      return live_or_none(obj, pid);
    }
  }
  None
}
/// Heuristic aux lookup: exact exe-stem == multi-word
/// game name, then the install-folder walk. Runs after the Proton AC probe
/// in the scan loop — a DB-declared path (even `win32`) beats guessing.
pub(crate) fn match_name_or_folder(
  process_path: &str,
  pid: u64,
  name_map: &SortedIndex,
  name_map_nodot: &SortedIndex,
  detectable_list: &[Arc<ScannedEntry>],
) -> Option<ScannedHit> {
  let stem = exe_stem(process_path);
  if name_matchable(stem)
    && let Some(&idx) = name_map.get(stem)
    && let Some(obj) = detectable_list.get(idx)
  {
    debug!(
      "[Process Scanner] Name match: {} (exe stem `{}`)",
      obj.name, stem
    );
    return live_or_none(obj, pid);
  }

  // Install-folder fallback (Hydra / non-Steam shortcuts / renamed exes):
  // the folder often carries the title when the exe doesn't, e.g.
  // `.../Meccha Chameleon/MECCHA CHAMELEON/Chameleon/Binaries/Win64/
  // PenguinHotel-Win64-Shipping.exe`. Nearest ancestor wins; the map only
  // holds multi-word names, so generic folders (`binaries`, `win64`) and
  // single-word ones (`chameleon`) can never hit. Dotted components are
  // versions/hidden dirs, never titles.
  for component in process_path.rsplit('/').skip(1) {
    if component.contains('.') {
      continue;
    }
    let folder = normalize_name(component);
    if name_matchable(&folder)
      && let Some(&idx) = name_map.get(&folder)
      && let Some(obj) = detectable_list.get(idx)
    {
      debug!(
        "[Process Scanner] Folder match: {} (folder `{}`)",
        obj.name, folder
      );
      return live_or_none(obj, pid);
    }
  }

  // Last tier: dotted titles (`R.E.P.O.`, `Q.U.B.E.`, `Mr. Bomber`).
  // Version/hidden-dir components can never match (single-word or short
  // after de-dotting, still gated) — but a dotted title folder can, so
  // compare de-dotted against the de-dotted twin map. Strictly after the
  // exact pass above, so exact matches always win.
  for component in process_path.rsplit('/').skip(1) {
    if !component.contains('.') {
      continue;
    }
    let folder = normalize_name(&dedot(component));
    if name_matchable(&folder)
      && let Some(&idx) = name_map_nodot.get(&folder)
      && let Some(obj) = detectable_list.get(idx)
    {
      debug!(
        "[Process Scanner] Folder match (de-dotted): {} (folder `{}`)",
        obj.name, folder
      );
      return live_or_none(obj, pid);
    }
  }

  None
}

/// Outcome of one conditional refresh: either the database changed (new
/// ETag + parsed activities), its bytes are identical (new ETag, same
/// bytes: CDN etags flap without content changes), its trimmed content is
/// identical (new ETag, different bytes, same games: volatile CDN
/// whitespace/ordering/metadata), or the server said 304 (keep everything).
/// Both hashes always travel with the outcome, so the next check's
/// fast paths stay armed no matter which arm produced them.
pub(crate) enum FetchOutcome {
  Unchanged,
  SameContent {
    etag: Option<String>,
    content_hash: u64,
    trimmed_hash: u64,
  },
  Updated {
    etag: Option<String>,
    content_hash: u64,
    trimmed_hash: u64,
    detectable: Vec<DetectableActivity>,
  },
}

/// Content hash for change detection: see [`crate::detection::body_hash`].
pub(crate) use crate::detection::body_hash;

/// Fetch Discord's detection exclusions (installer/crash-reporter names +
/// regex patterns). Tiny payload (a few KB): plain GET with a 1 MiB cap, no
/// ETag dance — the hourly cadence dominates the cost, and parsing is
/// `tolerant by design` (see [`parse_exclusions`]).
pub(crate) fn fetch_exclusions(url: &str) -> crate::error::Result<Exclusions> {
  let body = crate::http_agent(std::time::Duration::from_secs(30))
    .get(url)
    .call()?
    .into_body()
    .with_config()
    .limit(1024 * 1024)
    .read_to_string()?;
  Ok(parse_exclusions(&body))
}

/// Fetch the detectable games database, skipping the download when it has
/// not changed since `etag` (Discord answers `304`, `ETag` + `max-age=3600`
/// line up with the hourly cadence). A 304 costs one header round trip and
/// zero parsing, so idle hours leave RSS untouched.
pub(crate) fn fetch_detectable_etag(
  url: &str,
  etag: Option<&str>,
  known_raw: Option<u64>,
  known_trimmed: Option<u64>,
) -> crate::error::Result<FetchOutcome> {
  let mut request = crate::http_agent(std::time::Duration::from_secs(30)).get(url);
  if let Some(tag) = etag {
    request = request.header("If-None-Match", tag);
  }
  let response = request.call()?;
  if response.status().as_u16() == 304 {
    return Ok(FetchOutcome::Unchanged);
  }
  let etag = response
    .headers()
    .get("etag")
    .and_then(|value| value.to_str().ok())
    .map(str::to_string);
  let body = response
    .into_body()
    .with_config()
    .limit(64 * 1024 * 1024)
    .read_to_string()?;

  // Same bytes under a new tag (CDN etag flaps): skip everything below —
  // no parse, no rebuild. This is where the retained memory comes from,
  // not the download.
  let content_hash = body_hash(&body);
  if known_raw.is_some_and(|known| known == content_hash) {
    return Ok(FetchOutcome::SameContent {
      etag,
      content_hash,
      // Same bytes hash to the same canonical form: the trimmed value
      // is whatever it was when these bytes were last seen.
      trimmed_hash: known_trimmed.unwrap_or(content_hash),
    });
  }

  // Direct parse first: serde skips unknown fields, so the full body
  // parses with zero DOM overhead (~5x less transient memory than the
  // trimmed-Value pass). The trimming pass stays as fallback for entries
  // missing required fields (it defaults them). The first error is
  // reused below: re-parsing the same body a third time just to produce
  // an identical error would double the failure cost for nothing.
  let parsed = match serde_json::from_str::<Vec<DetectableActivity>>(&body) {
    Ok(parsed) => parsed,
    Err(first_err) => {
      if let Ok(trimmed) = super::super::detection::trim_detectable_value(&body)
        && let Ok(parsed) = serde_json::from_value::<Vec<DetectableActivity>>(trimmed)
      {
        parsed
      } else {
        return Err(first_err.into());
      }
    }
  };
  // The raw body has served its purposes (raw hash above, parse input
  // here): drop its ~13MB BEFORE the canonical hash and the automata
  // build below, instead of letting it ride along to the end of this
  // function and inflating the rebuild peak.
  drop(body);
  finish_fetch(etag, content_hash, known_trimmed, parsed)
}

/// Hash the canonical scanner projection and decide whether the parsed
/// games actually changed: volatile CDN bytes (whitespace, metadata the
/// scanner never reads) hash identically, so those hours skip the
/// automaton rebuild entirely. Entry order is preserved (first-wins
/// index ties depend on it), so a pure reorder rebuilds.
fn finish_fetch(
  etag: Option<String>,
  content_hash: u64,
  known_trimmed: Option<u64>,
  detectable: Vec<DetectableActivity>,
) -> crate::error::Result<FetchOutcome> {
  let trimmed_hash = super::super::detection::canonical_content_hash(&detectable);
  if known_trimmed.is_some_and(|known| known == trimmed_hash) {
    return Ok(FetchOutcome::SameContent {
      etag,
      content_hash,
      trimmed_hash,
    });
  }
  Ok(FetchOutcome::Updated {
    etag,
    content_hash,
    trimmed_hash,
    detectable,
  })
}

/// Generate matching variants of a process path, removing 64-bit markers
/// (parity with arrpc/pog5-rsrpc). E.g. `/games/wow64.exe` produces
/// `/games/wow.exe` which matches a `wow.exe` database entry.
///
/// Writes into caller-owned buffers and returns how many are filled, so the
/// per-process scan allocates nothing at steady state (buffers are reused
/// across processes and scans; only marker hits allocate one temp string).
pub(crate) fn path_variants_into(path: &str, out: &mut [String; 5]) -> usize {
  out[0].clear();
  out[0].push_str(path);
  let mut count = 1;
  for marker in ["64", ".x64", "x64", "_64"] {
    if !path.contains(marker) {
      continue;
    }
    let variant = path.replace(marker, "");
    if variant != path && !out[..count].contains(&variant) && count < out.len() {
      out[count].clear();
      out[count].push_str(&variant);
      count += 1;
    }
  }
  count
}

/// Exe file name when the normalized argv[0] carries no directories
/// (bare exe, e.g. some Proton launches): `None` when directories are
/// present (the direct-path match covers those) or the name is empty.
#[inline]
pub(crate) fn bare_exe(normalized_path: &str) -> Option<&str> {
  let trimmed = normalized_path.strip_prefix('/').unwrap_or(normalized_path);
  if trimmed.is_empty() || trimmed.contains('/') {
    return None;
  }
  Some(trimmed)
}

/// Process working directory via `/proc/<pid>/cwd` (one readlink, no file
/// content). Proton launches with the game dir as cwd, so joining it with
/// a bare exe reconstructs the install path the DB knows (DOOM Eternal
/// case). Read lazily: only bare-exe misses pay for it.
#[cfg(target_os = "linux")]
fn read_cwd(pid: u64) -> Option<String> {
  let cwd = std::fs::read_link(format!("/proc/{pid}/cwd")).ok()?;
  Some(cwd.to_str()?.to_ascii_lowercase())
}

/// No cwd outside Linux (Steam matching there is already limited).
#[cfg(not(target_os = "linux"))]
fn read_cwd(_pid: u64) -> Option<String> {
  None
}

fn build_ac_patterns(
  detectables: &[Arc<ScannedEntry>],
) -> Result<(AhoCorasick, Vec<[usize; 2]>), aho_corasick::BuildError> {
  build_ac_patterns_with_os_filter(detectables, true)
}

fn build_ac_patterns_allow_all_os(
  detectables: &[Arc<ScannedEntry>],
) -> Result<(AhoCorasick, Vec<[usize; 2]>), aho_corasick::BuildError> {
  build_ac_patterns_with_os_filter(detectables, false)
}

/// Build one self-consistent detection generation from slim entries:
/// every automaton, index table, list and aux map derived from the same
/// inputs. The caller swaps the resulting bundle in with a single pointer
/// write — readers never observe a torn mix, no matter when the refresh
/// lands. Callers convert full [`DetectableActivity`] inputs once via
/// [`ScannedEntry::from_activity`] before calling.
fn build_bundle(
  detectable: Vec<Arc<ScannedEntry>>,
  custom: Vec<Arc<ScannedEntry>>,
) -> Result<DetectablesBundle, aho_corasick::BuildError> {
  let (ac, idx) = build_ac_patterns(&detectable)?;
  let (proton_ac, proton_idx) = build_proton_ac_patterns(&detectable)?;
  log!(
    "[Process Scanner] Automata heap: native {} bytes, proton {} bytes",
    ac.memory_usage(),
    proton_ac.as_ref().map(|ac| ac.memory_usage()).unwrap_or(0)
  );
  let (steam_map, name_map) = build_aux_maps(&detectable);
  let name_map_nodot = undotted_names(&name_map);
  let (custom_ac, custom_idx) = if custom.is_empty() {
    (None, Vec::new())
  } else {
    let (ac, idx) = build_ac_patterns_allow_all_os(&custom)?;
    (Some(ac), idx)
  };
  Ok(DetectablesBundle {
    list: detectable,
    ac,
    indexes: idx,
    proton_ac,
    proton_indexes: proton_idx,
    steam_map,
    name_map,
    name_map_nodot,
    custom,
    custom_ac,
    custom_indexes: custom_idx,
  })
}

fn build_ac_patterns_with_os_filter(
  detectables: &[Arc<ScannedEntry>],
  enforce_os: bool,
) -> Result<(AhoCorasick, Vec<[usize; 2]>), aho_corasick::BuildError> {
  let mut exe_patterns: Vec<String> = Vec::new();
  let mut exe_indexes: Vec<[usize; 2]> = Vec::new();

  for (activity_index, activity) in detectables.iter().enumerate() {
    for (exe_index, executable) in activity.executables.iter().enumerate() {
      if executable.is_launcher {
        continue;
      }

      // Only build patterns for executables that could run on this platform
      // For custom overrides (enforce_os=false) we skip the OS filter entirely
      // so that win32 executables can be detected on Linux via Proton/Wine
      // — this is the fix for NFS HP Remastered etc that only ships win32 entries.
      if enforce_os && !executable.os.is_empty() && !os_matches(executable.os.as_str()) {
        continue;
      }

      exe_patterns.push(normalize_exe_pattern(&executable.name));
      exe_indexes.push([activity_index, exe_index]);
    }
  }

  Ok((build_ac_automaton(&exe_patterns)?, exe_indexes))
}

/// Build the automaton with ASCII case-insensitive matching: process
/// paths are compared in their original case, so the scan loop never
/// allocates a lowercased copy per process (the single hottest
/// allocation in the profiler). Slashes/case in patterns are normalized
/// at build time (rare), never per scan (hot).
fn build_ac_automaton(exe_patterns: &[String]) -> Result<AhoCorasick, aho_corasick::BuildError> {
  AhoCorasick::builder()
    .ascii_case_insensitive(true)
    .build(exe_patterns)
}

/// Normalize one DB executable name into a reversed AC pattern:
/// consistent slashes, leading `/` (`>` becomes `/`, arrpc parity).
/// Case is left alone — the automaton matches insensitively.
fn normalize_exe_pattern(name: &str) -> String {
  // Make paths consistent, and fix some additional checks
  let mut exec_name = name.replace('\\', "/");

  // Checks adapted from arrpc, remain the '>' in DetectableActivity for later argument checks
  if exec_name.starts_with(">") {
    exec_name.replace_range(0..1, "/");
  } else if !exec_name.starts_with("/") {
    exec_name.insert(0, '/');
  }

  exec_name.chars().rev().collect::<String>()
}

/// Proton fallback automaton: `win32` executables from the main DB, for
/// Wine/Proton games on Linux whose store id is unreadable and whose exe
/// is too generic for the stem/folder heuristics. Linux-only: `None`
/// (plus empty indexes) elsewhere, so the probe is a cheap miss off-Linux.
fn build_proton_ac_patterns(
  detectables: &[Arc<ScannedEntry>],
) -> Result<(Option<AhoCorasick>, Vec<[usize; 2]>), aho_corasick::BuildError> {
  #[cfg(not(target_os = "linux"))]
  {
    let _ = detectables;
    Ok((None, Vec::new()))
  }
  #[cfg(target_os = "linux")]
  {
    let mut exe_patterns: Vec<String> = Vec::new();
    let mut exe_indexes: Vec<[usize; 2]> = Vec::new();

    for (activity_index, activity) in detectables.iter().enumerate() {
      for (exe_index, executable) in activity.executables.iter().enumerate() {
        if executable.is_launcher || executable.os != OsName::Win32 {
          continue;
        }
        exe_patterns.push(normalize_exe_pattern(&executable.name));
        exe_indexes.push([activity_index, exe_index]);
      }
    }

    if exe_patterns.is_empty() {
      return Ok((None, Vec::new()));
    }
    log!(
      "[Process Scanner] Proton fallback: {} win32 patterns",
      exe_patterns.len()
    );
    Ok((Some(build_ac_automaton(&exe_patterns)?), exe_indexes))
  }
}
