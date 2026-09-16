use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::detection::{DetectableActivity, ThirdPartySku};
use crate::server::process::{
  RefreshConfig, ScannedEntry, ScannedHit, build_aux_maps, exe_stem, first_sightings,
  match_name_or_folder, match_steam_id, name_matchable, normalize_name, undotted_names,
};
use crate::server::utils::QueueGauge;

fn activity(id: &str, name: &str, steam_id: Option<&str>) -> Arc<DetectableActivity> {
  Arc::new(DetectableActivity {
    bot_public: None,
    bot_require_code_grant: None,
    cover_image: None,
    description: None,
    developers: None,
    executables: None,
    flags: None,
    guild_id: None,
    hook: true,
    icon: None,
    id: id.to_string(),
    name: name.to_string(),
    publishers: None,
    rpc_origins: None,
    splash: None,
    third_party_skus: steam_id.map(|sid| {
      vec![ThirdPartySku {
        distributor: "steam".to_string(),
        id: Some(sid.to_string()),
        sku: None,
      }]
    }),
    type_field: None,
    verify_key: None,
    primary_sku_id: None,
    slug: None,
    aliases: None,
    overlay: None,
    overlay_compatibility_hook: None,
    privacy_policy_url: None,
    terms_of_service_url: None,
    eula_id: None,
    deeplink_uri: None,
    tags: None,
    pid: None,
    timestamp: None,
  })
}

/// Slim scanner form of full public entries, like the bundle holds them
/// (converted once at the build boundary).
fn slim_db(entries: Vec<Arc<DetectableActivity>>) -> Vec<Arc<ScannedEntry>> {
  entries
    .iter()
    .map(|entry| Arc::new(ScannedEntry::from_activity(entry)))
    .collect()
}

#[test]
fn scanned_entry_keeps_only_matcher_inputs() {
  use crate::detection::ThirdPartySku;
  use crate::server::process::ScannedEntry;

  let mut mixed = (*activity("1", "Mixed Game", Some("123"))).clone();
  mixed
    .third_party_skus
    .as_mut()
    .unwrap()
    .push(ThirdPartySku {
      distributor: "epic".to_string(),
      id: Some("epic-id".to_string()),
      sku: None,
    });
  // Only the steam distributor id survives the conversion; epic is dead
  // weight no matcher reads.
  let slim = ScannedEntry::from_activity(&mixed);
  let (id, name): (&str, &str) = (&slim.id, &slim.name);
  assert_eq!(id, "1");
  assert_eq!(name, "Mixed Game");
  assert_eq!(slim.steam_ids.len(), 1);
  let steam: &str = &slim.steam_ids[0];
  assert_eq!(steam, "123");
}

#[test]
fn exe_stem_strips_dirs_and_extension() {
  assert_eq!(
    exe_stem("/home/user/games/how to fish/how to fish.exe"),
    "how to fish"
  );
  assert_eq!(exe_stem("/usr/bin/fish"), "fish");
  assert_eq!(exe_stem("/games/nfs11remastered.exe"), "nfs11remastered");
}

#[test]
fn name_matchable_rejects_generic_stems() {
  assert!(!name_matchable("fish"));
  assert!(!name_matchable("steam"));
  assert!(!name_matchable("game"));
  assert!(!name_matchable("reaper"));
  assert!(name_matchable("how to fish"));
  assert!(name_matchable("need for speed hot pursuit remastered"));
}

#[test]
fn normalize_name_drops_windows_forbidden_punctuation() {
  // Titles ship `:`/`?`/… (legal in DB text); Windows folders never can.
  assert_eq!(
    normalize_name("Starlight Odyssey: Emberfall"),
    "starlight odyssey emberfall"
  );
  assert_eq!(
    normalize_name("  Starlight   Odyssey  |  Emberfall  "),
    "starlight odyssey emberfall"
  );
  assert_eq!(normalize_name("How to Fish"), "how to fish");
  assert_eq!(normalize_name("Fish"), "fish");
}

#[test]
fn first_sightings_reports_each_id_once() {
  use std::collections::HashSet;

  use crate::server::process::ScannedEntry;

  // Slim scanner form, like the bundle holds it: full public entries
  // convert once at the boundary (see `ScannedEntry::from_activity`).
  let full = vec![
    activity("1", "How to Fish", Some("4001890")),
    activity("2", "Meccha Chameleon", Some("4704690")),
  ];
  fn hit(entry: Arc<ScannedEntry>) -> ScannedHit {
    ScannedHit::stamp(entry, 4242)
  }
  let db: Vec<ScannedHit> = slim_db(full.clone()).into_iter().map(hit).collect();
  let mut seen = HashSet::new();
  // First tick: everything new.
  let first = first_sightings(&mut seen, &db);
  assert_eq!(first.len(), 2);
  // Repeat tick: nothing new, even reordered/duplicated.
  let again = vec![db[1].clone(), db[0].clone(), db[0].clone()];
  assert!(first_sightings(&mut seen, &again).is_empty());
  // A newcomer is reported alone.
  let db2: Vec<ScannedHit> = slim_db(vec![full[0].clone(), activity("3", "Bluefin Tuna", None)])
    .into_iter()
    .map(hit)
    .collect();
  let fresh = first_sightings(&mut seen, &db2);
  assert_eq!(fresh.len(), 1);
  assert_eq!(&*fresh[0].entry.id, "3");
}

#[test]
fn aux_maps_cover_empty_executable_entries() {
  let db = slim_db(vec![
    activity("1", "How to Fish", Some("4001890")),
    activity("2", "Fish", Some("999")),
  ]);
  let (steam_map, name_map) = build_aux_maps(&db);
  assert_eq!(steam_map.get("4001890"), Some(&0));
  // multi-word name indexed, single-word name excluded
  assert_eq!(name_map.get("how to fish"), Some(&0));
  assert!(!name_map.contains_key("fish"));
}

#[test]
fn aux_match_finds_custom_steam_sku_without_executables() {
  let db = slim_db(vec![activity("1", "How to Fish", Some("4001890"))]);
  let (steam_map, _) = build_aux_maps(&db);
  // Override carrying only a steam distributor id (no executables to
  // index): invisible to the map, must still match as custom fallback.
  let custom = slim_db(vec![activity("9", "Custom Fish Port", Some("1234567"))]);

  let hit = match_steam_id(Some("1234567"), 321, &steam_map, &db, &custom);
  assert_eq!(&*hit.unwrap().entry.id, "9");

  // Canonical map hits still win over the custom fallback.
  let hit = match_steam_id(Some("4001890"), 322, &steam_map, &db, &custom);
  assert_eq!(&*hit.unwrap().entry.id, "1");

  // Unknown ids miss everywhere.
  let miss = match_steam_id(Some("7654321"), 323, &steam_map, &db, &custom);
  assert!(miss.is_none());
}

#[test]
fn colon_titled_game_matches_spaceless_folder() {
  use crate::server::process::Exec;

  // DB title carries a colon (Windows-forbidden, so no real folder ever
  // has one); the on-disk folder spells the same words with spaces; the
  // exe stem is single-word (gated by design). The folder must carry it.
  // Mirrors SKU-only entries (no executables): automata cannot help.
  let db = vec![proton_entry(
    "4242",
    "Starlight Odyssey: Emberfall",
    None,
    Some("565656"),
  )];
  let server = proton_server(db);
  let bundle = server.bundle();
  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed_path = String::with_capacity(256);
  let mut obs_open = false;
  let hit = server
    .match_process(
      &Exec {
        pid: u64::MAX,
        path: "/games/starlight odyssey emberfall/StarlightOdyssey.exe".to_string(),
        arguments: None,
      },
      &bundle,
      &mut variant_bufs,
      &mut reversed_path,
      &mut obs_open,
    )
    .expect("folder words must match the punctuated title");
  assert_eq!(&*hit.entry.id, "4242");
}

#[test]
fn main_pattern_beats_custom_across_stripped_variants() {
  use crate::server::process::Exec;

  // Main knows `wow.exe`; the override knows the 64-bit `wow64.exe`.
  // The stripped variant collides: main must still win (precedence is
  // per automaton, not per variant).
  let db = vec![proton_entry(
    "1",
    "Wow Game",
    Some(("wow.exe", "linux", false)),
    None,
  )];
  let server = proton_server(db);
  let custom = proton_entry(
    "9",
    "Custom Wow Port",
    Some(("wow64.exe", "linux", false)),
    None,
  );
  let custom = Arc::try_unwrap(custom).unwrap_or_else(|shared| (*shared).clone());
  server.append_detectables(vec![custom]);

  let bundle = server.bundle();
  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed_path = String::with_capacity(256);
  let mut obs_open = false;
  let hit = server
    .match_process(
      &Exec {
        pid: u64::MAX,
        path: "/games/wow/wow64.exe".to_string(),
        arguments: None,
      },
      &bundle,
      &mut variant_bufs,
      &mut reversed_path,
      &mut obs_open,
    )
    .expect("stripped variant must match");
  assert_eq!(&*hit.entry.id, "1");
}

#[test]
fn aux_match_prefers_steam_then_name() {
  let db = slim_db(vec![activity("1", "How to Fish", Some("4001890"))]);
  let (steam_map, name_map) = build_aux_maps(&db);

  // Steam AppId hit (legit Steam install)
  let hit = match_steam_id(Some("4001890"), 123, &steam_map, &db, &[]);
  assert_eq!(&*hit.unwrap().entry.id, "1");

  // No AppId (launcher shortcut): exe-stem fallback hits the same entry
  let hit = match_name_or_folder(
    "/home/user/games/how to fish/how to fish.exe",
    456,
    &name_map,
    &undotted_names(&name_map),
    &db,
  );
  assert_eq!(&*hit.unwrap().entry.id, "1");

  // Generic shell must never match
  let miss = match_name_or_folder(
    "/usr/bin/fish",
    789,
    &name_map,
    &undotted_names(&name_map),
    &db,
  );
  assert!(miss.is_none());
}

#[test]
fn aux_match_falls_back_to_install_folder() {
  let db = slim_db(vec![
    activity("1", "How to Fish", Some("4001890")),
    activity("2", "Meccha Chameleon", Some("4704690")),
  ]);
  let (steam_map, name_map) = build_aux_maps(&db);

  // Hydra-style layout: generic Unreal exe, title only in the folders
  // (wine path, already slash-normalized and lowercased by the caller).
  let hit = match_name_or_folder(
    "/home/user/games/meccha chameleon/meccha chameleon/chameleon/binaries/win64/penguinhotel-win64-shipping.exe",
    201,
    &name_map,
    &undotted_names(&name_map),
    &db,
  );
  assert_eq!(&*hit.unwrap().entry.id, "2");

  // Steam AppId still wins over a conflicting folder name (the folder
  // alone would say "2", the store id says "1").
  let hit = match_steam_id(Some("4001890"), 202, &steam_map, &db, &[]);
  assert_eq!(&*hit.unwrap().entry.id, "1");
  let folder_says = match_name_or_folder(
    "/home/user/steamapps/common/meccha chameleon/game.exe",
    202,
    &name_map,
    &undotted_names(&name_map),
    &db,
  );
  assert_eq!(&*folder_says.unwrap().entry.id, "2");

  // Generic folders alone never match, even nested deep.
  let miss = match_name_or_folder(
    "/home/user/games/some game/binaries/win64/game-win64-shipping.exe",
    203,
    &name_map,
    &undotted_names(&name_map),
    &db,
  );
  assert!(miss.is_none());

  // Dotted components (versions, hidden dirs) are skipped, real title
  // behind them still hits.
  let hit = match_name_or_folder(
    "/home/user/.local/share/games/how to fish/v1.2.3/how to fish.bin",
    204,
    &name_map,
    &undotted_names(&name_map),
    &db,
  );
  assert_eq!(&*hit.unwrap().entry.id, "1");
}

#[test]
fn path_variants_into_matches_legacy_semantics() {
  use crate::server::process::path_variants_into;

  fn variants(path: &str) -> Vec<String> {
    let mut bufs: [String; 5] = Default::default();
    let count = path_variants_into(path, &mut bufs);
    bufs[..count].to_vec()
  }

  // Base path always first, then 64-bit-stripped forms.
  assert_eq!(
    variants("/games/wow64.exe"),
    vec!["/games/wow64.exe".to_string(), "/games/wow.exe".to_string()]
  );
  // No markers: single variant, and buffers reuse without reallocating.
  let mut bufs: [String; 5] = Default::default();
  let first = path_variants_into("/usr/bin/fish", &mut bufs);
  let ptrs: Vec<*const String> = bufs.iter().map(|s| s as *const String).collect();
  let second = path_variants_into("/usr/bin/fish", &mut bufs);
  assert_eq!((first, second), (1, 1));
  assert_eq!(
    ptrs,
    bufs.iter().map(|s| s as *const String).collect::<Vec<_>>()
  );
  // Dedup and capacity cap hold (base + 4 markers max).
  let many = variants("/64/x64/.x64/_64/game64.exe");
  assert_eq!(many[0], "/64/x64/.x64/_64/game64.exe");
  assert!(many.len() <= 5);
  assert!(many.iter().all(|v| !v.is_empty()));
}

#[test]
#[cfg(target_os = "linux")]
fn parse_stat_state_reads_after_comm() {
  use crate::server::process::parse_stat_state;

  // comm may contain spaces and parens; state is the field after the
  // LAST ')'. Shapes from proc(5).
  assert_eq!(parse_stat_state("1234 (fish) S 1 2 3"), Some('S'));
  assert_eq!(
    parse_stat_state("61442 (PenguinHotel-Win64-Shipping.exe) T 1 2 3"),
    Some('T')
  );
  assert_eq!(parse_stat_state("99 (weird (name)) R 1"), Some('R'));
  assert_eq!(parse_stat_state("garbage without parens"), None);
  assert_eq!(parse_stat_state("1 (x)"), None);
  assert_eq!(parse_stat_state(""), None);
}

#[test]
#[cfg(target_os = "linux")]
fn own_running_process_is_not_suspended() {
  use crate::server::process::is_suspended;

  // The test runner itself is running (R/S), never stopped.
  assert!(!is_suspended(u64::from(std::process::id())));
}

#[test]
fn bare_exe_detects_missing_directories_only() {
  use crate::server::process::bare_exe;

  assert_eq!(bare_exe("/doomx64.exe"), Some("doomx64.exe"));
  assert_eq!(bare_exe("/DOOMX64.EXE"), Some("DOOMX64.EXE"));
  assert_eq!(bare_exe("/games/doom/doomx64.exe"), None);
  assert_eq!(bare_exe("/"), None);
  assert_eq!(bare_exe(""), None);
}

#[test]
fn ac_probe_needs_directories_that_cwd_reconstructs() {
  use std::sync::Arc;

  use crate::detection::{DetectableActivity, Executable};
  use crate::server::process::{ProcessEventListeners, ProcessServer};

  let entry = DetectableActivity {
    bot_public: None,
    bot_require_code_grant: None,
    cover_image: None,
    description: None,
    developers: None,
    executables: Some(vec![Executable {
      name: "doom/doomx64.exe".to_string(),
      is_launcher: false,
      os: "linux".to_string(),
      arguments: None,
    }]),
    flags: None,
    guild_id: None,
    hook: true,
    icon: None,
    id: "424242424242424242".to_string(),
    name: "Doom Eternal Probe".to_string(),
    publishers: None,
    rpc_origins: None,
    splash: None,
    third_party_skus: None,
    type_field: None,
    verify_key: None,
    primary_sku_id: None,
    slug: None,
    aliases: None,
    overlay: None,
    overlay_compatibility_hook: None,
    privacy_policy_url: None,
    terms_of_service_url: None,
    eula_id: None,
    deeplink_uri: None,
    tags: None,
    pid: None,
    timestamp: None,
  };
  let arcs = vec![Arc::new(entry)];
  let (_tx, _rx) = QueueGauge::pair();
  let server = ProcessServer::new_with_custom(
    arcs.clone(),
    Vec::new(),
    _tx,
    ProcessEventListeners::default(),
    RefreshConfig::default(),
    Vec::new(),
  );
  let bundle = server.bundle();
  let reversed = |path: &str| path.chars().rev().collect::<String>();
  // Bare exe alone misses (no directories for the suffix to anchor on)...
  assert!(
    server
      .ac_probe(&reversed("/doomx64.exe"), &bundle)
      .is_none()
  );
  // ...while the cwd-joined candidate hits the same entry.
  assert_eq!(
    server
      .ac_probe(&reversed("/games/doom/doomx64.exe"), &bundle)
      .map(|(obj, _)| obj.id.to_string()),
    Some("424242424242424242".to_string())
  );
}

#[test]
#[cfg(target_os = "linux")]
fn appid_memo_caches_negative_lookups() {
  use crate::server::process::{ProcessEventListeners, ProcessServer};

  let (_tx, _rx) = QueueGauge::pair();
  let server = ProcessServer::new_with_custom(
    Vec::new(),
    Vec::new(),
    _tx,
    ProcessEventListeners::default(),
    RefreshConfig::default(),
    Vec::new(),
  );
  // No such pid, so environ is unreadable: verified Absent, and the
  // second lookup serves the memo (no re-read).
  let dead = u64::MAX - 7;
  assert_eq!(server.cached_app_id(dead), None);
  assert_eq!(server.cached_app_id(dead), None);
  // EXEC invalidation forces exactly one re-read, then Absent again.
  server.drop_appid(dead);
  assert_eq!(server.cached_app_id(dead), None);
  assert_eq!(server.cached_app_id(dead), None);
}

#[test]
fn conditional_refresh_skips_unchanged_database() {
  use std::io::{Read, Write};

  use crate::server::process::fetch_detectable_etag;

  // Local stub server: 304 when the tag matches the last issued one;
  // 200 + tiny DB with a rotating tag otherwise (CDN flap simulator).
  let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let port = listener.local_addr().unwrap().port();
  let issued: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
  let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
  std::thread::spawn(move || {
    for stream in listener.incoming().take(3) {
      let mut stream = match stream {
        Ok(stream) => stream,
        Err(_) => continue,
      };
      let mut buf = vec![0u8; 4096];
      let n = stream.read(&mut buf).unwrap_or(0);
      let request = String::from_utf8_lossy(&buf[..n]).into_owned();
      let sent_tag = request.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.trim().to_lowercase() == "if-none-match").then(|| value.trim().to_string())
      });

      let body = if sent_tag.is_some() && sent_tag.as_deref() == issued.lock().unwrap().as_deref() {
        "HTTP/1.1 304 Not Modified\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
      } else {
        let tag = format!(
          "\"tag-{}\"",
          counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        *issued.lock().unwrap() = Some(tag.clone());
        let db = "[]";
        format!(
          "HTTP/1.1 200 OK\r\nETag: {tag}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
          db.len(),
          db
        )
      };
      let _ = stream.write_all(body.as_bytes());
    }
  });
  let url = format!("http://127.0.0.1:{port}/db");
  let no_hash = None;

  // No tag yet: full fetch, empty DB, tag captured.
  let (tag, empty) = match fetch_detectable_etag(&url, None, no_hash, no_hash).unwrap() {
    crate::server::process::FetchOutcome::Updated {
      etag, detectable, ..
    } => (etag, detectable),
    crate::server::process::FetchOutcome::Unchanged => panic!("first fetch must download"),
    crate::server::process::FetchOutcome::SameContent { .. } => {
      panic!("nothing known yet, cannot be same-content")
    }
  };
  assert!(empty.is_empty());
  let tag = tag.expect("stub always tags 200s");

  // Same tag: 304, nothing downloaded or parsed.
  assert!(matches!(
    fetch_detectable_etag(&url, Some(&tag), no_hash, no_hash),
    Ok(crate::server::process::FetchOutcome::Unchanged)
  ));

  // Rotated tag, identical bytes: no rebuild (the flap case).
  let known = crate::detection::body_hash("[]");
  assert!(matches!(
    fetch_detectable_etag(&url, Some("\"stale\""), Some(known), no_hash),
    Ok(crate::server::process::FetchOutcome::SameContent { .. })
  ));
}

#[test]
fn conditional_refresh_skips_identical_games_under_new_bytes() {
  use std::io::{Read, Write};

  use crate::server::process::fetch_detectable_etag;

  // Same games, different raw bytes (whitespace churn around the payload):
  // the trimmed hash matches, so no rebuild happens.
  let bodies = [
    r#"[{"id":"1","name":"Doom"}]"#,
    "[ { \"id\" : \"1\" , \"name\" : \"Doom\" } ]",
  ];
  let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let port = listener.local_addr().unwrap().port();
  let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
  std::thread::spawn(move || {
    // Three serves: first fetch, rotated-tag fetch, and the hash
    // round-trip fetch below.
    for stream in listener.incoming().take(3) {
      let mut stream = match stream {
        Ok(stream) => stream,
        Err(_) => continue,
      };
      let mut buf = vec![0u8; 4096];
      let _ = stream.read(&mut buf);
      let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize % bodies.len();
      let db = bodies[n];
      let body = format!(
        "HTTP/1.1 200 OK\r\nETag: \"tag-{n}\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        db.len(),
        db
      );
      let _ = stream.write_all(body.as_bytes());
    }
  });
  let url = format!("http://127.0.0.1:{port}/db");

  // First fetch: new content, full update.
  let (raw, trimmed) = match fetch_detectable_etag(&url, None, None, None).unwrap() {
    crate::server::process::FetchOutcome::Updated {
      content_hash,
      trimmed_hash,
      detectable,
      ..
    } => {
      assert_eq!(detectable.len(), 1);
      (content_hash, trimmed_hash)
    }
    other => panic!(
      "first fetch must update, got {}",
      matches!(other, crate::server::process::FetchOutcome::Unchanged)
    ),
  };

  // Second fetch: same game, different bytes, new tag — same content.
  // The carried hashes must also round-trip: a third fetch of either
  // body takes a fast path without re-parsing.
  let (raw2, trimmed2) = match fetch_detectable_etag(&url, None, Some(raw), Some(trimmed)).unwrap()
  {
    crate::server::process::FetchOutcome::SameContent {
      content_hash,
      trimmed_hash,
      ..
    } => (content_hash, trimmed_hash),
    other => panic!(
      "second fetch must be same-content, got {}",
      matches!(other, crate::server::process::FetchOutcome::Unchanged)
    ),
  };
  assert!(matches!(
    fetch_detectable_etag(&url, None, Some(raw2), Some(trimmed2)),
    Ok(crate::server::process::FetchOutcome::SameContent { .. })
  ));
}

#[test]
fn conditional_refresh_ignores_metadata_churn() {
  use std::io::{Read, Write};

  use crate::server::process::fetch_detectable_etag;

  // Same scanner-visible games in the same order, but the CDN rewrote
  // every scanner-ignored field (descriptions, hook flags): the
  // canonical projection hash matches, so no rebuild happens. (Entry
  // *order* is significant — see the reorder test below.)
  let bodies = [
    r#"[{"id":"1","name":"Doom","description":"A","hook":true},{"id":"2","name":"Quake","description":"B","hook":true}]"#,
    r#"[ { "id" : "1" , "description" : "ALSO CHANGED" , "name" : "Doom" } ,
         { "name" : "Quake" , "description" : "CHANGED" , "id" : "2" } ]"#,
  ];
  let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let port = listener.local_addr().unwrap().port();
  let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
  std::thread::spawn(move || {
    for stream in listener.incoming().take(2) {
      let mut stream = match stream {
        Ok(stream) => stream,
        Err(_) => continue,
      };
      let mut buf = vec![0u8; 4096];
      let _ = stream.read(&mut buf);
      let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize % bodies.len();
      let db = bodies[n];
      let body = format!(
        "HTTP/1.1 200 OK\r\nETag: \"tag-{n}\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        db.len(),
        db
      );
      let _ = stream.write_all(body.as_bytes());
    }
  });
  let url = format!("http://127.0.0.1:{port}/db");

  let (raw, trimmed) = match fetch_detectable_etag(&url, None, None, None).unwrap() {
    crate::server::process::FetchOutcome::Updated {
      content_hash,
      trimmed_hash,
      detectable,
      ..
    } => {
      assert_eq!(detectable.len(), 2);
      (content_hash, trimmed_hash)
    }
    _ => panic!("first fetch must update"),
  };
  assert!(matches!(
    fetch_detectable_etag(&url, None, Some(raw), Some(trimmed)),
    Ok(crate::server::process::FetchOutcome::SameContent { .. })
  ));
}

#[test]
fn conditional_refresh_rebuilds_on_reorder() {
  use std::io::{Read, Write};

  use crate::server::process::fetch_detectable_etag;

  // Same games, swapped entry order: the canonical hash preserves body
  // order because first-wins index ties depend on it (two entries
  // sharing a matchable name would swap winners). A reorder must
  // rebuild, never silently keep the stale winner.
  let bodies = [
    r#"[{"id":"1","name":"Doom"},{"id":"2","name":"Quake"}]"#,
    r#"[{"id":"2","name":"Quake"},{"id":"1","name":"Doom"}]"#,
  ];
  let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let port = listener.local_addr().unwrap().port();
  let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
  std::thread::spawn(move || {
    for stream in listener.incoming().take(2) {
      let mut stream = match stream {
        Ok(stream) => stream,
        Err(_) => continue,
      };
      let mut buf = vec![0u8; 4096];
      let _ = stream.read(&mut buf);
      let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize % bodies.len();
      let db = bodies[n];
      let body = format!(
        "HTTP/1.1 200 OK\r\nETag: \"tag-{n}\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        db.len(),
        db
      );
      let _ = stream.write_all(body.as_bytes());
    }
  });
  let url = format!("http://127.0.0.1:{port}/db");

  let (raw, trimmed) = match fetch_detectable_etag(&url, None, None, None).unwrap() {
    crate::server::process::FetchOutcome::Updated {
      content_hash,
      trimmed_hash,
      ..
    } => (content_hash, trimmed_hash),
    _ => panic!("first fetch must update"),
  };
  assert!(matches!(
    fetch_detectable_etag(&url, None, Some(raw), Some(trimmed)),
    Ok(crate::server::process::FetchOutcome::Updated { .. })
  ));
}

#[test]
fn apply_ignore_list_drops_only_ignored_ids() {
  use std::sync::Arc;

  use crate::server::process::ScannedHit;
  use crate::server::process::apply_ignore_list;

  fn activity(id: &str) -> Arc<DetectableActivity> {
    Arc::new(DetectableActivity {
      bot_public: None,
      bot_require_code_grant: None,
      cover_image: None,
      description: None,
      developers: None,
      executables: None,
      flags: None,
      guild_id: None,
      hook: true,
      icon: None,
      id: id.to_string(),
      name: format!("Game {id}"),
      publishers: None,
      rpc_origins: None,
      splash: None,
      third_party_skus: None,
      type_field: None,
      verify_key: None,
      primary_sku_id: None,
      slug: None,
      aliases: None,
      overlay: None,
      overlay_compatibility_hook: None,
      privacy_policy_url: None,
      terms_of_service_url: None,
      eula_id: None,
      deeplink_uri: None,
      tags: None,
      pid: None,
      timestamp: None,
    })
  }

  let detected: Vec<ScannedHit> = slim_db(vec![activity("1"), activity("2"), activity("3")])
    .into_iter()
    .map(|entry| ScannedHit::stamp(entry, 100))
    .collect();
  // Empty list: everything passes, order preserved.
  let kept = apply_ignore_list(detected.clone(), &HashSet::new());
  assert_eq!(kept.len(), 3);
  // Ignored ids drop; the rest keep order (first-element semantics kept).
  let kept = apply_ignore_list(detected, &["2".to_string()].into_iter().collect());
  let ids: Vec<_> = kept.iter().map(|game| game.entry.id.to_string()).collect();
  assert_eq!(ids, vec!["1".to_string(), "3".to_string()]);
}

#[test]
fn app_id_from_args_parses_steam_launcher_token() {
  use crate::server::process::app_id_from_args;

  // Real reaper shape: token stands alone, digits follow.
  assert_eq!(
    app_id_from_args(Some("SteamLaunch AppId=4508340 -- /games/nte")),
    Some("4508340")
  );
  // No token, empty input, token without digits: nothing.
  assert_eq!(app_id_from_args(Some("htgame.exe /Game/Map")), None);
  assert_eq!(app_id_from_args(None), None);
  assert_eq!(app_id_from_args(Some("AppId= -- flag")), None);
  // Suffix of a longer key is not a token (`SomeAppId=`).
  assert_eq!(app_id_from_args(Some("SomeAppId=123")), None);
  // First boundary-valid token with digits wins.
  assert_eq!(app_id_from_args(Some("SomeAppId=1 AppId=22")), Some("22"));
}

// --- Proton (`win32`) fallback automaton (F1.1) ---

/// Synthetic DB entry with one executable (or a Steam-only entry when
/// `exe` is `None`, like How to Fish).
fn proton_entry(
  id: &str,
  name: &str,
  exe: Option<(&str, &str, bool)>,
  steam_id: Option<&str>,
) -> Arc<DetectableActivity> {
  use crate::detection::Executable;

  let mut entry = activity(id, name, steam_id);
  let entry_mut = Arc::get_mut(&mut entry).expect("fresh Arc");
  entry_mut.executables = exe.map(|(exe_name, os, is_launcher)| {
    vec![Executable {
      name: exe_name.to_string(),
      is_launcher,
      os: os.to_string(),
      arguments: None,
    }]
  });
  entry
}

fn proton_server(db: Vec<Arc<DetectableActivity>>) -> crate::server::process::ProcessServer {
  use crate::server::process::{ProcessEventListeners, ProcessServer};

  let (_tx, _rx) = QueueGauge::pair();
  ProcessServer::new_with_custom(
    db,
    Vec::new(),
    _tx,
    ProcessEventListeners::default(),
    RefreshConfig::default(),
    Vec::new(),
  )
}

#[test]
fn os_name_maps_hot_values_and_owns_exotics() {
  use crate::server::process::{OsName, ScannedEntry};

  // Hot values cost nothing and compare by variant.
  assert_eq!(OsName::from_os("win32"), OsName::Win32);
  assert_eq!(OsName::from_os("linux"), OsName::Linux);
  assert_eq!(OsName::from_os("darwin"), OsName::Darwin);
  assert!(OsName::from_os("").is_empty());
  // Exotics round-trip owned (no process-lifetime leak): each conversion
  // owns its copy, freed with its generation.
  let exotic = proton_entry("1", "Exotic Game", Some(("game.exe", "plan9", false)), None);
  let slim = ScannedEntry::from_activity(&exotic);
  assert_eq!(slim.executables[0].os.as_str(), "plan9");
  let moved = ScannedEntry::from_owned((*exotic).clone());
  assert_eq!(moved.executables[0].os.as_str(), "plan9");
}

#[test]
fn proton_automaton_holds_win32_only() {
  let db = vec![
    proton_entry(
      "111",
      "Native Game",
      Some(("native/game", "linux", false)),
      None,
    ),
    proton_entry(
      "222",
      "Proton Game",
      Some(("quest\\game.exe", "win32", false)),
      None,
    ),
    proton_entry(
      "333",
      "Proton Launcher",
      Some(("launcher.exe", "win32", true)),
      None,
    ),
  ];
  let server = proton_server(db.clone());
  let bundle = server.bundle();
  let reversed = |path: &str| path.chars().rev().collect::<String>();

  // win32 entry hits (backslash in the DB name is normalized).
  assert_eq!(
    server
      .proton_probe(&reversed("/games/quest/game.exe"), &bundle)
      .map(|(obj, _)| obj.id.to_string()),
    Some("222".to_string())
  );
  // Native entries stay out of the fallback automaton...
  assert!(
    server
      .proton_probe(&reversed("/native/game"), &bundle)
      .is_none()
  );
  // ...as do launchers.
  assert!(
    server
      .proton_probe(&reversed("/launcher.exe"), &bundle)
      .is_none()
  );
  // Native automaton is untouched by win32 entries.
  assert!(
    server
      .ac_probe(&reversed("/games/quest/game.exe"), &bundle)
      .is_none()
  );
  assert_eq!(
    server
      .ac_probe(&reversed("/native/game"), &bundle)
      .map(|(obj, _)| obj.id.to_string()),
    Some("111".to_string())
  );
}

#[test]
fn match_process_detects_win32_game_and_prefers_steam_id() {
  use crate::server::process::Exec;

  let db = vec![
    proton_entry(
      "222",
      "Proton Quest",
      Some(("quest/game.exe", "win32", false)),
      None,
    ),
    proton_entry("999", "Steam Other", None, Some("999999")),
  ];
  let server = proton_server(db.clone());
  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed_path = String::with_capacity(256);
  let mut obs_open = false;
  // Impossible pid: every lazy /proc read (environ, cwd, stat) fails
  // deterministically, so only the pure matching chain is exercised.
  let classify = |server: &crate::server::process::ProcessServer,
                  arguments: Option<String>,
                  variant_bufs: &mut [String; 5],
                  reversed_path: &mut String,
                  obs_open: &mut bool| {
    let bundle = server.bundle();
    server.match_process(
      &Exec {
        pid: u64::MAX,
        path: "C:\\games\\quest\\game.exe".to_string(),
        arguments,
      },
      &bundle,
      variant_bufs,
      reversed_path,
      obs_open,
    )
  };

  // No AppId anywhere: the Proton fallback catches the win32 path.
  let hit = classify(
    &server,
    None,
    &mut variant_bufs,
    &mut reversed_path,
    &mut obs_open,
  );
  let hit = hit.expect("win32 path must match via Proton automaton");
  assert_eq!(&*hit.entry.id, "222");
  assert_eq!(hit.pid, u64::MAX);

  // Same process, but the command line carries another game's Steam AppId:
  // the authoritative store id wins over the fuzzy path match.
  let hit = classify(
    &server,
    Some("reaper SteamLaunch AppId=999999 -- /games/other".to_string()),
    &mut variant_bufs,
    &mut reversed_path,
    &mut obs_open,
  );
  let hit = hit.expect("steam AppId must match");
  assert_eq!(&*hit.entry.id, "999");
}

// --- Aliases + exclusions (F1.2) ---

#[test]
fn trim_keeps_aliases_drops_unknown() {
  use crate::detection::trim_detectable_value;

  let body = r#"[{
    "id": "1", "name": "PUBG: Battlegrounds", "hook": true,
    "aliases": ["PUBG", "PlayerUnknown's Battlegrounds", "", 42],
    "executables": [{"name": "tslgame.exe", "is_launcher": false, "os": "win32"}],
    "third_party_skus": [{"distributor": "steam", "id": "578080"}],
    "themes": ["shooter"], "overlay": true
  }]"#;
  let trimmed = trim_detectable_value(body).unwrap();
  let entry = &trimmed.as_array().unwrap()[0];
  // Aliases survive the trim (strings only, blanks/non-strings dropped)...
  assert_eq!(
    entry.get("aliases").unwrap().as_array().unwrap(),
    &vec![
      serde_json::Value::String("PUBG".to_string()),
      serde_json::Value::String("PlayerUnknown's Battlegrounds".to_string())
    ]
  );
  // ...unknown top-level fields do not.
  assert!(entry.get("themes").is_none());
  assert!(entry.get("overlay").is_none());
  assert_eq!(entry.get("id").unwrap(), "1");
}

#[test]
fn aliases_indexed_and_gated() {
  // Multi-word alias indexed; single-word alias rejected by the same
  // conservative gate as canonical names; canonical names win ties.
  let mut pubg = activity("1", "PUBG: Battlegrounds", None);
  Arc::get_mut(&mut pubg).expect("fresh Arc").aliases = Some(vec![
    "PUBG".to_string(),
    "PlayerUnknown's Battlegrounds".to_string(),
  ]);
  let mut clash = activity("2", "PlayerUnknown's Battlegrounds", None);
  Arc::get_mut(&mut clash).expect("fresh Arc").aliases = Some(vec!["Some Other Title".to_string()]);
  let db = slim_db(vec![pubg, clash]);
  let (steam_map, name_map) = build_aux_maps(&db);
  assert_eq!(name_map.get("playerunknown's battlegrounds"), Some(&0));
  assert!(!name_map.contains_key("pubg"));
  assert_eq!(name_map.get("some other title"), Some(&1));
  let _ = steam_map;

  // The alias resolves through the real stem fallback.
  let hit = match_name_or_folder(
    "/games/playerunknown's battlegrounds/tslgame.exe",
    11,
    &name_map,
    &undotted_names(&name_map),
    &db,
  );
  assert_eq!(&*hit.unwrap().entry.id, "1");
}

#[test]
fn undotted_names_cover_dotted_titles_only() {
  let db = slim_db(vec![
    activity("1", "How to Fish", Some("4001890")),
    proton_entry("2", "Rook R.E.P.O. Detail", None, None),
  ]);
  let (_, name_map) = build_aux_maps(&db);
  let nodot = undotted_names(&name_map);
  // Dotted title present de-dotted (dots become spaces, runs collapse);
  // dotless titles absent.
  assert_eq!(nodot.get("rook r e p o detail"), Some(&1));
  assert!(!nodot.contains_key("how to fish"));
  // Canonical-first ties: a later duplicate cannot shadow.
  let db2 = slim_db(vec![
    proton_entry("1", "Rook R.E.P.O. Detail", None, None),
    proton_entry("2", "Rook R E P O Detail", None, None),
  ]);
  let (_, name_map2) = build_aux_maps(&db2);
  assert_eq!(
    undotted_names(&name_map2).get("rook r e p o detail"),
    Some(&0)
  );
}

#[test]
fn dotted_title_folder_matches_as_last_tier() {
  use crate::server::process::Exec;

  // On-disk folder keeps the dots (`R.E.P.O.`); the exact walk skips
  // dotted components as versions, so only the de-dotted tier can hit.
  // Exe stem is single-word (gated by design): the folder carries it.
  let db = vec![proton_entry(
    "7",
    "Rook R.E.P.O. Detail",
    None,
    Some("7654321"),
  )];
  let server = proton_server(db);
  let bundle = server.bundle();
  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed_path = String::with_capacity(256);
  let mut obs_open = false;
  let hit = server
    .match_process(
      &Exec {
        pid: u64::MAX,
        path: "/games/Rook R.E.P.O. Detail/game.bin".to_string(),
        arguments: None,
      },
      &bundle,
      &mut variant_bufs,
      &mut reversed_path,
      &mut obs_open,
    )
    .expect("de-dotted folder must match the dotted title");
  assert_eq!(&*hit.entry.id, "7");

  // Exact matches still win: a version-looking folder never shadows a
  // real exact folder hit for another entry.
  let db = vec![
    proton_entry("1", "How to Fish", None, None),
    proton_entry("2", "Rook R.E.P.O. Detail", None, None),
  ];
  let server = proton_server(db);
  let bundle = server.bundle();
  let hit = server
    .match_process(
      &Exec {
        pid: u64::MAX,
        path: "/games/how to fish/v1.2.3/how to fish.bin".to_string(),
        arguments: None,
      },
      &bundle,
      &mut variant_bufs,
      &mut reversed_path,
      &mut obs_open,
    )
    .expect("exact folder still matches");
  assert_eq!(&*hit.entry.id, "1");
}

#[test]
fn exclusions_parse_and_match() {
  use crate::detection::{Exclusions, parse_exclusions};

  // Shape mirrors the live endpoint (subset): exact names, one regex,
  // plus garbage a tolerant parser must swallow.
  let body = r#"{
    "executables": ["crashreportclient.exe", "  ", 42, "UnityCrashHandler64.exe"],
    "patterns": ["vcredist.*\\.exe$", "([invalid", 7],
    "unexpected": [1, 2]
  }"#;
  let exclusions = parse_exclusions(body);
  let mut names: Vec<_> = exclusions.executables.iter().cloned().collect();
  names.sort();
  assert_eq!(
    names,
    vec!["crashreportclient.exe", "unitycrashhandler64.exe"]
  );
  assert_eq!(exclusions.patterns.len(), 1);

  // Exact (basenames arrive lowercased from the scanner)...
  assert!(exclusions.is_excluded("crashreportclient.exe"));
  assert!(exclusions.is_excluded("unitycrashhandler64.exe"));
  // ...regex, case-insensitively (Windows-centric DB, Proton paths)...
  assert!(exclusions.is_excluded("vcredist_x64.exe"));
  assert!(exclusions.is_excluded("VCREDIST_X86.EXE"));
  // ...and real games pass through.
  assert!(!exclusions.is_excluded("htgame.exe"));
  assert!(!exclusions.is_excluded("client-win64-shipping.exe"));

  // Wholly invalid body degrades to empty, never errors.
  let empty = parse_exclusions("not json{{{");
  assert!(!empty.is_excluded("crashreportclient.exe"));
  let _ = Exclusions::default();
}

#[test]
fn match_process_honors_exclusions() {
  use crate::detection::parse_exclusions;
  use crate::server::process::Exec;

  let db = vec![proton_entry(
    "222",
    "Proton Quest",
    Some(("quest/game.exe", "win32", false)),
    None,
  )];
  let server = proton_server(db.clone());
  server.set_exclusions(parse_exclusions(
    r#"{"executables": ["game.exe"], "patterns": []}"#,
  ));
  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed_path = String::with_capacity(256);
  let mut obs_open = false;

  // Would match via the Proton automaton — excluded first.
  let bundle = server.bundle();
  let miss = server.match_process(
    &Exec {
      pid: u64::MAX,
      path: "C:\\games\\quest\\game.exe".to_string(),
      arguments: None,
    },
    &bundle,
    &mut variant_bufs,
    &mut reversed_path,
    &mut obs_open,
  );
  assert!(miss.is_none());

  // Same game under a non-excluded exe name still detects (fresh entry:
  // cloning the Arc would share ownership and block mutation).
  let db2 = vec![proton_entry(
    "222",
    "Proton Quest",
    Some(("quest/launcher-free.exe", "win32", false)),
    None,
  )];
  let server2 = proton_server(db2.clone());
  server2.set_exclusions(parse_exclusions(
    r#"{"executables": ["game.exe"], "patterns": []}"#,
  ));
  let bundle2 = server2.bundle();
  let hit = server2
    .match_process(
      &Exec {
        pid: u64::MAX,
        path: "/games/quest/launcher-free.exe".to_string(),
        arguments: None,
      },
      &bundle2,
      &mut variant_bufs,
      &mut reversed_path,
      &mut obs_open,
    )
    .expect("non-excluded path must match");
  assert_eq!(&*hit.entry.id, "222");
}

#[test]
fn ac_matches_mixed_case_without_lowercasing() {
  use crate::server::process::Exec;

  // DB patterns stay as-written; the automaton matches insensitively so
  // the scan loop never allocates a lowercased path per process.
  let db = vec![proton_entry(
    "444",
    "Mixed Case",
    Some(("Mixed/Dir/GAME.EXE", "linux", false)),
    None,
  )];
  let server = proton_server(db.clone());
  let bundle = server.bundle();
  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed_path = String::with_capacity(256);
  let mut obs_open = false;

  for path in [
    "/games/mixed/dir/game.exe",
    "/GAMES/MIXED/DIR/GAME.EXE",
    "C:\\Games\\Mixed\\Dir\\Game.Exe",
  ] {
    let hit = server
      .match_process(
        &Exec {
          pid: u64::MAX,
          path: path.to_string(),
          arguments: None,
        },
        &bundle,
        &mut variant_bufs,
        &mut reversed_path,
        &mut obs_open,
      )
      .expect("case must not matter");
    assert_eq!(&*hit.entry.id, "444");
  }
}

#[test]
fn exit_wake_fires_once_per_second_per_tracked_pid() {
  use std::time::{Duration, Instant};

  let db = vec![proton_entry("123", "Vdf Game", None, Some("12345"))];
  let server = proton_server(db);

  // Untracked pid: never wakes, nothing consumed.
  assert!(!server.should_wake_on_exit(111));

  // Tracked pid, last scan 2s ago: wakes once, pid consumed...
  server.detected_pids.lock().unwrap().insert(222);
  *server.last_scan.lock().unwrap() = Instant::now() - Duration::from_secs(2);
  assert!(server.should_wake_on_exit(222));
  // ...and the same exit right after does not wake again.
  server.detected_pids.lock().unwrap().insert(222);
  *server.last_scan.lock().unwrap() = Instant::now();
  assert!(!server.should_wake_on_exit(222));
}

#[test]
fn idle_wait_stretches_and_caps() {
  use std::time::Duration;

  use crate::server::process::idle_wait;

  let base = Duration::from_secs(5);
  assert_eq!(idle_wait(base, 0), Duration::from_secs(5));
  assert_eq!(idle_wait(base, 1), Duration::from_secs(10));
  assert_eq!(idle_wait(base, 2), Duration::from_secs(20));
  assert_eq!(idle_wait(base, 3), Duration::from_secs(30));
  assert_eq!(idle_wait(base, 100), Duration::from_secs(30));
  // A base already above the cap never stretches further.
  assert_eq!(
    idle_wait(Duration::from_secs(60), 3),
    Duration::from_secs(30)
  );
  // Overflow-safe at the extremes.
  assert_eq!(idle_wait(Duration::MAX, 4), Duration::from_secs(30));
}

#[test]
fn concurrent_swap_and_scan_never_tears() {
  use std::sync::Arc;

  // The hourly refresh swaps the detection bundle while scans (and EXEC
  // events) classify against it. Generations swap atomically, so any
  // interleave is safe: this can only fail on a real torn read (index
  // out of bounds, wrong mapping) — never flake on correct code.
  // Tiny DB keeps rebuilds cheap; scans read the real /proc table.
  let db = vec![proton_entry(
    "444",
    "Mixed Case",
    Some(("Mixed/Dir/GAME.EXE", "linux", false)),
    None,
  )];
  let server = Arc::new(proton_server(db));
  let mut writers = Vec::new();
  for i in 0..2u32 {
    let server = server.clone();
    writers.push(std::thread::spawn(move || {
      for round in 0..20 {
        let entry = proton_entry(&format!("custom-{i}-{round}"), "Custom Game", None, None);
        let entry = Arc::try_unwrap(entry).unwrap_or_else(|shared| (*shared).clone());
        server.append_detectables(vec![entry]);
      }
    }));
  }
  for _ in 0..50 {
    let _ = server.scan_for_processes();
  }
  for writer in writers {
    writer.join().unwrap();
  }
  // And the map is still coherent afterwards: the original entry matches
  // through 40 generation swaps.
  use crate::server::process::Exec;
  let bundle = server.bundle();
  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed_path = String::with_capacity(256);
  let mut obs_open = false;
  let hit = server
    .match_process(
      &Exec {
        pid: u64::MAX,
        path: "/games/mixed/dir/game.exe".to_string(),
        arguments: None,
      },
      &bundle,
      &mut variant_bufs,
      &mut reversed_path,
      &mut obs_open,
    )
    .expect("map coherent after concurrent swaps");
  assert_eq!(&*hit.entry.id, "444");
}

#[test]
fn proc_events_watcher_defaults_on_and_opts_out() {
  let mut server = proton_server(Vec::new());
  assert!(server.enable_proc_events);
  assert!(crate::RPCConfig::default().enable_proc_events);
  server.set_proc_events(false);
  assert!(!server.enable_proc_events);
}

/// Hermetic fake Steam root under the temp dir (unique per process, so
/// parallel test binaries never collide). Removed on drop, even on panic.
fn fake_steam_root(tag: &str, manifests: &[(&str, &str)]) -> crate::tests::TempDir {
  let root = crate::tests::TempDir::new(&format!("steam-{tag}"));
  let apps = root.join("steamapps");
  std::fs::create_dir_all(&apps).unwrap();
  let folders = format!(
    "\"libraryfolders\"\n{{\n\"0\"\n{{\n\"path\"\t\t\"{}\"\n}}\n}}",
    root.to_string_lossy().replace('\\', "\\\\")
  );
  std::fs::write(apps.join("libraryfolders.vdf"), folders).unwrap();
  for (appid, dir) in manifests {
    let manifest =
      format!("\"AppState\"\n{{\n\"appid\"\t\t\"{appid}\"\n\"installdir\"\t\t\"{dir}\"\n}}");
    std::fs::write(apps.join(format!("appmanifest_{appid}.acf")), manifest).unwrap();
  }
  root
}

/// Watch-marker pruning moved to `rsrpc-steam/tests/steam.rs`
/// (`refresh_drops_libraries_of_vanished_roots`, behavioral, no seams).

#[test]
fn steam_libraries_match_prefix_and_refresh() {
  use std::time::SystemTime;

  use rsrpc_steam::SteamLibraries;

  // Direct injection (no env): hermetic by construction, no races possible.
  let root = fake_steam_root("prefix", &[("12345", "Vdf Game")]);
  let db = vec![proton_entry("123", "Vdf Game", None, Some("12345"))];
  let server = proton_server(db);
  server.set_steam_libraries(SteamLibraries::from_root(&root));
  let prefix = format!(
    "{}/steamapps/common/vdf game/",
    root.to_string_lossy().to_lowercase()
  );
  // Case-insensitive prefix hit (query arrives lowercased from scanner).
  assert_eq!(
    server.steam_prefix_app_id(&format!("{prefix}game.exe")),
    Some("12345".to_string())
  );
  // Outside every install dir: no match.
  assert!(server.steam_prefix_app_id("/usr/bin/fish").is_none());

  // Rewrite the manifest under a new name with bumped mtimes:
  // the next staleness check rebuilds instead of serving the old prefix.
  // Both mtimes are set explicitly: the fingerprint truncates to
  // milliseconds, so relying on wall-clock advance flakes when setup
  // and rewrite land in the same millisecond (fast machines, CI).
  let manifest = root.join("steamapps/appmanifest_12345.acf");
  std::fs::write(
    &manifest,
    "\"AppState\"\n{\n\"appid\"\t\t\"12345\"\n\"installdir\"\t\t\"Renamed Game\"\n}",
  )
  .unwrap();
  // Explicit mtimes on both files: the fingerprint truncates to
  // milliseconds, so relying on wall-clock advance flakes when setup
  // and rewrite land in the same millisecond (fast machines, CI).
  let future = SystemTime::now() + std::time::Duration::from_secs(120);
  std::fs::File::options()
    .write(true)
    .open(&manifest)
    .unwrap()
    .set_modified(future)
    .unwrap();
  let folders = root.join("steamapps/libraryfolders.vdf");
  let future = SystemTime::now() + std::time::Duration::from_secs(60);
  std::fs::File::options()
    .write(true)
    .open(&folders)
    .unwrap()
    .set_modified(future)
    .unwrap();
  server.refresh_steam_libraries();
  assert!(
    server
      .steam_prefix_app_id(&format!("{prefix}game.exe"))
      .is_none()
  );
  let renamed = format!(
    "{}/steamapps/common/renamed game/",
    root.to_string_lossy().to_lowercase()
  );
  assert_eq!(
    server.steam_prefix_app_id(&format!("{renamed}game.exe")),
    Some("12345".to_string())
  );
}

#[test]
fn match_process_prefers_vdf_over_folder() {
  use crate::server::process::Exec;

  // Folder walk alone would say "Meccha Chameleon"; Steam's install dir
  // says AppId 777777 ("Steam Other"). With no launcher AppId, the
  // library wins over guessing.
  let root = fake_steam_root("precedence", &[("777777", "Meccha Chameleon")]);
  let db = vec![
    proton_entry("888", "Meccha Chameleon", None, None),
    proton_entry("999", "Steam Other", None, Some("777777")),
  ];
  let server = proton_server(db.clone());
  server.set_steam_libraries(rsrpc_steam::SteamLibraries::from_root(&root));
  let bundle = server.bundle();
  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed_path = String::with_capacity(256);
  let mut obs_open = false;

  let install_path = format!(
    "{}/steamapps/common/meccha chameleon/game.exe",
    root.to_string_lossy().to_lowercase()
  );
  let hit = server
    .match_process(
      &Exec {
        pid: u64::MAX,
        path: install_path,
        arguments: None,
      },
      &bundle,
      &mut variant_bufs,
      &mut reversed_path,
      &mut obs_open,
    )
    .expect("steam library must match");
  assert_eq!(&*hit.entry.id, "999");
}

#[test]
fn match_process_shortcut_id_prefers_name() {
  use crate::server::process::Exec;

  // Same layout as above, but the launcher reports an AppId Steam itself
  // assigned to a non-Steam shortcut (high bit set, e.g. the real
  // 2532755798 for "How to Fish", here a synthetic 2999999999): the game
  // self-identifies as non-Steam, so its folder name beats the library.
  // A small unknown id (real game missing from the DB) keeps the
  // library-first order instead.
  let root = fake_steam_root("shortcut", &[("777777", "Meccha Chameleon")]);
  let db = vec![
    proton_entry("888", "Meccha Chameleon", None, None),
    proton_entry("999", "Steam Other", None, Some("777777")),
  ];
  let server = proton_server(db.clone());
  server.set_steam_libraries(rsrpc_steam::SteamLibraries::from_root(&root));
  let bundle = server.bundle();
  let mut variant_bufs: [String; 5] = Default::default();
  let mut reversed_path = String::with_capacity(256);
  let mut obs_open = false;
  let classify = |arguments: Option<String>,
                  variant_bufs: &mut [String; 5],
                  reversed_path: &mut String,
                  obs_open: &mut bool| {
    server.match_process(
      &Exec {
        pid: u64::MAX,
        path: format!(
          "{}/steamapps/common/meccha chameleon/game.exe",
          root.to_string_lossy().to_lowercase()
        ),
        arguments,
      },
      &bundle,
      variant_bufs,
      reversed_path,
      obs_open,
    )
  };

  // Shortcut-range id, unknown to the DB: folder ("888") wins.
  let hit = classify(
    Some("reaper SteamLaunch AppId=2999999999 -- /games/other".to_string()),
    &mut variant_bufs,
    &mut reversed_path,
    &mut obs_open,
  )
  .expect("folder must match for shortcut ids");
  assert_eq!(&*hit.entry.id, "888");

  // Small unknown id (real game not in the DB yet): library ("999").
  let hit = classify(
    Some("reaper SteamLaunch AppId=9876543 -- /games/other".to_string()),
    &mut variant_bufs,
    &mut reversed_path,
    &mut obs_open,
  )
  .expect("library must match for small unknown ids");
  assert_eq!(&*hit.entry.id, "999");
}

#[test]
fn steam_cache_roundtrip_and_corrupt_fallback() {
  use rsrpc_steam::SteamLibraries;

  // Borrowed process-global env (serialized with the overrides test
  // via lock_env); TempDir + EnvRestore guards clean up dirs and env on
  // drop, panic or not (lock declared first, drops last).
  let _guard = crate::tests::lock_env();
  let root = fake_steam_root("cache", &[("12345", "Vdf Game")]);
  let cache = crate::tests::TempDir::new("cache-home");
  let _env_root = crate::tests::EnvRestore::set("RSRPC_STEAM_ROOT", &root.to_string_lossy());
  let _env_cache = crate::tests::EnvRestore::set("XDG_CACHE_HOME", &cache.to_string_lossy());
  // First discovery parses and persists the cache.
  let libraries = SteamLibraries::discover();
  assert_eq!(
    libraries.match_prefix(&format!(
      "{}/steamapps/common/vdf game/game.exe",
      root.to_string_lossy().to_lowercase()
    )),
    Some("12345")
  );
  let cache_file = cache.join("rsrpc/steam-libraries.json");
  let body = std::fs::read_to_string(&cache_file).expect("cache must be written");
  assert!(body.contains("\"version\":1"));
  assert!(body.contains("12345"));

  // Corrupt cache degrades to a full parse, never an error.
  std::fs::write(&cache_file, "{not json").unwrap();
  let libraries = SteamLibraries::discover();
  assert_eq!(
    libraries.match_prefix(&format!(
      "{}/steamapps/common/vdf game/game.exe",
      root.to_string_lossy().to_lowercase()
    )),
    Some("12345")
  );
  // TempDir + EnvRestore guards clean up dirs and env, panic or not.
}

#[test]
#[cfg(target_os = "linux")]
fn read_exec_sees_proc_files_despite_zero_size() {
  use crate::server::process::read_exec;

  // Regression: /proc files report st_size 0 despite having content — a
  // metadata size check here skipped EVERY process (total blindness).
  // Our own pid always exists with a non-empty cmdline, so this is
  // deterministic without mocks or fixtures.
  let here = std::process::id() as u64;
  let exec = read_exec(here).expect("own cmdline must be readable");
  assert_eq!(exec.pid, here);
  assert!(!exec.path.is_empty());
}

#[test]
fn scan_guard_serializes_and_releases() {
  use std::sync::Arc;
  use std::sync::atomic::AtomicBool;

  use crate::server::process::ScanGuard;

  let flag = Arc::new(AtomicBool::new(false));
  let first = ScanGuard::try_acquire(&flag);
  assert!(first.is_some());
  // Held: second acquisition fails instead of interleaving.
  assert!(ScanGuard::try_acquire(&flag).is_none());
  drop(first);
  // Released (even via Drop): acquirable again.
  assert!(ScanGuard::try_acquire(&flag).is_some());
}
