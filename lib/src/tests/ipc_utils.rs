use std::sync::mpsc;

use crate::server::ipc_utils::{MAX_IPC_PAYLOAD, PacketType, close_frame, send_empty};

#[test]
fn send_empty_routes_as_set_activity_clear() {
  // Regression: disconnect clears must carry cmd == SET_ACTIVITY, or
  // event_loop misroutes them to broadcast_raw and the presence (and the
  // bridge replay cache) is never cleared — stuck card forever.
  let (mut tx, rx) = mpsc::sync_channel(1);
  send_empty(&mut tx, 3).unwrap();
  let cmd = rx.try_recv().unwrap();
  assert_eq!(cmd.cmd, "SET_ACTIVITY");
  let args = cmd.args.unwrap();
  assert_eq!(args.pid, Some(3));
  assert!(args.activity.is_none());
}

#[test]
fn unknown_packet_types_are_refused_not_misread_as_frames() {
  assert!(PacketType::try_from_u32(0).is_some());
  assert!(PacketType::try_from_u32(4).is_some());
  assert!(PacketType::try_from_u32(5).is_none());
  assert!(PacketType::try_from_u32(u32::MAX).is_none());
}

#[test]
fn payload_limit_matches_discord_1mib() {
  assert_eq!(MAX_IPC_PAYLOAD, 1024 * 1024);
}

#[test]
fn close_frame_carries_code_and_message() {
  let frame = close_frame(1003, "Payload too large");
  let r_type = u32::from_le_bytes(frame[0..4].try_into().expect("header"));
  let len = u32::from_le_bytes(frame[4..8].try_into().expect("header")) as usize;
  assert_eq!(r_type, 2);
  assert_eq!(len, frame.len() - 8);
  let body: serde_json::Value = serde_json::from_slice(&frame[8..]).expect("json close body");
  assert_eq!(body["code"], 1003);
  assert_eq!(body["message"], "Payload too large");
}

// Unix socket fan-out below: official dir order, symlinks everywhere.
#[cfg(not(target_os = "windows"))]
mod unix_socket_dirs {
  use std::path::PathBuf;

  use crate::server::ipc_unix::{
    candidate_dirs_from, fanout_socket_link, remove_socket_links, socket_file_name,
  };

  fn var<'a>(name: &'a str, value: &str) -> (&'a str, Option<String>) {
    (name, Some(value.to_string()))
  }

  #[test]
  fn candidate_dirs_follow_official_order_deduped() {
    // Full chain, blanks and duplicates dropped, /tmp always last.
    assert_eq!(
      candidate_dirs_from([
        var("XDG_RUNTIME_DIR", "/run/user/1000/"),
        var("TMPDIR", "/tmp"),
        var("TMP", ""),
        var("TEMP", "/var/tmp/"),
      ]),
      vec![
        PathBuf::from("/run/user/1000"),
        PathBuf::from("/tmp"),
        PathBuf::from("/var/tmp"),
      ]
    );
    // Nothing set: /tmp alone.
    assert_eq!(
      candidate_dirs_from([
        ("XDG_RUNTIME_DIR", None),
        ("TMPDIR", None),
        ("TMP", None),
        ("TEMP", None),
      ]),
      vec![PathBuf::from("/tmp")]
    );
  }

  fn scratch(tag: &str) -> crate::tests::TempDir {
    crate::tests::TempDir::new(&format!("ipc-{tag}"))
  }

  #[test]
  fn fanout_links_every_dir_but_touches_nothing_foreign() {
    let root = scratch("fanout");
    let (a, b, c, d, e) = (
      root.join("a"),
      root.join("b"),
      root.join("c"),
      root.join("d"),
      root.join("e"),
    );
    for dir in [&a, &b, &c, &d, &e] {
      std::fs::create_dir_all(dir).expect("subdir");
    }
    // Stand-in for the bound socket (symlink targets need not exist).
    let bound = a.join("discord-ipc-3");
    std::fs::write(&bound, b"socket").expect("fake socket");
    let bound_str = bound.to_string_lossy().to_string();
    // Foreign regular file: never touched.
    std::fs::write(c.join("discord-ipc-3"), b"foreign").expect("foreign file");
    // Stale ours-shaped symlink: repointed.
    std::os::unix::fs::symlink("/nonexistent/discord-ipc-9", d.join("discord-ipc-3"))
      .expect("stale link");
    // Foreign symlink (not discord-ipc-*): left alone.
    std::os::unix::fs::symlink("/tmp/something-else", e.join("discord-ipc-3"))
      .expect("foreign link");

    let dirs = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone()];
    fanout_socket_link(&dirs, &bound_str, "discord-ipc-3");

    // Empty dir linked at the live socket.
    assert_eq!(
      std::fs::read_link(b.join("discord-ipc-3")).expect("link in b"),
      bound
    );
    // Foreign file untouched.
    assert_eq!(
      std::fs::read_to_string(c.join("discord-ipc-3")).expect("foreign intact"),
      "foreign"
    );
    // Stale repointed at the live socket.
    assert_eq!(
      std::fs::read_link(d.join("discord-ipc-3")).expect("repointed"),
      bound
    );
    // Foreign symlink untouched.
    assert_eq!(
      std::fs::read_link(e.join("discord-ipc-3")).expect("foreign link intact"),
      PathBuf::from("/tmp/something-else")
    );

    // Cleanup removes ours, keeps foreign content.
    remove_socket_links(&dirs, &bound_str);
    assert!(
      !b.join("discord-ipc-3").exists() && std::fs::read_link(b.join("discord-ipc-3")).is_err()
    );
    assert!(
      !d.join("discord-ipc-3").exists() && std::fs::read_link(d.join("discord-ipc-3")).is_err()
    );
    assert_eq!(
      std::fs::read_to_string(c.join("discord-ipc-3")).expect("foreign still intact"),
      "foreign"
    );
    assert_eq!(
      std::fs::read_link(e.join("discord-ipc-3")).expect("foreign link still intact"),
      PathBuf::from("/tmp/something-else")
    );
  }

  #[test]
  fn socket_file_name_extracts_index() {
    assert_eq!(
      socket_file_name("/run/user/1000/discord-ipc-7"),
      "discord-ipc-7"
    );
    assert_eq!(socket_file_name("discord-ipc-0"), "discord-ipc-0");
  }
}
