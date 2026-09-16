//! Netlink parser and sequence-tracker tests (pure: run everywhere, but
//! the wire format only exists on Linux, hence the gate).

#![cfg(target_os = "linux")]

use rsrpc_proc_events::{ProcEvent, SelfTestReport, SeqTracker, parse_event};

/// One synthetic kernel datagram: `nlmsghdr` + `cn_msg` (idx/val = 1/1)
/// + `proc_event` with `what` and the pid at the exec/exit union offset.
fn proc_buf(what: u32, pid: u32) -> Vec<u8> {
  let total = 16 + 20 + 24;
  let mut buf = vec![0u8; total];
  buf[0..4].copy_from_slice(&(total as u32).to_le_bytes());
  buf[4..6].copy_from_slice(&16u16.to_le_bytes());
  buf[6..8].copy_from_slice(&1u16.to_le_bytes());
  buf[16..20].copy_from_slice(&1u32.to_le_bytes());
  buf[20..24].copy_from_slice(&1u32.to_le_bytes());
  buf[36..40].copy_from_slice(&what.to_le_bytes());
  buf[52..56].copy_from_slice(&pid.to_le_bytes());
  buf
}

#[test]
fn proc_event_parses_exec_and_exit() {
  assert_eq!(
    parse_event(&proc_buf(0x2, 1234)),
    Some(ProcEvent::Exec(1234))
  );
  // EXIT is a bitmask (0x80000000), not a sequence number.
  assert_eq!(
    parse_event(&proc_buf(0x8000_0000, 5678)),
    Some(ProcEvent::Exit(5678))
  );
  // Anything else is ignored, never an error: fork, uid-change...
  assert_eq!(parse_event(&proc_buf(0x1, 1)), None);
  assert_eq!(parse_event(&proc_buf(0x4, 1)), None);
  // ...unknown discriminants...
  assert_eq!(parse_event(&proc_buf(0x9999, 1)), None);
  // ...foreign connector traffic...
  let mut foreign = proc_buf(0x2, 9);
  foreign[16..20].copy_from_slice(&7u32.to_le_bytes());
  assert_eq!(parse_event(&foreign), None);
  // ...control traffic (NOOP / zero-code ERROR ack)...
  let mut noop = proc_buf(0x2, 9);
  noop[4..6].copy_from_slice(&1u16.to_le_bytes());
  assert_eq!(parse_event(&noop), None);
  let mut ack = proc_buf(0x2, 9);
  ack[4..6].copy_from_slice(&2u16.to_le_bytes());
  ack[16..20].copy_from_slice(&0u32.to_le_bytes());
  assert_eq!(parse_event(&ack), None);
  // ...while the kernel wraps real events in NLMSG_DONE (seen live).
  let mut done_exec = proc_buf(0x2, 4242);
  done_exec[4..6].copy_from_slice(&3u16.to_le_bytes());
  assert_eq!(parse_event(&done_exec), Some(ProcEvent::Exec(4242)));
  // ...and short/corrupt buffers.
  assert_eq!(parse_event(&[]), None);
  assert_eq!(parse_event(&proc_buf(0x2, 1)[..10]), None);
}

#[test]
fn self_test_report_distinguishes_silence_from_drift() {
  // Proven delivery: any parsed event counts.
  assert!(
    SelfTestReport {
      datagrams: 3,
      parsed: 1,
      ..SelfTestReport::default()
    }
    .live()
  );
  // Kernel talks but nothing parses: framing drift, not liveness.
  assert!(
    !SelfTestReport {
      datagrams: 9,
      parsed: 0,
      ..SelfTestReport::default()
    }
    .live()
  );
  // Kernel silent: retryable stall, not proof of anything.
  assert!(!SelfTestReport::default().live());
}

#[test]
fn seq_tracker_counts_gaps_wraps_and_resets() {
  let mut tracker = SeqTracker::default();
  // Anchoring is silent, per cpu independently.
  assert_eq!(tracker.note(0, 100), 0);
  assert_eq!(tracker.note(1, 5000), 0);
  assert_eq!(tracker.note(0, 101), 0);
  assert_eq!(tracker.missed(), 0);
  // Forward jumps count their distance and re-anchor there.
  assert_eq!(tracker.note(0, 105), 3);
  assert_eq!(tracker.missed(), 3);
  assert_eq!(tracker.note(0, 106), 0);
  // u32 wrap is continuity, not a gap.
  assert_eq!(tracker.note(2, u32::MAX - 1), 0);
  assert_eq!(tracker.note(2, u32::MAX), 0);
  assert_eq!(tracker.note(2, 0), 0);
  assert_eq!(tracker.note(2, 2), 1);
  // Backward jump (counter restart, e.g. CPU hotplug) re-anchors silently.
  assert_eq!(tracker.note(3, 9000), 0);
  assert_eq!(tracker.note(3, 12), 0);
  assert_eq!(tracker.note(3, 13), 0);
  assert_eq!(tracker.missed(), 4);
}

#[test]
fn walk_observes_every_message_while_parse_takes_first() {
  use rsrpc_proc_events::{ProcEvent, parse_event, walk_proc_messages};

  // Two EXEC messages in one datagram, on different (cpu, seq).
  let mut first = proc_buf(0x2, 111);
  let mut second = proc_buf(0x2, 222);
  first[24..28].copy_from_slice(&10u32.to_le_bytes());
  second[24..28].copy_from_slice(&20u32.to_le_bytes());
  second[40..44].copy_from_slice(&1u32.to_le_bytes());
  let mut both = first;
  both.extend_from_slice(&second);

  // Forwarding keeps first-event-wins.
  assert_eq!(parse_event(&both), Some(ProcEvent::Exec(111)));
  // Tracking sees both (no early stop): continuity, not a gap.
  let mut seen = Vec::new();
  walk_proc_messages(&both, &mut |cpu, seq, event| {
    seen.push((cpu, seq, event.is_some()));
    true
  });
  assert_eq!(seen, vec![(0, 10, true), (1, 20, true)]);
}
