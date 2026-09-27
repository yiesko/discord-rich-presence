/// Cap on watcher-to-dispatch backlog: bursts of hundreds of EXECs under
/// load (build storms) fit comfortably; a sustained flood past this sheds
/// (counted) instead of growing memory without bound. Shed EXECs are
/// best-effort hints only — the periodic scan backstops every one, so no
/// presence state depends on them.
pub const MAX_WATCH_BACKLOG: usize = 1024;

/// Netlink family for the kernel connector multiplexer.
#[cfg(target_os = "linux")]
pub(crate) const NETLINK_CONNECTOR: i32 = 11;
/// Steady-state receive deadline inside `watch()`: every expiry re-checks
/// the shutdown flag, so a silent socket never pins the thread past
/// shutdown (join-safe). One extra syscall per interval per daemon is
/// unmeasurable next to the 5s scan cadence.
#[cfg(target_os = "linux")]
pub(crate) const WATCH_RECV_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
/// `nlmsghdr` size: len + type + flags (u32/u16/u16) + seq + pid (u32/u32).
pub(crate) const SIZE_NLMSGHDR: usize = 16;
/// Application message type for the subscription request.
#[cfg(target_os = "linux")]
pub(crate) const NLMSG_MIN_TYPE: u16 = 16;
/// Control message types the kernel may interleave.
pub(crate) const NLMSG_NOOP: u16 = 1;
pub(crate) const NLMSG_ERROR: u16 = 2;
pub(crate) const NLMSG_OVERRUN: u16 = 4;
/// Request flag for the subscription message.
#[cfg(target_os = "linux")]
pub(crate) const NLM_F_REQUEST: u16 = 1;
/// `cn_proc` identifiers (`linux/connector.h`: `CN_IDX_PROC`/`CN_VAL_PROC`).
pub(crate) const CN_IDX_PROC: u32 = 0x1;
pub(crate) const CN_VAL_PROC: u32 = 0x1;
/// `struct cn_msg` header size: idx + val + seq + ack (u32) + len + flags (u16).
pub(crate) const SIZE_CN_MSG: usize = 20;
/// Multicast ops (`linux/cn_proc.h`).
#[cfg(target_os = "linux")]
pub(crate) const PROC_CN_MCAST_LISTEN: u32 = 0x1;
/// `proc_event.what` bitmask values (`linux/cn_proc.h` — NOT sequential).
pub(crate) const PROC_EVENT_EXEC: u32 = 0x0000_0002;
pub(crate) const PROC_EVENT_EXIT: u32 = 0x8000_0000;

/// Lifecycle event worth waking up for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcEvent {
  Exec(u64),
  Exit(u64),
}

/// Parse one netlink datagram into a lifecycle event. Walks the
/// `nlmsghdr`-framed messages: the kernel wraps `cn_proc` traffic —
/// events AND the subscription acknowledgement — in `NLMSG_DONE`
/// (verified live against Fedora kernel 7.2: a busy desktop streams
/// identical 76-byte DONE datagrams carrying fork/exec/comm events).
/// `NOOP` is skipped, nonzero `ERROR` aborts the datagram.
/// Unknown/short/corrupt input is `None` (the watcher skips it — the
/// periodic scan is the backstop, so a dropped event only costs
/// latency, never correctness).
/// Walk every message in a netlink datagram, invoking `visit` for each
/// `cn_proc` data message (valid idx/val) with `(cpu, seq, event)` —
/// cpu first, matching `SeqTracker::note` (watch side) argument order.
///
/// `event` is `None` for types we do not forward (FORK, COMM…) — the
/// kernel counter advances per message regardless of type (verified
/// live: consecutive per-cpu sequences span mixed types), so continuity
/// tracking must see all of them, not just forwarded events. Returning
/// `false` stops the walk early; short/corrupt datagrams and nonzero
/// `ERROR`s stop it with nothing further — exactly the historical
/// `parse_event` outcomes, which delegates here (single walk, single
/// WHAT mapping, no duplicated parse logic).
pub fn walk_proc_messages(buf: &[u8], visit: &mut impl FnMut(u32, u32, Option<ProcEvent>) -> bool) {
  let mut offset = 0;
  while buf.len() - offset >= SIZE_NLMSGHDR {
    let len = u32::from_le_bytes([
      buf[offset],
      buf[offset + 1],
      buf[offset + 2],
      buf[offset + 3],
    ]) as usize;
    let msg_type = u16::from_le_bytes([buf[offset + 4], buf[offset + 5]]);
    if len < SIZE_NLMSGHDR || buf.len() - offset < len {
      return;
    }
    let body = &buf[offset + SIZE_NLMSGHDR..offset + len];
    match msg_type {
      NLMSG_NOOP | NLMSG_OVERRUN => {}
      NLMSG_ERROR => {
        // Error acks carry a nonzero code in the first 4 bytes; a zero
        // code is the subscription acknowledgement — both skipped.
        if body.len() >= 4 && i32::from_le_bytes([body[0], body[1], body[2], body[3]]) != 0 {
          return;
        }
      }
      // Data messages: the kernel wraps cn_proc traffic — events AND the
      // subscription acknowledgement — in NLMSG_DONE (type 3, verified
      // live), and unknown future types attempt the same parse
      // (forward-compatible).
      _ => {
        // Sequence + cpu ride in every cn_proc header; the event itself
        // only when the full body is present (historical rule, kept in
        // `parse_proc_event` below).
        if body.len() >= SIZE_CN_MSG + 8 {
          let idx = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
          let val = u32::from_le_bytes([body[4], body[5], body[6], body[7]]);
          if idx == CN_IDX_PROC && val == CN_VAL_PROC {
            let seq = u32::from_le_bytes([body[8], body[9], body[10], body[11]]);
            let cpu = u32::from_le_bytes([body[24], body[25], body[26], body[27]]);
            if !visit(cpu, seq, parse_proc_event(body)) {
              return;
            }
          }
        }
      }
    }
    // Messages are 4-byte aligned; guard against a zero stride.
    offset += len.max(SIZE_NLMSGHDR);
    if offset >= buf.len() {
      break;
    }
  }
}

/// First lifecycle event in a netlink datagram, if any (single-shot helper
/// for tests and slow paths; the watcher uses the streaming walker).
pub fn parse_event(buf: &[u8]) -> Option<ProcEvent> {
  let mut found = None;
  walk_proc_messages(buf, &mut |_, _, event| {
    found = found.or(event);
    true
  });
  found
}

/// Parse the `cn_msg` + `proc_event` body of one data message.
pub(crate) fn parse_proc_event(body: &[u8]) -> Option<ProcEvent> {
  if body.len() < SIZE_CN_MSG + 20 {
    return None;
  }
  let idx = u32::from_le_bytes(body[0..4].try_into().ok()?);
  let val = u32::from_le_bytes(body[4..8].try_into().ok()?);
  if idx != CN_IDX_PROC || val != CN_VAL_PROC {
    return None;
  }
  let event = &body[SIZE_CN_MSG..];
  let what = u32::from_le_bytes(event[0..4].try_into().ok()?);
  let pid = u32::from_le_bytes(event[16..20].try_into().ok()?) as u64;
  match what {
    PROC_EVENT_EXEC => Some(ProcEvent::Exec(pid)),
    PROC_EVENT_EXIT => Some(ProcEvent::Exit(pid)),
    _ => None,
  }
}
