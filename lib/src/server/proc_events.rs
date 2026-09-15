//! Linux process-event watcher (netlink `cn_proc`): EXEC/EXIT in real time.
//!
//! The periodic `/proc` scan stays as the source of truth (and the only
//! path off-Linux), but it is blind between ticks: game START waits up to
//! a full interval, game EXIT waits just as long to clear. This watcher
//! closes both gaps without polling:
//!
//! - `EXEC(pid)`: classify that ONE process immediately (same
//!   [`crate::server::process::ProcessServer::match_process`] as the scan
//!   loop) and emit hits at once — cards appear in milliseconds.
//! - `EXIT(pid)`: when a tracked game pid dies, wake the scan loop early
//!   so the natural full scan clears the slot at once instead of waiting
//!   out the interval.
//!
//! Protocol notes (validated against the kernel headers via the
//! `proc-connector` reference implementation):
//! - Every datagram is `nlmsghdr` (16B) + `cn_msg` (20B) + payload —
//!   sending a bare `cn_msg` is silently dropped by the kernel.
//! - `proc_event.what` is a bitmask (`EXEC = 0x2`, `EXIT = 0x80000000`;
//!   NOT sequential), pid sits at the exec/exit union base.
//! - Subscribe is acknowledged with `NLMSG_ERROR` (code 0); event flow
//!   itself is proven by the boot self-test below.
//!
//! Best-effort by design: setup failure (hardened kernel, containers,
//! missing caps) logs once and leaves pure polling in charge — never an
//! error, never a panic. Steady-state cost is ~zero syscalls (one
//! blocking `recv`); per-EXEC cost is one cmdline read plus the normal
//! classify chain, misses included. Build storms (`cargo build` forks
//! thousands of short-lived processes) only cost cmdline reads + AC
//! probes, still orders of magnitude below a full `/proc` sweep.
//!
//! Loss accounting: netlink delivery is officially lossy (`connector.rst`:
//! memory pressure, queue overruns), so every observed message feeds a
//! [`SeqTracker`] over the kernel per-CPU `cn_msg.seq` counter — silent
//! drops surface as debug-level gap lines plus a counter instead of
//! vanishing without a trace.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::{debug, log};

/// Netlink family for the kernel connector multiplexer.
const NETLINK_CONNECTOR: i32 = 11;
/// `nlmsghdr` size: len + type + flags (u32/u16/u16) + seq + pid (u32/u32).
const SIZE_NLMSGHDR: usize = 16;
/// Application message type for the subscription request.
const NLMSG_MIN_TYPE: u16 = 16;
/// Control message types the kernel may interleave.
const NLMSG_NOOP: u16 = 1;
const NLMSG_ERROR: u16 = 2;
const NLMSG_OVERRUN: u16 = 4;
/// Request flag for the subscription message.
const NLM_F_REQUEST: u16 = 1;
/// `cn_proc` identifiers (`linux/connector.h`: `CN_IDX_PROC`/`CN_VAL_PROC`).
const CN_IDX_PROC: u32 = 0x1;
const CN_VAL_PROC: u32 = 0x1;
/// `struct cn_msg` header size: idx + val + seq + ack (u32) + len + flags (u16).
const SIZE_CN_MSG: usize = 20;
/// Multicast ops (`linux/cn_proc.h`).
const PROC_CN_MCAST_LISTEN: u32 = 0x1;
/// `proc_event.what` bitmask values (`linux/cn_proc.h` — NOT sequential).
const PROC_EVENT_EXEC: u32 = 0x0000_0002;
const PROC_EVENT_EXIT: u32 = 0x8000_0000;

/// Lifecycle event worth waking up for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProcEvent {
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
/// cpu first, matching [`SeqTracker::note`].
///
/// `event` is `None` for types we do not forward (FORK, COMM…) — the
/// kernel counter advances per message regardless of type (verified
/// live: consecutive per-cpu sequences span mixed types), so continuity
/// tracking must see all of them, not just forwarded events. Returning
/// `false` stops the walk early; short/corrupt datagrams and nonzero
/// `ERROR`s stop it with nothing further — exactly the historical
/// `parse_event` outcomes, which delegates here (single walk, single
/// WHAT mapping, no duplicated parse logic).
pub(crate) fn walk_proc_messages(
  buf: &[u8],
  visit: &mut impl FnMut(u32, u32, Option<ProcEvent>) -> bool,
) {
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

pub(crate) fn parse_event(buf: &[u8]) -> Option<ProcEvent> {
  let mut found = None;
  walk_proc_messages(buf, &mut |_, _, event| {
    found = found.or(event);
    true
  });
  found
}

/// Parse the `cn_msg` + `proc_event` body of one data message.
fn parse_proc_event(body: &[u8]) -> Option<ProcEvent> {
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

/// Tracks the kernel per-CPU event sequence (`cn_msg.seq`) to detect
/// silently lost netlink traffic at runtime: delivery is officially
/// lossy (memory pressure, queue overruns — see `connector.rst`), and a
/// stall leaves no other trace.
///
/// Generic by construction: no assumed CPU count, topology, counter
/// phase or kernel version — each cpu anchors on first sight with
/// wrapping arithmetic throughout. Self-neutralizing where the counter
/// semantics do not hold: a constant seq re-anchors every datagram
/// (backward jump, no alarm), so the worst case on exotic kernels is a
/// quiet no-op, never false alarms.
#[derive(Clone, Debug, Default)]
pub(crate) struct SeqTracker {
  last: HashMap<u32, u32>,
  missed: u64,
}

impl SeqTracker {
  /// Observe one `(cpu, seq)` pair. Returns missed events since the last
  /// observation on that cpu: 0 when continuous, on first anchoring, or
  /// on re-anchoring after a counter restart (e.g. CPU hotplug).
  /// Forward jumps — including across the u32 wrap — count their
  /// wrapping distance.
  pub fn note(&mut self, cpu: u32, seq: u32) -> u64 {
    match self.last.entry(cpu) {
      Entry::Vacant(slot) => {
        slot.insert(seq);
        0
      }
      Entry::Occupied(mut slot) => {
        let expected = slot.get().wrapping_add(1);
        if seq == expected {
          // Advance the anchor: without this every later observation
          // compares against a stale value and misfires.
          slot.insert(seq);
          0
        } else {
          // Circular comparison (TCP/RTP style): forward jumps count,
          // backward jumps mean the counter restarted — re-anchor.
          let missed = u64::from(seq.wrapping_sub(expected));
          slot.insert(seq);
          if missed <= u64::from(u32::MAX) / 2 {
            self.missed = self.missed.saturating_add(missed);
            missed
          } else {
            0
          }
        }
      }
    }
  }

  /// Total missed events observed (saturating).
  #[must_use]
  pub fn missed(&self) -> u64 {
    self.missed
  }
}

/// Subscribe a netlink connector socket to `cn_proc` broadcasts: socket,
/// bind (kernel-assigned pid, group member), framed LISTEN request, then
/// read the kernel's ACK. Returns the fd plus whether any ack datagram
/// arrived (an ACK proves the LISTEN registered; its absence with later
/// silence points at registration, not traffic), or a message when the
/// kernel refuses (caller falls back to polling).
fn subscribe() -> Result<(i32, bool), String> {
  // SAFETY: socket/bind/sendmsg/recv/close are called with valid
  // arguments; the fd is closed by the caller on every path (see `watch`).
  let fd = unsafe {
    libc::socket(
      libc::AF_NETLINK,
      libc::SOCK_DGRAM | libc::SOCK_CLOEXEC,
      NETLINK_CONNECTOR,
    )
  };
  if fd < 0 {
    return Err(format!("socket failed: {}", last_os_error()));
  }
  // Room for exec storms: a burst overflowing the default ~200KB rcvbuf
  // surfaces as ENOBUFS (a dropped event, not a dead socket) — size up
  // best-effort, the overrun paths below stay correct regardless.
  {
    let size = 1024 * 1024 as libc::c_int;
    // SAFETY: setsockopt with a valid int pointer and length.
    unsafe {
      libc::setsockopt(
        fd,
        libc::SOL_SOCKET,
        libc::SO_RCVBUF,
        &size as *const libc::c_int as *const libc::c_void,
        size_of::<libc::c_int>() as u32,
      );
    }
  }
  // Explicit init for every meaningful field; padding stays zeroed
  // (all-zero `sockaddr_nl` padding is the correct value). `nl_pad` is
  // private in libc 0.2, so full struct-literal init is impossible.
  // SAFETY: `Padding<u16>` has no validity invariant beyond its bytes,
  // and `nl_family`/`nl_pid`/`nl_groups` are overwritten below before
  // `bind` reads the struct.
  let mut addr: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
  addr.nl_family = libc::AF_NETLINK as u16;
  addr.nl_pid = 0; // kernel picks our port id
  addr.nl_groups = CN_IDX_PROC;
  let bound = unsafe {
    libc::bind(
      fd,
      &addr as *const libc::sockaddr_nl as *const libc::sockaddr,
      size_of::<libc::sockaddr_nl>() as u32,
    )
  };
  if bound != 0 {
    let err = last_os_error();
    unsafe {
      libc::close(fd);
    }
    return Err(format!("bind failed: {err}"));
  }
  // Framed subscription: nlmsghdr + cn_msg + one u32 payload.
  let payload_len = 4;
  let total_len = SIZE_NLMSGHDR + SIZE_CN_MSG + payload_len;
  let mut message = vec![0u8; total_len];
  message[0..4].copy_from_slice(&(total_len as u32).to_le_bytes());
  message[4..6].copy_from_slice(&NLMSG_MIN_TYPE.to_le_bytes());
  message[6..8].copy_from_slice(&NLM_F_REQUEST.to_le_bytes());
  message[8..12].copy_from_slice(&0u32.to_le_bytes());
  message[12..16].copy_from_slice(&std::process::id().to_le_bytes());
  let cn = SIZE_NLMSGHDR;
  message[cn..cn + 4].copy_from_slice(&CN_IDX_PROC.to_le_bytes());
  message[cn + 4..cn + 8].copy_from_slice(&CN_VAL_PROC.to_le_bytes());
  message[cn + 8..cn + 12].copy_from_slice(&0u32.to_le_bytes());
  message[cn + 12..cn + 16].copy_from_slice(&0u32.to_le_bytes());
  message[cn + 16..cn + 18].copy_from_slice(&(payload_len as u16).to_le_bytes());
  message[cn + 18..cn + 20].copy_from_slice(&0u16.to_le_bytes());
  message[cn + 20..cn + 24].copy_from_slice(&PROC_CN_MCAST_LISTEN.to_le_bytes());
  let sent = unsafe {
    libc::send(
      fd,
      message.as_ptr() as *const libc::c_void,
      message.len(),
      0,
    )
  };
  if sent != message.len() as isize {
    let err = last_os_error();
    unsafe {
      libc::close(fd);
    }
    return Err(format!("subscribe failed: {err}"));
  }
  // Read the kernel's ACK (bounded: this must not hang boot when the
  // kernel takes the message yet answers nothing). A timeout is NOT a
  // refusal — quiet systems emit nothing to answer with — so only an
  // explicit nonzero ERROR aborts; delivery itself is proven by the
  // self-test below.
  set_recv_timeout(fd, Some(std::time::Duration::from_secs(2)));
  let mut ack = [0u8; 4096];
  let received = unsafe { libc::recv(fd, ack.as_mut_ptr() as *mut libc::c_void, ack.len(), 0) };
  let mut ack_seen = false;
  if received > 0 {
    ack_seen = true;
    // A nonzero ERROR code refuses us; anything else (zero-ack, an early
    // event that won the race) means proceed.
    let ack = &ack[..received as usize];
    if ack.len() >= SIZE_NLMSGHDR + 4 {
      let msg_type = u16::from_le_bytes(ack[4..6].try_into().unwrap_or([0, 0]));
      if msg_type == NLMSG_ERROR {
        let code = i32::from_le_bytes(
          ack[SIZE_NLMSGHDR..SIZE_NLMSGHDR + 4]
            .try_into()
            .unwrap_or([0, 0, 0, 0]),
        );
        if code != 0 {
          let err = std::io::Error::from_raw_os_error(code);
          unsafe {
            libc::close(fd);
          }
          return Err(format!("subscribe refused: {err}"));
        }
      }
    }
  }
  Ok((fd, ack_seen))
}

fn last_os_error() -> String {
  std::io::Error::last_os_error().to_string()
}

/// Set (or clear) the receive timeout on the netlink fd.
fn set_recv_timeout(fd: i32, timeout: Option<std::time::Duration>) {
  let (secs, usecs) = match timeout {
    Some(duration) => (
      duration.as_secs() as libc::time_t,
      duration.subsec_micros() as libc::suseconds_t,
    ),
    None => (0, 0),
  };
  let timeval = libc::timeval {
    tv_sec: secs,
    tv_usec: usecs,
  };
  // SAFETY: setsockopt with a valid timeval pointer and length.
  unsafe {
    libc::setsockopt(
      fd,
      libc::SOL_SOCKET,
      libc::SO_RCVTIMEO,
      &timeval as *const libc::timeval as *const libc::c_void,
      size_of::<libc::timeval>() as u32,
    );
  }
}

/// What the boot self-test observed. `live()` decides the watcher:
///
/// - `parsed > 0`: delivery proven (any EXEC/EXIT, need not be ours).
/// - `datagrams > 0, parsed == 0`: the kernel talks but nothing parses
///   (framing drift — a parser bug, reportable with these counts).
/// - zeros: the kernel is silent for this socket (transient stall or a
///   filtering kernel/LSM — retryable, see the caller).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SelfTestReport {
  pub datagrams: u32,
  pub parsed: u32,
  /// Recvs that timed out (EAGAIN) — silence, not failure.
  pub timeouts: u32,
  /// First fatal recv errno, if any (anything but timeout/interrupt).
  /// Distinguishes a deaf socket (timeouts only) from a broken one.
  pub fatal_errno: Option<i32>,
}

impl SelfTestReport {
  /// Delivery is proven only by a successfully parsed event.
  #[must_use]
  pub fn live(self) -> bool {
    self.parsed > 0
  }
}

/// Prove the subscription actually delivers: spawn a trivial child (its
/// exec postdates our subscribe) and wait up to a second for ANY valid
/// proc event. Some kernels/LSMs accept the LISTEN message yet deliver
/// nothing — without this check the watcher would idle forever claiming
/// to be live while polling does all the work unnoticed.
fn self_test(fd: i32) -> SelfTestReport {
  // Bound every recv below: without this, a silently non-delivering
  // kernel hangs the watcher thread forever with zero logs.
  set_recv_timeout(fd, Some(std::time::Duration::from_secs(1)));
  let probe = ["/bin/true", "/usr/bin/true"]
    .iter()
    .find(|path| std::path::Path::new(path).exists());
  let Some(probe) = probe else {
    // Nowhere to probe with: fall back to polling (safe direction) —
    // claiming live without proof hid real outages before.
    return SelfTestReport::default();
  };
  // The child must be spawned after our subscribe (its exec postdates
  // it) and reaped whatever happens next.
  let mut child = std::process::Command::new(probe).spawn().ok();
  let mut report = SelfTestReport::default();
  if child.is_some() {
    let mut buf = [0u8; 65536];
    // Up to ~10s total (socket timeout bounds each recv): first valid
    // event proves delivery — it need not be ours, any exec on a live
    // desktop arrives within milliseconds.
    for _ in 0..10 {
      let received = unsafe { libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
      if received <= 0 {
        // Timeout (EAGAIN/EWOULDBLOCK) or EINTR: keep waiting out the
        // second. ENOBUFS means the kernel IS delivering (faster than we
        // drain) — that alone proves liveness. Anything else aborts.
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOBUFS) {
          report.datagrams += 1;
          report.parsed += 1;
          break;
        }
        match err.kind() {
          std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted => {
            report.timeouts += 1;
            continue;
          }
          _ => {
            report.fatal_errno = report.fatal_errno.or(err.raw_os_error());
            break;
          }
        }
      }
      report.datagrams += 1;
      if parse_event(&buf[..received as usize]).is_some() {
        report.parsed += 1;
        break;
      }
    }
  }
  if let Some(mut child) = child.take() {
    let _ = child.wait();
  }
  report
}

/// Block on `cn_proc` broadcasts forever, forwarding lifecycle events.
/// Returns only on receive errors (the caller logs once and keeps
/// polling); the fd is closed on the way out.
pub(crate) fn watch(events: super::utils::GaugeSender<ProcEvent>) -> Result<(), String> {
  let (fd, ack_seen) = subscribe()?;
  let report = self_test(fd);
  if !report.live() {
    unsafe {
      libc::close(fd);
    }
    let ack = if ack_seen {
      "subscribe acked"
    } else {
      "no subscribe ack"
    };
    let fatal = report.fatal_errno.map_or_else(
      || "none".to_string(),
      |errno| std::io::Error::from_raw_os_error(errno).to_string(),
    );
    return Err(format!(
      "self-test saw {} datagram(s), {} parsed, {} timeouts, fatal errno {} in ~10s ({})",
      report.datagrams, report.parsed, report.timeouts, fatal, ack
    ));
  }
  log!(
    "[Process Scanner] proc-events watcher live (netlink cn_proc; best-effort, may rarely go silent — polling backstops)"
  );
  // Back to blocking: the self-test's timeout was temporary.
  set_recv_timeout(fd, None);
  // 64KiB datagrams: one netlink message is ~76B, so bursts of hundreds
  // of EXECs under load (build storms) arrive intact instead of being
  // truncated and dropped wholesale by the length guard below.
  let mut buf = [0u8; 65536];
  let mut seqs = SeqTracker::default();
  loop {
    let received = unsafe { libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
    if received < 0 {
      let err = std::io::Error::last_os_error();
      if err.kind() == std::io::ErrorKind::Interrupted {
        continue;
      }
      // Overrun under burst load drops events but the socket stays valid:
      // stay subscribed, polling backstops the gap.
      if err.raw_os_error() == Some(libc::ENOBUFS) {
        continue;
      }
      unsafe {
        libc::close(fd);
      }
      return Err(format!(
        "recv failed: {err} ({} seq gap(s) seen)",
        seqs.missed()
      ));
    }
    if received == 0 {
      continue;
    }
    // One shared walk feeds both continuity tracking and forwarding:
    // every message advances the per-cpu sequence (even unforwarded
    // types), while forwarding keeps first-event-wins. Walking the whole
    // (usually single-message) datagram costs nothing measurable.
    let bytes = &buf[..received as usize];
    let mut found = None;
    walk_proc_messages(bytes, &mut |cpu, seq, event| {
      let missed = seqs.note(cpu, seq);
      if missed > 0 {
        debug!("[Process Scanner] cn_proc sequence gap on cpu {cpu}: missed {missed} event(s)");
      }
      found = found.or(event);
      true
    });
    if let Some(event) = found
      && events.send(event).is_err()
    {
      // Receiver gone (daemon shutting down): quiet exit.
      unsafe {
        libc::close(fd);
      }
      return Ok(());
    }
  }
}
