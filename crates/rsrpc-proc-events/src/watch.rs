use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(target_os = "linux")]
use rsrpc_telemetry::{GaugeSender, SendChecked};

#[cfg(target_os = "linux")]
use super::protocol::{
  CN_IDX_PROC, CN_VAL_PROC, MAX_WATCH_BACKLOG, NETLINK_CONNECTOR, NLM_F_REQUEST, NLMSG_ERROR,
  NLMSG_MIN_TYPE, PROC_CN_MCAST_LISTEN, SIZE_CN_MSG, SIZE_NLMSGHDR, WATCH_RECV_TIMEOUT,
  parse_event,
};
use super::protocol::{ProcEvent, walk_proc_messages};

/// Walk one datagram, recording per-CPU sequence continuity and
/// forwarding EVERY parsed lifecycle event through `emit` in order (a
/// datagram routinely carries several). Stops early when `emit` reports
/// the receiver is gone. Returns the number of events forwarded.
pub fn forward_proc_events(
  buf: &[u8],
  seqs: &mut SeqTracker,
  emit: &mut impl FnMut(ProcEvent) -> bool,
) -> usize {
  let mut forwarded = 0;
  walk_proc_messages(buf, &mut |cpu, seq, event| {
    let missed = seqs.note(cpu, seq);
    if missed > 0 {
      tracing::debug!(
        "[Process Scanner] cn_proc sequence gap on cpu {cpu}: missed {missed} event(s)"
      );
    }
    if let Some(event) = event {
      if !emit(event) {
        return false;
      }
      forwarded += 1;
    }
    true
  });
  forwarded
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
/// (backward jump, no alarm). False alarms are possible on CPU-hotplug
/// events where the counter resumes above the old anchor within the
/// forward half-window; the re-anchor logic handles the next datagram
/// correctly.
#[derive(Clone, Debug, Default)]
pub struct SeqTracker {
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
#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
fn last_os_error() -> String {
  std::io::Error::last_os_error().to_string()
}

/// Set (or clear) the receive timeout on the netlink fd.
#[cfg(target_os = "linux")]
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
pub struct SelfTestReport {
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
#[cfg(target_os = "linux")]
fn self_test(fd: i32, stop: &AtomicBool) -> SelfTestReport {
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
      // Directed shutdown short-circuits the ~10s probe: the caller
      // exits quietly right after (see below).
      if stop.load(Ordering::Acquire) {
        break;
      }
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

/// Block on `cn_proc` broadcasts, forwarding lifecycle events. Returns on
/// receive errors (the caller logs once and keeps polling), when the
/// receiver is gone, or when `stop` is set (directed shutdown: the retry
/// thread must never wait out a blocked receive); the fd is closed on
/// the way out.
///
/// Linux only: other platforms get a stub that always refuses (the
/// periodic scan is the only path there).
#[cfg(target_os = "linux")]
pub fn watch(events: &GaugeSender<ProcEvent>, stop: &AtomicBool) -> Result<(), String> {
  // Preset stop skips subscribe, self-test and the blocking receive
  // entirely: shutdown before the first watch must not touch netlink.
  if stop.load(Ordering::Acquire) {
    return Ok(());
  }
  let (fd, ack_seen) = subscribe()?;
  let report = self_test(fd, stop);
  // Directed shutdown during the self-test: exit quietly instead of
  // reporting stale liveness from a socket we are abandoning.
  if stop.load(Ordering::Acquire) {
    tracing::debug!("[Process Scanner] watcher stopping on shutdown (self-test cut)");
    // SAFETY: `fd` is the netlink socket owned by this function; this
    // return ends ownership (same discipline as the paths below).
    unsafe {
      libc::close(fd);
    }
    return Ok(());
  }
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
  tracing::info!(
    "[Process Scanner] proc-events watcher live (netlink cn_proc; best-effort, may rarely go silent — polling backstops)"
  );
  // Short deadline instead of blocking: every expiry re-checks the
  // shutdown flag below, so a silent socket never pins the thread past
  // shutdown. The self-test's longer timeout was temporary.
  set_recv_timeout(fd, Some(WATCH_RECV_TIMEOUT));
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
      // Receive deadline expiry (not an error): re-check directed
      // shutdown, then keep watching. Without this arm a silent socket
      // would pin the thread (and the daemon's join) indefinitely.
      if err.kind() == std::io::ErrorKind::WouldBlock {
        if stop.load(Ordering::Acquire) {
          tracing::debug!("[Process Scanner] watcher stopping on shutdown");
          unsafe {
            libc::close(fd);
          }
          return Ok(());
        }
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
    // Directed shutdown even under continuous event flood: without this,
    // a busy socket would keep the loop in recv/forward while join waits
    // (the receiver-drop chain only fires once dispatch exits).
    if stop.load(Ordering::Acquire) {
      tracing::debug!("[Process Scanner] watcher stopping on shutdown (flood cut)");
      // SAFETY: `fd` is the netlink socket owned by this function; this
      // return ends ownership (same discipline as the error paths below).
      unsafe {
        libc::close(fd);
      }
      return Ok(());
    }
    // One shared walk feeds both continuity tracking and forwarding:
    // every message advances the per-cpu sequence (even unforwarded
    // types) while every parsed event is forwarded at once. Walking the
    // whole (usually single-message) datagram costs nothing measurable.
    let bytes = &buf[..received as usize];
    let mut gone = false;
    forward_proc_events(bytes, &mut seqs, &mut |event| {
      // Shed under burst load stays subscribed (counted upstream, polling
      // backstops it); only a gone receiver ends the watch.
      match events.send_checked(event, MAX_WATCH_BACKLOG) {
        SendChecked::Closed => {
          gone = true;
          false
        }
        SendChecked::Sent | SendChecked::Shed => true,
      }
    });
    if gone {
      // Receiver gone (daemon shutting down): quiet exit.
      unsafe {
        libc::close(fd);
      }
      return Ok(());
    }
  }
}

/// Non-Linux stub: `cn_proc` does not exist there, so watching always
/// refuses and the periodic scan stays the only path.
#[cfg(not(target_os = "linux"))]
pub fn watch(
  _events: &rsrpc_telemetry::GaugeSender<ProcEvent>,
  stop: &AtomicBool,
) -> Result<(), String> {
  if stop.load(Ordering::Acquire) {
    return Ok(());
  }
  Err("proc-events unsupported on this platform".to_string())
}
