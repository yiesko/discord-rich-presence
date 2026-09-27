//! Linux process-event watcher (netlink `cn_proc`): EXEC/EXIT in real time.
//!
//! The periodic `/proc` scan stays as the source of truth (and the only
//! path off-Linux), but it is blind between ticks: game START waits up to
//! a full interval, game EXIT waits just as long to clear. This watcher
//! closes both gaps without polling:
//!
//! - `EXEC(pid)`: classify that ONE process immediately (same matcher as
//!   the scan loop) and emit hits at once — cards appear in milliseconds.
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

mod protocol;
mod watch;

pub use protocol::{MAX_WATCH_BACKLOG, ProcEvent, parse_event, walk_proc_messages};
pub use watch::{SelfTestReport, SeqTracker, forward_proc_events, watch};
