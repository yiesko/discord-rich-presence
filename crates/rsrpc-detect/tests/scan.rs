//! Scanner pure-function tests: normalization, buffer reuse, exec reads.
//!
//! Only public-API behavior (fixtures needing crate-private `SortedIndex`
//! stay inline in `scan.rs` by design).

use rsrpc_detect::scan::{
  MatchScratch, normalize_lowered_into, normalize_name, normalize_name_into,
};
#[cfg(target_os = "linux")]
use rsrpc_detect::types::Exec;

/// Corpus covering case, forbidden punctuation, dots, whitespace
/// runs, empties, separators, `>` prefix and non-ASCII.
const CORPUS: &[&str] = &[
  "Game Name",
  "Name: Subtitle",
  "R.E.P.O. Ghost Haul",
  "Q.U.B.E.",
  "Mr. Bomber",
  "a  b   c",
  "",
  "   ",
  "a/b\\c",
  ">game",
  "/",
  "fish",
  "École: LÉGENDE",
  "MECCHA CHAMELEON",
  "PenguinHotel-Win64-Shipping.exe",
  "  padded  ",
  "a.b.c",
  "...",
];

/// `normalize_name_into` reproduces `normalize_name` byte-for-byte.
#[test]
fn normalize_into_matches_normalize() {
  let mut lower = String::new();
  let mut out = String::new();
  for input in CORPUS {
    normalize_name_into(input, &mut lower, &mut out);
    assert_eq!(&out, &normalize_name(input), "mismatch for {input:?}");
  }
}

/// On already-lowercase input, the lowered variant (no lowercase
/// stage) matches too — this is the hot path (`&lowered`).
#[test]
fn normalize_lowered_into_matches_on_lowercase_input() {
  let mut out = String::new();
  for input in CORPUS {
    let lowered = input.to_ascii_lowercase();
    normalize_lowered_into(&lowered, &mut out);
    assert_eq!(&out, &normalize_name(input), "mismatch for {input:?}");
  }
}

#[cfg(target_os = "linux")]
use rsrpc_detect::scan::{ExecScratch, read_exec, read_exec_into};

/// Own pid is always readable: deterministic fixture, no /proc guessing.
#[cfg(target_os = "linux")]
fn self_pid() -> u64 {
  u64::from(std::process::id())
}

/// `read_exec_into` classifies exactly like `read_exec`: same presence,
/// same pid, same path, same arguments.
#[cfg(target_os = "linux")]
#[test]
fn read_exec_into_matches_read_exec() {
  for pid in [self_pid(), 1] {
    let mut slot = Exec::default();
    let mut scratch = ExecScratch::default();
    let present = read_exec_into(pid, &mut slot, &mut scratch);
    match read_exec(pid) {
      None => assert!(!present, "into() must agree on unreadable pid {pid}"),
      Some(expected) => {
        assert!(present, "into() must agree on readable pid {pid}");
        assert_eq!(slot.pid, expected.pid);
        assert_eq!(slot.path, expected.path);
        assert_eq!(slot.arguments, expected.arguments);
      }
    }
  }
  // Out-of-range pid: both agree on absence without touching buffers.
  let mut slot = Exec::default();
  let mut scratch = ExecScratch::default();
  assert!(!read_exec_into(u64::MAX, &mut slot, &mut scratch));
  assert!(read_exec(u64::MAX).is_none());
}

/// Scratch buffers are stable across calls: no reallocations on
/// repeated use.
#[test]
fn match_scratch_buffers_are_stable() {
  let mut scratch = MatchScratch::default();
  let mut lower = String::new();
  normalize_name_into("Some Game: Title", &mut lower, &mut scratch.norm_out);
  normalize_lowered_into("some game title", &mut scratch.norm_out);
  let out_ptr = scratch.norm_out.as_ptr();
  let out_cap = scratch.norm_out.capacity();
  let lower_ptr = lower.as_ptr();
  normalize_name_into("Other: Name Here", &mut lower, &mut scratch.norm_out);
  normalize_lowered_into("other name here", &mut scratch.norm_out);
  assert!(
    std::ptr::eq(out_ptr, scratch.norm_out.as_ptr()),
    "norm_out moved"
  );
  assert!(std::ptr::eq(lower_ptr, lower.as_ptr()), "lower moved");
  assert!(scratch.norm_out.capacity() >= out_cap);
}
