//! Hasher choice regression guard: FxHash must beat SipHash on the
//! scanner's key shapes (`u64` pids), or the `FxHashMap` use below is
//! unjustified removed.
//!
//! Run with: cargo bench -p rsrpc-detect

use criterion::{Criterion, criterion_group, criterion_main};
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::hint::black_box;

const PIDS: usize = 500;

/// Scattered pid-like keys (golden-ratio stride, no clustering).
fn pid_keys() -> Vec<u64> {
  (1..=PIDS as u64)
    .map(|pid| pid.wrapping_mul(0x9E3779B97F4A7C15))
    .collect()
}

/// Lookup race: std SipHash vs FxHash on pid keys.
fn bench_get_hit(c: &mut Criterion) {
  let keys = pid_keys();
  let std_map: HashMap<u64, u64> = keys.iter().map(|&k| (k, k)).collect();
  let fx_map: FxHashMap<u64, u64> = keys.iter().map(|&k| (k, k)).collect();

  c.bench_function("map_get_hit_std", |b| {
    b.iter(|| {
      let mut sum = 0u64;
      for key in &keys {
        sum = sum.wrapping_add(*std_map.get(black_box(key)).unwrap());
      }
      black_box(sum)
    })
  });
  c.bench_function("map_get_hit_fx", |b| {
    b.iter(|| {
      let mut sum = 0u64;
      for key in &keys {
        sum = sum.wrapping_add(*fx_map.get(black_box(key)).unwrap());
      }
      black_box(sum)
    })
  });
}

/// Insert race: std SipHash vs FxHash on pid keys.
fn bench_insert(c: &mut Criterion) {
  let keys = pid_keys();
  c.bench_function("map_insert_std", |b| {
    b.iter(|| {
      let mut map = HashMap::with_capacity(PIDS);
      for key in &keys {
        map.insert(*key, black_box(*key));
      }
      black_box(map)
    })
  });
  c.bench_function("map_insert_fx", |b| {
    b.iter(|| {
      let mut map = FxHashMap::with_capacity_and_hasher(PIDS, Default::default());
      for key in &keys {
        map.insert(*key, black_box(*key));
      }
      black_box(map)
    })
  });
}

criterion_group!(benches, bench_get_hit, bench_insert);
criterion_main!(benches);
