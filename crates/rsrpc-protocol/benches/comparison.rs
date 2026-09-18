//! Benchmarks comparing JSON vs MessagePack activity payloads.
//!
//! Run with: cargo bench

use criterion::{Criterion, criterion_group, criterion_main};
use rsrpc_protocol::commands::{
  RecentActivities, activity_fingerprint, cached_activity, set_activity_response,
};
use rsrpc_types::cmd::ActivityCmd;
use std::hint::black_box;

fn sample_command() -> ActivityCmd {
  serde_json::from_str(
    r#"{
      "cmd": "SET_ACTIVITY",
      "args": {
        "pid": 12345,
        "activity": {
          "name": "Awesome Game Title",
          "details": "Playing competitive ranked match",
          "state": "Score: 10-5 in Round 3",
          "timestamps": { "start": 1704067200000 },
          "buttons": [
            { "label": "Join Game", "url": "https://example.com/join" },
            { "label": "View Profile", "url": "https://example.com/profile" }
          ]
        }
      },
      "nonce": "bench"
    }"#,
  )
  .expect("sample command")
}

fn benchmark_cached_activity_json(c: &mut Criterion) {
  let mut cmd = sample_command();
  cmd.fix();

  c.bench_function("cached_activity_json_build", |b| {
    b.iter(|| {
      let fp = activity_fingerprint(black_box(&mut cmd));
      let cached = cached_activity(black_box(&mut cmd), fp).expect("payload");
      black_box(cached.json.len())
    })
  });
}

fn benchmark_cached_activity_msgpack(c: &mut Criterion) {
  let mut cmd = sample_command();
  cmd.fix();

  c.bench_function("cached_activity_msgpack_build", |b| {
    b.iter(|| {
      let fp = activity_fingerprint(black_box(&mut cmd));
      let cached = cached_activity(black_box(&mut cmd), fp).expect("payload");
      black_box(cached.msgpack.len())
    })
  });
}

fn benchmark_json_decode(c: &mut Criterion) {
  let mut cmd = sample_command();
  cmd.fix();
  let fp = activity_fingerprint(&mut cmd);
  let cached = cached_activity(&mut cmd, fp).expect("payload");
  let encoded = cached.json.clone();

  c.bench_function("json_decode_activity", |b| {
    b.iter(|| {
      let payload: serde_json::Value = serde_json::from_str(black_box(&encoded)).unwrap();
      black_box(payload)
    })
  });
}

fn benchmark_msgpack_decode(c: &mut Criterion) {
  let mut cmd = sample_command();
  cmd.fix();
  let fp = activity_fingerprint(&mut cmd);
  let cached = cached_activity(&mut cmd, fp).expect("payload");
  let encoded = cached.msgpack.clone();

  c.bench_function("msgpack_decode_activity", |b| {
    b.iter(|| {
      let payload: serde_json::Value = rmp_serde::from_slice(black_box(&encoded)).unwrap();
      black_box(payload)
    })
  });
}

fn benchmark_set_activity_response(c: &mut Criterion) {
  let mut cmd = sample_command();
  cmd.fix();

  c.bench_function("set_activity_response_build", |b| {
    b.iter(|| black_box(set_activity_response(black_box(&cmd)).expect("response")))
  });
}

fn benchmark_flood_drop_pipeline(c: &mut Criterion) {
  // The flood path after the reorder: fingerprint once, then drop on the
  // repeat publish — without ever building the envelope. The guard is
  // seeded once outside the loop so every measured iteration is a drop.
  let mut cmd = sample_command();
  cmd.fix();
  let mut guard = RecentActivities::default();
  {
    let fp = activity_fingerprint(&mut cmd).expect("fingerprint");
    assert!(!guard.should_drop("123", 42, Some(fp.as_slice()), std::time::Instant::now()));
  }
  c.bench_function("publish_flood_drop", |b| {
    b.iter(|| {
      let fp = activity_fingerprint(black_box(&mut cmd)).expect("fingerprint");
      black_box(guard.should_drop("123", 42, Some(fp.as_slice()), std::time::Instant::now()))
    })
  });
}

fn benchmark_message_size(c: &mut Criterion) {
  let mut cmd = sample_command();
  cmd.fix();
  let fp = activity_fingerprint(&mut cmd);
  let cached = cached_activity(&mut cmd, fp).expect("payload");

  let json_size = cached.json.len();
  let msgpack_size = cached.msgpack.len();

  println!("\nMessage size comparison:");
  println!("  JSON:     {} bytes", json_size);
  println!("  MsgPack:  {} bytes", msgpack_size);
  println!(
    "  Savings:  {:.1}%",
    (1.0 - msgpack_size as f64 / json_size as f64) * 100.0
  );

  c.bench_function("size_comparison", |b| {
    b.iter(|| (cached.json.len(), cached.msgpack.len()))
  });
}

criterion_group!(
  benches,
  benchmark_cached_activity_json,
  benchmark_cached_activity_msgpack,
  benchmark_json_decode,
  benchmark_msgpack_decode,
  benchmark_set_activity_response,
  benchmark_flood_drop_pipeline,
  benchmark_message_size,
);

criterion_main!(benches);
