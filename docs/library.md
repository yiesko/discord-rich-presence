# Using rsRPC as a library

The `rsrpc-core` crate exposes the daemon: load a detectable database,
stage overrides, run one-shot diagnostics, or run the whole bridge until
your own future completes.

## Add the dependency

`rsrpc-core` is not published to crates.io — depend on the git tag:

```toml
[dependencies]
rsrpc-core = { git = "https://github.com/yiesko/discord-rich-presence", tag = "v0.36.0" }
tokio = { version = "1.53", features = ["rt-multi-thread", "macros", "signal"] }
```

Add `rsrpc-detect` from the same repository if you need to name
`DetectableActivity` yourself (to build entries by hand, for example):

```toml
rsrpc-detect = { git = "https://github.com/yiesko/discord-rich-presence", tag = "v0.36.0" }
```

## Minimal example

```rust
use rsrpc_core::{Daemon, RPCConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let daemon = Daemon::from_bundled(RPCConfig::default())?;
    daemon
        .run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
```

`run_until` takes a shutdown future: whatever you pass resolves when the
daemon should stop (a Ctrl+C signal, a channel closing, a test ending).
Teardown is total — transports, bridge, scanner threads and the event
pump all stop and join, in reverse startup order. When `run_until`
returns, no rsRPC thread is still running, so embedding more than one
daemon lifetime per process is safe.

## Loading the database

| Constructor | Input |
|---|---|
| `Daemon::from_bundled(config)` | The snapshot embedded in the crate. Works offline. |
| `Daemon::from_file(path, config)` | A `detectable.json` file on disk. |
| `Daemon::from_json_str(body, config)` | A JSON string you already have (arRPC's list works). |
| `Daemon::from_parsed(entries, config)` | A `Vec<DetectableActivity>` you parsed yourself. Never fails — parsing already happened. |

```rust
use rsrpc_core::{Daemon, RPCConfig};

// From a file.
let daemon = Daemon::from_file(
    std::path::Path::new("./detectable.json"),
    RPCConfig::default(),
)?;

// From a string you fetched yourself.
async fn from_body(body: String) -> Result<Daemon, Box<dyn std::error::Error>> {
    Ok(Daemon::from_json_str(body, RPCConfig::default())?)
}
```

## Configuration

`RPCConfig` is `#[non_exhaustive]`, so build it with
`RPCConfig::builder()` (or start from `RPCConfig::default()` and set
fields). The builder is the forward-compatible choice: new fields will
never break it.

```rust
use rsrpc_core::RPCConfig;

let config = RPCConfig::builder()
    .port(1400)
    .bridge_port_end(1410)
    .scan_interval_secs(10)
    .enable_db_update(true)
    .build();
```

Every field, with its default:

| Field | Default | Meaning (CLI equivalent) |
|---|---|---|
| `enable_process_scanner` | `true` | Run the process scanner (`-n` disables) |
| `enable_proc_events` | `true` | Netlink `cn_proc` fast path (`--no-proc-events` disables) |
| `enable_ipc_connector` | `true` | Serve game clients over IPC (`discord-ipc-0..9`) |
| `enable_websocket_connector` | `true` | Serve game clients over WebSocket |
| `enable_secondary_events` | `true` | Forward browser/deep-link/callback commands |
| `port` | `1337` | First JSON bridge port (`--bridge-port`) |
| `bridge_port_end` | `1347` | Last JSON bridge port (`--bridge-port-end`) |
| `msgpack_port` | `1338` | MessagePack bridge port (`--msgpack-port`) |
| `ws_port_start` / `ws_port_end` | `6463` / `6472` | Game websocket range (`--ws-port-start` / `--ws-port-end`) |
| `scan_interval_secs` | `5` | Base scan cadence (`--scan-interval-secs`) |
| `db_url` | `None` | Database fetch URL (`--db-url`) |
| `enable_db_update` | `false` | Hourly refresh (`--enable-db-update`) |
| `initial_db_etag` / `initial_db_content_hash` | `None` / `None` | Seeds for the first conditional refresh (set these when you fetched the body yourself) |
| `ignored_ids` | `[]` | App ids the scanner never publishes (`--ignore-ids`) |
| `exclusions_url` | `None` | Discord exclusions feed (`--exclusions-url`) |
| `app_version` | this crate's version | Version stamped into state snapshots |

## One-shot diagnostics (no threads)

```rust
use rsrpc_core::{Daemon, DetectedGame, DetectableSummary};

// One process scan: staged overrides and the ignore-list apply, exactly
// what run_until() would publish. Returns id/name/pid.
let games: Vec<DetectedGame> = daemon.detect_once()?;

// Database inventory: counts and names, no scanning.
let summary: Vec<DetectableSummary> = daemon.database_summary();
let total: usize = daemon.database_len();
```

`DetectedGame` has `id: String`, `name: String`, `pid: Option<u64>`.
`DetectableSummary` has `id`, `name`, and `executables: usize`.

## Staging overrides and callbacks

```rust
// Add or replace entries before run_until(): they bypass the OS filter
// (win32 entries work under Proton/Wine) and win over the main database.
daemon.append_detectables(overrides);
daemon.remove_detectable_by_name("Game Name");

// Called after every scan tick with the OBS/streaming flag.
daemon.on_scan_complete(|state| {
    println!("obs open: {}", state.obs_open);
});
```

Both must happen before `run_until`, which consumes the daemon. To parse
override files yourself:

```rust
use rsrpc_core::overrides;

let entries = overrides::parse_overrides(r#"[{"id":"1","name":"G","hook":false}]"#)?;
let from_file = overrides::load_file(std::path::Path::new("overrides.json"))?;
let from_dir = overrides::load_dir(std::path::Path::new("overrides.d")); // broken files skipped
```

`overrides::default_file_path()` and `overrides::default_dir_path()` return
the same locations the CLI checks (`$RSRPC_OVERRIDES_FILE` /
`~/.config/rsrpc/overrides.json`, and the `.d` variants).

## Known limitation: `run_until` consumes the daemon

`run_until` takes `self` by value. It moves the database into the scanner
(single ownership — no duplicated ~25 MB generations), so diagnostics like
`detect_once` and `database_summary` must run **before** it. One-shot use
never needs `run_until` at all.

Teardown is total (see above): when `run_until` returns, no rsRPC
thread is still running, so starting another daemon afterwards is
safe — only the consumed `Daemon` value itself is single-use.

## Errors

Fallible calls return `Result<T, rsrpc_protocol::error::RsrpcError>`, a
`thiserror` enum with a variant per failure kind (`InvalidJson`,
`UnreadableFile`, `IpcBind`, `WsBind`, `BridgeBind`, …). It implements
`std::error::Error`, so `Box<dyn std::error::Error>` works with `?`
without depending on `rsrpc-protocol` directly. Add that crate (same
repository, same tag) if you want to `match` on specific variants.

Bind failures surface from `run_until`; nothing in the library exits the
process — your code decides what a failed bind means.

## See also

- [cli-options.md](cli-options.md) — the flag equivalents of every field.
- [process-detection.md](process-detection.md) — what the scanner does.
- [protocol.md](protocol.md) — what `run_until` starts serving.
