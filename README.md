<div align=center>
  <h1>rsRPC</h1>

  <div align="center">
    <img src="https://img.shields.io/github/actions/workflow/status/yiesko/rsRPC/build.yml" />
    <img src="https://img.shields.io/github/repo-size/yiesko/rsRPC" />
  </div>
  <p>Alternative Discord RPC server CLI tool and Rust library, inspired by <a href="https://github.com/OpenAsar/arRPC">arRPC</a></p>
</div>

# Features

* Process detection: Aho-Corasick over reversed paths (case-insensitive, with 64-bit variants like arrpc/pog5-rsrpc); a `win32` fallback automaton for Proton/Wine games whose store id is unreadable; Steam AppId via `/proc/<pid>/environ` (plus `AppId=` cmdline fallback and install-dir lookup from Steam's own `libraryfolders.vdf`/`appmanifest` files, with non-Steam shortcut ids detected by range); conservative exe-stem/folder fallback for DB entries with empty `executables` (e.g. Hydra/non-Steam layouts, multi-word names only); alternative titles (`aliases`) indexed too; Discord exclusion list honored (installers/crash reporters never match); bare-exe launches retried against the process cwd; SIGSTOP'd processes count as absent; event-driven `EXEC`/`EXIT` fast path (netlink `cn_proc`) with polling backstop and idle backoff
* IPC/Socket-based RPC detection
* Websocket-based RPC detection (loopback only)
* Bridge that forwards game activities to web clients via both a **JSON** port (`1337`) and a **MessagePack** port (`1338`)
* arRPC-shaped `SET_ACTIVITY` confirmation replies on both IPC and websocket transports
* Handshake validation (invalid versions / missing client IDs are rejected with close codes; oversize frames refused with `1003`, unknown opcodes rejected)
* `SUBSCRIBE`/`UNSUBSCRIBE` ACKs, `GET_USER` (current identity or `null`), `INVITE_BROWSER` / `GUILD_TEMPLATE_BROWSER` / `GIFT_CODE_BROWSER` (forward + ACK; websocket rejects missing codes with `4011`/`4017`/`4016`), `DEEP_LINK`, `CONNECTIONS_CALLBACK` refusal
* Official errors for unbacked commands: OAuth → `5000`, activity invites → `5006`, voice/guilds/overlay/store → honest "requires the real Discord client"
* Clickable-asset URL fields (`details_url`, `state_url`, `large_url`, `small_url`) preserved through the bridge
* Bundled offline detectable snapshot with optional automatic database refresh (fetch the detectable list every hour, like pog5-rsrpc)
* `plugin/rsrpc.js` - optional Vencord plugin / userscript / Node client that receives activity from the bridge (not embedded in the Rust binary)
* Custom overrides via `overrides.json` and/or `overrides.d/*.json` (arrays or single objects; added on the fly, bypass the OS filter so win32 entries work under Proton/Wine)
* Single-shot diagnostics via `--list-detected` (staged overrides + ignore-list apply, exactly what running would publish) and `--list-database`
* IPC-wins handoff: generic process detection shows immediately, yields
  to live game-SDK presence, and resumes when it clears (see below)

# Building

## Install (Linux, systemd)

```bash
curl -fsSL https://raw.githubusercontent.com/yiesko/rsRPC/main/scripts/install.sh | bash
```

This detects your arch, downloads the newest release binary (+ unit file),
SHA256-verifies it (plus a minisign check when the tool is present),
installs to `~/.local/bin` and `~/.config/systemd/user` (timestamped
backups of anything replaced), then enables + starts the user service.
Re-running it updates. Flags: `--yes`, `--force`, `--no-systemd`
(files only), `--auto-update` (opt into background update staging),
`--binary PATH` / `--unit PATH` / `--tag TAG` (pin sources).
Uninstall with `scripts/uninstall.sh` (`--purge` also drops config,
caches and backups). Details: `docs/systemd-user-units.md`.

## Requirements

- [Cargo and Rust](https://www.rust-lang.org/) 1.95+ (edition 2024 + let-chains)

## Testing it out

1. Download a binary from [releases](https://github.com/yiesko/rsRPC/releases), [GitHub Actions](https://www.github.com/yiesko/rsRPC/actions) or build it yourself below!
2. If you just want to use the bundled detectable snapshot, just run the binary (works fully offline, no `detectable.json` needed)!
3. If you want to use your own detectable list, place a `detectable.json` file in the same directory as the binary (you can use [the arRPC one](https://raw.githubusercontent.com/OpenAsar/arrpc/main/src/process/detectable.json) as an example), then run the binary with `./rsrpc-cli -d ./detectable.json`

### CLI options

```
  -d, --detectable-file <FILE>    Path to a custom detectable games list
  -n, --no-process-scan           Disable process detection
                                   (alias: --no-process-scanning)
      --no-proc-events              Disable only the event-driven proc-events
                                   watcher (netlink `cn_proc` fast path);
                                   polling continues (best-effort watcher,
                                   may rarely go silent)
      --bridge-port <PORT>        Bridge JSON port range start (default: 1337;
                                   scans up to --bridge-port-end, arRPC scans
                                   1337-1347)
      --bridge-port-end <PORT>    Bridge JSON port range end, inclusive
                                   (default: 1347)
      --msgpack-port <PORT>       Bridge MessagePack port (default: 1338;
                                   moves forward on collision with the JSON port)
      --ws-port-start <PORT>      First websocket port for games (default: 6463)
      --ws-port-end <PORT>        Last websocket port for games, inclusive (default: 6472)
      --scan-interval-secs <SECS> Process scan base cadence in seconds (default: 5;
                                    idle stretches ×2 per empty tick up to 30s)
      --db-url <URL>              Fetch the detectable list from this URL
                                   (with --enable-db-update and no --db-url,
                                   defaults to https://discord.com/api/v9/applications/detectable)
      --enable-db-update          Refresh the detectable list every hour
      --exclusions-url <URL>      Discord detection exclusions source
                                   (defaults to the official endpoint with
                                   --enable-db-update; same hourly refresh)
      --overrides-file <FILE>     Path to a JSON array (or single object) of
                                   DetectableActivity used as custom overrides
                                   (default: $RSRPC_OVERRIDES_FILE,
                                   $XDG_CONFIG_HOME/rsrpc/overrides.json,
                                   ~/.config/rsrpc/overrides.json)
      --overrides-dir <DIR>       Directory of override files (`*.json`, each
                                   an array or a single DetectableActivity),
                                   merged with --overrides-file (default:
                                   $RSRPC_OVERRIDES_DIR,
                                   $XDG_CONFIG_HOME/rsrpc/overrides.d,
                                   ~/.config/rsrpc/overrides.d)
      --ignore-ids <IDS>          Comma-separated application IDs the process
                                   scanner never publishes (full silence for
                                   those slots). Scan-only by design:
                                   forwarded client frames always pass.
                                   `$RSRPC_IGNORE_IDS`
      --list-detected             Run a single process scan, print detected
                                   games and exit (staged overrides and
                                   ignore-list apply, like the daemon)
      --list-database             Print a database summary (entry/executable
                                   counts + first entries) and exit
      --check-update              Check for a newer release and exit (exit
                                   code 2 when one is available)
      --update                    Download, verify (SHA256) and stage the
                                   newest release; it applies on the next
                                   start (self-swap + re-exec)
      --yes                       Answer "yes" to the staging prompt
      --rollback                  Restore the previous binary kept by the
                                   last update and exit
      --auto-update               In the daemon, also stage available updates
                                   in the background (opt-in; applying still
                                   happens on the next start)
  -D, --debug                     Print the resolved configuration
```

Every option also has a corresponding environment variable (e.g. `RSRPC_BRIDGE_PORT`, `RSRPC_MSGPACK_PORT`, `RSRPC_OVERRIDES_FILE`, `RSRPC_OVERRIDES_DIR`, `RSRPC_EXCLUSIONS_URL`, `RSRPC_STEAM_ROOT`, `RSRPC_STEAM_LIBRARIES`, `RSRPC_LIST_DETECTED`, `RSRPC_NO_PROC_EVENTS`, `RSRPC_DEBUG`). Bool flags accept `1/0/true/false/yes/no/on/off`.

### Logging

Logs go to stderr through `tracing` (`tracing_subscriber`). `RUST_LOG` takes precedence when set; otherwise `--debug` / `RSRPC_DEBUG=1` selects `debug`, and the default is `info`. Severities, chattiest first: `DEBUG` (per-tick internals), `INFO` (one line per state change: detects, clears, connects, hourly DB checks), `WARN` (degraded but continuing: fallbacks, retries, pruned clients), `ERROR` (failed operations). Module targets are omitted from the output.

### Detectable database (offline snapshot & refresh)

* Without flags the CLI uses the bundled snapshot (`crates/rsrpc-detect/resources/detectable.json`, embedded via `rsrpc_detect::db::BUNDLED_DETECTABLE`), so it works offline.
* `--db-url <URL>` fetches and trims the list at startup (keeps only `id/name/hook/aliases`, `executables{name,is_launcher,os,arguments}` and `third_party_skus{distributor,id}`), with fallback to the bundled snapshot on failure.
* `--enable-db-update` keeps refreshing that list every hour in the background. Without `--db-url` it defaults to `https://discord.com/api/v9/applications/detectable`.
* Regenerate the snapshot with: `cargo run --manifest-path tools/updater/Cargo.toml` (writes `crates/rsrpc-detect/resources/detectable.json`).

### Self-update (OTA)

* `--check-update` reports newer releases (exit 2 when one is available); `--update` downloads, verifies, and stages the binary under `~/.cache/rsrpc/ota/` (`$RSRPC_OTA_DIR` overrides).
* Verification is two-layer and fail-closed: the release `SHA256SUMS.txt` must carry a valid minisign signature from the key embedded in the binary (secret lives only in GitHub Secrets + offline backup), and only then is the binary hash checked against it. Unsigned or tampered releases are refused before anything executes.
* The staged binary applies on the next start: it is re-verified, atomically swapped in (previous image kept as `.prev` for `--rollback`), and the process re-executes — on Linux in the same PID, invisible to systemd.
* The daemon checks once a day in the background and only logs availability, unless `--auto-update`/`RSRPC_AUTO_UPDATE=1` opts into background staging (applying still waits for the next start; the daemon never restarts itself).
* Self-update refuses dev builds (`target/`), `cargo install` copies (`~/.cargo/bin`), oddly named binaries, and read-only install dirs with a plain message. MVP covers Linux x86_64 + ARM64 (other targets report "no published builds").

### Process detection notes

* Executable matching uses case-insensitive Aho-Corasick over reversed paths with `64`/`.x64`/`x64`/`_64` variants (e.g. `wow64.exe` matches `wow.exe`), plus a `win32` fallback automaton on Linux for Proton/Wine games.
* Entries with empty `executables` are still matched via Steam AppId (Linux reads `SteamAppId` from `/proc/<pid>/environ`, falling back to `AppId=` on the command line and to install-dir lookup from Steam's `libraryfolders.vdf`/`appmanifest` files; shortcut-range ids assigned by Steam itself to non-Steam shortcuts order name-before-location instead) or via an exact exe-stem == multi-word game-name fallback (e.g. `how to fish.exe` → `How to Fish`; single-word names like `fish` never match). Alternative titles (`aliases`) join the same fallback.
* Discord's detection exclusions (installer/crash-reporter names + patterns, refreshed hourly with `--enable-db-update`) never match.
* The main DB is filtered by executable OS (`win32`/`darwin`/`linux`, except the Proton fallback above); custom overrides from `overrides.json`/`overrides.d`/`append_detectables` bypass that filter so win32-only entries are detected under Proton/Wine, and always win over the main DB.
* Entries with empty `executables` are additionally matched by install-folder name (e.g. `.../Meccha Chameleon/...` → `MECCHA CHAMELEON`; multi-word names only, so generic folders never hit).
* Launches with a bare exe name (no directories in argv[0], common under Proton) are retried joined with the process cwd.
* Suspended (`SIGSTOP'd`) processes count as absent (a frozen frame is not gameplay); they are republished on resume.
* Event fast path (`EXEC`/`EXIT` via netlink `cn_proc`) is best-effort by kernel nature and may rarely go silent for stretches (delivery is officially lossy); polling backstops it either way, the watcher resubscribes every 5 minutes on its own, and `--no-proc-events` / `RSRPC_NO_PROC_EVENTS=1` disables the watcher thread entirely (polling only).

### Custom overrides (`overrides.json`, `overrides.d/`)

Files contain a JSON array (or a single object) of `DetectableActivity` objects. File resolution order: `--overrides-file` > `$RSRPC_OVERRIDES_FILE` > `$XDG_CONFIG_HOME/rsrpc/overrides.json` > `~/.config/rsrpc/overrides.json`; directory resolution: `--overrides-dir` > `$RSRPC_OVERRIDES_DIR` > `$XDG_CONFIG_HOME/rsrpc/overrides.d` > `~/.config/rsrpc/overrides.d`. Missing paths mean no overrides; corrupt directory files are skipped with a warning. Staged in the daemon before any branch: `--list-detected` sees them, and `run_until` applies them when the scanner starts.

### Diagnostics

```bash
./rsrpc-cli --list-detected
# How to Fish (id 4001890) pid 1234
```

Shows exactly what the daemon would publish: staged overrides and the ignore-list apply (previously main-DB-only).

### IPC-wins handoff (generic ↔ companion)

No `--ignore-ids` needed for companions anymore. When a game is only
process-detected, the generic card shows immediately. The moment a game
SDK (or companion like wwrpc) publishes `SET_ACTIVITY` for the same app,
the generic card is withdrawn; when that source clears, the generic card
comes back while the game process is still alive (liveness-checked, so no
flash on exit). Takeover rule across companions: **last publisher wins** —
a stale close from a superseded publisher is ignored instead of wrongly
resuming the generic card. No wire-format change: coexistence is keyed by
the existing `socketId = pid` convention.

```bash
./rsrpc-cli --list-database
# 24208 database entries, 11226 executables
# Overwatch (356875221078245376)
# ...
```

### Identity (`READY` user) and state snapshot

* The `DISPATCH`/`READY` identity defaults to arRPC's (`arRPC/1045800378228281345`); override at startup with `RSRPC_USER_ID`, `RSRPC_USER_USERNAME`, `RSRPC_USER_GLOBAL_NAME`, `RSRPC_USER_DISCRIMINATOR`, `RSRPC_USER_AVATAR`.
* Bridge clients can patch it at runtime with `SET_USER` (`{"type":"SET_USER","patch":{...}}`, whitelisted keys only) and restore it with `RESET_USER`; both are ACKed (`SET_USER_ACK`/`RESET_USER_ACK`), and identity changes fan out as the official `CURRENT_USER_UPDATE` DISPATCH to bridge clients (IPC/WS game clients learn it on their next handshake).

### Known limitations

* **No OAuth/`AUTHORIZE` flow**: the bridge forwards `SET_ACTIVITY` (and a few browser/deeplink commands) but cannot complete authorization — that needs a route game → real Discord client plus the app's `client_secret`, which only the game developer has. Games that log in via RPC need direct access to the Discord client socket (stop rsRPC while playing them).
* **`run_until` consumes the daemon**: it moves the database into the scanner (single ownership, no duplicated generations), so run `detect_once`/`database_summary` before it — the CLI's `--list-detected`/`--list-database` do exactly that. One-shot use never needs `run_until`.

## Building the binary

1. Clone the repository
2. `cargo build -p rsrpc-cli --release`
3. Your file will be in `target/release/`

The offline snapshot `crates/rsrpc-detect/resources/detectable.json` is committed, so a
fresh clone builds without network access to Discord. To refresh it, run
`cargo run --manifest-path tools/updater/Cargo.toml`.

## Using as a library

1. Add the following to your `Cargo.toml` file:

```toml
[dependencies]
rsrpc-core = { git = "https://www.github.com/yiesko/rsRPC", tag = "VERSION_NUMBER_HERE" }
tokio = { version = "1.53", features = ["rt-multi-thread", "macros", "signal"] }
```

2. Use the daemon in your code:

```rust
use rsrpc_core::{Daemon, RPCConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let daemon = Daemon::from_file(std::path::Path::new("./detectable.json"), RPCConfig::default())?;
  daemon.run_until(async {
    let _ = tokio::signal::ctrl_c().await;
  }).await?;
  Ok(())
}
```

You can also grab the `detectable.json` programmatically and pass it via string:
```rust
use rsrpc_core::{Daemon, RPCConfig};

async fn run(detectable: String) -> Result<(), Box<dyn std::error::Error>> {
  let daemon = Daemon::from_json_str(detectable, RPCConfig::default())?;
  daemon.run_until(async {
    let _ = tokio::signal::ctrl_c().await;
  }).await?;
  Ok(())
}
```

Works fully offline with the bundled snapshot (no file/network needed):
```rust
let daemon = Daemon::from_bundled(RPCConfig::default())
  .expect("Failed to create daemon");
```

If you already parsed the list yourself, skip the second parse with
`Daemon::from_parsed(vec, config)` (infallible). Prefer
`RPCConfig::builder()` over the struct literal for forward compatibility.

### `RPCConfig` fields (defaults)

| Field | Default | Meaning |
|---|---|---|
| `port` | `1337` | Bridge JSON port range start (`--bridge-port` / `RSRPC_BRIDGE_PORT`) |
| `bridge_port_end` | `1347` | Bridge JSON port range end, inclusive |
| `msgpack_port` | `1338` | Bridge MessagePack port (`--msgpack-port` / `RSRPC_MSGPACK_PORT`) |
| `ws_port_start` / `ws_port_end` | `6463` / `6472` | Game websocket range, inclusive |
| `scan_interval_secs` | `5` | Process scan interval (`--scan-interval-secs` / `RSRPC_SCAN_INTERVAL`) |
| `db_url` / `enable_db_update` | `None` / `false` | Hourly DB refresh source + toggle |
| `initial_db_etag` / `initial_db_content_hash` | `None` / `None` | Seeds from the startup fetch so the first hourly refresh skips parse/rebuild like later checks |
| `ignored_ids` | `[]` | App IDs the scanner never publishes (`--ignore-ids` / `RSRPC_IGNORE_IDS`) |

### Runtime API

```rust
use rsrpc_core::DetectedGame;

// Single scan without threads (staged overrides + ignore-list apply,
// returns id/name/pid).
let games: Vec<DetectedGame> = daemon.detect_once()?;

// Database summary without threads (entry/executable counts + names).
let summary: Vec<rsrpc_core::DetectableSummary> = daemon.database_summary();

// Stage entries before run() (bypass the OS filter, win over main DB);
// diagnostics below see exactly what running would publish.
daemon.append_detectables(overrides);
daemon.remove_detectable_by_name("Game Name");

// OBS/streaming flag callback (must be set before run()).
daemon.on_scan_complete(|state| {
  println!("obs open: {}", state.obs_open);
});
```

## Web client (browser / Vencord, optional)

> `plugin/rsrpc.js` is **not** part of the Rust build: nothing is bundled via `include_str!`/`build.rs`, and the server works without it. Any arRPC-compatible websocket client can consume the bridge.

The bridge exposes two websocket endpoints:

| Port | Protocol | URL |
|------|----------|-----|
| 1337 | JSON | `ws://127.0.0.1:1337?format=json` |
| 1338 | MessagePack | `ws://127.0.0.1:1338?format=msgpack` |

The protocol is auto-detected, so connecting to either port works regardless of the `format` query parameter. Use `plugin/rsrpc.js` (Vencord plugin or userscript) or instantiate `RsRpcClient` directly:

```javascript
const client = new RsRpcClient(true); // true = use MessagePack
client.onActivity = (activity) => console.log('Activity:', activity);
client.connect();
```

Notes:

* Default is arRPC-compatible JSON (`new RsRpcClient(false)`); no extra dependency.
* MessagePack (`true`) needs `@msgpack/msgpack`: `<script src="https://unpkg.com/@msgpack/msgpack"></script>`.
* Ports are configurable (`--bridge-port`/`--bridge-port-end`/`--msgpack-port`); the JSON bridge scans its range like arRPC (1337-1347) and the MessagePack port steps aside on collision; the client accepts `options: { jsonPort, msgpackPort, reconnectInterval }`.
* Bridge control messages (JSON port): `SET_USER`/`RESET_USER` (see Identity above). Presence frames still echo to the sender; cached activities replay to late joiners and refresh every 30s.

## Testing & benchmarks

* Unit and integration tests: `cargo test`
* Benchmarks (JSON vs MessagePack): `cargo bench`

Unit tests live beside each crate (`crates/*/src/`, `crates/*/tests/`),
integration tests in `crates/*/tests/`, benchmarks in
`crates/rsrpc-detect/benches/` (hash maps) and
`crates/rsrpc-protocol/benches/` (JSON vs MessagePack).

## Credits

* [OpenAsar / arRPC](https://github.com/OpenAsar/arRPC) - the original project this work is inspired by. The `detectable.json` format, the executable-matching checks, and the arRPC-shaped bridge behavior (replies, websocket protocol) follow its design.
* [pog5 / rsrpc](https://github.com/pog5/rsrpc) - reference for process-detection parity: 64-bit executable path variants, executable argument checks, the hourly detectable-database refresh, and the integration test coverage.
