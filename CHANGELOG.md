# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security
- The bridge now validates the WebSocket `Origin` header like the game
  transport already did: absent `Origin` (native clients) and Discord's
  own pages pass, anything else is closed at connect before READY unless
  listed in `--bridge-allowed-origins` / `RSRPC_BRIDGE_ALLOWED_ORIGINS`.
  A malicious page can no longer read local activities or send bridge
  commands through `ws://127.0.0.1:1337`; frames pipelined past the
  close are dropped as unregistered. Client ids are now unique
  process-wide across both bridge servers (per-server counters could
  alias registrations), and messages from unregistered ids never reach
  control handling.

### Changed
- Unix IPC pumps are capped at 16 concurrent peers like Windows (which
  keeps 32): connection floods past the cap drop peers at accept instead
  of parking blocking threads without bound, leaving room in the
  32-thread blocking pool for unrelated work.
- Split the two biggest modules internally, same crates and same
  behavior: `rsrpc-detect/src/server.rs` is now foundation plus
  `runtime` (threads, shutdown, join), `database` (bundle ownership and
  swaps), `scanner` (enumeration and classification), `steam`
  (library roots) alongside the existing `bundle`/`db`/`refresh`/`scan`
  modules; `rsrpc-bridge/src/bridge.rs` is now construction plus pumps
  alongside `consumer` (fan-out and pruning), `router` (activity
  decisions), `replay` (cache and ghost-reap pid resolution), `snapshot`
  (persistence) and `control` (`SET_USER`/`RESET_USER`). `Shared` fields
  are crate-visible for the new units; wire protocols unchanged. One
  breaking change for library callers: `rsrpc_proc_events::watch` now
  takes a shutdown flag (`watch(events, stop)`), so the watcher thread
  joins on directed shutdown instead of blocking on netlink.

### Fixed
- Scanner threads now belong to the daemon lifecycle: `ProcessServer`
  gained directed `shutdown()` + `join()` (plus `rsrpc-*` thread names),
  every long sleep became an interruptible park, the netlink watcher
  honors a stop flag (including its self-test), and `Daemon::run_until`
  shuts down, joins and drops the scanner on success and bind failures.
  Future abandonment (dropping the future via `select!`/timeout/abort)
  signals shutdown through a cancellation guard instead, so threads exit
  on their own within about one poll interval. When `run_until` returns,
  no rsRPC thread is still running, so library callers may run another
  daemon afterwards (`crates/rsrpc-detect/src/server.rs`,
  `crates/rsrpc-proc-events/src/lib.rs`,
  `crates/rsrpc-core/src/daemon.rs`).

## [0.36.1] - 2026-09-24

### Fixed
- `--rollback` no longer re-executes into an infinite toggle loop. The
  re-exec passes the original argv back and `cmd_rollback` had no
  applied-guard, so every pass swapped `rsrpc-cli`/`rsrpc-cli.prev` again
  (reproduced: 460 swaps in 3 seconds, killed by timeout). It now does
  one swap, re-execs once and exits, mirroring the guard in
  `apply_pending_on_boot` (`cli/src/update.rs`).
- `RSRPC_STATE_FILE` is wired up again: `--state-file` /
  `RSRPC_STATE_FILE` points the bridge at the temp-dir snapshot slots
  (`<tmpdir>/rsrpc-state-{0..9}`), as documented since 0.32.0. The env
  check was lost in the 0.36.0 workspace refactor while
  `BridgeConfig::state_dir` stayed `None`, so the CLI activation never
  happened; `RPCConfig::state_file` now carries it through to
  `Bridge::bind` (`crates/rsrpc-core/src/{config,daemon}.rs`,
  `cli/src/main.rs`).
- `systemd/rsrpc.service` keeps `%h/.local/bin` writable via
  `ReadWritePaths`: under `ProtectHome=read-only` + `ProtectSystem=strict`
  the sandboxed service could not swap in its own staged self-updates
  (discarded at boot with `cannot replace binary`), and the dead
  `RSRPC_LOGS_ENABLED=1` line (no longer read by the logger) is gone.

### Changed
- The repository moved from `yiesko/rsRPC` to
  `yiesko/discord-rich-presence`; every checked-in URL (crates,
  install script, docs, changelog links, self-update API) now points
  at the new location.
- The README was rewritten in plain English with a screenshots section,
  and the deep guides moved into `docs/` (`cli-options`,
  `process-detection`, `protocol`, `web-client`, `library`,
  `self-update`, `systemd-operations`); `systemd-user-units.md` is now
  a step-by-step setup guide quoting the unit byte-for-byte.

## [0.36.0] - 2026-09-23

### Breaking
- The `lib/` monolith is now a 12-crate workspace plus the thin `cli`
  binary (`rsrpc-ws`, `rsrpc-types`, `rsrpc-protocol`, `rsrpc-transport-ws`,
  `rsrpc-transport-ipc`, `rsrpc-state`, `rsrpc-bridge`, `rsrpc-telemetry`,
  `rsrpc-proc-events`, `rsrpc-steam`, `rsrpc-detect`, `rsrpc-core`): one
  Tokio runtime owned by the caller, `[workspace.lints]` on every member,
  fuzz targets under `fuzz/`, benches under `rsrpc-detect` /
  `rsrpc-protocol`. The old `rsrpc` library name is gone: embedders use
  `rsrpc-core::Daemon` / `RPCConfig::builder()`. `Message::Text(String)`
  is now `Text(Utf8Bytes)` and bridge payloads are `Arc<CachedActivity>`
  (`json: Utf8Bytes, msgpack: Bytes, is_clear: bool`).

### Fixed
- `Shared::broadcast_raw` now prunes dead bridge clients the same
  way `send_to_all` already did: when `Responder::send` returns `false` the
  client is removed and a `Pruning dead consumer` warning is emitted.
  Previously a bridge client that died without a clean disconnect (killed
  process, closed browser tab) and only received `INVITE_BROWSER` /
  `DEEP_LINK` events would keep its `Responder` and queued frames pinned in
  memory forever (`Shared::broadcast_raw` in `crates/rsrpc-bridge/src/bridge.rs`).
- WebSocket game-client handlers (`handle_browser_command`,
  `handle_deep_link`, `handle_connections_callback`, `handle_set_activity`)
  now report whether their reply was delivered and the pump
  (`Event::Disconnect` in `crates/rsrpc-transport-ws/src/transport.rs:324`)
  removes the client and clears every published pid when delivery fails,
  freeing the per-client slots the same way a clean `Disconnect` does
  (`crates/rsrpc-transport-ws/src/handlers.rs:33`). Verified with real
  `Responder` tests (RFC6455 handshake + killed-connection simulation with
  condition-wait for `send==false` and `Disconnect`).
- Hourly bundle-swap RSS floor ratchet: the swap ran `malloc_trim` while
  the old generation was still pinned by an in-flight tick, stranding
  ~10 MB per rebuild in allocator arenas (production floor ratcheted
  52→104 MB). The swap now trims once more 60 s past any tick and
  reports the result (six consecutive recounts showed the old
  generation always releases within 30 s); the post-retrim line also
  reports live vs free heap bytes from `mallinfo2` (glibc-only,
  cfg-gated) (`crates/rsrpc-detect/src/db.rs`, `server.rs`). The
  canonical content hash is factored per entry (byte-identical stream,
  pinned by a fold test).
- First-sighting `INFO` logs report every new id again: the
  `MAX_SEEN_IDS` cap is dropped, restoring main-branch log parity.

### Added
- Read-only resource census for long-session memory diagnosis (no behavior
  change): `QueueGauge` / `GaugeSender` / `GaugeReceiver` wrappers around
  the three previously unbounded channels sharing an `AtomicUsize` depth,
  plus a `StatsSnapshot` census (`rss` via `/proc/self/statm` on Linux,
  `n/a` elsewhere) with bridge `json`/`msgpack` and `ws` client counts and
  all three queue depths
  (`crates/rsrpc-telemetry/src/lib.rs:206`). Helpers, rendering and the
  daemon wiring are covered by `crates/rsrpc-telemetry/tests/telemetry.rs`
  (gauge sharing, `rss_bytes`, snapshot shape) and the bridge census
  suite (live counts, session-boundary lines): the bridge logs the
  census hourly plus `game-start`/`game-end` on process-table
  transitions, with consumer counts and input queue depths sampled live.
- Tests `crates/rsrpc-ws/tests/integration.rs` and
  `crates/rsrpc-transport-ws/tests/integration.rs` (dead vs live
  `Responder` liveness).

### Changed
- Cheaper `SET_ACTIVITY` pipeline: the activity fingerprint is computed
  first and flood-dropped publishes return before the JSON + MessagePack
  envelope is ever built (~0.76µs vs ~2.5µs per dropped publish); accepted
  publishes compare stored fingerprint bytes instead of re-serializing and
  re-parsing (`CachedActivity.activity_json`, same pattern as `is_clear`).
  No behavior change (covered by the flood/change/replay suites plus a
  `publish_flood_drop` bench).
- Per-tick buffer reuse in the scan and match paths: ~400 processes ×
  fresh `String`/`Vec` temporaries every 5 s tick become caller-owned
  reuse buffers — `ExecScratch` + `read_exec_into` (path, cmdline and
  args buffers allocated once per thread), `process_list_into`
  refilling every `Exec` slot across ticks with high-water retention
  (no `truncate`: surplus slots keep their string buffers, only the
  filled prefix is classified), and `MatchScratch` for the lowered
  path and normalized folders (lowercase stage skipped on the
  already-lowercase hot path). Behavior is byte-identical, covered by
  parity and no-realloc regression tests
  (`crates/rsrpc-detect/src/scan.rs`, `server.rs`).
- Single-pass automaton build: entries are walked once, partitioning
  executables into native and Proton pattern vectors (identical
  contents, order and filters as the two walks), with pattern, index
  and aux-map vectors pre-sized from the entry counts and each source
  vector dropped right after its automaton compiles — peak is one
  automaton plus one vector plus builder scratch
  (`crates/rsrpc-detect/src/bundle.rs`).
- `tokio-util` trimmed to default features workspace-wide: only
  `sync::CancellationToken` is used, so the explicit full features
  pulled codec, compat, io, net, rt and time machinery for nothing
  (`crates/rsrpc-bridge/Cargo.toml`, `crates/rsrpc-ws/Cargo.toml`).

## [0.35.0] - 2026-09-15

### Breaking
- Removed `rsrpc::cmd::Button` (unreferenced; buttons travel as plain
  values) and the deprecated `get_user_response` shim (its one-release
  cycle since 0.33.0 is over — use `user_response`); `empty_activity`
  is now private. New `RPCConfig::initial_db_content_hash` field
  (seeded by the CLI; `..Default::default()` constructions are
  unaffected, struct literals need the field).

### Added
- Trimmed-content refresh guard: the hourly DB check now hashes the
  canonical scanner projection (trimmed fields, body order preserved
  because first-wins ties depend on it) alongside the raw body, so
  volatile CDN bytes around identical games skip the automaton rebuild
  entirely (previously every new ETag rebuilt, spiking RSS to ~165MB).
- `RPCServer::from_parsed` (infallible): build from already-parsed
  activities, letting the CLI boot path parse the fetched body once
  instead of twice.

### Fixed
- Staged overrides fold into the initial scanner build
  (`ProcessServer::new_with_custom`): boot no longer builds the
  automata twice (three times counting the fetch probe parse).
- Negative SteamAppId lookups are memoized (`Absent` vs `Stale`): desktop
  processes without a store id no longer re-read kilobytes of environ
  every scan tick (the biggest per-process I/O cost).
- Scan loop forwards detection deltas only, and the bridge skips fan-out
  for byte-identical republications: steady-state wakeups, channel
  traffic and snapshot rewrites drop to ~zero with no observable change
  (dedup already lived downstream).
- Scanner bundle stores a slim entry form (id/name/executables/steam
  ids/aliases) instead of the ~30-field public struct, and hits carry
  `(entry, pid, start)` instead of a full-struct clone per hit per tick.
- Steam refresh returns early when all fingerprints match (no per-tick
  prefix recopy) and re-collects roots every 120th tick instead of 30th.
- OTA staging streams the binary to disk with incremental hashing
  instead of holding the whole image in RAM.
- Slim scanner strings (`Box<str>`) and an `os` enum (hot values as
  unit variants, exotics owned per generation instead of leaked): ~130k
  string fields drop the `String` capacity word, and the 11k `os`
  values point at 3 shared statics instead of heap-allocating.
- Aux lookups (`steam_map`/`name_map`/`nodot`) are sorted vectors with
  binary search instead of hash maps: no buckets, no hashing, same
  first-wins tie rules; lookups only happen on match misses.
- Boot/rebuild logs report automaton heap bytes (`memory_usage`), and a
  compile-time assert pins the slim entry layout against regrowth.
- Hourly refresh seeds content hashes from the boot fetch
  (`initial_db_content_hash`): the first check skips parse/rebuild like
  later ones instead of one guaranteed redundant rebuild per boot; the
  fat parse structs are dropped before the automata build, halving the
  rebuild peak overlap.
- Client event queues are bounded (backpressure, no drops): a spinning
  local client throttles to bridge speed instead of growing the queue
  without bound.
- Steam watch markers prune vanished roots; dead helpers removed
  (`Button`, deprecated `get_user_response` shim); duplicate trim/parse
  paths unified.

## [0.34.0] - 2026-09-13

### Added
- One-liner install (`scripts/install.sh`, Linux/systemd): arch
  detection, newest-release download with SHA256 (+ minisign when
  available) verification, install to `~/.local/bin` +
  `~/.config/systemd/user` with timestamped backups, service
  enable/start/linger, optional opt-in auto-update drop-in; idempotent
  re-runs update. `scripts/uninstall.sh` removes service + binary
  (`--purge` also drops config/caches/backups). Ships
  `systemd/rsrpc.service` (hardened user unit: `ProtectSystem=strict`
  + host `/tmp` bind for the IPC fan-out, `AF_NETLINK` for the proc
  watcher, personal overrides via commented drop-in examples).
- Proc-events honesty knob: the `watcher live` line now states the
  best-effort nature inline, and `--no-proc-events` /
  `RSRPC_NO_PROC_EVENTS=1` disables the netlink watcher thread entirely
  (process polling continues either way).

### Fixed
- OTA rollback actually restores the previous image: it used the same
  swap helper as apply, which deleted the `.prev` source before moving
  it back — printing success while changing nothing and leaving no
  backup behind. Rollback is now a true exchange (rolling back twice
  restores the starting state), and the sidecar name is a single
  predictable suffix (`rsrpc-cli.prev`).
- Proc-events watcher resubscribes instead of dying on one failed
  self-test: a transient kernel stall no longer pins polling until the
  next daemon restart (first failure still warns, later retries stay in
  debug). The self-test now reports datagram/parsed counts, telling a
  silent kernel apart from framing drift in the log line.
- Proc-events loss accounting: every observed message feeds a per-CPU
  sequence tracker over the kernel `cn_msg.seq` counter (delivery is
  officially lossy), so silent drops surface as debug gap lines plus a
  counter. Generic across topologies (wrapping-aware, re-anchors on
  counter restarts) and self-neutralizing where counter semantics do
  not hold. The self-test also reports timeout-vs-fatal breakdown and
  whether the subscribe ACK arrived, so the next silent window names
  its mechanism instead of just its symptom.

## [0.33.1] - 2026-09-11

### Added
- OTA manifest signatures (minisign): releases now publish
  `SHA256SUMS.txt.minisig`, and staging requires a valid signature from
  the embedded release key before any hash is trusted (fail-closed;
  legacy non-prehashed signatures rejected). CI signs with
  `MINISIGN_SECRET_KEY` and self-verifies before publishing; the secret
  key lives only in GitHub Secrets plus an offline backup.
- Self-update (OTA) for `rsrpc-cli` (Linux x86_64 + ARM64): `--check-update`
  (exit 2 when a newer release exists), `--update [--yes]` (downloads,
  SHA256-verifies against the release manifest, stages under
  `~/.cache/rsrpc/ota/`), `--rollback` (restores the kept `.prev` image),
  and opt-in `--auto-update`/`RSRPC_AUTO_UPDATE=1` for background staging
  in the daemon (daily check + log either way). Staged binaries apply on
  the next start via atomic swap + re-exec (same PID on Linux); dev builds,
  `cargo install` copies, foreign names and read-only dirs are refused with
  a plain message. CI publishes raw `rsrpc-cli-{target}` binaries plus
  `SHA256SUMS.txt` on tag releases.

### Fixed
- `SET_ACTIVITY` replies now echo the activity intact (official echo
  semantics): `name`/`type` (Playing/Listening/Watching/Competing) and
  every other field survive the round-trip instead of being rewritten to
  `""`/`0`. The lock-step guarantee stays — a reply is always sent.
- `SET_ACTIVITY` flood guard: byte-identical republishes from one
  `(application_id, pid)` inside 5s are collapsed before broadcast
  (changed bytes and clears always pass; clears re-arm the slot), so a
  spinning SDK can no longer fan out to bridge consumers at full rate.

### Changed
- Documented the `SUBSCRIBE` blind-ACK scope: only `READY`, `ERROR` and
  `CURRENT_USER_UPDATE` are ever dispatched; voice/guild/message/invite/
  relationship/entitlement subscriptions are ACKed and then silent (they
  need the real Discord client).
- **Breaking:** fallible APIs now return `rsrpc::error::RsrpcError`
  (`thiserror`) instead of boxed errors; `ClientConnector::new`,
  `WebsocketConnector::new` and `RPCServer::start` return `Result`
  (bind failures surface to the caller — the library never exits the
  process anymore); `AppId`/`SocketId` newtypes replace raw strings for
  slot ids (same wire format); `remove_detectable_by_name(&str)`;
  `get_user_response` renamed to `user_response` (deprecated shim kept);
  `process_alive`/`suppresses` renamed to `is_process_alive`/
  `is_suppressed`; over-`pub` items narrowed to `pub(crate)`.
  Error messages are lowercase; public constructors document `# Errors`.

- Proton-aware detection: `win32` executables from the main database are
  now indexed in a fallback automaton on Linux. Wine/Proton games whose
  Steam AppId is unreadable match by path instead of staying invisible.
  Precedence is native patterns, then user overrides, then the Steam
  AppId, then the Proton patterns, then the stem/folder heuristics.
- Alternative titles (`aliases`) are preserved by the trim and indexed
  for stem/folder matching (same conservative multi-word gate as
  canonical names, which win ties). The bundled snapshot was regenerated
  with aliases (1397 entries carry them).
- Discord detection exclusions (`GET /games/detectable/exclusions`):
  installer/crash-reporter basenames + regex patterns are dropped before
  any matching. Fetched hourly alongside the DB when `--enable-db-update`
  is set (`--exclusions-url`/`RSRPC_EXCLUSIONS_URL` overrides); empty set
  behaves exactly like before.
- `tools/updater` now uses the shared `rsrpc::detection::trim_detectable`
  instead of a hand-rolled copy (the copy had silently dropped aliases
  from the bundled snapshot).
- Steam library provider: install-dir -> AppId from Steam's own files
  (`libraryfolders.vdf` + `appmanifest_<id>.acf` + `compatdata/<id>/pfx`),
  for games whose per-process store id is unreadable. Roots are found
  dynamically (running `steam` via `/proc`, `PATH`, mounted partitions
  via `/proc/mounts`, home fallbacks — a secondary library never shadows
  the primary root); `$RSRPC_STEAM_ROOT` overrides exclusively,
  `$RSRPC_STEAM_LIBRARIES` (`:`-separated) adds manually. A JSON cache
  with per-library fingerprints revalidates by stat and reparses only
  what changed. Precedence: Steam AppId, Proton patterns, Steam
  install-dir, stem/folder — except shortcut-range AppIds (high bit set,
  assigned by Steam itself to non-Steam shortcuts, e.g. 2532755798):
  those self-identify as non-Steam, so the exe/folder name wins over
  location instead.
- Event-driven detection (Linux): a netlink `cn_proc` watcher classifies
  `EXEC` processes the moment they spawn (cards in milliseconds, not next
  tick) and wakes the scan loop early when a tracked game `EXIT`s
  (clears at once). Best-effort with a boot self-test — kernels/LSMs
  that silently drop `cn_proc` fall back to pure polling with one honest
  log line; steady state is a single blocking `recv`, and only tracked
  pids can trigger an early scan. The scan cadence itself moved from
  `sleep` to `park_timeout` to allow the wakeups. Operational note:
  systemd units sandboxing with `RestrictAddressFamilies` must add
  `AF_NETLINK`, otherwise the subscription fails and the scanner stays
  on polling (this was the production failure mode — diagnosed via the
  missing `watcher live` line).
- Overrides v2: `--overrides-dir`/`RSRPC_OVERRIDES_DIR` loads every
  `*.json` in a directory (array or single object per file, sorted,
  corrupt files skipped with a warning) additive to `--overrides-file`
  — the home for MultiMC/Prism/Hydra mappings and Proton-only titles.
  Loading moved into a library module (`rsrpc::overrides`, shared
  defaults) and `append_detectables` stages pre-start: `--list-detected`
  now applies staged overrides AND the ignore-list, so diagnostics show
  exactly what the daemon would publish (previously main-DB-only).
- Hardening (correctness + DoS): the detection database (automata,
  indexes, lists, aux maps) now swaps atomically per generation — a
  refresh landing mid-scan can no longer panic or mis-attribute games;
  the VDF parser is iterative with depth/file/token caps; exclusion
  regexes are capped and matched as one size-limited `RegexSet`; the
  handoff tables are bounded (dead owners purged, hard cap 64); all
  network fetches carry timeouts; `/proc`/VDF reads are size-capped;
  hot-path locks survive poisoning instead of killing the daemon.
- Scan efficiency (measured with hotpath-rs, see `cargo profile
  profiling`): case-insensitive automata (no per-process lowercase
  allocation), borrowed paths (`Cow`, zero alloc on the common case),
  memoized SteamAppId per pid (one environ read per process lifetime,
  swept per tick, invalidated on EXEC), `HashSet` exclusions, hoisted
  broadcast serialization, and idle backoff (5s → 30s cap, reset by any
  detection or early wake — EXEC still delivers starts instantly).
  Steady state: ~0.4% CPU, ~57MB RSS; per-process classify 54µs → 23µs.

- Total detection blindness from size-checking `/proc` files: they
  report `st_size 0` despite having content, so the check skipped every
  process (only IPC-driven cards kept showing). Removed the checks, kept
  the byte caps; regression-tested against our own pid.
- EXIT-wake storm: tracked short-lived Proton helpers unparked the scan
  loop several times per second (0.76s effective cadence during NFS).
  Wakes are debounced to at most one per second now.
- Exclusive sources diluted on refresh: `RSRPC_STEAM_ROOT` (and injected
  layouts) were merged with re-discovered roots on every folders change.
  Refresh re-resolves from the same source discovery used.
- `RSRPC_DEBUG=1` (and friends) rejected by clap bool parsing, crashing
  the daemon at startup: all env-backed bool flags accept
  `1/0/true/false/yes/no/on/off` now.
- Dead re-entrancy guard (`scanning` checked but never set): real RAII
  guard, acquired atomically and released on every exit path.
- IPC/bridge bind exhaustion no longer panics the daemon: `create_socket`
  failures surface as typed `RsrpcError::IpcBind` / `WsBind` (keeping the
  last `io::Error` as source) through the fallible `start()`; `to_fs_name`
  failures propagate instead of unwrapping.
- Poison-tolerance gaps closed: the last bare `lock().unwrap()`s on hot
  paths (bridge, handoff, replay cache, Steam prefix, `start()`) use
  `into_inner`; double `start()` on any connector is a logged no-op
  instead of a panic (including a `started` guard on the scan loop).
- Automaton build failures (pathological DB exceeding builder limits)
  keep the current generation with a warning instead of panicking the
  refresh thread or startup; same fail-open as the empty-DB guard.
- Error fidelity: `InvalidJson` keeps the serde error as `source`;
  `database_summary` returns `RsrpcError`; bridge bind message lowercased
  without log prefix; `# Errors` on all fallible constructors.
- Custom steam SKUs participate in AppId matching (linear fallback,
  canonical map wins); main patterns beat custom ones across 64-bit
  path variants (automata probed outer, variants inner) — both
  regression-tested.
- Stale AppId race closed: memoized environ carries an invalidation
  sequence, so an EXEC racing an in-flight read discards (never pins)
  the pre-exec environ; EXIT-wake debounce peeks before consuming, so a
  recent tracked EXIT can still wake later.
- Diagnostics parity: `--list-detected` applies exclusions when hourly
  updates are on (fail-open fetch, like the daemon); a fetched-but-garbage
  DB falls back to the bundled snapshot instead of killing boot.
- Watcher honesty: netlink fallback logs at `warn` (the documented
  one-line diagnosis for `AF_NETLINK` sandboxing); the boot self-test
  falls back to polling (never claims live) when it cannot prove
  delivery; steady-state datagrams sized 64KiB for EXEC storms.
- Steam provider hardening: cache entries re-validate library ownership
  on load; fingerprints cover `compatdata` appearances; `read_limited`
  re-checks size after reading (stat/read TOCTOU); VDF tokenizer caps
  tokens and counts mid-stream; mounts-table escapes decoded in one pass;
  library scans capped; `mount_library_roots_for` tested for escapes.
- Input robustness: WebSocket handlers drop (logged) frames that cannot
  encode instead of panicking the poll loop; fan-out encode failures and
  dead-peer IPC replies log at debug; `last_process` bounded like the
  handoff tables; remaining bool flags (`--list-detected`,
  `--list-database`) and the logger accept boolish values.
- Dotted titles match their folders as a last tier (`R.E.P.O.`, `Q.U.B.E.`,
  `Mr. Bomber`): the exact walk still skips dotted components as versions,
  then a de-dotted twin map (`undotted_names`, canonical-first ties) is
  consulted — exact matches always win. Punctuation forbidden in Windows
  filenames (`: ? " < > | * / \`) already folds in `normalize_name`.
- Observability without debug builds: boot logs the database source and
  entry count (`fetched-direct/trimmed/bundled-fallback`, `file`,
  `bundled`, `empty`); the scan loop logs first sightings per game id
  per boot at INFO (bounded, same cadence as bridge publishes). A silent
  daemon is now distinguishable from a blind scanner.
- Scan liveness tripwire: the first completed tick logs its game count
  at INFO once per boot (`First tick complete: N game(s)`), so a scan
  loop that never completes its first pass is visible without debug.
- Native-presence capture: a peer that connects but never handshakes
  (bailed SDK probe, crashed launcher) logs one INFO line instead of
  debug-only, so clients the bridge never identifies leave a trace.
- Rich presence fidelity: `SET_ACTIVITY` frames round-trip every modeled
  field (details/state, timestamps, assets+urls, party+privacy, secrets,
  buttons, flags, emoji, display type) on both JSON and MessagePack legs,
  and unmodeled future keys survive via a flattened catch-all instead of
  being dropped on parse — verified live against the daemon and pinned by
  a round-trip test.
- IPC sockets in every official dir: the bound `discord-ipc-{n}` is
  symlinked into `XDG_RUNTIME_DIR`/`TMPDIR`/`TMP`/`TEMP`/`/tmp` (Discord's
  resolution order), so games probing any location find the bridge; stale
  ours-shaped links repoint, foreign files are never touched, links die
  with the socket.

## [0.32.2] - 2026-09-09

### Fixed
- Generic process payloads carried `timestamps.start` as a string;
  strict clients (Equibop) silently drop mistyped frames, so generic
  cards never displayed. Epoch millis are now emitted as a number end
  to end (`DetectableActivity.timestamp`, `ScannedGame.start`,
  `ProcessTimestamps.start`).
- The scan loop forwarded only the first detected game, starving
  co-running games (WuWa + NTE). Every detected game now gets its own
  per-slot event with per-id dedup and no cross-clearing; the null
  event drains every armed slot.
- App slots whose IPC/WS owner died without CLEAR stayed suppressed by
  a ghost owner forever (`send_empty` carries no app id). Abrupt closes
  now release every slot owned by the dead pid and resume their
  generics.
- Steam AppId fallback via command line (`reaper SteamLaunch
  AppId=4508340 ...`) when `/proc/<pid>/environ` is unreadable.
  Sandboxed Proton runtimes (pressure-vessel/bwrap) hide environ from
  service contexts while cmdline stays readable — without this, games
  like NTE are invisible to automatic detection.

## [0.32.1] - 2026-09-08

### Added
- `docs/systemd-user-units.md`: practical guide to user units
  (anatomy, drop-ins, lifecycle, logs, linger, gotchas, from-scratch
  recipe) with real examples from this setup.

### Fixed
- `party.privacy` (0 = private, 1 = public) and `status_display_type`
  (0 = name, 1 = state, 2 = details) were silently dropped on parse
  (serde ignores unknown fields) — real SDKs (pypresence, Tauon) send
  them. Both now cross the bridge untouched.

## [0.32.0] - 2026-09-08

### Added
- arRPC protocol parity: `SUBSCRIBE`/`UNSUBSCRIBE` blind ACKs, `Unknown
  command` / `CONNECTIONS_CALLBACK is not supported` ERROR replies
  (code 1000), invite-code validation (4011/4017), malformed-frame
  rejection (4005), 1 MiB IPC payload limit with `1003` close, refusal
  of unknown packet types (1003) instead of misreading them as frames.
- Microsecond/nanosecond timestamp normalization to milliseconds
  (seconds/milliseconds behavior unchanged).
- Bridge port-range scan (`--bridge-port`..`--bridge-port-end`,
  default 1337-1347 like arRPC) with MessagePack collision skip.
- Bounded bridge replay cache (50 entries, oldest-first eviction) with
  30s rebroadcast refresh for late/missed clients.
- Runtime identity: `RSRPC_USER_*` startup overrides plus bridge
  `SET_USER`/`RESET_USER` with ACKs (whitelisted keys only, blank
  strings ignored like the env overrides, huge integers clamped
  instead of wrapping); READY frames reflect the current identity on
  every transport.
- Presence state snapshot (`RSRPC_STATE_FILE=1` writes
  `<tmpdir>/rsrpc-state-{0..9}` with servers + activities, arRPC
  layout, `rsrpc-` prefix); `--list-database` summary diagnostics.
- Official-protocol gaps closed: `GET_USER` (current identity, or
  `null` for other ids) on IPC + websocket; `GIFT_CODE_BROWSER`
  forwarded like the other `*_BROWSER` commands (`4016` on missing
  code); clickable-asset URL fields (`details_url`, `state_url`,
  `large_url`, `small_url`) preserved through the bridge;
  `CURRENT_USER_UPDATE` DISPATCH fanned out to bridge clients on
  `SET_USER`/`RESET_USER`.
- Honest errors for known-but-unbacked commands (shared table):
  OAuth → `5000`, activity invites → `5006`, voice/guilds/overlay/
  store → "requires the real Discord client"; genuinely unknown
  commands still get `Unknown command`.
- IPC-wins handoff: generic process detection shows immediately and
  yields its slot when a game SDK publishes for the same app (last
  publisher wins across companions; stale closes ignored), resuming
  while the game process is still alive when the source clears.
  `--ignore-ids` stays as full-silence opt-in, no longer needed for
  companions.
- Stale IPC socket PING/PONG probe (1s): wedged holders are reclaimed,
  live holders (Discord/arRPC/rsRPC) are left alone.
- `--ignore-ids` / `RSRPC_IGNORE_IDS`: scan-only coexistence filter —
  ignored application IDs behave as absent (null event, clear) so a
  richer publisher owns those slots; forwarded client frames always
  pass, and `--list-detected` still shows ignored games.
- Log severity system (`DEBUG`/`INFO`/`WARN`/`ERROR`): `RSRPC_LOG_LEVEL`
  sets the floor (default `info`), `--debug` / `RSRPC_DEBUG=1` forces
  debug and prints the resolved config; `INFO` keeps the historical
  untagged shape, other levels are tagged.
- Presence log lines: `Published: {name} (app {id}, pid {pid})` via a
  `display_name()` fallback (name → details → state → `?`), `INFO` on
  change and `DEBUG` on duplicate republishes/clears.

### Changed
- Tree-wide log reclassification: per-tick chatter (scan ticks, match
  details, repeat sends) demoted to `DEBUG`, fallbacks/retries/prunes
  to `WARN`, so the default log shows one line per state change.
- Hourly DB refresh logs condensed to one line per check (`DB check:
  unchanged (etag …)` / `same bytes` / `updated: N entries`).
- `SUBSCRIBE`/`UNSUBSCRIBE` and unknown/unbacked commands are answered
  at the edge (ACK/ERROR) and no longer forwarded to bridge clients;
  only `SET_ACTIVITY` and the known secondary commands
  (`INVITE_BROWSER`, `GUILD_TEMPLATE_BROWSER`, `GIFT_CODE_BROWSER`,
  `DEEP_LINK`) reach the bridge.
- State snapshot is rewritten on every 30s refresh tick, so a
  live-but-idle daemon never looks stale to slot reuse.

### Fixed
- `SET_USER` patch can no longer wipe identity fields with blank
  strings or wrap `flags`/`premium_type` on out-of-range integers.

## [0.31.0] - 2026-09-07

### Added
- Install-folder fallback in process detection (Hydra / non-Steam layouts
  with generic exes), cwd-joined probe for bare-exe Proton launches.
- Suspended (SIGSTOP'd) processes count as absent; republished on resume.
- Conditional database refresh (ETag + content hash): unchanged hours cost
  one header round trip, no parse or rebuild.
- Startup database ownership moves to the scanner (no pinned duplicate
  generation); refresh ETag seeded from the startup fetch.
- Fail-fast supervision: worker panics exit for systemd restart; hot-path
  channel sends are fail-soft.
- Bridge backpressure: dead websocket clients are pruned on failed send.
- `spectate`/`match` activity secrets preserved through the bridge.
- Empty database updates refused (keeps serving current data).
- CI: test + doc jobs, `--locked` everywhere, modern actions, Node 24
  runtimes, cross-platform (Linux/macOS/Windows) clippy.

### Fixed
- Stuck presence after game close (dual-keyspace clear: IPC pid-keyed vs
  process app-id-keyed desync).
- Server panic on malformed WS disconnect without args.
- macOS/Windows builds (Linux-only `malloc_trim`, `read_steam_app_id`,
  `parse_stat_state` gating; per-OS memory release).

### Changed
- Scan I/O: `/proc/<pid>/environ` read only on executable-pattern misses.
- Logging is lazy (no allocation when disabled); DB parses skip the JSON
  DOM (`serde` ignores unknown fields).
- Path-variant scratch buffers reused across processes/scans.

## [0.30.0] - 2026-09-06

### Added
- Dual-protocol bridge (JSON + MessagePack), bundled offline detectable
  snapshot with hourly refresh, custom overrides.
- Install-folder fallback in process detection.
- Single-shot `--list-detected` diagnostics.

### Fixed
- Stuck presence after game close.

[Unreleased]: https://github.com/yiesko/discord-rich-presence/compare/v0.36.1...HEAD
[0.36.1]: https://github.com/yiesko/discord-rich-presence/releases/tag/v0.36.1
[0.36.0]: https://github.com/yiesko/discord-rich-presence/releases/tag/v0.36.0
[0.35.0]: https://github.com/yiesko/discord-rich-presence/releases/tag/v0.35.0
[0.34.0]: https://github.com/yiesko/discord-rich-presence/releases/tag/v0.34.0
[0.33.1]: https://github.com/yiesko/discord-rich-presence/releases/tag/v0.33.1
[0.32.2]: https://github.com/yiesko/discord-rich-presence/releases/tag/v0.32.2
[0.32.1]: https://github.com/yiesko/discord-rich-presence/releases/tag/v0.32.1
[0.32.0]: https://github.com/yiesko/discord-rich-presence/releases/tag/v0.32.0
[0.31.0]: https://github.com/yiesko/discord-rich-presence/releases/tag/v0.31.0
[0.30.0]: https://github.com/yiesko/discord-rich-presence/releases/tag/v0.30.0
