# CLI options

`rsrpc-cli` is configured with flags on the command line. Almost every flag
also has an environment variable, so you can set values once in your shell
profile or in a systemd unit instead of repeating them. `--yes` is the only
flag without an environment variable.

Boolean flags (`--no-process-scan`, `--enable-db-update`, and the rest)
accept `1`, `0`, `true`, `false`, `yes`, `no`, `on`, and `off`.

## Detection and scanning

| Flag | Environment variable | Default | What it does |
|---|---|---|---|
| `-d, --detectable-file <FILE>` | `RSRPC_DETECTABLE_FILE` | — | Load your game list from this file instead of the built-in one. |
| `-n, --no-process-scan` (alias `--no-process-scanning`) | `RSRPC_NO_PROCESS_SCAN` | off | Turn off process detection entirely. |
| `--no-proc-events` | `RSRPC_NO_PROC_EVENTS` | off | Turn off only the fast event watcher (the netlink `cn_proc` path). Polling keeps running. |
| `--scan-interval-secs <SECS>` | `RSRPC_SCAN_INTERVAL` | `5` | Base seconds between scans. While the event watcher is live, quiet periods stretch ×2 per empty tick (5 → 10 → 20 → 30 seconds). |
| `--ignore-ids <IDS>` | `RSRPC_IGNORE_IDS` | — | Comma-separated application IDs the scanner never publishes. |

`--ignore-ids` applies to the process scanner only. Frames that a game
forwards through the bridge are always passed on. See
[process-detection.md](process-detection.md) for how matching works.

## Ports

| Flag | Environment variable | Default | What it does |
|---|---|---|---|
| `--bridge-port <PORT>` | `RSRPC_BRIDGE_PORT` | `1337` | First JSON bridge port to try. |
| `--bridge-port-end <PORT>` | `RSRPC_BRIDGE_PORT_END` | `1347` | Last JSON bridge port to try (inclusive). |
| `--msgpack-port <PORT>` | `RSRPC_MSGPACK_PORT` | `1338` | MessagePack bridge port. It steps forward if the port is taken. |
| `--bridge-allowed-origins <URLS>` | `RSRPC_BRIDGE_ALLOWED_ORIGINS` | empty | Extra browser origins allowed to drive bridge commands (comma-separated; normalized to browser form — lowercase, no trailing slash, no default port — then exact match), beyond Discord's own pages. Absent `Origin` (native clients) always passes. |
| `--ws-port-start <PORT>` | `RSRPC_WS_PORT_START` | `6463` | First websocket port offered to games. |
| `--ws-port-end <PORT>` | `RSRPC_WS_PORT_END` | `6472` | Last websocket port for games (inclusive). |

The JSON bridge scans its range until it finds a free port, like arRPC does.
See [protocol.md](protocol.md) for what runs on each port.

## Detectable database

| Flag | Environment variable | Default | What it does |
|---|---|---|---|
| `--db-url <URL>` | `RSRPC_DB_URL` | official Discord endpoint when `--enable-db-update` is set | Fetch the game list from this URL at startup. |
| `--enable-db-update` | `RSRPC_ENABLE_DB_UPDATE` | off | Refresh the game list every hour in the background. |
| `--exclusions-url <URL>` | `RSRPC_EXCLUSIONS_URL` | official Discord endpoint when `--enable-db-update` is set | Source for Discord's detection exclusions (installer and crash-reporter names). An empty value disables the fetch. |

Without any of these flags, rsRPC uses the snapshot bundled inside the
binary, so it works with no network access at all.

**Which source wins**, in order:

1. `--no-process-scan` gives an empty list (there is nothing to detect).
2. `--detectable-file` loads your file.
3. `--db-url` fetches your URL; if the fetch fails, rsRPC falls back to the
   bundled snapshot and says so on stderr.
4. `--enable-db-update` fetches Discord's official list; same offline
   fallback.
5. Otherwise the bundled snapshot is used.

Fetched lists are trimmed to the fields detection needs: `id`, `name`,
`hook`, `aliases`, `executables` (`name`, `is_launcher`, `os`, `arguments`)
and `third_party_skus` (`distributor`, `id`).

The bundled snapshot lives at
`crates/rsrpc-detect/resources/detectable.json`. Regenerate it with:

```bash
cargo run --manifest-path tools/updater/Cargo.toml
```

## Overrides

| Flag | Environment variable | Default location |
|---|---|---|
| `--overrides-file <FILE>` | `RSRPC_OVERRIDES_FILE` | `$XDG_CONFIG_HOME/rsrpc/overrides.json`, else `~/.config/rsrpc/overrides.json` |
| `--overrides-dir <DIR>` | `RSRPC_OVERRIDES_DIR` | `$XDG_CONFIG_HOME/rsrpc/overrides.d`, else `~/.config/rsrpc/overrides.d` |

Each file is a JSON array (or a single object) of `DetectableActivity`
entries. Resolution order is flag, then environment variable, then the
XDG path, then `~/.config`. A missing path simply means "no overrides".
A broken file inside the directory is skipped with a warning, never fatal.

Overrides are staged before anything else runs, so `--list-detected` sees
exactly what the daemon would publish. They bypass the executable-OS filter
and win over the built-in database, which is how win32-only entries get
detected under Proton/Wine. Details:
[process-detection.md](process-detection.md).

## Diagnostics

| Flag | Environment variable | What it does |
|---|---|---|
| `--list-detected` | `RSRPC_LIST_DETECTED` | Run one scan, print what would be published, and exit. |
| `--list-database` | `RSRPC_LIST_DATABASE` | Print database counts and the first ten entries, then exit. |
| `--state-file` | `RSRPC_STATE_FILE` | Write presence snapshots to `<tmpdir>/rsrpc-state-{0..9}` (arRPC layout) for external tooling. Off by default. |
| `-D, --debug` | `RSRPC_DEBUG` | Print the resolved configuration and enable debug logging. |

```bash
./rsrpc-cli --list-detected
# [rsrpc] Database: bundled (24299 entries)
# How to Fish (id 1542021058834468927) pid 1234
```

```bash
./rsrpc-cli --list-database
# [rsrpc] Database: bundled (24299 entries)
# 24299 database entries, 11229 executables
# Overwatch (356875221078245376)
# ...
# ... and 24289 more
```

When nothing matches, `--list-detected` prints
`No games detected (overrides and ignore-list apply here too).`

## Updates

| Flag | Environment variable | What it does |
|---|---|---|
| `--check-update` | `RSRPC_CHECK_UPDATE` | Check for a newer release and exit. Exit code `2` means one is available, `0` means you are up to date. |
| `--update` | `RSRPC_UPDATE` | Download and stage the newest release. It applies on the next start. |
| `--yes` | *(none)* | Answer "yes" to the staging prompt, for scripts. |
| `--rollback` | `RSRPC_ROLLBACK` | Restore the binary that the last update replaced, then exit. |
| `--auto-update` | `RSRPC_AUTO_UPDATE` | Also stage available updates in the background while the daemon runs. |

Full details in [self-update.md](self-update.md).

## Identity, Steam, and other environment variables

These have no command-line flag:

| Variable | What it does |
|---|---|
| `RSRPC_USER_ID`, `RSRPC_USER_USERNAME`, `RSRPC_USER_GLOBAL_NAME`, `RSRPC_USER_DISCRIMINATOR`, `RSRPC_USER_AVATAR` | Change the identity rsRPC reports to clients. Blank values are ignored. |
| `RSRPC_OTA_DIR` | Where updates are staged (default `~/.cache/rsrpc/ota/`). |
| `RSRPC_STEAM_ROOT` | Point at a specific Steam install directory. |
| `RSRPC_STEAM_LIBRARIES` | Point at specific Steam library folders (`:`-separated). |
| `RUST_LOG` | Full `tracing` filter; overrides the `--debug`/`RSRPC_DEBUG` choice (see below). |

## Logging

Logs go to stderr through `tracing`. `RUST_LOG` takes precedence when it is
set. Otherwise `--debug` or `RSRPC_DEBUG=1` selects `debug`, and the default
is `info`.

Severity levels, from chattiest to quietest:

- `DEBUG` — internals on every tick (scans, repeated sends, match details).
- `INFO` — one line per state change (a game appears, clears, connects, the
  hourly database check).
- `WARN` — something degraded but the daemon continues (fallbacks, retries,
  pruned clients).
- `ERROR` — an operation failed.

Module names are left out of the output.

## See also

- [process-detection.md](process-detection.md) — how games are matched.
- [self-update.md](self-update.md) — how updates are verified and applied.
- [protocol.md](protocol.md) — ports, handshakes, and command handling.
- [systemd-user-units.md](systemd-user-units.md) — running as a user service.
