# Process detection

rsRPC finds running games without asking them anything. It scans the process
list, matches executables against the detectable database, and publishes a
Rich Presence card for whatever it finds. This page explains how that
matching works and how it coexists with games that report themselves.

## How a process is matched

**Executable names.** The scanner builds one Aho-Corasick automaton over
the database's executable names and probes each process path against it.
Both sides are reversed and normalized to `/` separators (arRPC parity),
and matching is ASCII case-insensitive. Each process path is also tried
with its 64-bit markers (`64`, `.x64`, `x64`, `_64`) removed, so a game
installed as `wow64.exe` still matches a database entry for `wow.exe`.
This is the same approach arRPC and pog5's rsrpc use.

**Proton and Wine.** On Linux, entries marked `win32` are matched by a second
fallback automaton, because a Windows game running under Proton has a Windows
executable path. Without it, those games would never match.

**Steam application ids.** When no executable matches, the scanner falls
back to Steam AppId. The id is read from the process environment
(`/proc/<pid>/environ`, `SteamAppId=`), then from `AppId=` on the command
line (for sandboxed Proton runtimes that hide the environment), and
finally from the install folder via Steam's own `libraryfolders.vdf` and
`appmanifest_*.acf` files. This is how entries that ship no executables at
all are found. Ids in Steam's shortcut range (assigned by Steam itself to
non-Steam shortcuts) match by name before location.

**Names.** If an entry has no executables at all, the scanner falls back to
the executable stem or install-folder name — but only for multi-word names,
so a generic folder like `Games` never triggers a false positive
(`how to fish.exe` → `How to Fish`; `fish` alone never matches).
Alternative titles (`aliases`) are indexed too and join the same fallback.

**Discord's exclusion list.** Installer and crash-reporter names and patterns
from Discord's exclusion endpoint never match. With `--enable-db-update`
they are refreshed hourly along with the database.

**Executable OS filter.** The main database is filtered by the executable's
OS (`win32`, `darwin`, `linux`), with the Proton fallback above as the
exception. Custom overrides bypass this filter entirely, which is how a
win32-only entry gets detected under Proton/Wine.

**Edge cases handled:**

- Launches with a bare executable name (no directories in `argv[0]`, common
  under Proton) are retried joined with the process working directory.
- Suspended processes (`SIGSTOP`'d, state `T`) count as absent — a frozen
  frame is not gameplay — and are republished when they resume.
- Corrupt or unreadable entries are skipped rather than crashing the scan.

## Fast path and polling

The scanner normally polls on a timer (`--scan-interval-secs`, default 5
seconds; quiet periods stretch by ×2 per empty tick up to 30 seconds, and
only while the event watcher is live). On top of that, a netlink `cn_proc`
watcher receives `EXEC` and `EXIT` events from the kernel, so a game appears
the moment it starts and clears the moment it exits.

The kernel delivery is officially lossy, so the watcher is best-effort: it
can rarely go silent for stretches. It resubscribes on its own every five
minutes, and polling backstops everything either way. Use
`--no-proc-events` / `RSRPC_NO_PROC_EVENTS=1` to disable the watcher and
run polling only, or `-n` / `--no-process-scan` to disable detection
entirely.

## IPC-wins handoff (generic card vs. companion)

When a game is only process-detected, rsRPC shows a generic card right away.
The moment a game SDK (or a companion like wwrpc) sends its own
`SET_ACTIVITY` for the same app, the generic card is withdrawn and the
game's own presence takes over. When that source clears, the generic card
comes back while the game process is still alive — the check is
liveness-checked, so there is no flash when the game exits.

The takeover rule across companions is **last publisher wins**: a stale close
from a publisher that has already been superseded is ignored instead of
wrongly resuming the generic card. There is no wire-format change;
coexistence is keyed by the existing `socketId = pid` convention.

Because of this handoff, you usually no longer need `--ignore-ids` for
companions. The flag remains for slots you want silent no matter what.

## What the scanner never publishes

- `--ignore-ids <IDS>` / `RSRPC_IGNORE_IDS` — comma-separated application
  IDs the scanner treats as absent. **Scan-only by design**: frames that a
  game forwards through the bridge are always passed on, so a game that
  really is talking to rsRPC is never silenced by this list.
- Anything on Discord's exclusion list.
- Anything that fails the OS filter (main database only; overrides bypass it).

## Custom overrides

Overrides let you add, change, or replace entries without touching the
bundled database:

```json
[
  {
    "id": "1234567890",
    "name": "My Game",
    "hook": false,
    "executables": [
      { "name": "mygame.exe", "is_launcher": false, "os": "win32" }
    ]
  }
]
```

- Files are a JSON array or a single object.
- Resolution: `--overrides-file` > `$RSRPC_OVERRIDES_FILE` >
  `$XDG_CONFIG_HOME/rsrpc/overrides.json` > `~/.config/rsrpc/overrides.json`;
  the directory variant (`--overrides-dir`, `$RSRPC_OVERRIDES_DIR`,
  `~/.config/rsrpc/overrides.d`) works the same way and is merged with the
  file.
- Overrides are staged before anything else, so `--list-detected` shows
  exactly what the daemon would publish.
- They bypass the OS filter and always win over the main database.

The library exposes the same capability through
`append_detectables` / `remove_detectable_by_name` and
`rsrpc_core::overrides` — see [library.md](library.md).

## Checking your setup

```bash
./rsrpc-cli --list-detected
# [rsrpc] Database: bundled (24299 entries)
# How to Fish (id 1542021058834468927) pid 1234
```

`--list-detected` runs a single scan and exits. Staged overrides and the
ignore-list both apply, so the output is exactly what the daemon would
publish.

## See also

- [cli-options.md](cli-options.md) — every flag and environment variable.
- [protocol.md](protocol.md) — how games report themselves over IPC and
  websockets.
- [library.md](library.md) — calling detection from Rust.
