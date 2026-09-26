# Detection traceability

How to answer "how was this game found, and why did it disappear" from
logs and snapshots. Every lifecycle transition logs one structured line
(see formats below); correlate stages by pid + app id.

## Log levels

Lifecycle lines (detection, publish, takeover, clear, resume) are
`INFO` and show at defaults. Per-tick and per-frame detail is `DEBUG`
(`RUST_LOG=debug`). Logs go to stderr — the journal under systemd:

```bash
journalctl --user -u rsrpc.service --since "10 minutes ago" | grep -E "Detected|Published|Yielding|Clearing|cleared|took over|Reaping|went away without CLEAR"
```

## Detection: source and latency

First sighting of a game logs how it was classified and how long the
process had already been alive (`process age`):

```
[Process Scanner] Detected: Hades II (331224... , pid 4242) via steam-app-id, process age 32118ms
```

Sources (`via`):

| Source | Meaning |
|---|---|
| `automaton` | Database-declared path matched the process path |
| `cwd-joined` | Bare exe joined with the process working directory |
| `proton-automaton` | `win32` (Proton/Wine) database entries |
| `steam-app-id` | Authoritative store id (Steam environ, cmdline fallback, custom SKU) |
| `steam-library` | Process runs under a known Steam install dir |
| `exe-stem` | Executable stem equals a multi-word game name |
| `folder` | Install folder carries the title |

`process age` is omitted when the start time is unreadable (sandboxed
runtimes, exotic platforms). The start time is read once per newly
detected game — never per process per tick — so tracing costs nothing
on the scan hot path.

## Bridge: publish, takeover, clear

- `Published: {name} (app {app}, pid {pid})` — SDK card went out.
- `Published clear for {app} (pid {pid}): {reason}` — card cleared.
- `SDK presence for {app} (pid {pid}) took over slot from generic {name}`
  (or `now owns its slot` when no generic was showing) — a game-specific
  client (e.g. wwRPC-style companions) took the slot from generic
  detection. Logged once per takeover; steady republishes stay quiet.
- `Yielding generic {name} ({id}) to live IPC presence (pid {owner}): yielded`
  — generic card withdrawn for the live owner.
- `Source cleared for {app} (pid {pid}), resuming {n} slot(s)` — the SDK
  source sent CLEAR; generic detection re-asserts the still-running game.
- `Owner pid {pid} went away without CLEAR, resuming {n} slot(s)` —
  abrupt close; dead owners release their slots.
- `Clearing removed game slot for {app} (pid {pid}): process-vanished` —
  the scanner reports the process gone (per-slot remove, or every slot
  when the table empties).
- `Reaping ghost card for dead pid {pid}: abrupt-close` — a card whose
  pid is provably dead but never got a clear.

Clear reasons (`: {reason}` suffix):

| Reason | Meaning |
|---|---|
| `sdk-clear` | Genuine null-activity CLEAR frame from the owning connection |
| `abrupt-close` | Owning socket died without CLEAR (ghost reap, pid-owned release) |
| `process-vanished` | Scanner reports the slot gone and liveness confirms the pid dead |
| `scan-absent` | Slot left the classified scan but the pid still runs (database refresh or ignore-list delisted a live game) |
| `yielded` | Generic card withdrawn for a live SDK owner on the same slot |

## Snapshots (`--state-file`)

Published cards carry provenance additively (absent on old cards and
unclassified payloads, so old readers keep working):

- `detectionSource`: matcher source above, or `"sdk"` for
  client-published cards.
- `detectLatencyMs`: process age in ms at first publish, omitted when
  the start time is unreadable — always omitted for SDK cards, which
  have no detection event.

Clears evict their cards, so "why did it stop" lives in the journal
(clear lines above), not the snapshot.

## See also

- [process-detection.md](process-detection.md) — how games are found.
- [cli-options.md](cli-options.md) — flags and environment variables.
- [web-client.md](web-client.md) — bridge consumers.
