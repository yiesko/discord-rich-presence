# Self-update (over-the-air)

rsRPC can update itself. Updates are downloaded, verified, and staged on
disk; the swap happens the next time the binary starts.

## Commands

| Flag | Environment variable | What it does |
|---|---|---|
| `--check-update` | `RSRPC_CHECK_UPDATE` | Ask GitHub for a newer release and exit. Exit code `2` means an update is available, `0` means you are current. |
| `--update` | `RSRPC_UPDATE` | Download and stage the newest release. It applies on the next start. |
| `--yes` | *(none)* | Skip the confirmation prompt (for scripts). |
| `--rollback` | `RSRPC_ROLLBACK` | Restore the binary the last update replaced, then exit. |
| `--auto-update` | `RSRPC_AUTO_UPDATE` | While the daemon runs, also stage updates in the background. |

```bash
./rsrpc-cli --check-update
# [rsrpc] update available: v0.37.0 (release v0.37.0); run with --update to stage it
# (exit code 2; exit code 0 means you are current)

./rsrpc-cli --update --yes     # stage now, apply on next start
./rsrpc-cli --rollback         # undo the last update
```

## Where files live

Staged files go to `~/.cache/rsrpc/ota/`. `$RSRPC_OTA_DIR` overrides the
location, and `$XDG_CACHE_HOME` is honored when it is set.

## Verification (fail-closed)

Nothing runs unless both checks pass:

1. The release publishes `SHA256SUMS.txt` and `SHA256SUMS.txt.minisig`.
   The signature must verify against the minisign public key embedded in
   the binary. The signing secret exists only in GitHub Secrets (plus an
   offline backup); the release workflow refuses to publish an unsigned
   release.
2. Only then is the downloaded binary hashed and compared against the
   manifest.

An unsigned, tampered, or truncated file is refused before it ever
executes.

## What happens on the next start

1. The staged file's SHA-256 is checked again against the recorded hash
   (the signature was verified when the file was staged).
2. The running binary is swapped out atomically; the previous image is kept
   next to it as `rsrpc-cli.prev`.
3. The process re-executes — on Linux in the same PID, so systemd never
   notices a restart.

`--rollback` swaps `rsrpc-cli.prev` back in place (clearing a pending
staged update first), then re-executes once against the restored image
and exits — the applied-guard keeps that pass from toggling the swap
again.

The swap must be able to write the binary's own directory. If it cannot
(read-only install, or a sandbox that does not allow it), the staged file
is discarded with `cannot replace binary` and the current version keeps
running. The shipped user unit allows it (`~/.local/bin` is in its
`ReadWritePaths`).

## Background checks

While the daemon runs, it checks for a new release once a day, starting
five minutes after boot, plus a small per-process jitter (up to an hour) so
many machines do not all ask at the same moment. By default it only logs
that something newer exists. With `--auto-update` / `RSRPC_AUTO_UPDATE=1`
it also stages the update — but applying still waits for the next start,
and the daemon never restarts itself.

## When it refuses to update

Self-update only manages installed binaries. It says so plainly and stops
when it finds:

- a development build under `target/debug` or `target/release`
  ("rebuild or `cargo install` instead of self-updating");
- a `cargo install` copy under `~/.cargo/bin` (cargo owns that file);
- a binary not named `rsrpc-cli`;
- a read-only install directory.

## Supported targets

Published update binaries are Linux x86_64 and Linux ARM64
(`rsrpc-cli-x86_64-unknown-linux-gnu`, `rsrpc-cli-aarch64-unknown-linux-gnu`).
Anything else reports `no published builds for <triple>` and keeps the
version it has. Releases also ship `SHA256SUMS.txt` (plus its minisign
signature) and zipped builds for Linux armv7, macOS (x86_64 and ARM64) and
Windows (x86_64 and ARM64) on the
[releases page](https://github.com/yiesko/discord-rich-presence/releases).

## See also

- [cli-options.md](cli-options.md) — the update flags in context.
- [systemd-operations.md](systemd-operations.md) — applying updates and
  rollbacks while rsRPC runs as a systemd service.
- [systemd-user-units.md](systemd-user-units.md) — running rsRPC as a user
  service.
