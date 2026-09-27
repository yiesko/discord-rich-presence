#!/usr/bin/env bash
# rsRPC uninstaller. Stops + disables the user service and removes what
# install.sh laid down, including the auto-update drop-in (so a plain
# uninstall also clears the auto-update opt-in) and the OTA rollback
# image. Config, caches and backups are kept unless --purge is given.
#
#   ./scripts/uninstall.sh [--purge] [--yes]
set -euo pipefail

APP="rsrpc-cli"
UNIT_NAME="rsrpc.service"
[ -n "$HOME" ] || { echo "[uninstall] ERROR: HOME is not set" >&2; exit 1; }
BIN_DIR="${HOME}/.local/bin"
UNIT_DIR="${HOME}/.config/systemd/user"

PURGE=0 YES=0

while [ $# -gt 0 ]; do
  case "$1" in
    --purge) PURGE=1 ;;
    --yes) YES=1 ;;
    -h|--help)
      echo "Usage: uninstall.sh [--purge] [--yes]"
      echo "  --purge  also remove config (~/.config/rsrpc), caches (~/.cache/rsrpc) and .bak-* backups"
      exit 0
      ;;
    *) echo "[uninstall] ERROR: unknown argument: $1" >&2; exit 1 ;;
  esac
  shift
done

log() { printf '[uninstall] %s\n' "$*"; }

if [ "$YES" -eq 0 ] && [ "$PURGE" -eq 0 ]; then
  printf '[uninstall] remove the binary and unit (config/caches kept)? [y/N] ' >&2
  answer=""
  read -r answer </dev/tty 2>/dev/null || { log "cancelled"; exit 0; }
  case "$answer" in
    y|Y|yes|YES) ;;
    *) log "cancelled"; exit 0 ;;
  esac
fi

systemctl --user stop "$UNIT_NAME" 2>/dev/null \
  || log "warning: could not stop $UNIT_NAME (no user bus, or already stopped)"
systemctl --user disable "$UNIT_NAME" 2>/dev/null \
  || log "warning: could not disable $UNIT_NAME (no user bus, or already disabled)"

rm -f "${UNIT_DIR}/${UNIT_NAME}" "${BIN_DIR}/${APP}" "${BIN_DIR}/${APP}.prev"
rm -f "${UNIT_DIR}/${UNIT_NAME}.d/10-auto-update.conf"

# Claim success only when the service is actually gone: a swallowed
# stop failure otherwise leaves the daemon running with its binary
# deleted (it can never respawn after the next restart).
if systemctl --user is-active --quiet "$UNIT_NAME" 2>/dev/null; then
  log "warning: $UNIT_NAME is still active after stop; it was NOT stopped"
else
  log "removed unit and binary"
fi

if [ "$PURGE" -eq 1 ]; then
  rm -rf "${UNIT_DIR}/${UNIT_NAME}.d" "${HOME}/.config/rsrpc" "${HOME}/.cache/rsrpc"
  rm -f "${BIN_DIR}/${APP}".bak-* "${UNIT_DIR}/${UNIT_NAME}".bak-*
  log "purged config, caches, drop-ins and backups"
else
  log "kept: config (~/.config/rsrpc), caches (~/.cache/rsrpc), .bak-* backups"
fi

systemctl --user daemon-reload 2>/dev/null \
  || log "warning: daemon-reload failed (no user bus?); stale unit files may linger"
log "done"
