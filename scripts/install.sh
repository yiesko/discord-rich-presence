#!/usr/bin/env bash
# rsRPC one-liner installer (Linux, systemd user service).
#
#   curl -fsSL https://raw.githubusercontent.com/yiesko/discord-rich-presence/main/scripts/install.sh | bash
#
# What it does: detects the arch, downloads the matching release binary
# (+ unit file) for the newest tag, verifies both against the
# minisign-signed checksum manifest (minisign is required — there is no
# silent SHA256-only fallback), installs to ~/.local/bin and
# ~/.config/systemd/user (timestamped backups of anything replaced),
# then enables + starts the user service. Re-running it updates.
#
# Non-interactive / hermetic knobs (tests, provisioning):
#   --yes            answer yes to prompts (keeps safe defaults)
#   --force          reinstall even when the same version is present
#   --no-systemd     install the binary only, skip the unit file and all
#                    systemctl/loginctl calls
#   --auto-update    opt into background update staging (default: off)
#   --binary PATH    use this file instead of downloading (unit resolves
#                    from ./systemd/, --unit, or the tag unless --no-systemd)
#   --unit PATH      use this unit file instead of resolving one
#   --tag TAG        install this release instead of the newest (e.g. v0.33.1)
#   -h, --help       usage
set -euo pipefail

REPO="yiesko/discord-rich-presence"
APP="rsrpc-cli"
# Release-signing pubkey (same key the binary itself embeds for OTA).
PUBKEY="RWT97nYjybg6X/Q35LBD/thrjkAmYmEHbRm8TQjvpJeLO2kNONgb4ibw"

BIN_DIR="${HOME}/.local/bin"
UNIT_DIR="${HOME}/.config/systemd/user"
UNIT_NAME="rsrpc.service"
DROPIN_DIR="${UNIT_DIR}/${UNIT_NAME}.d"

YES=0 FORCE=0 NO_SYSTEMD=0 AUTO_UPDATE=0 BINARY="" UNIT="" TAG=""

log() { printf '[install] %s\n' "$*"; }
warn() { printf '[install] WARNING: %s\n' "$*" >&2; }
die() { printf '[install] ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
Usage: install.sh [--yes] [--force] [--no-systemd] [--auto-update]
                  [--binary PATH] [--unit PATH] [--tag TAG] [-h|--help]

Installs (or updates) the rsrpc-cli binary, systemd user unit, and
optionally enables the service + background update staging.
EOF
  exit "${1:-0}"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --yes) YES=1 ;;
    --force) FORCE=1 ;;
    --no-systemd) NO_SYSTEMD=1 ;;
    --auto-update) AUTO_UPDATE=1 ;;
    --binary) BINARY="${2:?--binary needs a path}"; shift ;;
    --unit) UNIT="${2:?--unit needs a path}"; shift ;;
    --tag) TAG="${2:?--tag needs a value}"; shift ;;
    -h|--help) usage 0 ;;
    *) die "unknown argument: $1 (see --help)" ;;
  esac
  shift
done

# confirm works under `curl | bash` (stdin is the script): read from tty.
confirm() {
  [ "$YES" -eq 1 ] && return 0
  local answer=""
  printf '[install] %s [y/N] ' "$1" >&2
  read -r answer </dev/tty 2>/dev/null || return 1
  case "${answer}" in
    y|Y|yes|YES) return 0 ;;
    *) return 1 ;;
  esac
}

need_cmd curl
need_cmd sha256sum

ARCH="$(uname -m)"
[ "$(uname -s)" = "Linux" ] || die "only Linux is supported by this script (see the releases page for other builds)"
case "$ARCH" in
  x86_64) TRIPLE="x86_64-unknown-linux-gnu" ;;
  aarch64|arm64) TRIPLE="aarch64-unknown-linux-gnu" ;;
  *) die "unsupported architecture: $ARCH (see the releases page)" ;;
esac

if [ -z "$TAG" ]; then
  log "resolving newest release..."
  TAG="$(curl -fsSL --max-time 30 "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep -o '"tag_name": *"[^"]*"' | head -n 1 | cut -d'"' -f4)"
  [ -n "$TAG" ] || die "could not resolve the newest tag (network/API?)"
fi
log "release: $TAG ($TRIPLE)"
BASE="https://github.com/${REPO}/releases/download/${TAG}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

if [ -n "$BINARY" ]; then
  [ -f "$BINARY" ] || die "binary not found: $BINARY"
  cp "$BINARY" "$WORK/$APP"
else
  log "downloading binary..."
  curl -fsSL --max-time 120 -o "$WORK/$APP" "${BASE}/${APP}-${TRIPLE}"
  log "downloading checksums..."
  curl -fsSL --max-time 30 -o "$WORK/SHA256SUMS.txt" "${BASE}/SHA256SUMS.txt"
  curl -fsSL --max-time 30 -o "$WORK/SHA256SUMS.txt.minisig" "${BASE}/SHA256SUMS.txt.minisig" \
    || die "no signature manifest: refusing to install without authentication"
fi

# Verify before anything executes or installs. The manifest lists every
# release file, so match our exact asset (a loose grep would also match
# the other arch and fail on its absence).
if [ -f "$WORK/SHA256SUMS.txt" ]; then
  (cd "$WORK" && awk -v "f=$APP-${TRIPLE}" '$2 == f || $2 == "*" f' SHA256SUMS.txt | sha256sum -c --status -) \
    || die "checksum mismatch or missing entry: refusing to install"
  log "checksum OK"
  if [ -f "$WORK/SHA256SUMS.txt.minisig" ]; then
    if command -v minisign >/dev/null 2>&1; then
      minisign -V -P "$PUBKEY" -m "$WORK/SHA256SUMS.txt" >/dev/null \
        || die "signature invalid: refusing to install"
      log "signature OK"
    else
      die "minisign not installed: refusing to install unverified (install minisign and retry)"
    fi
  fi
elif [ -n "$BINARY" ]; then
  log "local binary: skipping download verification (explicit --binary)"
fi
chmod +x "$WORK/$APP"

INSTALLED_VERSION=""
if [ -x "${BIN_DIR}/${APP}" ]; then
  INSTALLED_VERSION="$("${BIN_DIR}/${APP}" --version 2>/dev/null | awk '{print $NF}')" || true
fi
if [ -n "$INSTALLED_VERSION" ] && [ "$INSTALLED_VERSION" = "${TAG#v}" ] && [ "$FORCE" -eq 0 ]; then
  log "already at $INSTALLED_VERSION; nothing to do (use --force to reinstall)"
  exit 0
fi

if [ "$NO_SYSTEMD" -eq 0 ] && [ -z "$UNIT" ]; then
  if [ -f "./systemd/${UNIT_NAME}" ]; then
    UNIT="./systemd/${UNIT_NAME}"
  else
    # Explicit --binary mode never downloads a manifest, so there is
    # nothing to verify a remote unit against: pass --unit or run from
    # a checkout instead.
    [ -f "$WORK/SHA256SUMS.txt" ] \
      || die "no verified manifest (explicit --binary mode needs an explicit --unit file or a checkout)"
    log "downloading unit file..."
    curl -fsSL --max-time 30 -o "$WORK/$UNIT_NAME" "${BASE}/${UNIT_NAME}" \
      || die "unit asset missing for $TAG (pre-signed-unit release? pass --unit PATH)"
    UNIT="$WORK/$UNIT_NAME"
    # The unit rides the same signed manifest as the binary: verify
    # before anything is installed from it.
    UNIT_HASH="$(awk -v f="systemd/${UNIT_NAME}" '$2 == f {print $1}' "$WORK/SHA256SUMS.txt")"
    [ -n "$UNIT_HASH" ] || die "unit entry missing from checksum manifest: refusing to install"
    echo "$UNIT_HASH  $UNIT" | sha256sum -c --status - \
      || die "unit checksum mismatch: refusing to install"
    log "unit checksum OK"
  fi
fi
if [ "$NO_SYSTEMD" -eq 0 ]; then
  [ -f "$UNIT" ] || die "unit file not found: $UNIT"
fi

if [ -n "$INSTALLED_VERSION" ]; then
  confirm "replace $APP $INSTALLED_VERSION with ${TAG#v}?" || die "cancelled"
fi

mkdir -p "$BIN_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
[ -f "${BIN_DIR}/${APP}" ] && cp -a "${BIN_DIR}/${APP}" "${BIN_DIR}/${APP}.bak-${STAMP}" && log "backed up binary"
cp -a "$WORK/$APP" "${BIN_DIR}/${APP}"
if [ "$NO_SYSTEMD" -eq 0 ]; then
  mkdir -p "$UNIT_DIR"
  [ -f "${UNIT_DIR}/${UNIT_NAME}" ] && cp -a "${UNIT_DIR}/${UNIT_NAME}" "${UNIT_DIR}/${UNIT_NAME}.bak-${STAMP}" && log "backed up unit"
  cp -a "$UNIT" "${UNIT_DIR}/${UNIT_NAME}"
fi
log "installed ${BIN_DIR}/${APP} (${TAG})"

# Opt-in stays opt-in: --yes keeps the default (off); only an explicit
# --auto-update or an interactive yes enables staging.
if [ "$AUTO_UPDATE" -eq 0 ] && [ "$YES" -eq 0 ] && [ "$NO_SYSTEMD" -eq 0 ] && [ -t 0 ]; then
  if confirm "enable background update staging (opt-in auto-update)?"; then
    AUTO_UPDATE=1
  fi
fi
if [ "$AUTO_UPDATE" -eq 1 ] && [ "$NO_SYSTEMD" -eq 0 ]; then
  mkdir -p "$DROPIN_DIR"
  printf '[Service]\nEnvironment=RSRPC_AUTO_UPDATE=1\n' > "${DROPIN_DIR}/10-auto-update.conf"
  log "auto-update staging enabled"
fi

if [ "$NO_SYSTEMD" -eq 0 ]; then
  systemctl --user daemon-reload || warn "daemon-reload failed; run it manually"
  systemctl --user enable --now "$UNIT_NAME" \
    && log "service enabled and started" \
    || warn "could not enable/start; run: systemctl --user enable --now $UNIT_NAME"
  USER_NAME="${USER:-$(id -un)}"
  loginctl enable-linger "$USER_NAME" 2>/dev/null \
    || sudo -n loginctl enable-linger "$USER_NAME" 2>/dev/null \
    || warn "linger not enabled; the service stops at logout (run: sudo loginctl enable-linger $USER_NAME)"
  if systemctl --user is-active --quiet "$UNIT_NAME"; then
    log "status: active ($("${BIN_DIR}/${APP}" --version))"
  else
    warn "service is not active; inspect: journalctl --user -u $UNIT_NAME"
  fi
else
  log "systemd integration skipped (--no-systemd)"
fi

if [ "$NO_SYSTEMD" -eq 0 ]; then
  log "done. logs: journalctl --user -u $UNIT_NAME -f | update: ${BIN_DIR}/${APP} --update"
else
  log "done. update: ${BIN_DIR}/${APP} --update"
fi
