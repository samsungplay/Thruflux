#!/usr/bin/env bash
set -euo pipefail

APP="thru"
INSTALL_DIR="/usr/local/bin"
TARGET="${INSTALL_DIR}/${APP}"
UDP_BYTES=$((16 * 1024 * 1024))
URL_LINUX_X64="https://github.com/samsungplay/Thruflux-C-/releases/download/v18/thru_linux"

log()  { printf "%s\n" "$*"; }
warn() { printf "warning: %s\n" "$*" >&2; }
fail() { printf "error: %s\n" "$*" >&2; exit 1; }

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || fail "sudo not found (run as root)."
  SUDO="sudo"
fi

arch="$(uname -m)"
[[ "$arch" == "x86_64" || "$arch" == "amd64" ]] || fail "unsupported architecture: $arch (x86_64 only)"

download() {
  local url="$1" out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fL --progress-bar "$url" -o "$out"
  elif command -v wget >/dev/null 2>&1; then
    wget --progress=bar:force:noscroll "$url" -O "$out"
  else
    fail "curl or wget is required."
  fi
}

log "Thruflux installer (Linux)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

log "downloading…"
download "$URL_LINUX_X64" "$tmp/$APP"
[[ -s "$tmp/$APP" ]] || fail "downloaded file is empty."

log "installing to ${TARGET}"
$SUDO install -m 0755 "$tmp/$APP" "$TARGET"

conf="/etc/sysctl.d/99-thruflux-udp.conf"
log "configuring UDP buffers (16 MiB)"
$SUDO mkdir -p /etc/sysctl.d
$SUDO tee "$conf" >/dev/null <<EOF
net.core.rmem_max=${UDP_BYTES}
net.core.wmem_max=${UDP_BYTES}
net.core.rmem_default=${UDP_BYTES}
net.core.wmem_default=${UDP_BYTES}
EOF

$SUDO sysctl -w net.core.rmem_max="$UDP_BYTES" >/dev/null 2>&1 || true
$SUDO sysctl -w net.core.wmem_max="$UDP_BYTES" >/dev/null 2>&1 || true
$SUDO sysctl -w net.core.rmem_default="$UDP_BYTES" >/dev/null 2>&1 || true
$SUDO sysctl -w net.core.wmem_default="$UDP_BYTES" >/dev/null 2>&1 || true
$SUDO sysctl --system >/dev/null 2>&1 || true

rmem="$(sysctl -n net.core.rmem_max 2>/dev/null || echo 0)"
wmem="$(sysctl -n net.core.wmem_max 2>/dev/null || echo 0)"
if [[ "$rmem" -lt "$UDP_BYTES" || "$wmem" -lt "$UDP_BYTES" ]]; then
  warn "UDP buffers capped by kernel (rmem_max=$((rmem/1024/1024))MiB, wmem_max=$((wmem/1024/1024))MiB)"
else
  log "UDP buffers set (rmem_max=$((rmem/1024/1024))MiB, wmem_max=$((wmem/1024/1024))MiB)"
fi

if command -v "$APP" >/dev/null 2>&1; then
  log "installed: $(command -v "$APP")"
else
  warn "${INSTALL_DIR} is not on PATH for this shell"
  log "add to your shell config: export PATH=\"${INSTALL_DIR}:\$PATH\""
fi

log "done"
log "try: ${APP} --help"
log "re-run this installer anytime to update."
