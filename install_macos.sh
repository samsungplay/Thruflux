#!/usr/bin/env bash
set -euo pipefail

APP="thru"
INSTALL_DIR="/usr/local/bin"
TARGET="${INSTALL_DIR}/${APP}"
UDP_BYTES=$((16 * 1024 * 1024))

URL_MAC="https://github.com/samsungplay/Thruflux-C-/releases/download/v18/thru_mac"
PLIST="/Library/LaunchDaemons/com.thruflux.udptune.plist"
LABEL="com.thruflux.udptune"

log()  { printf "%s\n" "$*"; }
warn() { printf "warning: %s\n" "$*" >&2; }
fail() { printf "error: %s\n" "$*" >&2; exit 1; }

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || fail "sudo not found (run as root)."
  SUDO="sudo"
fi

command -v curl >/dev/null 2>&1 || fail "curl is required."

download() {
  local url="$1" out="$2"
  curl -fL --progress-bar "$url" -o "$out"
}

apply_sysctls_now() {
  $SUDO /usr/sbin/sysctl -w kern.ipc.maxsockbuf="$UDP_BYTES" >/dev/null 2>&1 || true
  $SUDO /usr/sbin/sysctl -w net.inet.udp.recvspace="$UDP_BYTES" >/dev/null 2>&1 || true
  $SUDO /usr/sbin/sysctl -w net.inet.udp.maxdgram=65535 >/dev/null 2>&1 || true
}

install_launchdaemon() {
  $SUDO tee "$PLIST" >/dev/null <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>${LABEL}</string>
  <key>RunAtLoad</key><true/>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/sh</string>
    <string>-c</string>
    <string>
      /usr/sbin/sysctl -w kern.ipc.maxsockbuf=${UDP_BYTES} &&
      /usr/sbin/sysctl -w net.inet.udp.recvspace=${UDP_BYTES} &&
      /usr/sbin/sysctl -w net.inet.udp.maxdgram=65535
    </string>
  </array>
</dict>
</plist>
EOF

  $SUDO chown root:wheel "$PLIST"
  $SUDO chmod 644 "$PLIST"

  if $SUDO launchctl print "system/${LABEL}" >/dev/null 2>&1; then
    $SUDO launchctl bootout "system/${LABEL}" >/dev/null 2>&1 || true
  fi
  $SUDO launchctl bootstrap system "$PLIST"
  $SUDO launchctl enable "system/${LABEL}" >/dev/null 2>&1 || true
}

verify_udp() {
  local maxsock recv maxdgram
  maxsock="$(/usr/sbin/sysctl -n kern.ipc.maxsockbuf 2>/dev/null || echo 0)"
  recv="$(/usr/sbin/sysctl -n net.inet.udp.recvspace 2>/dev/null || echo 0)"
  maxdgram="$(/usr/sbin/sysctl -n net.inet.udp.maxdgram 2>/dev/null || echo 0)"

  log "UDP: maxsockbuf=$((maxsock/1024/1024))MiB recvspace=$((recv/1024/1024))MiB maxdgram=${maxdgram}"
  if [[ "$maxsock" -lt "$UDP_BYTES" || "$recv" -lt "$UDP_BYTES" ]]; then
    warn "UDP buffers may be capped by macOS (best-effort applied)"
  fi
}

log "Thruflux installer (macOS)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

log "downloading…"
download "$URL_MAC" "$tmp/$APP"
[[ -s "$tmp/$APP" ]] || fail "downloaded file is empty."

log "installing to ${TARGET}"
$SUDO mkdir -p "$INSTALL_DIR"
$SUDO install -m 0755 "$tmp/$APP" "$TARGET"

if command -v codesign >/dev/null 2>&1; then
  $SUDO codesign --force --sign - --timestamp=none "$TARGET" >/dev/null 2>&1 || true
fi

log "configuring UDP buffers (16 MiB)"
apply_sysctls_now
install_launchdaemon
verify_udp

if command -v "$APP" >/dev/null 2>&1; then
  log "installed: $(command -v "$APP")"
else
  warn "${INSTALL_DIR} is not on PATH for this shell"
  log "add to your shell config: export PATH=\"${INSTALL_DIR}:\$PATH\""
fi

log "done"
log "try: ${APP} --help"
log "re-run this installer anytime to update."
