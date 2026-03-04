#!/usr/bin/env bash
set -euo pipefail

UDP_BYTES=$((16 * 1024 * 1024))

CONF="/etc/sysctl.d/99-thruflux-udp.conf"
cat > "$CONF" <<EOFCONF
net.core.rmem_max=${UDP_BYTES}
net.core.wmem_max=${UDP_BYTES}
net.core.rmem_default=${UDP_BYTES}
net.core.wmem_default=${UDP_BYTES}
EOFCONF

sysctl -w net.core.rmem_max="$UDP_BYTES" >/dev/null 2>&1 || true
sysctl -w net.core.wmem_max="$UDP_BYTES" >/dev/null 2>&1 || true
sysctl -w net.core.rmem_default="$UDP_BYTES" >/dev/null 2>&1 || true
sysctl -w net.core.wmem_default="$UDP_BYTES" >/dev/null 2>&1 || true
sysctl --system >/dev/null 2>&1 || true

exit 0
