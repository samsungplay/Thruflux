# Thruflux

Thruflux is a high-throughput, low-latency P2P file transfer toolkit. A lightweight signaling server (`thruserv`) brokers ICE/QUIC handshakes, and the unified `thru` CLI lets you become a host or joiner without friction. Files stream over QUIC with smart resume, verbose diagnostics, and optional STUN/TURN fallback—without sacrificing directness. Startup sayings keep morale high; every transfer begins with a good omen.

## Features

- **Speed-first**: QUIC data streams over ICE/NAT-traversed peers deliver multi-gigabit transfers with per-file parallelism.
- **Resilient resume**: Hosts and joiners negotiate manifests, verify chunks, and restart mid-file if interrupted.
- **Minimal control plane**: `thruserv` only handles session discovery; data skips relays whenever possible.
- **Unified CLI**: `thru host …` and `thru join …` share a single binary with helpful banners, version output, and spicy startup messages.
- **Observability**: Sender and receiver log peer events, transfer stats, and error contexts (`snapshot sender failed`, `transfer failed: control stream timeout`, etc.).
- **Extensible networking**: Supply custom STUN/TURN endpoints, fine-tune QUIC windows, and adjust transfer concurrency with flags.

## Quickstart

```bash
# build every binary (thru, thruserv, etc.)
go build ./...

# start the server (listening on localhost:8080)
thruserv

# host files (runs sender logic) — defaults to https://bytepipe.app and the bundled STUN list
thru host ./photos ./videos

# join a session (runs receiver logic)
thru join ABCDEFGH --out ./downloads
```

You can also run the server via `go run ./cmd/thruserv` and the legacy host/join wrappers (`go run ./cmd/thruflux-sender …`, `go run ./cmd/thruflux-receiver …`), but `thru` centralizes everything while keeping the old entry points for compatibility.

## Command reference

### `thruserv` (signaling server)

```
thruserv [--port N] [--max-sessions N] [--max-receivers-per-sender N] [--ws-* flags] [--ws-idle-timeout D]
```

| Flag | Description |
|---|---|
| `--port` | TCP port to listen on (default `8080`). |
| `--max-sessions` | Max concurrent signaling sessions (default `1000`, `0` disables). |
| `--max-receivers-per-sender` | Limits how many receivers a sender may invite (default `10`). |
| `--max-message-bytes` | Max WebSocket payload size (default `65536`). |
| `--ws-connects-per-min` / `--ws-connects-burst` | Per-IP connect rate cap (default `30`/`10`). |
| `--ws-msgs-per-sec` / `--ws-msgs-burst` | Per-connection message throttle (default `50`/`100`). |
| `--session-creates-per-min` / `--session-creates-burst` | Per-IP session creation throttle (default `10`/`5`). |
| `--max-ws-connections` | Total WebSocket cap (default `2000`, `0` disables). |
| `--ws-idle-timeout` | Idle connection timeout (default `10m`, `0` disables). |
| `--version`, `-v` | Print the Thruflux server version. |

Environment variables: `SHEERBYTES_PORT`.

### `thru host` (sender)

```
thru host <paths...> [flags]
```

| Flag | Description |
|---|---|
| `--server-url` | Signaling server URL (default `https://bytepipe.app`). |
| `--max-receivers` | Max concurrent receivers to invite (default `4`). |
| `--stun-server` | Comma-separated STUN URLs (default `stun:stun.l.google.com:19302,stun:stun.cloudflare.com:3478,stun:stun.bytepipe.app:3478`). |
| `--turn-server` | Comma-separated TURN URLs (default none). |
| `--quic-conn-window-bytes` / `--quic-stream-window-bytes` | QUIC flow-control knobs (defaults `1GiB` / `32MiB`). |
| `--quic-max-incoming-streams` | Max QUIC incoming streams (default `100`). |
| `--chunk-size` | Chunk size in bytes (default auto). |
| `--parallel-files` | Concurrent file transfers (1..8). |
| `--benchmark` | Print throughput stats. |
| `--version`, `-v` | Print the Thruflux CLI version. |

Environment variables: `SHEERBYTES_SERVER_URL`, `SHEERBYTES_PEER_ID`.

### `thru join` (receiver)

```
thru join <join-code> [flags]
```

| Flag | Description |
|---|---|
| `--out` | Output directory (default `.`). |
| `--server-url` | Signaling server URL (default `https://bytepipe.app`). |
| `--stun-server` / `--turn-server` | ICE servers just like `thru host`. |
| `--quic-conn-window-bytes`, `--quic-stream-window-bytes`, `--quic-max-incoming-streams` | QUIC tuning knobs. |
| `--benchmark` | Print throughput stats. |
| `--version`, `-v` | Print the Thruflux CLI version. |

Environment variables: `SHEERBYTES_SERVER_URL`, `SHEERBYTES_JOIN_CODE`, `SHEERBYTES_PEER_ID`.

## Self-hosting guide (Ubuntu 24)

1. **Prepare the machine**
   ```bash
   sudo apt update && sudo apt upgrade -y
   sudo apt install -y build-essential curl git
   curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh # optional
   sudo snap install --classic go
   ```

2. **Build the binaries**
   ```bash
   git clone <repo>
   cd sheerbytes
   go build ./cmd/thru ./cmd/thruserv
   sudo mv thruserv /usr/local/bin/
   sudo mv thru /usr/local/bin/
   ```

3. **Optional TLS + WSS (recommended)**
   - Install Caddy or Nginx; Caddy example:
     ```bash
     sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https
     curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/deb/debian/gpg.key' | sudo tee /etc/apt/trusted.gpg.d/caddy-stable.asc
     curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/deb/debian/codename.list' \
       | sudo tee /etc/apt/sources.list.d/caddy-stable.list
     sudo apt update
     sudo apt install caddy
     ```
   - Configure `/etc/caddy/Caddyfile`:
     ```
     your.domain {
       reverse_proxy localhost:8080
     }
     ```
   - Reload: `sudo systemctl reload caddy`.

4. **Run `thruserv`**
   - Create a non-root user, then a systemd unit `/etc/systemd/system/thruserv.service`:
     ```
     [Unit]
     Description=Thruflux signaling server
     After=network.target

     [Service]
     ExecStart=/usr/local/bin/thruserv --port 8080
     Restart=on-failure
     User=thruflux
     WorkingDirectory=/opt/thruflux

     [Install]
     WantedBy=multi-user.target
     ```
   - Enable and start:
     ```bash
     sudo systemctl daemon-reload
     sudo systemctl enable --now thruserv
     ```

5. **Point clients to `https://your.domain`**
   - Use `thru host … --server-url https://your.domain`.
   - Receivers connect via `thru join ABCDEFGH --server-url https://your.domain`.

### Monitoring & troubleshooting

- `sudo journalctl -u thruserv -f` streams server logs.
- Use `thru --version` to verify the shipped revision.
- When `thru host` or `thru join` fail to complete ICE, the CLI logs `snapshot sender/receiver failed` with the wrapped cause (timeouts, `failed to establish ICE connection`, etc.).

May TURN never be needed!
