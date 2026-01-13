# Thruflux

Thruflux is a high‑throughput, low‑latency P2P file transfer toolkit. A lightweight signaling server (`thruserv`) handles discovery and ICE negotiation, while the unified `thru` CLI lets you host or join in seconds. Data flows directly over QUIC between peers for fast, resilient transfers.

## Why Thruflux ✨

The vision is simple: make high‑performance, mass file sharing easy and accessible to everyone — at no cost. Thruflux ships with free defaults out of the box:
- **Signaling server** at `https://bytepipe.app` (capacity‑limited, but free to use).
- **STUN defaults** so most users can connect immediately without extra setup.

If you need full control or higher limits, self‑host in minutes.

## Key features ✅

- **Direct QUIC transfers** for high throughput and low latency.
- **Multi‑receiver sessions** so one host can share with many peers at once.
- **Resumable transfers** with last‑chunk verification for safety.
- **Unified CLI**: `thru host` and `thru join` live in one binary.
- **Flexible networking**: bring your own STUN/TURN, tune QUIC, and set concurrency.
- **Operational guardrails**: server rate‑limits and idle timeouts baked in.

## Quickstart 🚀

1. Build binaries locally yourself (see below) or download from releases section. Package managers support coming soon!
2. Basic usage:
```bash
# host files (defaults to https://bytepipe.app + bundled STUN list)
thru host ./photos ./videos

# share the join code with multiple peers
thru join ABCDEFGH --out ./downloads
```

Multiple receivers can join the same code concurrently (subject to `--max-receivers` and server limits).

## Building the CLIs locally 🛠️

1. **Prerequisites** – install Go (1.22+ recommended) for your platform and ensure `GOPATH/bin` is on your `PATH`.
3. **Clone and bootstrap**
   ```bash
   git clone <repo>
   cd thruflux
   go mod download
   ```
4. **Build the binaries**
   ```bash
   go build ./cmd/thru ./cmd/thruserv
   ```
   On Windows the outputs will be `thru.exe` and `thruserv.exe`; on Unix-like systems they are `thru` and `thruserv`.
5. **(Optional) Install globally**
   ```bash
   mkdir -p "$HOME/bin"
   mv thru thruserv "$HOME/bin/"
   ```
   Then add `$HOME/bin` to your `PATH` (e.g., `export PATH="$HOME/bin:$PATH"`).

If you change dependencies, rerun `go mod tidy` before rebuilding to keep the module tidy.

## Command reference

### `thruserv` (signaling server)

```
thruserv [--port N] [--max-sessions N] [--max-receivers-per-sender N] [--ws-* flags] [--ws-idle-timeout D] [--session-timeout D]
```

| Flag | Description |
|---|---|
| `--port` | TCP port to listen on (default `8080`). |
| `--max-sessions` | Max concurrent signaling sessions (default `1000`, `0` disables). |
| `--max-receivers-per-sender` | Limits how many receivers a sender may invite (default `10`). |
| `--max-message-bytes` | Max WebSocket payload size (default `65536`). |
| `--ws-connects-per-min` / `--ws-connects-burst` | Per‑IP connect rate cap (default `30`/`10`). |
| `--ws-msgs-per-sec` / `--ws-msgs-burst` | Per‑connection message throttle (default `50`/`100`). |
| `--session-creates-per-min` / `--session-creates-burst` | Per‑IP session creation throttle (default `10`/`5`). |
| `--max-ws-connections` | Total WebSocket cap (default `2000`, `0` disables). |
| `--ws-idle-timeout` | Idle connection timeout (default `10m`, `0` disables). |
| `--session-timeout` | Max session lifetime (default `24h`, `0` disables). |
| `--version`, `-v` | Print the Thruflux server version. |
| `--help`, `-h` | Show usage and flag descriptions. |

### `thru host` (sender)

```
thru host <paths...> [flags]
```

| Flag | Description |
|---|---|
| `--server-url` | Signaling server URL (default `https://bytepipe.app`). |
| `--max-receivers` | Max concurrent receivers to invite (default `4`). |
| `--stun-server` | Comma‑separated STUN URLs (default `stun:stun.l.google.com:19302,stun:stun.cloudflare.com:3478,stun:stun.bytepipe.app:3478`). |
| `--turn-server` | Comma‑separated TURN URLs (default none). |
| `--quic-conn-window-bytes` / `--quic-stream-window-bytes` | QUIC flow‑control knobs (defaults `1GiB` / `32MiB`). |
| `--quic-max-incoming-streams` | Max QUIC incoming streams (default `100`). |
| `--chunk-size` | Chunk size in bytes (default auto). |
| `--parallel-files` | Concurrent file transfers (1..8). |
| `--benchmark` | Print throughput stats. |
| `--version`, `-v` | Print the Thruflux CLI version. |
| `--help`, `-h` | Show usage and flag descriptions. |

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
| `--help`, `-h` | Show usage and flag descriptions. |

## Self‑hosting guide (Ubuntu) 🐧

1. **Prepare the machine**
   ```bash
   sudo apt update && sudo apt upgrade -y
   sudo apt install -y build-essential curl git
   sudo snap install --classic go
   ```

2. **Build the binaries**
   ```bash
   git clone <repo>
   cd thruflux
   go build ./cmd/thru ./cmd/thruserv
   sudo mv thruserv /usr/local/bin/
   sudo mv thru /usr/local/bin/
   ```

3. **Optional TLS + WSS (recommended)**
   - Install Caddy:
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

4. **Run `thruserv` as a systemd service**
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
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now thruserv
   ```

6. **Point clients to your server**
   - Host: `thru host … --server-url https://your.domain`
   - Join: `thru join ABCDEFGH --server-url https://your.domain`

## Contributing 🤝

Thruflux is community‑driven. Contributions, testing, and feedback help keep it fast, free, and accessible.

May TURN never be needed!
