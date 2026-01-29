# Thruflux
**Status:** Thruflux is under heavy maintenance at the moment; Things will NOT work. Please wait for next release (v0.2.0) as huge improvements are coming.

Thruflux is a **throughput-maximized, peer-to-peer** file transfer toolkit built for moving multiple files and folders **as fast as your network allows**.

It uses a lightweight signaling server (thruserv) for discovery and ICE negotiation, while the unified thru CLI lets you host or join transfers in seconds. Data flows directly between peers over QUIC for fast, resilient, and encrypted transfers.

https://github.com/user-attachments/assets/db7aebf8-322f-44cc-8d4b-b3c6b765f994

## Why Thruflux?

The vision is simple: **maximize throughput without sacrificing ease of use**. Thruflux makes high-performance, large-scale file sharing fast, simple, and freely available to everyone.

It is designed to **work out of the box.**:
- **Default Signaling server** at `https://bytepipe.app` (capacity‑limited, but free to use. Currently supports up to ~2k concurrent users. May be expanded in the future).
- **STUN defaults** so most users can connect immediately without extra setup.
- **Default TURN relays** for tougher networks (shared ~900 Mbps bandwidth right now, may be expanded in the future).

If you need full control or higher limits, self‑host in minutes.

**Heads-up**: The hosted TURN pool is shared under a fair-use policy, with bandwidth divided among active users. TURN relaying is only used on restrictive networks when direct peer-to-peer connectivity is not possible, but during periods of high usage it may reduce throughput. If you need guaranteed capacity, self-host a TURN server (coturn works great); the final section shows how to set it up.

## Key features ✅

- **Aggressive UDP hole-punching** that maximizes direct peer-to-peer connectivity, even across the toughest NATs.
- **High-performance, massively parallel QUIC over UDP transfers** delivering exceptional throughput with modern, built-in encryption.
- **First-class multi-file and directory transfers** — this is what thruflux is built for, fully leveraging QUIC’s parallel streams for sustained high throughput.
- **Transport-level security** bound directly to the secure join code and QUIC session, protecting against Man-in-the-Middle attacks by design.
- **Native multi-receiver support**, allowing a single host to share with many peers simultaneously.
- **Fully resumable transfers**, so large sends continue seamlessly even after interruptions.
- **Clean, intuitive CLI**: `thru host` and `thru join` — nothing extra, nothing confusing, all bundled with sane defaults.
- **Power-user flexibility**: bring your own STUN/TURN, fine-tune QUIC parameters, and customize dozens of advanced options.
- **Automatic TURN/TURNS fallback**, ensuring reliable connectivity across restrictive networks for a true “just works” experience.
- **Fully self-hostable**, giving you complete control, guaranteed capacity, and private deployments when you need them.


## Quickstart 🚀

**Install**

**macOS / Linux (Homebrew)**

```bash
brew tap samsungplay/thruflux
brew install thru
```

**Windows (Scoop)**

```bash
scoop bucket add thruflux https://github.com/samsungplay/scoop-thruflux
scoop install thru
```

**Use**

```bash
# host files (defaults to https://bytepipe.app + bundled STUN list)
thru host ./photos ./videos

# share the join code with multiple peers
thru join ABCDEFGH --out ./downloads
```

Multiple receivers can join the same code concurrently (subject to `--max-receivers` and server limits).

## Building the CLIs locally 🛠️

1. **Prerequisites** – install Go (1.22+ recommended) for your platform and ensure `GOPATH/bin` is on your `PATH`.
2. **Clone and bootstrap**
   ```bash
   git clone https://github.com/samsungplay/Thruflux.git
   cd Thruflux
   go mod download && go mod tidy
   ```
3. **Build the binaries**
   ```bash
   go -o /your/output/directory build ./cmd/thru ./cmd/thruserv
   ```
   On Windows the outputs will be `thru.exe` and `thruserv.exe`; on Unix-like systems they are `thru` and `thruserv`.
4. **(Optional) Install globally**
   ```bash
   mkdir -p "$HOME/bin"
   mv thru thruserv "$HOME/bin/"
   ```
   Then add `$HOME/bin` to your `PATH` (e.g., `export PATH="$HOME/bin:$PATH"`).

If you change dependencies, rerun `go mod tidy` before rebuilding to keep the module tidy.

## Command reference

### `thruserv` (signaling server)

```
thruserv [--port N] [--max-sessions N] [--max-receivers-per-sender N] [--ws-* flags] [--ws-idle-timeout D] [--session-timeout D] [--turn-* flags]
```

| Flag                                                    | Description                                                       |
| ------------------------------------------------------- | ----------------------------------------------------------------- |
| `--port`                                                | TCP port to listen on (default `8080`).                           |
| `--max-sessions`                                        | Max concurrent signaling sessions (default `1000`, `0` disables). |
| `--max-receivers-per-sender`                            | Limits how many receivers a sender may invite (default `10`).     |
| `--max-message-bytes`                                   | Max WebSocket payload size (default `65536`).                     |
| `--ws-connects-per-min` / `--ws-connects-burst`         | Per‑IP connect rate cap (default `30`/`10`).                      |
| `--ws-msgs-per-sec` / `--ws-msgs-burst`                 | Per‑connection message throttle (default `50`/`100`).             |
| `--session-creates-per-min` / `--session-creates-burst` | Per‑IP session creation throttle (default `10`/`5`).              |
| `--max-ws-connections`                                  | Total WebSocket cap (default `2000`, `0` disables).               |
| `--ws-idle-timeout`                                     | Idle connection timeout (default `10m`, `0` disables).            |
| `--session-timeout`                                     | Max session lifetime (default `24h`, `0` disables).               |
| `--turn-server`                                         | TURN server URL(s) for issuing ephemeral credentials.             |
| `--turn-static-auth-secret`                             | TURN REST static auth secret (coturn `use-auth-secret`).          |
| `--turn-cred-ttl`                                       | TURN credential TTL (default `1h`).                               |
| `--version`, `-v`                                       | Print the Thruflux server version.                                |
| `--help`, `-h`                                          | Show usage and flag descriptions.                                 |

### `thru host` (sender)

```
thru host <paths...> [flags]
```

| Flag                                                      | Description                                                                                                                   |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `--server-url`                                            | Signaling server URL (default `https://bytepipe.app`).                                                                        |
| `--max-receivers`                                         | Max concurrent receivers to invite (default `4`).                                                                             |
| `--stun-server`                                           | Comma‑separated STUN URLs (default `stun:stun.l.google.com:19302,stun:stun.cloudflare.com:3478,stun:stun.bytepipe.app:3478`). |
| `--turn-server`                                           | Comma‑separated TURN URLs (default none). Supports `turn:` and `turns:` schemes.                                              |
| `--test-turn`                                             | Only use TURN relay candidates (no direct probing).                                                                           |
| `--quic-conn-window-bytes` / `--quic-stream-window-bytes` | QUIC flow‑control knobs (defaults `512MiB` / `64MiB`).                                                                        |
| `--quic-max-incoming-streams`                             | Max QUIC incoming streams (default `256`).                                                                                    |
| `--chunk-size`                                            | Chunk size in bytes (default auto).                                                                                           |
| `--total-connections`                                     | Total QUIC connections (default `4`).                                                                                         |
| `--total-streams`                                         | Total concurrent transfer streams (default `12`, `1..32`).                                                                     |
| `--udp-read-buffer-bytes`                                 | UDP read buffer size (default `8388608`).                                                                                      |
| `--udp-write-buffer-bytes`                                | UDP write buffer size (default `8388608`).                                                                                     |
| `--benchmark`                                             | Print throughput stats.                                                                                                       |
| `--verbose`                                               | Enable verbose UI/logging.                                                                                                    |
| `--version`, `-v`                                         | Print the Thruflux CLI version.                                                                                               |
| `--help`, `-h`                                            | Show usage and flag descriptions.                                                                                             |

### `thru join` (receiver)

```
thru join <join-code> [flags]
```

| Flag                                                                                    | Description                                            |
| --------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| `--out`                                                                                 | Output directory (default `.`).                        |
| `--server-url`                                                                          | Signaling server URL (default `https://bytepipe.app`). |
| `--stun-server` / `--turn-server`                                                       | ICE servers just like `thru host`.                     |
| `--test-turn`                                                                           | Only use TURN relay candidates (no direct probing).    |
| `--quic-conn-window-bytes`, `--quic-stream-window-bytes`, `--quic-max-incoming-streams` | QUIC tuning knobs.                                     |
| `--benchmark`                                                                           | Print throughput stats.                                |
| `--verbose`                                                                             | Enable verbose UI/logging.                             |
| `--udp-read-buffer-bytes` / `--udp-write-buffer-bytes`                                   | UDP buffer sizes (default `8388608`).                  |
| `--version`, `-v`                                                                       | Print the Thruflux CLI version.                        |
| `--help`, `-h`                                                                          | Show usage and flag descriptions.                      |

## Self‑hosting guide (Ubuntu) 🐧

1. **Install Go**

   ```bash
   sudo apt update && sudo apt upgrade -y
   sudo apt install -y build-essential curl git
   sudo snap install --classic go
   ```

2. **Build the thruserv binary**

   ```bash
   git clone https://github.com/samsungplay/Thruflux.git
   cd Thruflux
   go build ./cmd/thruserv
   sudo mv thruserv /usr/local/bin/
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

4. **Run `thruserv` as a systemd service (Example path : /etc/systemd/system/thruserv.service)**

   ```
   [Unit]
   Description=Thruflux Output Service
   # Ensure the network is up before starting the service
   After=network.target
   # Only start if the executable exists
   ConditionPathExists=/path/to/thruserv_dir

   [Service]
   Type=simple
   User=yourusername  
   Group=yourusername
   # The folder where your app expects to find local files (configs, assets, etc.)
   WorkingDirectory=/path/to/thruserv_dir
   # The absolute path to the binary
   ExecStart=/path/to/thruserv_dir/thruserv  
   # Restart the service automatically if it crashes
   Restart=on-failure
   RestartSec=10

   [Install]
   # Start this service when the system reaches a normal multi-user state
   WantedBy=multi-user.target
   ```

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now thruserv
   ```

5. **Point clients to your server**
   - Host: `thru host … --server-url https://your.domain`
   - Join: `thru join ABCDEFGH --server-url https://your.domain`

6. **(Optional) Enable built-in, auto-provisioned TURN relay support using coturn REST credentials**

   This step enables automatic TURN fallback so clients can still connect when direct peer-to-peer UDP paths fail without specifying --turn-server flag by themselves (e.g. strict NATs, firewalls).

   - Configure **coturn** with `use-auth-secret` and the same `static-auth-secret` that will be shared with `thruserv`.
   - Example coturn server config (for more info, check out the [coturn](https://github.com/coturn/coturn) repository) :
   ```
   # ===== Core =====
   listening-port=3478
   tls-listening-port=5349
   listening-ip=0.0.0.0

   # Public mapping
   external-ip=YOUR_PUBLIC_IP
   relay-ip=YOUR_PUBLIC_IP

   # Identity (realm must match what your backend uses in TURN creds)
   realm=yourdomain.com
   server-name=stun.yourdomain.com

   # ===== Auth (TURN REST / ephemeral) =====
   fingerprint
   lt-cred-mech
   use-auth-secret
   static-auth-secret=SOME_SAFE_SECRET
   stale-nonce

   # ===== Lifetimes / anti-abuse =====
   max-allocate-lifetime=600
   total-quota=2000
   user-quota=50
   no-loopback-peers
   no-multicast-peers

   # Prefer UDP TURN (enable TCP/TLS only if you need corporate networks)
   no-tcp-relay

   # ===== Logging =====
   log-file=/var/log/turn.log
   simple-log
   # Relay port range (lock it down)
   min-port=49152
   max-port=65535
   ```
   - Start `thruserv` with TURN options enabled. This allows it to **mint time-limited TURN credentials** and distribute them to clients automatically:
     ```
     thruserv \
       --port 8080 \
       --turn-server turn:stun.bytepipe.app:3478 \
       --turn-static-auth-secret <your-static-auth-secret> \
       --turn-cred-ttl 1h
     ```
   - Clients do **not** need to specify `--turn-server` manually unless you want to override the TURN server provided by `thruserv`.


## Contributing 🤝

Thruflux is community‑driven. Contributions, testing, and feedback help keep it fast, free, and accessible.

May TURN never be needed!

## TURN / TURNS usage

Thruflux performs manual hole‑punching first and only falls back to TURN relay when needed.

Examples:

```bash
# TURN over UDP (most common)
thru host ./data --turn-server "turn://user:pass@turn.example.com:3478"
thru join ABCDEFGH --turn-server "turn://user:pass@turn.example.com:3478"

# TURN over TLS (TURNS). Useful on restrictive networks.
thru host ./data --turn-server "turns://user:pass@turn.example.com:5349"
thru join ABCDEFGH --turn-server "turns://user:pass@turn.example.com:5349"

# Override TLS SNI / cert name if needed
thru host ./data --turn-server "turns://user:pass@turn.example.com:5349?servername=turn.example.com"

# Debug only: skip TLS verification
thru host ./data --turn-server "turns://user:pass@turn.example.com:5349?insecure=1"
```

Notes:
- If thruserv is configured to provide TURN access via time-limited REST credentials (via --turn-server and --turn-static-auth-secret), clients do not need to specify a TURN server
- `turn:` and `turn://` are equivalent; `turns:` / `turns://` enables TLS for the TURN control channel.
- If you use `turns://`, the hostname in the URL must match the TURN server TLS certificate (unless `insecure=1` is set).
