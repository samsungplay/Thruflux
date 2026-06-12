# Thruflux (Open Beta, Newly Re-written in C++)
**New Update**: Android version has been released! Check the website (link down below) for more information.
<img width="2816" height="1536" alt="Thruflux_Logo" src="https://github.com/user-attachments/assets/cf853fd9-50ff-4d43-ac86-7de4c9042371" />


Built in C++, Thruflux is a cross-platform, **highly speed-focused, peer-to-peer** file transfer toolkit built for moving multiple files and folders **as fast as your network allows** between **any two random devices**. Unlike traditional p2p file tools, Thruflux is entirely focused on use of the modern QUIC UDP protocol.


Thruflux provides all of these:

- 🖥 **Desktop UI application**
- 💻 **Command-line interface (CLI)**
- 📱 **Android App**
- IOS app is planned

CLI consists of four core subcommands:

thru server - opens a lightweight signaling server for discovery & ICE negotiation (for self-hosting).

thru host - share any number of file of any size with other peers

thru join - join an existing host session to download the files served by the host

thru ui - opens a local web interface that lets you host/join via REST endpoint (mainly for internal use for the app version)

---

# Website

For the **desktop/android application download** and **project landing page**:

https://thruflux.bytepipe.app/

---
## Desktop Demo
https://github.com/user-attachments/assets/f21e6749-8360-4538-b4c4-9439e608b1e6

## CLI Demo
https://github.com/user-attachments/assets/50624352-120a-4d9f-8a8c-e5d8aac5f3df

## Quickstart 🚀

**Dekstop UI Install**

Install from here: 
https://thruflux.bytepipe.app/ 
or directly from github releases

**CLI Install**

**Linux Kernel 3.10+ / glibc 2.17+ (Ubuntu, Debian, CentOS, etc.)**
```bash
curl -fsSL https://raw.githubusercontent.com/samsungplay/Thruflux/refs/heads/main/install_linux.sh | bash
```

**Mac 11.0+ (Intel & Apple Silicon)**
```bash
curl -fsSL https://raw.githubusercontent.com/samsungplay/Thruflux/refs/heads/main/install_macos.sh | bash
```

**Windows 10+ (10+ Recommended, technically still could work on Windows 7/8)**
```bash
iwr -useb https://raw.githubusercontent.com/samsungplay/Thruflux/refs/heads/main/install_windows.ps1 | iex
```

**Use**

```bash
# host files
thru host ./photos ./videos

# share the join code with multiple peers
thru join ABCDEFGH --out ./downloads
```

Multiple receivers can join the same code concurrently (subject to `--max-receivers` and server limits).

## Why Thruflux?

The vision is simple: In a world where time is valuable, **maximize throughput without sacrificing ease of use**. Thruflux dreams of a world where secure, large-scale file sharing is fast, simple, and freely available to everyone.

It is designed to **work out of the box**:
- **Default Signaling server** at `https://bytepipe.app` (capacity‑limited, but free to use. Currently supports up to ~2k concurrent users. May be expanded in the future).
- **STUN default** so most users can connect immediately without extra setup.
- **Default TURN relay** for tougher networks (shared ~900 Mbps bandwidth right now, may be expanded in the future).

If you need full control or higher limits, self‑host in minutes.

**Note**: The hosted TURN pool is shared under a fair-use policy, with bandwidth divided among active users. TURN relaying is only used on restrictive networks when direct peer-to-peer connectivity is not possible, but during periods of high usage it may reduce throughput. If you need guaranteed capacity, self-host a TURN server (coturn works great); the final section shows how to set it up.

## Key features
* **Industry-grade ICE exchange** for reliable peer-to-peer connectivity — even through the toughest NATs.
* **Blazing-fast QUIC over UDP** with modern encryption and battle-tested C++ performance.
 * **First-class multi-file & directory transfers support** — uncompressed, as fast as sending a single file.
* **Security by default**: encrypted QUIC streams, secure WSS signaling, and high-entropy 16-digit join codes to prevent MITM attacks.
* **Native multi-receiver support** — share with multiple peers simultaneously.
* **End-to-end integrity** backed by QUIC’s built-in network guarantees.
* **Fully resumable transfers** that continue smoothly after interruptions.
* **Minimal, intuitive CLI**: `thru host` and `thru join` — simple, powerful, no clutter.
* **Advanced configurability**: custom STUN/TURN, tunable QUIC parameters, and more.
* **Automatic TURN/TURNS fallback** for dependable “just works” connectivity.
* **Fully self-hostable** for total control, private deployments, and guaranteed capacity.

## Benchmarks (rough comparison)

The table below is **not meant to be a hard proof or a definitive ranking**.  
Its goal is to give a **rough, practical picture** of how thruflux compares to commonly used tools under realistic conditions.

These results are **not intended to deride or diminish any of the tools listed**.  
Each exists for good reasons, and more high-quality tools ultimately make the ecosystem better.  
The goal here is simply to provide a **big-picture reference** so users can understand where thruflux fits.

Performance depends heavily on network paths, routing, congestion, system load, cpu, and disk specs.

Thruflux is under **active development**, and performance improvements are ongoing.  
This section will be **updated over time** as the implementation evolves.

### Environment
- Vultr instance: 2 vCPU (AMD EPYC Genoa), 4 GB RAM, NVMe SSD, Ubuntu 24.04
- Tests conducted over the public internet between Chicago (sender) and Seoul (receiver), representing a high-latency link.
- Methodology: results show the median of 3 runs. Times represent end-to-end wall-clock duration, measured from the moment the sender command is executed until the receiver completes (or the sender finishes if no separate receive command exists), not just the raw transfer time.
- For specific benchmarking script used, refer to the ultimate_bench.sh script in the repo

### Summary

| Tool | Transport | Random Remote Peers | Multi-Receiver | 10 GiB File | 1000 × 10 MiB |
|-----|-----|-----|-----|-----|-----|
| Thruflux | QUIC | Yes | Yes | **2m 20s** | **2m 18s** |
| Croc | TCP | Yes | No | 2m 40s | 19m 33s |
| Wormhole | TCP (relay) | Yes | No | 22m 20s | N/A (stalled around ~38:59) |
| SCP | TCP | No | No | 15m 06s | 26m 14s |
| Rsync | TCP | No | No | 15m 18s | 14m 53s |

> **Note (TURN relay mode)**  
> Thruflux supports TURN relay fallback when a direct peer-to-peer connection cannot be established via ICE.  
> In normal conditions, ICE prioritizes direct connectivity for maximum throughput. Using QUIC over UDP improves the likelihood of establishing a direct connection compared to TCP.  
>  
> Relay performance depends primarily on the relay server’s bandwidth, load, and location rather than the transfer protocol itself. For this reason, TURN results are excluded from the primary benchmark comparison and are intended only as a fallback scenario.
>
> | Mode | 10 GiB File | 1000 × 10 MiB |
> |-----|-----|-----|
> | Thruflux (TURN relay) | **5m 40s** | **5m 35s** |

---

### What this table shows

- **Strong P2P performance:** Thruflux delivers the fastest throughput among peer-to-peer tools tested, particularly for large directory transfers.
- **Competitive with established tools:** Thruflux significantly outperforms SCP and Rsync on high-latency links, demonstrating the efficiency of QUIC-based P2P transfers.
- **Reliable fallback via relay:** When direct connections are not possible, TURN relay mode still provides solid performance, remaining competitive with or faster than several traditional tools.



## Manual Build Instructions

## Linux (Ubuntu 20.04 or later) 
- **Note**: For other linux distros, the high level steps are similar. Follow along with your own package tools. But there can be some discrepencies, so pay attention to the build log outputs and debug them. Yeah, I know, building in C++ is one tough task..

### 1) Install CMake (>= 3.24)

If your Ubuntu CMake is older than 3.24, install a newer one (Refer to Kitware APT repo: [https://apt.kitware.com/](https://apt.kitware.com/)).

Check version:

```bash
cmake --version
```

### 2) Install vcpkg

```bash
git clone https://github.com/microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh
```

### 3) Install essential build tools

```bash
sudo apt update
sudo apt install -y software-properties-common
sudo apt install -y python3-distutils
sudo apt install bison nasm build-essential zip unzip tar pkg-config curl ninja-build
```

### 4) Install GCC 10 & Set it as default
If you already have gcc version >=10 you can skip this step. (Check : gcc --version)

```bash
sudo apt install gcc-10 g++-10
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 120 --slave /usr/bin/g++ g++ /usr/bin/g++-10 --slave /usr/bin/gcov gcov /usr/bin/gcov-10
```

### 4) Configure and build

From the project root:

```bash
git clone https://github.com/samsungplay/Thruflux.git
cd Thruflux/

cmake -S . -B build -G Ninja \
  -DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake \
  -DVCPKG_TARGET_TRIPLET=x64-linux \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
  -DCMAKE_C_FLAGS_RELEASE="-O3 -DNDEBUG -flto=auto" \
  -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG -flto=auto" \
  -DCMAKE_EXE_LINKER_FLAGS="-static-libgcc -static-libstdc++ -Wl,-O1"
  
cmake --build build --parallel
```

### 6) Output binary

```bash
./build/thru
```

----------

## Windows (Visual Studio 2022)

### 0) Install tools

-   Install **Visual Studio 2022** with **Desktop development with C++** 
- Link for downloading **Visual Studio 2022**: https://aka.ms/vs/17/release/vs_community.exe
    
-   Install **CMake >= 3.24**
    
    ```powershell
    winget install Kitware.CMake
    ```
    

Check version:

```powershell
cmake --version
```

### 1) Install vcpkg

```powershell
cd C:\dev
git clone https://github.com/microsoft/vcpkg.git
cd vcpkg
.\bootstrap-vcpkg.bat
.\vcpkg integrate install
```

### 2) Clone the repo

```powershell
cd C:\dev
git clone https://github.com/samsungplay/Thruflux.git
cd Thruflux
```

### 3) Install prerequisites

```powershell
choco install winflexbison3 nasm -y
```

### 4) Configure and build
- **Note**: The x64-windows-static target is important. There are some patches I made without which build will fail.
```powershell
cmake -S . -B build `
  -G "Visual Studio 17 2022" -A x64 `
  -DCMAKE_TOOLCHAIN_FILE="C:\dev\vcpkg\scripts\buildsystems\vcpkg.cmake" `
  -DVCPKG_TARGET_TRIPLET=x64-windows-static `
  -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON `
  -DCMAKE_C_FLAGS_RELEASE="/O2 /DNDEBUG /GL" `
  -DCMAKE_CXX_FLAGS_RELEASE="/O2 /DNDEBUG /GL" `
  -DCMAKE_EXE_LINKER_FLAGS_RELEASE="/LTCG /OPT:REF /OPT:ICF"

cmake --build build --config Release --parallel
```

### 5) Output binary

Common output locations:

-   `.\build\Release\thru.exe`
    
-   or `.\build\thru.exe`
    

Find it quickly:

```powershell
dir .\build -Recurse -Filter "thru*.exe"
```
----------

## macOS (Apple Silicon or Intel)

### 1) Install Xcode Command Line Tools

```bash
xcode-select --install
```

### 2) Install Homebrew (if you don’t have it)

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 3) Install dependencies (CMake, pkg-config, etc.)

```bash
brew update
brew install cmake pkg-config bison nasm ninja
```

Verify CMake version:
```bash
cmake --version
```

### 4) Install vcpkg

```bash
git clone https://github.com/microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
```

### 5) Clone the repo

```bash
git clone https://github.com/samsungplay/Thruflux.git
cd Thruflux
```

### 6) Configure and build (Release)
- If you want to build for intel mac:

```bash
export MACOSX_DEPLOYMENT_TARGET=11.0

cmake -S . -B build -G Ninja \
  -DCMAKE_TOOLCHAIN_FILE="~/dev/vcpkg/scripts/buildsystems/vcpkg.cmake" \
  -DVCPKG_TARGET_TRIPLET=x64-osx \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_OSX_DEPLOYMENT_TARGET=11.0 \
  -DCMAKE_OSX_ARCHITECTURES=x86_64 \
  -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
  -DCMAKE_C_FLAGS_RELEASE="-O3 -DNDEBUG" \
  -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG"
 
cmake --build build --parallel
```
- If you want to build for apple silicon mac:
```bash
export MACOSX_DEPLOYMENT_TARGET=11.0

cmake -S . -B build -G Ninja \
-DCMAKE_TOOLCHAIN_FILE="~/dev/vcpkg/scripts/buildsystems/vcpkg.cmake" \
-DVCPKG_TARGET_TRIPLET=arm64-osx \
-DCMAKE_BUILD_TYPE=Release \
-DCMAKE_OSX_DEPLOYMENT_TARGET=11.0 \
-DCMAKE_OSX_ARCHITECTURES=arm64 \
-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
-DCMAKE_C_FLAGS_RELEASE="-O3 -DNDEBUG" \
-DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG"
 
cmake --build build --parallel
```

### 7) Output binary

Typically:

```bash
./build/thru
```

If you don’t see it there:

```bash
find build -maxdepth 3 -type f -name "thru*"
```

### 8) Sign the executable
Without this step, modern macs might refuse to run the binary (local use is probably fine). For safety, just give it an ad-hoc signing:

```bash
codesign --force --sign - --timestamp=none thru
```

- (Optional) If you built for both, and want to create a unified executable that will run on both intel and apple silicon mac (assuming intel mac binary was built to build_intel_mac folder):

```bash
lipo -create -output build_universal_mac/thru build/thru build_intel_mac/thru
```


## Android (Building on mac)

> Produces a native Android CLI binary (`thru`) for **Android 9+ (API 28+)** on **arm64-v8a** devices.

### 1) Install prerequisites

Install:

- Android Studio
- Android NDK **r27+** (recommended: `27.3.13750724`)
- CMake >= 3.24
- vcpkg
- Ninja
- pkgconf

macOS example:

```bash
brew install cmake ninja pkgconf bison nasm
```

Set NDK path:

```bash
export ANDROID_NDK_HOME="$HOME/Library/Android/sdk/ndk/27.3.13750724"
```

### 2) Configure and build

```bash
git clone https://github.com/samsungplay/Thruflux.git
cd Thruflux

cmake -S . -B build_android -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
  -DCMAKE_C_FLAGS_RELEASE="-O3 -DNDEBUG" \
  -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG" \
  -DCMAKE_MAKE_PROGRAM="$(which ninja)" \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
  -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE="$ANDROID_NDK_HOME/build/cmake/android.toolchain.cmake" \
  -DVCPKG_TARGET_TRIPLET=arm64-android \
  -DANDROID_ABI=arm64-v8a \
  -DANDROID_PLATFORM=android-28 \
  -DPKG_CONFIG_EXECUTABLE="$(which pkgconf)"

cmake --build build_android --parallel
```

### 3) Output binary

```bash
./build_android/thru
```

Verify architecture:

```bash
file build_android/thru
```

Expected:

```txt
ELF 64-bit LSB pie executable, ARM aarch64
```


## Command reference

### `thru server` (signaling server)

```
thru server [--port N] [--max-sessions N] [--max-receivers-per-sender N] \
            [--max-message-bytes N] [--max-ws-connections N] \
            [--ws-connections-per-min N] [--ws-connections-burst N] \
            [--ws-messages-per-sec N] [--ws-messages-burst N] \
            [--ws-idle-timeout D] [--session-timeout D] \
            [--turn-server URL --turn-static-auth-secret S --turn-static-cred-ttl N]
```

| Option                       | Default | Description                                       |
| ---------------------------- | ------- | ------------------------------------------------- |
| `--version`                  | —       | Print version         |
| `--help`                  | —       | Print Help        |
| `--port`                     | `8080`  | Port to run the server on                         |
| `--max-sessions`             | `1000`  | Max number of concurrent transfer sessions        |
| `--max-receivers-per-sender` | `10`    | Max receivers allowed per sender in a session     |
| `--max-message-bytes`        | `65536` | Max websocket message size (bytes)                |
| `--max-ws-connections`       | `2000`  | Max concurrent websocket connections              |
| `--ws-idle-timeout`          | `600`   | Websocket idle timeout (seconds)                  |
| `--session-timeout`          | `86400` | Transfer session lifetime (seconds)               |
| `--ws-connections-per-min`   | `30`    | New websocket connections per minute |
| `--ws-connections-burst`     | `10`    | Burst capacity for connections                    |
| `--ws-messages-per-sec`      | `50`    | Websocket messages per second        |
| `--ws-messages-burst`        | `100`   | Burst capacity for messages                       |
| `--turn-server`              | —       | TURN server URL (`turn://`)         |
| `--turn-static-auth-secret`  | —       | TURN static auth secret                           |
| `--turn-static-cred-ttl`     | `600`   | TURN REST credentials TTL (seconds)               |


### `thru host` (sender)

```
thru host PATH [PATH ...] \
          [--server-url URL] [--max-receivers N] \
          [--stun-server URL] [--turn-server URL] [--force-turn] \
          [--quic-stream-window-bytes N] [--quic-conn-window-bytes N] \
          [--quic-max-streams N] [--total-streams N] \
          [--udp-buffer-bytes N] [--custom-join-code CODE]
```

| Option                       | Default                          | Description                                    |
|------------------------------|----------------------------------| ---------------------------------------------- |
| `PATH`                       | —                                | File(s) or directory(ies) to transfer (required) |
| `--version`                  | —                                | Print version              |
| `--help`                     | —                                | Print Help |
| `--server-url`               | `wss://bytepipe.app/ws`          | WebSocket URL of signaling server (`ws://` or `wss://`) |
| `--max-receivers`            | `10`                             | Max concurrent receivers                       |
| `--stun-server`              | `stun://stun.cloudflare.com:3478` | STUN server URL (`stun://`)                    |
| `--turn-server`              | —                                | TURN server URL (`turn://user:pass@host:port`) |
| `--force-turn`               | `false`                          | Force TURN relay                               |
| `--quic-stream-window-bytes` | `33554432`                       | Initial QUIC stream flow-control window (bytes) |
| `--quic-conn-window-bytes`   | `268435456`                      | Initial QUIC connection flow-control window (bytes) |
| `--udp-buffer-bytes`         | `8388608`                        | UDP socket buffer size (bytes). Must be raised on your OS as well. Default installer raises max to 16 MiB                |
| `--custom-join-code`         | —                                | Option to specify your own custom join code (use at your own risk)    |


### `thru join` (receiver)
```
thru join JOIN_CODE \
         [--out DIR] [--server-url URL] \
         [--stun-server URL] [--turn-server URL] [--force-turn] \
         [--quic-conn-window-bytes N] [--quic-stream-window-bytes N] \
         [--quic-max-streams N] [--total-streams N] \
         [--overwrite] [--udp-buffer-bytes N]
```

| Option                       | Default                           | Description                                             |
| ---------------------------- | --------------------------------- | ------------------------------------------------------- |
| `JOIN_CODE`                  | —                                 | Join code for the transfer (required)                   |
| `--version`                  | —                                 | Print version                       |
| `--help`                  | —       | Print Help        |
| `--out`                      | `.`                               | Output directory                                        |
| `--server-url`               | `wss://bytepipe.app/ws`           | WebSocket URL of signaling server (`ws://` or `wss://`) |
| `--stun-server`              | `stun://stun.cloudflare.com:3478` | STUN server URL (`stun://`)                             |
| `--turn-server`              | —                                 | TURN server URL (`turn://user:pass@host:port`)          |
| `--force-turn`               | `false`                           | Force TURN relay                                        |
| `--quic-conn-window-bytes`   | `268435456`                       | Initial QUIC connection flow-control window (bytes)     |
| `--quic-stream-window-bytes` | `33554432`                        | Initial QUIC stream flow-control window (bytes)         |
| `--overwrite`                | `false`                           | Overwrite existing files (disable resume)               |
| `--udp-buffer-bytes`         | `8388608`                         | UDP socket buffer size (bytes). Must be raised on your OS as well. Default installer raises max to 16 MiB                   |

### `thru ui` (local web interface)
```
thru ui [--port N] [--ui-heartbeat-port N]
```

| Option | Default | Description |
| ---- | ---- | ---- |
| `--port` | 0 | Port to open the local webinterface at. Value of 0 delegates to the OS to assign a random port. |
| `--ui-heartbeat-port` | -1 | Port to periodically check the life of the attached UI app. The UI must expose a `/health` endpoint returning HTTP 200. Auto-kills the process if the UI app dies to prevent orphans. Set to -1 to disable. |

Got it — you want a **clean Markdown section** for a README, but **not** one giant fenced “raw markdown” block. Here it is as normal README Markdown (with code fences only for JSON examples).

---

## Local Web Interface HTTP REST API

### `POST /receive`

Starts receiving files.

**Example request payload**

```json
{
  "joinCode": "ALPHA-BRAVO-123",
  "out": "./downloads",
  "serverUrl": "wss://bytepipe.app/ws",
  "stunServers": "stun://stun.cloudflare.com:3478",
  "turnServers": "",
  "forceTurn": false,
  "quicConnWindowBytes": 268435456,
  "quicStreamWindowBytes": 33554432,
  "overwrite": true,
  "udpBufferBytes": 8388608
}
```

**Status codes**

* **200**: Receiver logic has begun. This only confirms startup — the frontend should listen on **`/events`** for ongoing progress.
* **503**: Engine is busy (already sending or receiving).
* **500**: Internal server error (response includes key: `error`).
* **400**: Invalid config/payload (response includes key: `error`).

---

### `POST /host`

Starts hosting files (supports multiple receivers).

**Example request payload**

```json
{
  "paths": ["/home/user/photos", "/var/data/logs"],
  "serverUrl": "wss://bytepipe.app/ws",
  "maxReceivers": 10,
  "stunServer": "stun://stun.cloudflare.com:3478",
  "turnServers": "user:pass@turn.example.com",
  "forceTurn": false,
  "quicStreamWindowBytes": 33554432,
  "quicConnWindowBytes": 268435456,
  "udpBufferBytes": 8388608
}
```

**Status codes**

* **200**: Sender logic has begun. This only confirms startup — the frontend should listen on **`/events`** for ongoing progress.
* **503**: Engine is busy (already sending or receiving).
* **500**: Internal server error (response includes key: `error`).
* **400**: Invalid config/payload (response includes key: `error`).

---

### `POST /abort`

Aborts whatever process is ongoing (sending or receiving).

**Status codes**

* **200**: Aborted successfully. Engine is free to start a new receive/host task.
* **504**: Abort was attempted, but the process did not end within 10s+ (possible deadlock).

---

### `POST /abortReceiver`

Aborts a single receiver from the sender side.

**Example request payload**

```json
{
  "receiverId": "eadjaeoid"
}
```

**Status codes**

* **200**: Abort handled by the engine.
* **503**: No sender session is currently running (endpoint not applicable).
* **504**: Abort attempted, but was not handled by the engine for 10s+.
* **400**: Invalid payload.

---

## Event Stream

### `GET /events` (SSE)

A continuous **Server-Sent Events (SSE)** endpoint.

All events are formatted as:

```json
{
  "type": "type",
  "message": ""
}
```

Events are organized by the `type` field below.

---

## Common event types

* **`connecting`**: Ongoing connection attempt to signaling server.
  `message`: empty

* **`connect_error`**: Failed connection attempt to signaling server.
  `message`: `{ "code": ..., "reason": ... }`

* **`connect_success`**: Connected to signaling server successfully.
  `message`: empty

* **`disconnected`**: Disconnected from signaling server.
  `message`: `{ "reason": ... }`

* **`progress`**: File transfer progress event.
  `message` fields:

  * `receiverId` (empty for receiver)
  * `ewmaThroughput`
  * `bytesMoved`
  * `skippedBytes`
  * `filesMoved`
  * `totalExpectedFilesCount`
  * `isRelayed`
  * `percent` (integer 0–100; **100 signals completion**)
  * `hasError` (boolean; **true signals transfer aborted with error**)

---

## Sender-only event types

* **`join_code_issued`**: Join code issued by server.
  `message`: `{ "join_code": ... }`

* **`manifest_build_start`**: Manifest scanning has begun.
  `message`: empty

* **`manifest_build_progress`**: Manifest build progress.
  `message`: `{ "files_count": ..., "total_size": ... }` (bytes)

* **`manifest_encoding`**: Encoding manifest into blob has begun.
  `message`: empty

* **`manifest_sealed`**: Manifest build/encode fully completed.
  `message`: empty

---

## Receiver-only event types

* **`p2p_failed`**: P2P negotiation failed.
  `message`: empty

* **`joining_session`**: Attempting to join the session.
  `message`: empty

* **`p2p_start`**: Join code verified; P2P negotiation started.
  `message`: empty

* **`p2p_success`**: P2P negotiation succeeded.
  `message`: empty

* **`manifest_receive_progress`**: Manifest receive progress.
  `message`: `{ "total_size": ..., "complete": ... }` (`total_size` in bytes)

* **`manifest_receive_error`**: Unexpected error receiving manifest.
  `message`: `{ "errno": ... }`

* **`quic_handshake_success`**: QUIC handshake successful (after `p2p_success`).
  `message`: empty

* **`ice_not_ready`**: ICE not ready for QUIC connection (unexpected).
  `message`: empty

* **`resume_notice`**: Session is resuming from previous progress.
  `message`: `{ "percent": ... }` (double 0–100)

* **`manifest_parsing`**: Manifest is being parsed.
  `message`: empty

* **`manifest_unsealed`**: Manifest fully processed and parsed.
  `message`: `{ "files_count": ..., "total_size": ... }` (bytes)

* **`receive_complete`**: Receiving completed.
  `message`: empty

## Self‑hosting guide (Ubuntu) 🐧

1. **Build or download the binary (Check previous section for detailed manual build guide)**

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

4. **Run `thruserver` as a systemd service (Example path : /etc/systemd/system/thruserver.service)**

   ```
   [Unit]
   Description=Thruflux Server
   After=network-online.target
   Wants=network-online.target

   [Service]
   Type=simple
   User=your_username
   Group=your_group
   WorkingDirectory=/home/thruflux/Thruflux
   ExecStart=/home/thruflux/Thruflux-C-/build/thru server
   Restart=always
   RestartSec=2

   NoNewPrivileges=true
   PrivateTmp=true
   ProtectSystem=full
   ProtectHome=false

   LimitNOFILE=65536

   StandardOutput=journal
   StandardError=journal

   [Install]
   WantedBy=multi-user.target
   ```

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now thruserv
   ```

5. **Point clients to your server**
   - Host: `thru host … --server-url wss://your.domain/ws`
   - Join: `thru join ABCDEFGH --server-url wss://your.domain/ws`

6. **(Optional) Enable built-in, auto-provisioned TURN relay support using coturn REST credentials**

   This step enables automatic TURN fallback so clients can still connect when direct peer-to-peer UDP paths fail without specifying --turn-server flag by themselves (e.g. strict NATs, firewalls).

   - Configure **coturn** with `use-auth-secret` and the same `static-auth-secret` that will be shared with `thru server`.
   - Example coturn server config (for more info, check out the [coturn](https://github.com/coturn/coturn) repository) :
   ```
   # ---- Identity ----
   realm=yourdomain.com
   server-name=yourdomain.com

   # ---- Auth: TURN REST (static secret) ----
   lt-cred-mech
   use-auth-secret
   static-auth-secret=some_strong_secret

   # ---- UDP only ----
   listening-port=3478
   no-tcp
   no-tls
   no-dtls

   # ---- Relay port range (match firewall) ----
   min-port=49152
   max-port=65535

   # ---- Safety hardening ----
   fingerprint
   no-loopback-peers
   no-multicast-peers

   # Limit how many simultaneous allocations a single username can create.
   user-quota=4

   # Global cap for allocations across all users.
   total-quota=2000

   # ---- Logging ----
   log-file=/var/log/turnserver/turnserver.log
   simple-log

   ```
   - Start `thru server` with TURN options enabled. This allows it to **mint time-limited TURN credentials** and distribute them to clients automagically:
     ```
     thru server \
       --turn-server turn://yourdomain.com:3478 \
       --turn-static-auth-secret <your-static-auth-secret> \
       --turn-cred-ttl 600
     ```
   - Here's the magic: Clients do **not** need to specify `--turn-server` manually unless you want to override the TURN server provided by `thru server`.


## TURN usage

Thruflux performs manual hole‑punching first and only falls back to TURN relay when needed.

Examples:

```bash
# TURN over UDP (UDP mandatory for QUIC; TCP won't work.)
thru host ./data --turn-server "turn://user:pass@turn.example.com:3478"
thru join ABCDEFGH --turn-server "turn://user:pass@turn.example.com:3478"

# TURN over TLS (TURNS). Useful on restrictive networks.
# Unfortunately, Thruflux does not support this yet!

```

Notes:
- If the signaling server is configured to provide TURN access via time-limited REST credentials (via --turn-server and --turn-static-auth-secret), clients do not need to specify a TURN server

## Contributing 🤝

Thruflux is released under the MIT License and is fully open source. Contributions, testing, and issue reports are always appreciated. It’s an ambitious project and currently in beta, so expect occasional bugs here and there. But the goal is to make a battle-tested, robust, and above all - the fastest CLI tool that makes file transfers simpler and smoother for everyone.

May TURN not be needed for you!
