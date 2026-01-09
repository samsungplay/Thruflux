# SheerBytes

Speed-first P2P file transfer application. The server component serves as a signaling-only service.

## Building

Build all binaries:

```bash
go build ./...
```

## Running

### Server

Run the server:

```bash
go run ./cmd/sheerbytes-server
```

The server will start on `:8080` by default and exposes the following endpoints:

#### Health Check

```bash
curl http://localhost:8080/health
# Output: {"ok":true}
```

#### Create Session

Create a new signaling session:

```bash
curl -X POST http://localhost:8080/session
```

Sample response:

```json
{
  "session_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "join_code": "ABCDEFGH",
  "expires_at": "2024-01-01T12:30:00Z"
}
```

Sessions expire after 30 minutes and are automatically cleaned up every minute.

#### WebSocket Join

Join a session via WebSocket:

```bash
wscat -c "ws://localhost:8080/ws?join_code=ABCDEFGH&peer_id=peer123&role=sender"
```

Query parameters:
- `join_code` (required): The join code from the session creation response
- `peer_id` (required): Unique identifier for this peer
- `role` (required): Either `"sender"` or `"receiver"`

On successful connection:
- You'll receive a `peer_list` message with all current peers in the session
- All peers in the session will receive a `peer_joined` notification
- When you disconnect, all peers will receive a `peer_left` notification

Example using wscat:

```bash
# Terminal 1: Create a session
curl -X POST http://localhost:8080/session
# Response: {"session_id":"...","join_code":"ABCDEFGH","expires_at":"..."}

# Terminal 2: Join as sender
wscat -c "ws://localhost:8080/ws?join_code=ABCDEFGH&peer_id=sender1&role=sender"

# Terminal 3: Join as receiver
wscat -c "ws://localhost:8080/ws?join_code=ABCDEFGH&peer_id=receiver1&role=receiver"
```

#### Message Routing

Once connected via WebSocket, you can send messages as JSON envelopes. The server routes messages based on the `To` field:

- **Targeted send**: If `To` is set to a peer_id, the message is sent only to that peer
- **Broadcast**: If `To` is empty, the message is broadcast to all peers in the session except the sender

Example: Sending an offer message (targeted):

```json
{
  "v": 1,
  "type": "offer",
  "msg_id": "msg1234567890abcd",
  "session_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "from": "sender1",
  "to": "receiver1",
  "payload": {
    "sdp": "v=0\r\no=- 123456789 123456789 IN IP4 127.0.0.1\r\n..."
  }
}
```

Example: Broadcasting a message to all peers:

```json
{
  "v": 1,
  "type": "ice_candidate",
  "msg_id": "msg0987654321dcba",
  "session_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "from": "sender1",
  "payload": {
    "candidate": "candidate:1 1 UDP 2130706431 192.168.1.100 54321 typ host"
  }
}
```

**Note**: The server automatically:
- Sets `From` to match the `peer_id` query parameter
- Sets `SessionID` if missing
- Validates message size (max 64KB)
- Rejects invalid envelopes
- Returns an error if target peer is not found (for targeted sends)

### Sender

Run the sender to create a session and connect:

```bash
go run ./cmd/sheerbytes-sender --server-url http://localhost:8080 --path ./someFolder
```

The sender will:
1. Create a session via HTTP POST
2. Display the join code prominently to stdout (e.g., `=== Join Code: ABCDEFGH ===`)
3. Connect via WebSocket
4. Scan the specified path (default: current directory) and create a manifest
5. Broadcast manifest offer to all peers in the session
6. Log peer presence events (peer_list, peer_joined, peer_left)
7. Log manifest acceptances from receivers

Example output:

```
=== Join Code: ABCDEFGH ===

```

The sender logs all peer events in JSON format to stderr (structured logging), while the join code is printed to stdout for easy copying.

Use Ctrl+C to gracefully disconnect.

### Receiver

Run the receiver to join an existing session:

```bash
go run ./cmd/sheerbytes-receiver --server-url http://localhost:8080 --join-code ABCDEFGH --peer-id bob
```

Or omit the join code to be prompted interactively:

```bash
go run ./cmd/sheerbytes-receiver --server-url http://localhost:8080 --peer-id bob
# Will prompt: "Enter join code: "
```

The receiver will:
1. Connect to the server via WebSocket using the join code
2. Log peer presence events (peer_list, peer_joined, peer_left)
3. Automatically accept any manifest offers with Mode="all"

**Note**: The join code is printed to stdout by the sender when it creates a session (e.g., `=== Join Code: ABCDEFGH ===`). Copy this code and use it with the receiver.

**Manifest Negotiation**: When a receiver receives a manifest offer, it automatically responds with an accept message (Mode="all"). The sender logs all acceptances and tracks accepted receivers.

Use Ctrl+C to gracefully disconnect.

## Configuration

Configuration can be set via command-line flags or environment variables. Flags take precedence over environment variables.

### Server Configuration

Flags:
- `--addr`: server address (default: `:8080`)
- `--log-level`: log level - debug, info, warn, error (default: `info`)

Environment variables:
- `SHEERBYTES_ADDR`: server address
- `SHEERBYTES_LOG_LEVEL`: log level

Example:

```bash
# Using flags
go run ./cmd/sheerbytes-server --addr :9090 --log-level debug

# Using environment variables
SHEERBYTES_ADDR=:9090 SHEERBYTES_LOG_LEVEL=debug go run ./cmd/sheerbytes-server
```

### Client Configuration (Sender/Receiver)

Flags:
- `--server-url`: server URL (default: `http://localhost:8080`)
- `--log-level`: log level - debug, info, warn, error (default: `info`)
- `--peer-id`: peer identifier (default: random 10-character hex string)
- `--join-code`: session join code (receiver only, optional - prompts if empty)
- `--path`: path to scan and send (sender only, default: current directory)

Environment variables:
- `SHEERBYTES_SERVER_URL`: server URL
- `SHEERBYTES_LOG_LEVEL`: log level
- `SHEERBYTES_PEER_ID`: peer identifier
- `SHEERBYTES_JOIN_CODE`: session join code (receiver only)

Example:

```bash
# Sender - scan a folder
go run ./cmd/sheerbytes-sender --server-url http://localhost:9090 --peer-id mypeer123 --path ./myFolder

# Sender - using environment variables
SHEERBYTES_SERVER_URL=http://localhost:9090 SHEERBYTES_PEER_ID=mypeer123 go run ./cmd/sheerbytes-sender --path ./myFolder

# Receiver - with join code
go run ./cmd/sheerbytes-receiver --server-url http://localhost:9090 --join-code ABCDEFGH --peer-id bob

# Receiver - with environment variable
SHEERBYTES_SERVER_URL=http://localhost:9090 SHEERBYTES_JOIN_CODE=ABCDEFGH SHEERBYTES_PEER_ID=bob go run ./cmd/sheerbytes-receiver
```

#### Example Workflow

1. **Start the server:**
   ```bash
   go run ./cmd/sheerbytes-server
   ```

2. **Start the sender** (in Terminal 1):
   ```bash
   go run ./cmd/sheerbytes-sender --server-url http://localhost:8080 --path ./someFolder
   ```
   Output shows: `=== Join Code: ABCDEFGH ===`

3. **Start the receiver** (in Terminal 2):
   ```bash
   go run ./cmd/sheerbytes-receiver --server-url http://localhost:8080 --join-code ABCDEFGH --peer-id bob
   ```

4. The receiver will:
   - Connect to the session
   - Receive the manifest offer from the sender
   - Automatically accept it (Mode="all")
   - The sender logs the acceptance

Both sender and receiver will log peer presence events as peers join and leave the session.

