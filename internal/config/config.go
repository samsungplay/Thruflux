package config

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"os"
	"strings"
)

// ServerConfig holds configuration for the server binary.
type ServerConfig struct {
	Addr     string
	LogLevel string
}

// ClientConfig holds configuration for client binaries (sender/receiver).
type ClientConfig struct {
	ServerURL        string
	LogLevel         string
	PeerID           string
	JoinCode         string
	Paths            []string // Paths to scan (sender only, default ["."])
	ICETest          bool     // Enable ICE connectivity test mode
	QUICTest         bool     // Enable QUIC connectivity test mode
	QUICTransferTest bool     // Enable QUIC transfer test mode
	ChunkSize        uint32   // Chunk size in bytes for file transfer (default: 4 MiB)
	WindowSize       uint32   // Window size in chunks for file transfer (default: 16)
	ReadAhead        uint32   // Read-ahead depth in chunks (default: window+4, min 1, max 256)
	MultiStream      bool     // Use multi-stream QUIC transfers (control + per-file data streams)
	ParallelFiles    int      // Max concurrent file transfers (1..32)
}

// ParseServerConfig parses server configuration from flags and environment variables.
// Flags take precedence over environment variables.
// Defaults: addr=":8080", logLevel="info"
func ParseServerConfig() ServerConfig {
	return parseServerConfigWithFlagSet(flag.CommandLine, os.Args[1:])
}

// parseServerConfigWithFlagSet is an internal helper for testing with isolated flag sets.
func parseServerConfigWithFlagSet(fs *flag.FlagSet, args []string) ServerConfig {
	cfg := ServerConfig{
		Addr:     ":8080",
		LogLevel: "info",
	}

	// Read from environment first
	if addr := os.Getenv("SHEERBYTES_ADDR"); addr != "" {
		cfg.Addr = addr
	}
	if logLevel := os.Getenv("SHEERBYTES_LOG_LEVEL"); logLevel != "" {
		cfg.LogLevel = logLevel
	}

	// Flags override environment
	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "server address")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level (debug, info, warn, error)")
	fs.Parse(args)

	return cfg
}

// ParseClientConfig parses client configuration from flags and environment variables.
// Flags take precedence over environment variables.
// Defaults: serverURL="http://localhost:8080", logLevel="info", peerID=random
func ParseClientConfig(appName string) ClientConfig {
	return parseClientConfigWithFlagSet(flag.CommandLine, os.Args[1:])
}

// parseClientConfigWithFlagSet is an internal helper for testing with isolated flag sets.
func parseClientConfigWithFlagSet(fs *flag.FlagSet, args []string) ClientConfig {
	cfg := ClientConfig{
		ServerURL:     "http://localhost:8080",
		LogLevel:      "info",
		PeerID:        generatePeerID(),
		JoinCode:      "",
		Paths:         []string{"."},
		MultiStream:   true,
		ParallelFiles: 8,
	}

	// Read from environment first
	if serverURL := os.Getenv("SHEERBYTES_SERVER_URL"); serverURL != "" {
		cfg.ServerURL = serverURL
	}
	if logLevel := os.Getenv("SHEERBYTES_LOG_LEVEL"); logLevel != "" {
		cfg.LogLevel = logLevel
	}
	if peerID := os.Getenv("SHEERBYTES_PEER_ID"); peerID != "" {
		cfg.PeerID = peerID
	}
	if joinCode := os.Getenv("SHEERBYTES_JOIN_CODE"); joinCode != "" {
		cfg.JoinCode = joinCode
	}

	// Flags override environment
	fs.StringVar(&cfg.ServerURL, "server-url", cfg.ServerURL, "server URL")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level (debug, info, warn, error)")
	fs.StringVar(&cfg.PeerID, "peer-id", cfg.PeerID, "peer identifier")
	fs.StringVar(&cfg.JoinCode, "join-code", cfg.JoinCode, "session join code")
	fs.BoolVar(&cfg.ICETest, "ice-test", false, "enable ICE connectivity test mode")
	fs.BoolVar(&cfg.QUICTest, "quic-test", false, "enable QUIC connectivity test mode")
	fs.BoolVar(&cfg.QUICTransferTest, "quic-transfer-test", false, "enable QUIC transfer test mode")
	fs.BoolVar(&cfg.MultiStream, "multistream", cfg.MultiStream, "use multi-stream QUIC transfer (control + per-file data streams)")
	fs.IntVar(&cfg.ParallelFiles, "parallel-files", cfg.ParallelFiles, "max concurrent file transfers (1..32)")

	// ChunkSize flag - use uint64 and convert
	var chunkSizeUint64 uint64
	fs.Uint64Var(&chunkSizeUint64, "chunk-size", 0, "chunk size in bytes for file transfer (default: 4 MiB)")

	// WindowSize flag - use uint64 and convert
	var windowSizeUint64 uint64
	fs.Uint64Var(&windowSizeUint64, "window", 0, "window size in chunks for file transfer (default: 16)")

	// ReadAhead flag - use uint64 and convert
	var readAheadUint64 uint64
	fs.Uint64Var(&readAheadUint64, "read-ahead", 0, "read-ahead depth in chunks (default: window+4, min 1, max 256)")

	// Handle repeatable --path flag
	paths := make([]string, 0)
	fs.Var((*stringSlice)(&paths), "path", "path to scan (sender only, repeatable)")

	fs.Parse(args)

	// Convert chunk size and window size after parsing
	cfg.ChunkSize = uint32(chunkSizeUint64)
	cfg.WindowSize = uint32(windowSizeUint64)
	cfg.ReadAhead = uint32(readAheadUint64)

	// If paths were provided, use them; otherwise keep default ["."]
	if len(paths) > 0 {
		cfg.Paths = paths
	}

	if cfg.ParallelFiles < 1 {
		cfg.ParallelFiles = 1
	}
	if cfg.ParallelFiles > 32 {
		cfg.ParallelFiles = 32
	}

	return cfg
}

// generatePeerID generates a random 10-character hex string for peer identification.
func generatePeerID() string {
	b := make([]byte, 5) // 5 bytes = 10 hex characters
	if _, err := rand.Read(b); err != nil {
		// Fallback: use a simple counter-based approach if rand fails
		// This should be extremely rare
		return "0000000000"
	}
	return hex.EncodeToString(b)
}

// stringSlice implements flag.Value for repeatable string flags.
type stringSlice []string

func (s *stringSlice) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func (s *stringSlice) Get() interface{} {
	return []string(*s)
}

func (s *stringSlice) IsBoolFlag() bool {
	return false
}

var _ flag.Value = (*stringSlice)(nil)
var _ flag.Getter = (*stringSlice)(nil)
