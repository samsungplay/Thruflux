package config

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"os"
	"strings"
	"time"
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
	TargetPeer       string
	SessionOnly      bool
	Paths            []string // Paths to scan (sender only, default ["."])
	ICETest          bool     // Enable ICE connectivity test mode
	QUICTest         bool     // Enable QUIC connectivity test mode
	QUICTransferTest bool     // Enable QUIC transfer test mode
	ChunkSize        uint32   // Chunk size in bytes for file transfer (default: 4 MiB)
	MultiStream      bool     // Use multi-stream QUIC transfers (control + per-file data streams)
	ParallelFiles    int      // Max concurrent file transfers (1..8)
	SmallThreshold   int64    // Bytes threshold for small files
	MediumThreshold  int64    // Bytes threshold for medium files
	SmallSlotFrac    float64  // Fraction of slots reserved for small files
	AgingAfter       time.Duration
	Resume           bool
	ResumeTimeout    time.Duration
	ResumeVerify     string
	HashAlg          string
	Destination      string
	ResumeVerifyTail uint32
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
		ServerURL:        "http://localhost:8080",
		LogLevel:         "info",
		PeerID:           generatePeerID(),
		JoinCode:         "",
		TargetPeer:       "",
		SessionOnly:      false,
		Paths:            []string{"."},
		MultiStream:      true,
		ParallelFiles:    8,
		SmallThreshold:   4 * 1024 * 1024,
		MediumThreshold:  64 * 1024 * 1024,
		SmallSlotFrac:    0.25,
		AgingAfter:       5 * time.Second,
		Resume:           true,
		ResumeTimeout:    10 * time.Second,
		ResumeVerify:     "last",
		HashAlg:          "crc32c",
		Destination:      "",
		ResumeVerifyTail: 1,
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
	fs.StringVar(&cfg.TargetPeer, "target-peer", cfg.TargetPeer, "send to a specific receiver peer_id (sender only)")
	fs.BoolVar(&cfg.SessionOnly, "session-only", cfg.SessionOnly, "create session and wait for receivers without sending")
	fs.BoolVar(&cfg.ICETest, "ice-test", false, "enable ICE connectivity test mode")
	fs.BoolVar(&cfg.QUICTest, "quic-test", false, "enable QUIC connectivity test mode")
	fs.BoolVar(&cfg.QUICTransferTest, "quic-transfer-test", false, "enable QUIC transfer test mode")
	fs.BoolVar(&cfg.MultiStream, "multistream", cfg.MultiStream, "use multi-stream QUIC transfer (control + per-file data streams)")
	fs.IntVar(&cfg.ParallelFiles, "parallel-files", cfg.ParallelFiles, "max concurrent file transfers (1..8)")
	fs.Float64Var(&cfg.SmallSlotFrac, "small-slot-frac", cfg.SmallSlotFrac, "fraction of slots reserved for small files")
	fs.DurationVar(&cfg.AgingAfter, "aging-after", cfg.AgingAfter, "duration before aging boosts a file")
	fs.BoolVar(&cfg.Resume, "resume", cfg.Resume, "enable resume for QUIC transfers")
	fs.DurationVar(&cfg.ResumeTimeout, "resume-timeout", cfg.ResumeTimeout, "resume response timeout")
	fs.StringVar(&cfg.ResumeVerify, "resume-verify", cfg.ResumeVerify, "resume verification mode (last|none|all)")
	fs.StringVar(&cfg.HashAlg, "hash-alg", cfg.HashAlg, "resume hash algorithm (crc32c|xxhash64|none)")
	fs.StringVar(&cfg.Destination, "destination", cfg.Destination, "receiver output directory for QUIC transfer test")

	// ChunkSize flag - use uint64 and convert
	var chunkSizeUint64 uint64
	fs.Uint64Var(&chunkSizeUint64, "chunk-size", 0, "chunk size in bytes for file transfer (default: 4 MiB)")
	var smallThresholdUint64 uint64
	fs.Uint64Var(&smallThresholdUint64, "small-threshold", uint64(cfg.SmallThreshold), "bytes threshold for small files")
	var mediumThresholdUint64 uint64
	fs.Uint64Var(&mediumThresholdUint64, "medium-threshold", uint64(cfg.MediumThreshold), "bytes threshold for medium files")

	// Handle repeatable --path flag
	paths := make([]string, 0)
	fs.Var((*stringSlice)(&paths), "path", "path to scan (sender only, repeatable)")

	fs.Parse(args)

	// Convert chunk size after parsing
	cfg.ChunkSize = uint32(chunkSizeUint64)
	if smallThresholdUint64 > 0 {
		cfg.SmallThreshold = int64(smallThresholdUint64)
	}
	if mediumThresholdUint64 > 0 {
		cfg.MediumThreshold = int64(mediumThresholdUint64)
	}
	if cfg.SmallSlotFrac <= 0 || cfg.SmallSlotFrac > 1 {
		cfg.SmallSlotFrac = 0.25
	}
	if cfg.AgingAfter <= 0 {
		cfg.AgingAfter = 5 * time.Second
	}
	if cfg.ResumeTimeout <= 0 {
		cfg.ResumeTimeout = 10 * time.Second
	}
	switch cfg.ResumeVerify {
	case "", "last", "none", "all":
	default:
		cfg.ResumeVerify = "last"
	}
	if cfg.HashAlg == "" {
		cfg.HashAlg = "crc32c"
	}
	cfg.ResumeVerifyTail = 1

	// If paths were provided, use them; otherwise keep default ["."]
	if len(paths) > 0 {
		cfg.Paths = paths
	}

	if cfg.ParallelFiles < 1 {
		cfg.ParallelFiles = 1
	}
	if cfg.ParallelFiles > 8 {
		cfg.ParallelFiles = 8
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
