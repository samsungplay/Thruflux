package config

import (
	"flag"
	"os"
	"testing"
)

func TestParseServerConfig_Defaults(t *testing.T) {
	os.Clearenv()

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseServerConfigWithFlagSet(fs, []string{})

	if cfg.Addr != ":8080" {
		t.Errorf("expected Addr to be :8080, got %s", cfg.Addr)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected LogLevel to be info, got %s", cfg.LogLevel)
	}
}

func TestParseServerConfig_Flags(t *testing.T) {
	os.Clearenv()

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseServerConfigWithFlagSet(fs, []string{"-addr", ":9090", "-log-level", "debug"})

	if cfg.Addr != ":9090" {
		t.Errorf("expected Addr to be :9090, got %s", cfg.Addr)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel to be debug, got %s", cfg.LogLevel)
	}
}

func TestParseServerConfig_EnvFallback(t *testing.T) {
	os.Clearenv()

	os.Setenv("SHEERBYTES_ADDR", ":7070")
	os.Setenv("SHEERBYTES_LOG_LEVEL", "warn")
	defer os.Unsetenv("SHEERBYTES_ADDR")
	defer os.Unsetenv("SHEERBYTES_LOG_LEVEL")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseServerConfigWithFlagSet(fs, []string{})

	if cfg.Addr != ":7070" {
		t.Errorf("expected Addr to be :7070, got %s", cfg.Addr)
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("expected LogLevel to be warn, got %s", cfg.LogLevel)
	}
}

func TestParseServerConfig_FlagsOverrideEnv(t *testing.T) {
	os.Clearenv()

	os.Setenv("SHEERBYTES_ADDR", ":7070")
	os.Setenv("SHEERBYTES_LOG_LEVEL", "warn")
	defer os.Unsetenv("SHEERBYTES_ADDR")
	defer os.Unsetenv("SHEERBYTES_LOG_LEVEL")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseServerConfigWithFlagSet(fs, []string{"-addr", ":9090", "-log-level", "error"})

	// Flags should override env
	if cfg.Addr != ":9090" {
		t.Errorf("expected Addr to be :9090 (from flag), got %s", cfg.Addr)
	}
	if cfg.LogLevel != "error" {
		t.Errorf("expected LogLevel to be error (from flag), got %s", cfg.LogLevel)
	}
}

func TestParseClientConfig_Defaults(t *testing.T) {
	os.Clearenv()

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseClientConfigWithFlagSet(fs, []string{})

	if cfg.ServerURL != "http://localhost:8080" {
		t.Errorf("expected ServerURL to be http://localhost:8080, got %s", cfg.ServerURL)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected LogLevel to be info, got %s", cfg.LogLevel)
	}
	if cfg.PeerID == "" || len(cfg.PeerID) != 10 {
		t.Errorf("expected PeerID to be 10 hex characters, got %s (len=%d)", cfg.PeerID, len(cfg.PeerID))
	}
}

func TestParseClientConfig_Flags(t *testing.T) {
	os.Clearenv()

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseClientConfigWithFlagSet(fs, []string{"-server-url", "http://example.com:9090", "-log-level", "debug", "-peer-id", "abc123def4"})

	if cfg.ServerURL != "http://example.com:9090" {
		t.Errorf("expected ServerURL to be http://example.com:9090, got %s", cfg.ServerURL)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel to be debug, got %s", cfg.LogLevel)
	}
	if cfg.PeerID != "abc123def4" {
		t.Errorf("expected PeerID to be abc123def4, got %s", cfg.PeerID)
	}
}

func TestParseClientConfig_EnvFallback(t *testing.T) {
	os.Clearenv()

	os.Setenv("SHEERBYTES_SERVER_URL", "http://env.example.com:7070")
	os.Setenv("SHEERBYTES_LOG_LEVEL", "warn")
	os.Setenv("SHEERBYTES_PEER_ID", "envpeer123")
	defer os.Unsetenv("SHEERBYTES_SERVER_URL")
	defer os.Unsetenv("SHEERBYTES_LOG_LEVEL")
	defer os.Unsetenv("SHEERBYTES_PEER_ID")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseClientConfigWithFlagSet(fs, []string{})

	if cfg.ServerURL != "http://env.example.com:7070" {
		t.Errorf("expected ServerURL to be http://env.example.com:7070, got %s", cfg.ServerURL)
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("expected LogLevel to be warn, got %s", cfg.LogLevel)
	}
	if cfg.PeerID != "envpeer123" {
		t.Errorf("expected PeerID to be envpeer123, got %s", cfg.PeerID)
	}
}

func TestParseClientConfig_FlagsOverrideEnv(t *testing.T) {
	os.Clearenv()

	os.Setenv("SHEERBYTES_SERVER_URL", "http://env.example.com:7070")
	os.Setenv("SHEERBYTES_LOG_LEVEL", "warn")
	os.Setenv("SHEERBYTES_PEER_ID", "envpeer123")
	os.Setenv("SHEERBYTES_JOIN_CODE", "ENVCODE")
	defer os.Unsetenv("SHEERBYTES_SERVER_URL")
	defer os.Unsetenv("SHEERBYTES_LOG_LEVEL")
	defer os.Unsetenv("SHEERBYTES_PEER_ID")
	defer os.Unsetenv("SHEERBYTES_JOIN_CODE")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseClientConfigWithFlagSet(fs, []string{"-server-url", "http://flag.example.com:9090", "-log-level", "error", "-peer-id", "flagpeer456", "-join-code", "FLAGCODE"})

	// Flags should override env
	if cfg.ServerURL != "http://flag.example.com:9090" {
		t.Errorf("expected ServerURL to be http://flag.example.com:9090 (from flag), got %s", cfg.ServerURL)
	}
	if cfg.LogLevel != "error" {
		t.Errorf("expected LogLevel to be error (from flag), got %s", cfg.LogLevel)
	}
	if cfg.PeerID != "flagpeer456" {
		t.Errorf("expected PeerID to be flagpeer456 (from flag), got %s", cfg.PeerID)
	}
	if cfg.JoinCode != "FLAGCODE" {
		t.Errorf("expected JoinCode to be FLAGCODE (from flag), got %s", cfg.JoinCode)
	}
}

func TestParseClientConfig_JoinCode_Flag(t *testing.T) {
	os.Clearenv()

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseClientConfigWithFlagSet(fs, []string{"-join-code", "ABCDEFGH"})

	if cfg.JoinCode != "ABCDEFGH" {
		t.Errorf("expected JoinCode to be ABCDEFGH, got %s", cfg.JoinCode)
	}
}

func TestParseClientConfig_JoinCode_Env(t *testing.T) {
	os.Clearenv()

	os.Setenv("SHEERBYTES_JOIN_CODE", "XYZ12345")
	defer os.Unsetenv("SHEERBYTES_JOIN_CODE")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseClientConfigWithFlagSet(fs, []string{})

	if cfg.JoinCode != "XYZ12345" {
		t.Errorf("expected JoinCode to be XYZ12345, got %s", cfg.JoinCode)
	}
}

func TestParseClientConfig_JoinCode_Default(t *testing.T) {
	os.Clearenv()

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseClientConfigWithFlagSet(fs, []string{})

	if cfg.JoinCode != "" {
		t.Errorf("expected JoinCode to be empty by default, got %s", cfg.JoinCode)
	}
}

func TestParseClientConfig_JoinCode_FlagOverridesEnv(t *testing.T) {
	os.Clearenv()

	os.Setenv("SHEERBYTES_JOIN_CODE", "ENVCODE")
	defer os.Unsetenv("SHEERBYTES_JOIN_CODE")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseClientConfigWithFlagSet(fs, []string{"-join-code", "FLAGCODE"})

	if cfg.JoinCode != "FLAGCODE" {
		t.Errorf("expected JoinCode to be FLAGCODE (from flag), got %s", cfg.JoinCode)
	}
}

