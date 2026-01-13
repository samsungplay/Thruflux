package config

import (
	"flag"
	"testing"
	"time"
)

func TestParseServerConfigDefaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseServerConfigWithFlagSet(fs, []string{})
	if cfg.Port != 8080 {
		t.Fatalf("expected default port 8080, got %d", cfg.Port)
	}
	if cfg.MaxSessions != 1000 {
		t.Fatalf("expected default max sessions 1000, got %d", cfg.MaxSessions)
	}
	if cfg.WSIdleTimeout != 10*time.Minute {
		t.Fatalf("expected default idle timeout 10m, got %s", cfg.WSIdleTimeout)
	}
}

func TestParseServerConfigFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseServerConfigWithFlagSet(fs, []string{"--port", "9090", "--max-sessions", "50", "--ws-idle-timeout", "2m"})
	if cfg.Port != 9090 {
		t.Fatalf("expected port 9090, got %d", cfg.Port)
	}
	if cfg.MaxSessions != 50 {
		t.Fatalf("expected max sessions 50, got %d", cfg.MaxSessions)
	}
	if cfg.WSIdleTimeout != 2*time.Minute {
		t.Fatalf("expected idle timeout 2m, got %s", cfg.WSIdleTimeout)
	}
}

func TestParseClientConfigDefaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseClientConfigWithFlagSet(fs, []string{})
	if cfg.ServerURL != "https://bytepipe.app" {
		t.Fatalf("expected default server url https://bytepipe.app, got %s", cfg.ServerURL)
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("expected default log level info, got %s", cfg.LogLevel)
	}
	if cfg.PeerID == "" || len(cfg.PeerID) != 10 {
		t.Fatalf("expected peer id to be 10 hex chars, got %q", cfg.PeerID)
	}
}

func TestParseClientConfigFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := parseClientConfigWithFlagSet(fs, []string{
		"--server-url", "http://example.com:9090",
		"--log-level", "debug",
		"--peer-id", "abc123def4",
		"--join-code", "FLAGCODE",
	})
	if cfg.ServerURL != "http://example.com:9090" {
		t.Fatalf("expected server url http://example.com:9090, got %s", cfg.ServerURL)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("expected log level debug, got %s", cfg.LogLevel)
	}
	if cfg.PeerID != "abc123def4" {
		t.Fatalf("expected peer id abc123def4, got %s", cfg.PeerID)
	}
	if cfg.JoinCode != "FLAGCODE" {
		t.Fatalf("expected join code FLAGCODE, got %s", cfg.JoinCode)
	}
}
