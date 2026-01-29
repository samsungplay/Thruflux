package logging

import (
	"log/slog"
	"os"

	"github.com/sheerbytes/sheerbytes/internal/termio"
)

// New creates a new structured logger with text output.
// app: application name (e.g., "thruserv")
// level: one of "debug", "info", "warn", "error" (default: "info")
func New(app string, level string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: parseLevel(level),
	}
	handler := slog.NewTextHandler(termio.Stdout(), opts)
	logger := slog.New(handler)

	// Add default attributes: app and pid
	return logger.With(
		slog.String("app", app),
		slog.Int("pid", os.Getpid()),
	)
}

func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "info":
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}
