package logging

import (
	"log/slog"
	"os"
)

// New creates a new structured logger with text output.
// app: application name (e.g., "thruserv")
// level: one of "debug", "info", "warn", "error" (default: "info")
// logFile: optional path to a log file (default: stdout)
func New(app string, level string, logFile string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: parseLevel(level),
	}
	var writer *os.File = os.Stdout
	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			writer = f
		} else {
			// Fallback to stderr if file fails, so we don't silence logs completely
			// and print an error
			os.Stderr.WriteString("failed to open log file: " + err.Error() + "\n")
		}
	}
	handler := slog.NewTextHandler(writer, opts)
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
