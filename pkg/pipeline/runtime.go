package pipeline

import (
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"raw-data-layer/pkg/config"
)

// NewLogger builds a slog logger from config. Honors level + format; file output
// is best-effort (opened, not closed — the process closes it on exit). Never panics.
func NewLogger(cfg config.LoggingConf) *slog.Logger {
	var level slog.Level = slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	w := os.Stdout
	if cfg.Output == "file" && cfg.File.Path != "" {
		if f, err := os.OpenFile(cfg.File.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			w = f
		}
	}

	opts := &slog.HandlerOptions{Level: level}
	if cfg.Format == "text" {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

// WaitForSignal blocks until SIGINT/SIGTERM and returns the signal. Never panics.
func WaitForSignal() os.Signal {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	return <-quit
}

// SpoolPath returns a per-edge on-disk spool path under the WAL directory and
// ensures its parent exists. Each IPC client uses its own spool so a downstream
// outage is absorbed losslessly (FIFO replay on reconnect).
func SpoolPath(cfg config.Config, edge string) string {
	dir := filepath.Join(cfg.Storage.WAL.Directory, "spool")
	_ = os.MkdirAll(dir, 0o755)
	return filepath.Join(dir, edge+".bin")
}
