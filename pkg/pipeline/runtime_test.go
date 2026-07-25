package pipeline

import (
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"raw-data-layer/pkg/config"
)

func TestNewLogger_DebugLevelFile(t *testing.T) {
	f, err := os.CreateTemp("", "log-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(f.Name())

	log := NewLogger(config.LoggingConf{Level: "debug", Format: "text", Output: "file", File: config.FileConf{Path: f.Name()}})
	log.Debug("hello-debug")
	log.Info("hello-info")

	data, _ := os.ReadFile(f.Name())
	s := string(data)
	if !strings.Contains(s, "hello-debug") {
		t.Errorf("debug msg missing in:\n%s", s)
	}
	if !strings.Contains(s, "hello-info") {
		t.Errorf("info msg missing in:\n%s", s)
	}
}

func TestNewLogger_WarnLevelHidesDebug(t *testing.T) {
	f, _ := os.CreateTemp("", "log-*.txt")
	f.Close()
	defer os.Remove(f.Name())

	log := NewLogger(config.LoggingConf{Level: "warn", Format: "text", Output: "file", File: config.FileConf{Path: f.Name()}})
	log.Debug("should-not-appear")
	log.Warn("should-appear")

	data, _ := os.ReadFile(f.Name())
	s := string(data)
	if strings.Contains(s, "should-not-appear") {
		t.Errorf("debug leaked at warn level:\n%s", s)
	}
	if !strings.Contains(s, "should-appear") {
		t.Errorf("warn msg missing:\n%s", s)
	}
}

func TestNewLogger_JSONFormat(t *testing.T) {
	f, _ := os.CreateTemp("", "log-*.json")
	f.Close()
	defer os.Remove(f.Name())

	log := NewLogger(config.LoggingConf{Level: "info", Format: "json", Output: "file", File: config.FileConf{Path: f.Name()}})
	log.Info("json-msg")
	data, _ := os.ReadFile(f.Name())
	if !strings.Contains(string(data), "json-msg") {
		t.Errorf("json msg missing:\n%s", data)
	}
}

func TestSpoolPath(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Config{}
	cfg.Storage.WAL.Directory = dir + "/wal"

	p := SpoolPath(cfg, "adapter")
	if !strings.HasSuffix(p, "spool/adapter.bin") {
		t.Errorf("unexpected path: %s", p)
	}
	if _, err := os.Stat(dir + "/wal/spool"); err != nil {
		t.Errorf("spool dir not created: %v", err)
	}
}

func TestWaitForSignal(t *testing.T) {
	go func() {
		time.Sleep(80 * time.Millisecond)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	}()
	sig := WaitForSignal()
	if sig != syscall.SIGTERM {
		t.Errorf("got %v, want SIGTERM", sig)
	}
}
