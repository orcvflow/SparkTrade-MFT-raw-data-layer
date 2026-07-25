package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Write a temp yaml and verify Load parses it.
func TestLoad_FromYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yml := []byte(`
adapters:
  binance:
    enabled: true
    endpoint: "wss://testnet.example/ws"
    symbols: ["btcusdt", "ethusdt"]
    reconnect:
      max_attempts: 5
      backoff_seconds: [1, 2, 4]
worker_pool:
  workers: 25
  queue_size: 5000
storage:
  wal:
    directory: "/tmp/wal-test"
  dolphindb:
    enabled: false
    host: "db.local"
    port: 8848
    password: "secret-from-yaml"
health:
  http_port: 9090
processes:
  adapter_health_port: 18081
`)
	if err := os.WriteFile(path, yml, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Adapters.Binance.Endpoint != "wss://testnet.example/ws" {
		t.Errorf("endpoint: %q", cfg.Adapters.Binance.Endpoint)
	}
	if cfg.WorkerPool.Workers != 25 {
		t.Errorf("workers: %d", cfg.WorkerPool.Workers)
	}
	if cfg.Storage.DolphinDB.Password != "secret-from-yaml" {
		t.Errorf("password from yaml lost: %q", cfg.Storage.DolphinDB.Password)
	}
	if cfg.Health.HTTPPort != 9090 {
		t.Errorf("http_port: %d", cfg.Health.HTTPPort)
	}
	if cfg.Processes.AdapterHealthPort != 18081 {
		t.Errorf("adapter health port: %d", cfg.Processes.AdapterHealthPort)
	}
	// Defaults fill unspecified fields.
	if cfg.Processes.PublisherHealthPort != 8083 {
		t.Errorf("default publisher port lost: %d", cfg.Processes.PublisherHealthPort)
	}
	if cfg.IPC.AdapterToCanonicalizer == "" {
		t.Error("IPC default path not filled")
	}
}

func TestLoad_MissingFileFallsBackToDefaults(t *testing.T) {
	cfg, err := Load("/nonexistent/path/config.yaml")
	if err != nil {
		t.Fatalf("Load(missing) should not error: %v", err)
	}
	if cfg.WorkerPool.Workers != 50 {
		t.Errorf("default workers: %d", cfg.WorkerPool.Workers)
	}
	if cfg.Processes.StorageHealthPort != 8084 {
		t.Errorf("default storage port: %d", cfg.Processes.StorageHealthPort)
	}
}

func TestLoad_EnvOverridesYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yml := []byte(`
storage:
  dolphindb:
    host: "from-yaml"
    port: 8848
    password: "yaml-pass"
health:
  http_port: 9090
`)
	if err := os.WriteFile(path, yml, 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOLPHINDB_HOST", "from-env")
	t.Setenv("DOLPHINDB_PORT", "9999")
	t.Setenv("DOLPHINDB_PASSWORD", "env-pass")
	t.Setenv("HEALTH_PORT", "8080")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.DolphinDB.Host != "from-env" {
		t.Errorf("env host override lost: %q", cfg.Storage.DolphinDB.Host)
	}
	if cfg.Storage.DolphinDB.Port != 9999 {
		t.Errorf("env port override lost: %d", cfg.Storage.DolphinDB.Port)
	}
	if cfg.Storage.DolphinDB.Password != "env-pass" {
		t.Errorf("env password override lost: %q", cfg.Storage.DolphinDB.Password)
	}
	if cfg.Health.HTTPPort != 8080 {
		t.Errorf("env health port override lost: %d", cfg.Health.HTTPPort)
	}
}

func TestParseDur(t *testing.T) {
	if d := ParseDur("30s", time.Second); d != 30*time.Second {
		t.Errorf("ParseDur: %v", d)
	}
	if d := ParseDur("garbage", time.Second); d != time.Second {
		t.Errorf("ParseDur fallback: %v", d)
	}
	if d := ParseDur("", time.Second); d != time.Second {
		t.Errorf("ParseDur empty: %v", d)
	}
}

func TestBackoffDurations(t *testing.T) {
	ds := BackoffDurations([]int{1, 2, 4})
	if len(ds) != 3 || ds[0] != time.Second || ds[2] != 4*time.Second {
		t.Errorf("BackoffDurations: %v", ds)
	}
}

func TestLoad_RealProjectYAML(t *testing.T) {
	// The checked-in config/config.yaml must load cleanly.
	cfg, err := Load("../../config/config.yaml")
	if err != nil {
		t.Fatalf("Load real config: %v", err)
	}
	if cfg.Adapters.Binance.Endpoint != "wss://stream.binance.com:9443/ws" {
		t.Errorf("binance endpoint: %q", cfg.Adapters.Binance.Endpoint)
	}
	// Addım F F3: testnet /ws is 404 (verified live); mainnet public market-data
	// WS is the production endpoint. WAL defaults to batched (F1).
	if cfg.Storage.WAL.Mode != "batched" {
		t.Errorf("wal mode: %q (want batched — Addım F default)", cfg.Storage.WAL.Mode)
	}
	if cfg.WorkerPool.Workers != 50 {
		t.Errorf("workers: %d", cfg.WorkerPool.Workers)
	}
	if cfg.Publisher.Zeromq.Port != 5555 {
		t.Errorf("zmq port: %d", cfg.Publisher.Zeromq.Port)
	}
}
