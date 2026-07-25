package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// An almost-empty yaml forces nearly every applyDefaults branch to fire.
func TestLoad_EmptyYamlFillsDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	// Explicitly zero out fields so applyDefaults' fill-branches all fire
	// (DefaultConfig pre-fills them, so a key must be present with a zero value
	// to force the zero-check to take the fill path).
	if err := os.WriteFile(path, []byte(`
worker_pool: {workers: 0, queue_size: 0, autoscale: {max_workers: 0, min_workers: 0}}
storage:
  wal: {rotation_size: 0, rotation_count: 0, sync_interval: ""}
  dolphindb: {database: "", batch_size: 0, batch_timeout: ""}
publisher: {zeromq: {hwm: 0, heartbeat_interval: ""}}
adapters: {binance: {heartbeat_interval: "", reconnect: {backoff_seconds: []}}}
health: {http_port: 0}
ipc: {adapter_to_canonicalizer: "", canonicalizer_to_publisher: "", publisher_to_storage: ""}
processes: {adapter_health_port: 0, canonicalizer_health_port: 0, publisher_health_port: 0, storage_health_port: 0}
logging: {level: ""}
`), 0644); err != nil {
		t.Fatal(err)
	}
	// Clear env so defaults aren't masked.
	for _, k := range []string{"MAPPINGS_DIR", "DOLPHINDB_HOST", "DOLPHINDB_PORT", "DOLPHINDB_USER",
		"DOLPHINDB_PASSWORD", "ZMQ_PORT", "HEALTH_PORT", "WAL_DIR", "LOG_LEVEL", "IB_HOST", "IB_PORT",
		"BINANCE_ENDPOINT", "ADAPTER_HEALTH_PORT", "CANONICALIZER_HEALTH_PORT",
		"PUBLISHER_HEALTH_PORT", "STORAGE_HEALTH_PORT"} {
		os.Unsetenv(k)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Defaults that should now be filled from applyDefaults (not the yaml).
	if cfg.WorkerPool.Workers != 50 {
		t.Errorf("workers default: %d", cfg.WorkerPool.Workers)
	}
	if cfg.WorkerPool.Autoscale.MaxWorkers != 100 {
		t.Errorf("max workers default: %d", cfg.WorkerPool.Autoscale.MaxWorkers)
	}
	if cfg.WorkerPool.Autoscale.MinWorkers != 25 {
		t.Errorf("min workers default: %d", cfg.WorkerPool.Autoscale.MinWorkers)
	}
	if cfg.Storage.DolphinDB.Database != "dfs://raw_data" {
		t.Errorf("database default: %q", cfg.Storage.DolphinDB.Database)
	}
	if cfg.Storage.DolphinDB.BatchSize != 1000 {
		t.Errorf("batch size default: %d", cfg.Storage.DolphinDB.BatchSize)
	}
	if cfg.Storage.DolphinDB.BatchTimeout != "1s" {
		t.Errorf("batch timeout default: %q", cfg.Storage.DolphinDB.BatchTimeout)
	}
	if cfg.Storage.WAL.RotationSize != 100*1024*1024 {
		t.Errorf("rotation size default: %d", cfg.Storage.WAL.RotationSize)
	}
	if cfg.Storage.WAL.SyncInterval != "1s" {
		t.Errorf("sync interval default: %q", cfg.Storage.WAL.SyncInterval)
	}
	if cfg.Publisher.Zeromq.HWM != 10000 {
		t.Errorf("hwm default: %d", cfg.Publisher.Zeromq.HWM)
	}
	if cfg.Publisher.Zeromq.HeartbeatInterval != "5s" {
		t.Errorf("heartbeat default: %q", cfg.Publisher.Zeromq.HeartbeatInterval)
	}
	if cfg.Adapters.Binance.HeartbeatInterval != "30s" {
		t.Errorf("binance heartbeat default: %q", cfg.Adapters.Binance.HeartbeatInterval)
	}
	if cfg.IPC.PublisherToStorage == "" {
		t.Error("ipc publisher->storage default not filled")
	}
	if cfg.Health.HTTPPort != 8080 {
		t.Errorf("http port default: %d", cfg.Health.HTTPPort)
	}
	if cfg.Logging.Level != "info" {
		t.Errorf("log level default: %q", cfg.Logging.Level)
	}
	if len(cfg.Adapters.Binance.Reconnect.BackoffSeconds) != 6 {
		t.Errorf("backoff default len: %d", len(cfg.Adapters.Binance.Reconnect.BackoffSeconds))
	}
	// ParseDur default path via SyncInterval
	if d := ParseDur(cfg.Storage.WAL.SyncInterval, time.Second); d != time.Second {
		t.Errorf("parse sync interval: %v", d)
	}
}

// envInt's parse-error branch (non-numeric) must not override and not panic.
func TestEnvInt_GarbageIgnored(t *testing.T) {
	t.Setenv("DOLPHINDB_PORT", "not-a-number")
	dst := 1234
	envInt("DOLPHINDB_PORT", &dst)
	if dst != 1234 {
		t.Errorf("garbage env overwrote dst: %d", dst)
	}
}
