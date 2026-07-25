// Package config loads config/config.yaml and applies environment overrides + sane
// defaults. This finally makes the previously-decorative yaml real: the 4 process
// binaries (cmd/{adapter,canonicalizer,publisher,storage}) each call Load and read
// the subset they need. The monolith (cmd/raw-data-layer) keeps its own inline
// Config as a single-process fallback.
//
// Design (CLAUDE.md paranoid rules):
//   - Never panic: Load always returns a usable Config (defaults fill gaps).
//   - Secrets via env, not yaml: DOLPHINDB_PASSWORD etc. override yaml so secrets
//     never have to live in the checked-in file.
//   - Unknown yaml fields are ignored (forward-compatible).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config mirrors config/config.yaml. Fields the multi-process split needs are
// modeled; axiom/performance sections are intentionally omitted (ignored on load).
type Config struct {
	MappingsDir string          `yaml:"mappings_dir"`
	Adapters    AdaptersConfig  `yaml:"adapters"`
	WorkerPool  WorkerPoolConf  `yaml:"worker_pool"`
	Publisher   PublisherConf   `yaml:"publisher"`
	Storage     StorageConf     `yaml:"storage"`
	Validation  ValidationConf `yaml:"validation"`
	Health      HealthConf      `yaml:"health"`
	Logging     LoggingConf     `yaml:"logging"`
	IPC         IPCConf         `yaml:"ipc"`       // multi-process UDS paths
	Processes   ProcessesConf  `yaml:"processes"`  // per-process health ports
}

type AdaptersConfig struct {
	Binance BinanceConfig `yaml:"binance"`
	IB      IBConfig      `yaml:"ib"`
}

type BinanceConfig struct {
	Enabled          bool            `yaml:"enabled"`
	Endpoint         string          `yaml:"endpoint"`
	Symbols          []string        `yaml:"symbols"`
	Reconnect        ReconnectConf   `yaml:"reconnect"`
	HeartbeatInterval string         `yaml:"heartbeat_interval"`
	SessionRotation  string          `yaml:"session_rotation"`
}

type IBConfig struct {
	Enabled        bool          `yaml:"enabled"`
	Host           string        `yaml:"host"`
	Port           int           `yaml:"port"`
	ClientID       int           `yaml:"client_id"`
	Symbols        []string      `yaml:"symbols"`
	Reconnect      ReconnectConf `yaml:"reconnect"`
	RequestTimeout string        `yaml:"request_timeout"`
}

type ReconnectConf struct {
	MaxAttempts   int   `yaml:"max_attempts"`
	BackoffSeconds []int `yaml:"backoff_seconds"`
}

type WorkerPoolConf struct {
	Workers   int           `yaml:"workers"`
	QueueSize int           `yaml:"queue_size"`
	Autoscale AutoscaleConf `yaml:"autoscale"`
}

type AutoscaleConf struct {
	Enabled       bool    `yaml:"enabled"`
	HighWaterMark float64 `yaml:"high_water_mark"`
	LowWaterMark  float64 `yaml:"low_water_mark"`
	MaxWorkers    int     `yaml:"max_workers"`
	MinWorkers    int     `yaml:"min_workers"`
}

type PublisherConf struct {
	Zeromq ZMQConf `yaml:"zeromq"`
}

type ZMQConf struct {
	Enabled           bool   `yaml:"enabled"`
	Protocol          string `yaml:"protocol"`
	BindAddress       string `yaml:"bind_address"`
	Port              int    `yaml:"port"`
	HeartbeatInterval string `yaml:"heartbeat_interval"`
	HWM               int    `yaml:"hwm"`
}

type StorageConf struct {
	WAL       WALConf       `yaml:"wal"`
	DolphinDB DolphinDBConf `yaml:"dolphindb"`
}

type WALConf struct {
	Enabled        bool   `yaml:"enabled"`
	Directory      string `yaml:"directory"`
	RotationSize   int64  `yaml:"rotation_size"`
	RotationCount  int64  `yaml:"rotation_count"`
	SyncInterval   string `yaml:"sync_interval"`
	Mode           string `yaml:"mode"`             // "sync" | "batched" (default "batched" — Addım F)
	BatchTimeoutMs int    `yaml:"batch_timeout_ms"`  // batched flush interval (default 50ms)
}

type DolphinDBConf struct {
	Enabled        bool          `yaml:"enabled"`
	Host           string        `yaml:"host"`
	Port           int           `yaml:"port"`
	Username       string        `yaml:"username"`
	Password       string        `yaml:"password"`
	Database       string        `yaml:"database"`
	BatchSize      int           `yaml:"batch_size"`
	BatchTimeout   string        `yaml:"batch_timeout"`
	ConnectionPool ConnPoolConf  `yaml:"connection_pool"`
}

type ConnPoolConf struct {
	MaxOpen     int    `yaml:"max_open"`
	MaxIdle     int    `yaml:"max_idle"`
	MaxLifetime string `yaml:"max_lifetime"`
}

type ValidationConf struct {
	Enabled bool       `yaml:"enabled"`
	Layers  LayersConf `yaml:"layers"`
}

type LayersConf struct {
	Connectivity    bool `yaml:"connectivity"`
	Protocol        bool `yaml:"protocol"`
	DataIntegrity   bool `yaml:"data_integrity"`
	FaultTolerance  bool `yaml:"fault_tolerance"`
	Performance     bool `yaml:"performance"`
}

type HealthConf struct {
	HTTPPort      int    `yaml:"http_port"`
	MetricsPort   int    `yaml:"metrics_port"`
	ReadinessPath string `yaml:"readiness_path"`
	LivenessPath  string `yaml:"liveness_path"`
}

type LoggingConf struct {
	Level  string     `yaml:"level"`
	Format string     `yaml:"format"`
	Output string     `yaml:"output"`
	File   FileConf   `yaml:"file"`
}

type FileConf struct {
	Path       string `yaml:"path"`
	MaxSize    int    `yaml:"max_size"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAge     int    `yaml:"max_age"`
}

// IPCConf holds the UDS socket paths for the multi-process pipeline.
type IPCConf struct {
	AdapterToCanonicalizer string `yaml:"adapter_to_canonicalizer"`
	CanonicalizerToPublisher string `yaml:"canonicalizer_to_publisher"`
	PublisherToStorage      string `yaml:"publisher_to_storage"`
}

// ProcessesConf holds per-process health/metrics ports (Addım C).
type ProcessesConf struct {
	AdapterHealthPort      int `yaml:"adapter_health_port"`
	CanonicalizerHealthPort int `yaml:"canonicalizer_health_port"`
	PublisherHealthPort    int `yaml:"publisher_health_port"`
	StorageHealthPort      int `yaml:"storage_health_port"`
}

// DefaultConfig returns a Config with sensible defaults (no file needed).
func DefaultConfig() Config {
	return Config{
		MappingsDir: "./mappings",
		Adapters: AdaptersConfig{
			Binance: BinanceConfig{
				Enabled: true, Endpoint: "wss://stream.binance.com:9443/ws", // mainnet public market-data WS (testnet /ws is 404 — verified Addım F F3)
				Symbols: []string{"btcusdt", "ethusdt", "bnbusdt"},
				Reconnect: ReconnectConf{MaxAttempts: 10, BackoffSeconds: []int{1, 2, 4, 8, 16, 30}},
				HeartbeatInterval: "30s", SessionRotation: "24h",
			},
			IB: IBConfig{
				Enabled: false, Host: "localhost", Port: 7497, ClientID: 1,
				Symbols: []string{"AAPL", "MSFT", "GOOGL"},
				Reconnect: ReconnectConf{MaxAttempts: 10, BackoffSeconds: []int{1, 2, 4, 8, 16, 30}},
				RequestTimeout: "10s",
			},
		},
		WorkerPool: WorkerPoolConf{Workers: 50, QueueSize: 10000, Autoscale: AutoscaleConf{
			Enabled: true, HighWaterMark: 0.8, LowWaterMark: 0.2, MaxWorkers: 100, MinWorkers: 25,
		}},
		Publisher: PublisherConf{Zeromq: ZMQConf{
			Enabled: true, Protocol: "tcp", BindAddress: "*", Port: 5555,
			HeartbeatInterval: "5s", HWM: 10000,
		}},
		Storage: StorageConf{
			WAL: WALConf{Enabled: true, Directory: "./data/wal", RotationSize: 104857600, RotationCount: 10000, SyncInterval: "1s", Mode: "batched", BatchTimeoutMs: 50},
			DolphinDB: DolphinDBConf{Enabled: false, Host: "localhost", Port: 8848, Username: "admin", Password: "123456", Database: "dfs://raw_data", BatchSize: 1000, BatchTimeout: "1s"},
		},
		Validation: ValidationConf{Enabled: true, Layers: LayersConf{true, true, true, true, true}},
		Health: HealthConf{HTTPPort: 8080, MetricsPort: 9090, ReadinessPath: "/ready", LivenessPath: "/live"},
		Logging: LoggingConf{Level: "info", Format: "json", Output: "stdout"},
		IPC: IPCConf{
			AdapterToCanonicalizer: "/tmp/raw-adapter-canonicalizer.sock",
			CanonicalizerToPublisher: "/tmp/raw-canonicalizer-publisher.sock",
			PublisherToStorage:      "/tmp/raw-publisher-storage.sock",
		},
		Processes: ProcessesConf{AdapterHealthPort: 8081, CanonicalizerHealthPort: 8082, PublisherHealthPort: 8083, StorageHealthPort: 8084},
	}
}

// Load reads a yaml config file, applies env overrides + defaults, and returns
// a usable Config. If path is empty or unreadable, falls back to defaults (so a
// missing file never crashes a process — CLAUDE.md "never panic"). Never panics.
func Load(path string) (Config, error) {
	cfg := DefaultConfig()
	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			if err := yaml.Unmarshal(data, &cfg); err != nil {
				return cfg, fmt.Errorf("config: parse %s: %w", path, err)
			}
		}
		// A missing file is not fatal: defaults + env still apply.
	}
	applyEnv(&cfg)
	applyDefaults(&cfg)
	return cfg, nil
}

// applyEnv overrides fields from well-known env vars (secrets + ports + endpoints).
func applyEnv(c *Config) {
	envStr("MAPPINGS_DIR", &c.MappingsDir)
	envStr("BINANCE_ENDPOINT", &c.Adapters.Binance.Endpoint)
	envStr("IB_HOST", &c.Adapters.IB.Host)
	envInt("IB_PORT", &c.Adapters.IB.Port)
	envStr("DOLPHINDB_HOST", &c.Storage.DolphinDB.Host)
	envInt("DOLPHINDB_PORT", &c.Storage.DolphinDB.Port)
	envStr("DOLPHINDB_USER", &c.Storage.DolphinDB.Username)
	envStr("DOLPHINDB_PASSWORD", &c.Storage.DolphinDB.Password)
	envInt("ZMQ_PORT", &c.Publisher.Zeromq.Port)
	envInt("HEALTH_PORT", &c.Health.HTTPPort)
	envStr("WAL_DIR", &c.Storage.WAL.Directory)
	envStr("LOG_LEVEL", &c.Logging.Level)
	envInt("ADAPTER_HEALTH_PORT", &c.Processes.AdapterHealthPort)
	envInt("CANONICALIZER_HEALTH_PORT", &c.Processes.CanonicalizerHealthPort)
	envInt("PUBLISHER_HEALTH_PORT", &c.Processes.PublisherHealthPort)
	envInt("STORAGE_HEALTH_PORT", &c.Processes.StorageHealthPort)
}

// applyDefaults fills any zero/empty fields left after yaml + env.
func applyDefaults(c *Config) {
	if c.WorkerPool.Workers <= 0 {
		c.WorkerPool.Workers = 50
	}
	if c.WorkerPool.QueueSize <= 0 {
		c.WorkerPool.QueueSize = 10000
	}
	if c.WorkerPool.Autoscale.MaxWorkers <= 0 {
		c.WorkerPool.Autoscale.MaxWorkers = c.WorkerPool.Workers * 2
	}
	if c.WorkerPool.Autoscale.MinWorkers <= 0 {
		c.WorkerPool.Autoscale.MinWorkers = c.WorkerPool.Workers / 2
	}
	if c.Storage.DolphinDB.Database == "" {
		c.Storage.DolphinDB.Database = "dfs://raw_data"
	}
	if c.Storage.DolphinDB.BatchSize <= 0 {
		c.Storage.DolphinDB.BatchSize = 1000
	}
	if c.Storage.DolphinDB.BatchTimeout == "" {
		c.Storage.DolphinDB.BatchTimeout = "1s"
	}
	if c.Storage.WAL.RotationSize <= 0 {
		c.Storage.WAL.RotationSize = 100 * 1024 * 1024
	}
	if c.Storage.WAL.RotationCount <= 0 {
		c.Storage.WAL.RotationCount = 10000
	}
	if c.Storage.WAL.SyncInterval == "" {
		c.Storage.WAL.SyncInterval = "1s"
	}
	// Addım F: production default is batched WAL. The Addım E E1 benchmark
	// measured sync WAL as fsync-bound (~20 msg/s, p99 ~104ms) vs batched
	// (148K msg/s, p99 26µs). Empty/unknown → batched (never crash).
	walMode := strings.ToLower(strings.TrimSpace(c.Storage.WAL.Mode))
	if walMode == "" {
		walMode = "batched"
		c.Storage.WAL.Mode = walMode
	}
	if c.Storage.WAL.BatchTimeoutMs <= 0 {
		c.Storage.WAL.BatchTimeoutMs = 50
	}
	if c.Publisher.Zeromq.HWM <= 0 {
		c.Publisher.Zeromq.HWM = 10000
	}
	if c.Publisher.Zeromq.HeartbeatInterval == "" {
		c.Publisher.Zeromq.HeartbeatInterval = "5s"
	}
	if c.Adapters.Binance.HeartbeatInterval == "" {
		c.Adapters.Binance.HeartbeatInterval = "30s"
	}
	if c.IPC.AdapterToCanonicalizer == "" {
		c.IPC.AdapterToCanonicalizer = "/tmp/raw-adapter-canonicalizer.sock"
	}
	if c.IPC.CanonicalizerToPublisher == "" {
		c.IPC.CanonicalizerToPublisher = "/tmp/raw-canonicalizer-publisher.sock"
	}
	if c.IPC.PublisherToStorage == "" {
		c.IPC.PublisherToStorage = "/tmp/raw-publisher-storage.sock"
	}
	if c.Processes.AdapterHealthPort == 0 {
		c.Processes.AdapterHealthPort = 8081
	}
	if c.Processes.CanonicalizerHealthPort == 0 {
		c.Processes.CanonicalizerHealthPort = 8082
	}
	if c.Processes.PublisherHealthPort == 0 {
		c.Processes.PublisherHealthPort = 8083
	}
	if c.Processes.StorageHealthPort == 0 {
		c.Processes.StorageHealthPort = 8084
	}
	if c.Health.HTTPPort == 0 {
		c.Health.HTTPPort = 8080
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if len(c.Adapters.Binance.Reconnect.BackoffSeconds) == 0 {
		c.Adapters.Binance.Reconnect.BackoffSeconds = []int{1, 2, 4, 8, 16, 30}
	}
}

// ParseDur parses a duration string with a fallback default. Never panics.
func ParseDur(s string, def time.Duration) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return def
}

// BackoffDurations converts []int seconds to []time.Duration.
func BackoffDurations(secs []int) []time.Duration {
	out := make([]time.Duration, len(secs))
	for i, s := range secs {
		out[i] = time.Duration(s) * time.Second
	}
	return out
}

// envStr overrides *dst if the env var is set and non-empty.
func envStr(key string, dst *string) {
	if v := os.Getenv(key); v != "" {
		*dst = v
	}
}

// envInt overrides *dst if the env var is set and parseable.
func envInt(key string, dst *int) {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			*dst = n
		}
	}
}
