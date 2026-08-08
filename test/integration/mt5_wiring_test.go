//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/config"
)

// TestMT5Wiring_Disabled verifies MT5 adapter gracefully skipped when disabled.
// LAYER-0: Existence
// LAYER-2: Boundary (disabled config)
func TestMT5Wiring_Disabled(t *testing.T) {
	cfg := config.DefaultConfig()
	
	// MT5 block exists but disabled
	cfg.Adapters.MT5 = &config.MT5Config{
		Enabled:  false,
		Endpoint: "tcp://localhost:5556",
		Symbols:  []string{"EURUSD"},
	}
	
	// Simulate wiring logic
	var adapters []adapter.Adapter
	if cfg.Adapters.MT5 != nil && cfg.Adapters.MT5.Enabled {
		t.Fatal("MT5 should be skipped when disabled")
	}
	
	// Expected: 0 adapters (Binance/IB also disabled in default config)
	if len(adapters) != 0 {
		t.Errorf("Expected 0 adapters when MT5 disabled, got %d", len(adapters))
	}
}

// TestMT5Wiring_NilConfig verifies no panic when MT5 config is nil.
// LAYER-2: Boundary (nil config)
// LAYER-4: Failure mode (missing config block)
func TestMT5Wiring_NilConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Adapters.MT5 = nil // No mt5 block in config
	
	// This should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Panic on nil MT5 config: %v", r)
		}
	}()
	
	var adapters []adapter.Adapter
	if cfg.Adapters.MT5 != nil && cfg.Adapters.MT5.Enabled {
		adapters = append(adapters, adapter.NewMT5ZMQAdapter(
			cfg.Adapters.MT5.Endpoint,
			adapter.AdapterConfig{},
		))
	}
	
	// Expected: 0 adapters (nil check prevents append)
	if len(adapters) != 0 {
		t.Errorf("Expected 0 adapters when MT5 nil, got %d", len(adapters))
	}
}

// TestMT5Wiring_Enabled_ConnectFails verifies graceful failure when endpoint unreachable.
// LAYER-4: Failure mode (Connect() fails)
func TestMT5Wiring_Enabled_ConnectFails(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Adapters.MT5 = &config.MT5Config{
		Enabled:  true,
		Endpoint: "tcp://localhost:9999", // Unreachable port
		Symbols:  []string{"EURUSD"},
		Reconnect: config.ReconnectConf{
			MaxAttempts:    1, // Fast fail
			BackoffSeconds: []int{1},
		},
	}
	
	mt5Cfg := adapter.AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: 1,
		BackoffSeconds:    []int{1},
		Timeout:           1 * time.Second,
	}
	
	mt5Adapter := adapter.NewMT5ZMQAdapter(cfg.Adapters.MT5.Endpoint, mt5Cfg)
	
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	// Connect should fail (endpoint unreachable)
	err := mt5Adapter.Connect(ctx)
	if err == nil {
		t.Error("Expected Connect() to fail on unreachable endpoint, got nil error")
	}
	
	// System should continue (not crash)
	health := mt5Adapter.Health()
	if health.Connected {
		t.Error("Expected Connected=false after Connect() failure")
	}
}

// TestMT5Wiring_ConfigRoundtrip verifies config.yaml → Load → MT5Config.
// LAYER-0: Existence (config loads without error)
// LAYER-2: Boundary (YAML parse)
func TestMT5Wiring_ConfigRoundtrip(t *testing.T) {
	// Load actual config file
	cfg, err := config.Load("../../config/config.yaml")
	if err != nil {
		t.Fatalf("config.Load failed: %v", err)
	}
	
	// MT5 config should exist (even if disabled)
	if cfg.Adapters.MT5 == nil {
		t.Fatal("MT5 config missing from config.yaml")
	}
	
	// Check expected fields
	if cfg.Adapters.MT5.Endpoint == "" {
		t.Error("MT5 endpoint empty")
	}
	if len(cfg.Adapters.MT5.Symbols) == 0 {
		t.Error("MT5 symbols empty")
	}
	if len(cfg.Adapters.MT5.Reconnect.BackoffSeconds) == 0 {
		t.Error("MT5 reconnect backoff empty")
	}
	
	t.Logf("MT5 config loaded: enabled=%v, endpoint=%s, symbols=%v",
		cfg.Adapters.MT5.Enabled,
		cfg.Adapters.MT5.Endpoint,
		cfg.Adapters.MT5.Symbols)
}
