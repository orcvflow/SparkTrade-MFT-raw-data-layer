package adapter

import (
	"context"
	"testing"
	"time"
)

// TestBinanceAdapter_Creation tests adapter creation
func TestBinanceAdapter_Creation(t *testing.T) {
	config := AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: 10,
		BackoffSeconds:    []int{1, 2, 4, 8, 16, 30},
		Timeout:           10 * time.Second,
	}
	
	symbols := []string{"btcusdt", "ethusdt"}
	adapter := NewBinanceAdapter("wss://testnet.binance.vision/ws", symbols, config)
	
	if adapter == nil {
		t.Fatal("Expected adapter to be created")
	}
	
	if adapter.Name() != "BINANCE" {
		t.Errorf("Expected name BINANCE, got %s", adapter.Name())
	}
	
	if len(adapter.symbols) != 2 {
		t.Errorf("Expected 2 symbols, got %d", len(adapter.symbols))
	}
	
	if adapter.heartbeatInterval != 30*time.Second {
		t.Errorf("Expected heartbeat 30s, got %v", adapter.heartbeatInterval)
	}
	
	if adapter.sessionRotation != 24*time.Hour {
		t.Errorf("Expected session rotation 24h, got %v", adapter.sessionRotation)
	}
}

// TestBinanceAdapter_Name tests adapter name
func TestBinanceAdapter_Name(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewBinanceAdapter("ws://test", []string{}, config)
	
	if adapter.Name() != "BINANCE" {
		t.Errorf("Expected name BINANCE, got %s", adapter.Name())
	}
}

// TestBinanceAdapter_InitialState tests initial state
func TestBinanceAdapter_InitialState(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewBinanceAdapter("ws://test", []string{}, config)
	
	if adapter.connected.Load() {
		t.Error("Expected adapter to not be connected initially")
	}
	
	if adapter.running.Load() {
		t.Error("Expected adapter to not be running initially")
	}
	
	if adapter.messagesRecv.Load() != 0 {
		t.Error("Expected messagesRecv to be 0 initially")
	}
	
	if adapter.reconnectCount.Load() != 0 {
		t.Error("Expected reconnectCount to be 0 initially")
	}
}

// TestBinanceAdapter_SubscriptionFormat tests subscription message format
// This test verifies the fix for empty subscription string bug (project-chrono)
func TestBinanceAdapter_SubscriptionFormat(t *testing.T) {
	config := AdapterConfig{}
	
	tests := []struct {
		name      string
		symbols   []string
		shouldErr bool
	}{
		{"Valid symbols", []string{"btcusdt", "ethusdt"}, false},
		{"Single symbol", []string{"btcusdt"}, false},
		{"Empty symbols", []string{}, true}, // Should return error
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := NewBinanceAdapter("ws://test", tt.symbols, config)
			err := adapter.subscribe()
			
			if tt.shouldErr && err == nil {
				t.Error("Expected error for empty symbols")
			}
			if !tt.shouldErr && err != nil && adapter.conn != nil {
				// Only check error if connection exists
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestBinanceAdapter_Health tests health status
func TestBinanceAdapter_Health(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewBinanceAdapter("ws://test", []string{"btcusdt"}, config)
	
	health := adapter.Health()
	
	if health.Connected {
		t.Error("Expected Connected to be false initially")
	}
	
	if health.MessagesRecv != 0 {
		t.Error("Expected MessagesRecv to be 0 initially")
	}
	
	if health.ReconnectCount != 0 {
		t.Error("Expected ReconnectCount to be 0 initially")
	}
	
	if len(health.Errors) != 0 {
		t.Errorf("Expected 0 errors, got %d", len(health.Errors))
	}
}

// TestBinanceAdapter_AddError tests error collection
func TestBinanceAdapter_AddError(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewBinanceAdapter("ws://test", []string{"btcusdt"}, config)
	
	// Add 15 errors (should keep only last 10)
	for i := 0; i < 15; i++ {
		adapter.addError(context.DeadlineExceeded)
	}
	
	health := adapter.Health()
	if len(health.Errors) != 10 {
		t.Errorf("Expected 10 errors (max), got %d", len(health.Errors))
	}
}

// TestBinanceAdapter_Stop tests graceful shutdown
func TestBinanceAdapter_Stop(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewBinanceAdapter("ws://test", []string{"btcusdt"}, config)
	
	// Set some state
	adapter.connected.Store(true)
	adapter.running.Store(true)
	
	err := adapter.Stop()
	if err != nil {
		t.Errorf("Stop failed: %v", err)
	}
	
	if adapter.connected.Load() {
		t.Error("Expected connected to be false after Stop")
	}
	
	if adapter.running.Load() {
		t.Error("Expected running to be false after Stop")
	}
}

// TestBinanceAdapter_PanicRecovery tests panic recovery in safeRead
func TestBinanceAdapter_PanicRecovery(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewBinanceAdapter("ws://test", []string{"btcusdt"}, config)
	
	// safeRead with nil connection should not panic
	msg, err := adapter.safeRead()
	
	if msg != nil {
		t.Error("Expected nil message with no connection")
	}
	
	if err == nil {
		t.Error("Expected error with no connection")
	}
	
	// Verify adapter is still functional after error
	health := adapter.Health()
	if health.Connected {
		t.Error("Expected not connected after failed read")
	}
}

// TestBinanceAdapter_AtomicOperations tests thread-safety of atomic operations
func TestBinanceAdapter_AtomicOperations(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewBinanceAdapter("ws://test", []string{"btcusdt"}, config)
	
	// Concurrent writes to atomic values
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				adapter.connected.Store(true)
				adapter.running.Store(true)
				adapter.messagesRecv.Add(1)
				adapter.reconnectCount.Add(1)
			}
			done <- true
		}()
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Verify atomic operations worked correctly
	if adapter.messagesRecv.Load() != 1000 {
		t.Errorf("Expected messagesRecv=1000, got %d", adapter.messagesRecv.Load())
	}
	
	if adapter.reconnectCount.Load() != 1000 {
		t.Errorf("Expected reconnectCount=1000, got %d", adapter.reconnectCount.Load())
	}
}

// BenchmarkBinanceAdapter_Creation benchmarks adapter creation
func BenchmarkBinanceAdapter_Creation(b *testing.B) {
	config := AdapterConfig{
		ReconnectAttempts: 10,
		BackoffSeconds:    []int{1, 2, 4, 8, 16, 30},
	}
	symbols := []string{"btcusdt", "ethusdt", "bnbusdt"}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewBinanceAdapter("ws://test", symbols, config)
	}
}

// BenchmarkBinanceAdapter_AddError benchmarks error addition
func BenchmarkBinanceAdapter_AddError(b *testing.B) {
	config := AdapterConfig{}
	adapter := NewBinanceAdapter("ws://test", []string{"btcusdt"}, config)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adapter.addError(context.DeadlineExceeded)
	}
}
