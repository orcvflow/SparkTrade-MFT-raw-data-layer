package workerpool

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"raw-data-layer/pkg/adapter"
)

// mockProcessor is a simple processor for testing
func mockProcessor(ctx context.Context, raw adapter.RawMessage) (ProcessedMessage, error) {
	return ProcessedMessage{
		Raw:         raw,
		Error:       nil,
		ProcessedAt: time.Now().UnixNano(),
	}, nil
}

// slowProcessor simulates slow processing
func slowProcessor(ctx context.Context, raw adapter.RawMessage) (ProcessedMessage, error) {
	time.Sleep(100 * time.Millisecond)
	return mockProcessor(ctx, raw)
}

// errorProcessor always returns error
func errorProcessor(ctx context.Context, raw adapter.RawMessage) (ProcessedMessage, error) {
	return ProcessedMessage{
		Raw:         raw,
		Error:       fmt.Errorf("processing error"),
		ProcessedAt: time.Now().UnixNano(),
	}, fmt.Errorf("processing error")
}

// TestDefaultPoolConfig tests default configuration
func TestDefaultPoolConfig(t *testing.T) {
	config := DefaultPoolConfig()
	
	if config.MinWorkers != 50 {
		t.Errorf("Expected MinWorkers=50, got %d", config.MinWorkers)
	}
	
	if config.MaxWorkers != 100 {
		t.Errorf("Expected MaxWorkers=100, got %d", config.MaxWorkers)
	}
	
	if config.QueueSize != 10000 {
		t.Errorf("Expected QueueSize=10000, got %d", config.QueueSize)
	}
	
	if !config.AutoscaleEnabled {
		t.Error("Expected AutoscaleEnabled=true")
	}
	
	if config.ScaleUpThreshold != 0.8 {
		t.Errorf("Expected ScaleUpThreshold=0.8, got %f", config.ScaleUpThreshold)
	}
}

// TestNewPool tests pool creation
func TestNewPool(t *testing.T) {
	config := PoolConfig{
		MinWorkers: 10,
		MaxWorkers: 20,
		QueueSize:  100,
	}
	
	pool := NewPool(config, mockProcessor)
	
	if pool == nil {
		t.Fatal("Expected pool to be created")
	}
	
	if pool.minWorkers != 10 {
		t.Errorf("Expected minWorkers=10, got %d", pool.minWorkers)
	}
	
	if pool.maxWorkers != 20 {
		t.Errorf("Expected maxWorkers=20, got %d", pool.maxWorkers)
	}
	
	if pool.queueSize != 100 {
		t.Errorf("Expected queueSize=100, got %d", pool.queueSize)
	}
}

// TestPool_Start tests pool start
func TestPool_Start(t *testing.T) {
	config := PoolConfig{
		MinWorkers: 5,
		MaxWorkers: 10,
		QueueSize:  100,
	}
	
	pool := NewPool(config, mockProcessor)
	
	if err := pool.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	
	// Give workers time to start
	time.Sleep(100 * time.Millisecond)
	
	stats := pool.Stats()
	if stats.ActiveWorkers != 5 {
		t.Errorf("Expected 5 active workers, got %d", stats.ActiveWorkers)
	}
	
	pool.Stop()
}

// TestPool_SubmitAndProcess tests message submission and processing
func TestPool_SubmitAndProcess(t *testing.T) {
	config := PoolConfig{
		MinWorkers: 5,
		MaxWorkers: 10,
		QueueSize:  100,
	}
	
	pool := NewPool(config, mockProcessor)
	pool.Start()
	defer pool.Stop()
	
	// Submit a message
	msg := adapter.RawMessage{
		Source:     "TEST",
		Payload:    []byte("test message"),
		ReceivedAt: time.Now().UnixNano(),
	}
	
	if err := pool.Submit(msg); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	
	// Receive processed message
	select {
	case processed := <-pool.Output():
		if string(processed.Raw.Payload) != "test message" {
			t.Errorf("Expected 'test message', got %s", string(processed.Raw.Payload))
		}
		if processed.Error != nil {
			t.Errorf("Expected no error, got %v", processed.Error)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for processed message")
	}
	
	stats := pool.Stats()
	if stats.Processed != 1 {
		t.Errorf("Expected 1 processed message, got %d", stats.Processed)
	}
}

// TestPool_Backpressure tests backpressure when queue is full
// This is a mandatory test (CLAUDE.md: Test_ChannelFull)
func TestPool_Backpressure(t *testing.T) {
	config := PoolConfig{
		MinWorkers:       2,
		MaxWorkers:       2,
		QueueSize:        10, // Small queue for testing
		AutoscaleEnabled: false,
	}
	
	pool := NewPool(config, slowProcessor) // Use slow processor
	pool.Start()
	defer pool.Stop()
	
	// Fill the queue
	for i := 0; i < 10; i++ {
		msg := adapter.RawMessage{
			Source:     "TEST",
			Payload:    []byte(fmt.Sprintf("msg-%d", i)),
			ReceivedAt: time.Now().UnixNano(),
		}
		if err := pool.Submit(msg); err != nil {
			t.Fatalf("Submit %d failed: %v", i, err)
		}
	}
	
	// Try to submit one more (should fail - backpressure)
	msg := adapter.RawMessage{
		Source:     "TEST",
		Payload:    []byte("overflow"),
		ReceivedAt: time.Now().UnixNano(),
	}
	
	err := pool.Submit(msg)
	if err == nil {
		t.Fatal("Expected backpressure error when queue is full")
	}
	
	stats := pool.Stats()
	if stats.Dropped != 1 {
		t.Errorf("Expected 1 dropped message, got %d", stats.Dropped)
	}
}

// TestPool_ErrorHandling tests error handling in processor
func TestPool_ErrorHandling(t *testing.T) {
	config := PoolConfig{
		MinWorkers: 2,
		MaxWorkers: 5,
		QueueSize:  100,
	}
	
	pool := NewPool(config, errorProcessor)
	pool.Start()
	defer pool.Stop()
	
	msg := adapter.RawMessage{
		Source:     "TEST",
		Payload:    []byte("test"),
		ReceivedAt: time.Now().UnixNano(),
	}
	
	if err := pool.Submit(msg); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	
	// Receive processed message with error
	select {
	case processed := <-pool.Output():
		if processed.Error == nil {
			t.Error("Expected error in processed message")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for processed message")
	}
	
	stats := pool.Stats()
	if stats.Errors != 1 {
		t.Errorf("Expected 1 error, got %d", stats.Errors)
	}
}

// TestPool_Autoscaling tests dynamic worker scaling
func TestPool_Autoscaling(t *testing.T) {
	config := PoolConfig{
		MinWorkers:         10,
		MaxWorkers:         50,
		QueueSize:          100,
		AutoscaleEnabled:   true,
		ScaleUpThreshold:   0.8,
		ScaleDownThreshold: 0.5,
	}
	
	pool := NewPool(config, slowProcessor)
	pool.Start()
	defer pool.Stop()
	
	// Initial workers
	time.Sleep(100 * time.Millisecond)
	stats := pool.Stats()
	initialWorkers := stats.ActiveWorkers
	
	// Fill queue to 80% (trigger scale up)
	for i := 0; i < 80; i++ {
		msg := adapter.RawMessage{
			Source:     "TEST",
			Payload:    []byte(fmt.Sprintf("msg-%d", i)),
			ReceivedAt: time.Now().UnixNano(),
		}
		pool.Submit(msg)
	}
	
	// Wait for autoscaler to check (5s interval)
	time.Sleep(6 * time.Second)
	
	stats = pool.Stats()
	if stats.ActiveWorkers <= initialWorkers {
		t.Errorf("Expected more workers after scaling up, got %d (initial: %d)", 
			stats.ActiveWorkers, initialWorkers)
	}
}

// TestPool_Stop tests graceful shutdown
func TestPool_Stop(t *testing.T) {
	config := PoolConfig{
		MinWorkers: 5,
		MaxWorkers: 10,
		QueueSize:  100,
	}
	
	pool := NewPool(config, mockProcessor)
	pool.Start()
	
	// Submit some messages
	for i := 0; i < 10; i++ {
		msg := adapter.RawMessage{
			Source:     "TEST",
			Payload:    []byte(fmt.Sprintf("msg-%d", i)),
			ReceivedAt: time.Now().UnixNano(),
		}
		pool.Submit(msg)
	}
	
	// Stop should wait for all messages to be processed
	if err := pool.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
	
	stats := pool.Stats()
	if stats.Processed < 10 {
		t.Errorf("Expected at least 10 processed messages, got %d", stats.Processed)
	}
}

// TestPoolStats_Utilization tests utilization calculation
func TestPoolStats_Utilization(t *testing.T) {
	tests := []struct {
		name       string
		queueDepth int
		queueSize  int
		expected   float64
	}{
		{"Empty queue", 0, 100, 0.0},
		{"Half full", 50, 100, 0.5},
		{"80% full", 80, 100, 0.8},
		{"Full queue", 100, 100, 1.0},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := PoolStats{
				QueueDepth: tt.queueDepth,
				QueueSize:  tt.queueSize,
			}
			
			util := stats.Utilization()
			if util != tt.expected {
				t.Errorf("Expected utilization %f, got %f", tt.expected, util)
			}
		})
	}
}

// TestPoolStats_IsHealthy tests health check
func TestPoolStats_IsHealthy(t *testing.T) {
	tests := []struct {
		name     string
		stats    PoolStats
		expected bool
	}{
		{
			name: "Healthy pool",
			stats: PoolStats{
				ActiveWorkers: 10,
				QueueDepth:    50,
				QueueSize:     100,
				Processed:     1000,
				Dropped:       10,
			},
			expected: true,
		},
		{
			name: "No workers",
			stats: PoolStats{
				ActiveWorkers: 0,
				QueueDepth:    50,
				QueueSize:     100,
			},
			expected: false,
		},
		{
			name: "Queue too full (>90%)",
			stats: PoolStats{
				ActiveWorkers: 10,
				QueueDepth:    95,
				QueueSize:     100,
			},
			expected: false,
		},
		{
			name: "High drop rate (>10%)",
			stats: PoolStats{
				ActiveWorkers: 10,
				QueueDepth:    50,
				QueueSize:     100,
				Processed:     800,
				Dropped:       200, // 20% drop rate
			},
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			healthy := tt.stats.IsHealthy()
			if healthy != tt.expected {
				t.Errorf("Expected IsHealthy=%v, got %v", tt.expected, healthy)
			}
		})
	}
}

// TestPool_ConcurrentSubmit tests concurrent message submission
// This verifies thread-safety
func TestPool_ConcurrentSubmit(t *testing.T) {
	config := PoolConfig{
		MinWorkers: 10,
		MaxWorkers: 20,
		QueueSize:  1000,
	}
	
	pool := NewPool(config, mockProcessor)
	pool.Start()
	defer pool.Stop()
	
	// Concurrent submissions
	var submitted atomic.Uint64
	done := make(chan bool)
	
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				msg := adapter.RawMessage{
					Source:     "TEST",
					Payload:    []byte(fmt.Sprintf("goroutine-%d-msg-%d", id, j)),
					ReceivedAt: time.Now().UnixNano(),
				}
				if err := pool.Submit(msg); err == nil {
					submitted.Add(1)
				}
			}
			done <- true
		}(i)
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Give time to process
	time.Sleep(1 * time.Second)
	
	stats := pool.Stats()
	if stats.Processed != submitted.Load() {
		t.Errorf("Expected %d processed, got %d", submitted.Load(), stats.Processed)
	}
}

// BenchmarkPool_Submit benchmarks message submission
func BenchmarkPool_Submit(b *testing.B) {
	config := PoolConfig{
		MinWorkers: 50,
		MaxWorkers: 100,
		QueueSize:  10000,
	}
	
	pool := NewPool(config, mockProcessor)
	pool.Start()
	defer pool.Stop()
	
	msg := adapter.RawMessage{
		Source:     "TEST",
		Payload:    []byte("benchmark message"),
		ReceivedAt: time.Now().UnixNano(),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Submit(msg)
	}
}

// BenchmarkPool_Process benchmarks end-to-end processing
func BenchmarkPool_Process(b *testing.B) {
	config := PoolConfig{
		MinWorkers: 50,
		MaxWorkers: 100,
		QueueSize:  10000,
	}
	
	pool := NewPool(config, mockProcessor)
	pool.Start()
	defer pool.Stop()
	
	// Drain output channel in background
	go func() {
		for range pool.Output() {
		}
	}()
	
	msg := adapter.RawMessage{
		Source:     "TEST",
		Payload:    []byte("benchmark message"),
		ReceivedAt: time.Now().UnixNano(),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Submit(msg)
	}
}
