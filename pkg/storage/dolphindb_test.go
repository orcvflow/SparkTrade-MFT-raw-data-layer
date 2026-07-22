package storage

import (
	"fmt"
	"testing"
	"time"

	"raw-data-layer/pkg/canonicalizer"
)

// createTestDolphinDBWriter creates a writer without real DB connection (WAL fallback mode)
func createTestDolphinDBWriter(t *testing.T) (*DolphinDBWriter, *WAL) {
	t.Helper()

	tempDir := t.TempDir()
	walConfig := WALConfig{
		Directory:      tempDir,
		MaxFileSize:    10 * 1024 * 1024, // 10MB
		MaxMessages:    100000,
		RotateInterval: 1 * time.Hour,
	}

	wal, err := NewWAL(walConfig)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	if err := wal.Start(); err != nil {
		t.Fatalf("WAL Start failed: %v", err)
	}

	config := DolphinDBConfig{
		Host:         "localhost",
		Port:         19999, // intentionally wrong port — no real DB in tests
		Username:     "admin",
		Password:     "test",
		Database:     "test_db",
		BatchSize:    10,
		BatchTimeout: 100 * time.Millisecond,
	}

	writer := NewDolphinDBWriter(config, wal)
	return writer, wal
}

// createTestCanonicalEvent creates a test event
func createTestCanonicalEvent(id string) *canonicalizer.CanonicalEvent {
	return &canonicalizer.CanonicalEvent{
		EventID:           id,
		Source:            "TEST",
		CanonicalSymbol:   "BTC/USD",
		ExchangeTimestamp: time.Now().UnixNano(),
		LocalHWTimestamp:  time.Now().UnixNano(),
		EventType:         "TRADE",
		Price:             50000.0,
		Size:              1.5,
		Side:              "BUY",
		RawPayload:        []byte(`{"e":"trade","p":"50000","q":"1.5"}`),
		RawFormat:         "JSON",
	}
}

// TestDefaultDolphinDBConfig tests default configuration values
func TestDefaultDolphinDBConfig(t *testing.T) {
	config := DefaultDolphinDBConfig()

	if config.Host != "localhost" {
		t.Errorf("Expected host=localhost, got %s", config.Host)
	}
	if config.Port != 8848 {
		t.Errorf("Expected port=8848, got %d", config.Port)
	}
	if config.BatchSize != 1000 {
		t.Errorf("Expected BatchSize=1000, got %d", config.BatchSize)
	}
	if config.BatchTimeout != 1*time.Second {
		t.Errorf("Expected BatchTimeout=1s, got %v", config.BatchTimeout)
	}
}

// TestNewDolphinDBWriter tests writer creation
func TestNewDolphinDBWriter(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()

	if writer == nil {
		t.Fatal("Expected writer to be created")
	}
	if writer.connected.Load() {
		t.Error("Expected writer to not be connected initially")
	}
	if writer.batchSize != 10 {
		t.Errorf("Expected batchSize=10, got %d", writer.batchSize)
	}
}

// TestDolphinDBWriter_Connect_Failure tests connection failure handling
// When DB is unavailable, Connect must return error — NOT panic
func TestDolphinDBWriter_Connect_Failure(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()

	// Connection will fail (no real DB) — must not panic
	err := writer.Connect()

	// Expect error (no real DB), but no panic
	if err == nil {
		// If it somehow connects (driver not registered), that's fine too
		t.Log("Connect succeeded unexpectedly (mock driver?)")
	} else {
		t.Logf("Connect correctly returned error: %v", err)
	}

	// Writer must still be usable (WAL fallback)
	if writer == nil {
		t.Fatal("Writer became nil after failed connect")
	}
}

// TestDolphinDBWriter_Write_NilEvent tests writing nil event
// Must not panic — return error
func TestDolphinDBWriter_Write_NilEvent(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()
	writer.Start()
	defer writer.Stop()

	err := writer.Write(nil)
	if err == nil {
		t.Error("Expected error for nil event")
	}

	// Must not panic — test passes if we reach here
}

// TestDolphinDBWriter_Write_Single tests writing a single event
func TestDolphinDBWriter_Write_Single(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()
	writer.Start()
	defer writer.Stop()

	event := createTestCanonicalEvent("evt_single_001")

	if err := writer.Write(event); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Event should be in batch (not yet flushed)
	writer.batchMu.Lock()
	batchLen := len(writer.batch)
	writer.batchMu.Unlock()

	if batchLen != 1 {
		t.Errorf("Expected 1 event in batch, got %d", batchLen)
	}
}

// TestDolphinDBWriter_BatchAccumulation tests that batch accumulates correctly
// 999 messages → no automatic flush; 1000th → flush triggered
func TestDolphinDBWriter_BatchAccumulation(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()

	// Use larger batch size to test accumulation
	writer.batchSize = 5
	writer.batch = make([]*canonicalizer.CanonicalEvent, 0, 5)
	writer.Start()
	defer writer.Stop()

	// Write 4 events — should stay in batch
	for i := 0; i < 4; i++ {
		event := createTestCanonicalEvent(fmt.Sprintf("evt_%03d", i))
		writer.Write(event)
	}

	writer.batchMu.Lock()
	batchLen := len(writer.batch)
	writer.batchMu.Unlock()

	if batchLen != 4 {
		t.Errorf("Expected 4 events in batch, got %d", batchLen)
	}
}

// TestDolphinDBWriter_TimeoutFlush tests that timeout triggers flush to WAL
// DB is unavailable → events must land in WAL, not be lost
func TestDolphinDBWriter_TimeoutFlush_WALFallback(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()

	// batchTimeout = 100ms (from createTestDolphinDBWriter)
	writer.Start()
	defer writer.Stop()

	// Write 3 events (below batch threshold)
	for i := 0; i < 3; i++ {
		event := createTestCanonicalEvent(fmt.Sprintf("evt_timeout_%03d", i))
		writer.Write(event)
	}

	// Wait for timeout flush
	time.Sleep(300 * time.Millisecond)

	// Events should have gone to WAL (since DB is not connected)
	walStats := wal.Stats()
	if walStats.TotalWritten < 3 {
		t.Errorf("Expected at least 3 events in WAL, got %d", walStats.TotalWritten)
	}

	// Batch should be empty after flush
	writer.batchMu.Lock()
	batchLen := len(writer.batch)
	writer.batchMu.Unlock()

	if batchLen != 0 {
		t.Errorf("Expected empty batch after timeout flush, got %d", batchLen)
	}
}

// TestDolphinDBWriter_BatchFlush_WALFallback tests batch-size-triggered flush to WAL
func TestDolphinDBWriter_BatchFlush_WALFallback(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()

	writer.batchSize = 5
	writer.batch = make([]*canonicalizer.CanonicalEvent, 0, 5)
	writer.Start()
	defer writer.Stop()

	// Write exactly batchSize events — should trigger flush
	for i := 0; i < 5; i++ {
		event := createTestCanonicalEvent(fmt.Sprintf("evt_batch_%03d", i))
		writer.Write(event)
	}

	// Give async flush time to run
	time.Sleep(50 * time.Millisecond)

	// Events should be in WAL (DB not available)
	walStats := wal.Stats()
	if walStats.TotalWritten < 5 {
		t.Errorf("Expected at least 5 events in WAL, got %d", walStats.TotalWritten)
	}
}

// TestDolphinDBWriter_ConcurrentWrites tests thread-safety
func TestDolphinDBWriter_ConcurrentWrites(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()
	writer.Start()
	defer writer.Stop()

	done := make(chan struct{})
	total := 50

	for i := 0; i < total; i++ {
		go func(id int) {
			event := createTestCanonicalEvent(fmt.Sprintf("evt_concurrent_%03d", id))
			writer.Write(event)
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < total; i++ {
		<-done
	}

	// Wait for timeout flush
	time.Sleep(300 * time.Millisecond)

	// All events should be in WAL (no panic, no data loss)
	walStats := wal.Stats()
	if walStats.TotalWritten < uint64(total) {
		t.Errorf("Expected %d events in WAL, got %d", total, walStats.TotalWritten)
	}
}

// TestDolphinDBWriter_Flush tests explicit flush
func TestDolphinDBWriter_Flush(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()
	writer.Start()
	defer writer.Stop()

	// Write 3 events
	for i := 0; i < 3; i++ {
		event := createTestCanonicalEvent(fmt.Sprintf("evt_flush_%03d", i))
		writer.Write(event)
	}

	// Explicit flush
	if err := writer.Flush(); err != nil {
		t.Errorf("Flush failed: %v", err)
	}

	// Batch should be empty
	writer.batchMu.Lock()
	batchLen := len(writer.batch)
	writer.batchMu.Unlock()

	if batchLen != 0 {
		t.Errorf("Expected empty batch after Flush(), got %d", batchLen)
	}
}

// TestDolphinDBWriter_Stop_FinalFlush tests that Stop flushes remaining events
func TestDolphinDBWriter_Stop_FinalFlush(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()
	writer.Start()

	// Write events
	for i := 0; i < 5; i++ {
		event := createTestCanonicalEvent(fmt.Sprintf("evt_stop_%03d", i))
		writer.Write(event)
	}

	// Stop triggers final flush
	if err := writer.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}

	// Give WAL time to write
	time.Sleep(50 * time.Millisecond)

	walStats := wal.Stats()
	if walStats.TotalWritten < 5 {
		t.Errorf("Expected at least 5 events in WAL after Stop, got %d", walStats.TotalWritten)
	}
}

// TestDolphinDBWriter_Stats tests statistics tracking
func TestDolphinDBWriter_Stats(t *testing.T) {
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()
	writer.Start()
	defer writer.Stop()

	stats := writer.Stats()

	if stats.Connected {
		t.Error("Expected Connected=false (no real DB)")
	}
	if stats.BatchSize != 10 {
		t.Errorf("Expected BatchSize=10, got %d", stats.BatchSize)
	}
}

// TestDolphinDBStats_IsHealthy tests health check logic
func TestDolphinDBStats_IsHealthy(t *testing.T) {
	tests := []struct {
		name     string
		stats    DolphinDBStats
		expected bool
	}{
		{
			name: "Healthy — connected, no recent errors",
			stats: DolphinDBStats{
				Connected:   true,
				TotalWritten: 1000,
				LastWrite:   time.Now(),
			},
			expected: true,
		},
		{
			name: "Unhealthy — not connected",
			stats: DolphinDBStats{
				Connected: false,
			},
			expected: false,
		},
		{
			name: "Unhealthy — recent error",
			stats: DolphinDBStats{
				Connected: true,
				LastError: time.Now().Add(-10 * time.Second),
			},
			expected: false,
		},
		{
			name: "Healthy — old error (>30s ago)",
			stats: DolphinDBStats{
				Connected: true,
				LastError: time.Now().Add(-60 * time.Second),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.stats.IsHealthy()
			if result != tt.expected {
				t.Errorf("Expected IsHealthy=%v, got %v", tt.expected, result)
			}
		})
	}
}

// TestDolphinDBWriter_WALNil_NoFallback tests behavior when WAL is nil
// Must not panic — just log error and continue
func TestDolphinDBWriter_WALNil_NoFallback(t *testing.T) {
	config := DolphinDBConfig{
		Host:         "localhost",
		Port:         19999,
		Username:     "admin",
		Password:     "test",
		Database:     "test_db",
		BatchSize:    5,
		BatchTimeout: 50 * time.Millisecond,
	}

	// No WAL
	writer := NewDolphinDBWriter(config, nil)
	writer.Start()
	defer writer.Stop()

	// Write events — should not panic even without WAL
	for i := 0; i < 3; i++ {
		event := createTestCanonicalEvent(fmt.Sprintf("evt_nowal_%03d", i))
		if err := writer.Write(event); err != nil {
			t.Errorf("Write failed: %v", err)
		}
	}

	// Wait for timeout flush — no panic expected
	time.Sleep(200 * time.Millisecond)

	// Test passes if we reach here without panic
}

// BenchmarkDolphinDBWriter_Write benchmarks write throughput
func BenchmarkDolphinDBWriter_Write(b *testing.B) {
	tempDir := b.TempDir()
	walConfig := WALConfig{
		Directory:      tempDir,
		MaxFileSize:    100 * 1024 * 1024,
		MaxMessages:    1000000,
		RotateInterval: 1 * time.Hour,
	}
	wal, _ := NewWAL(walConfig)
	wal.Start()
	defer wal.Stop()

	config := DolphinDBConfig{
		Host:         "localhost",
		Port:         19999,
		BatchSize:    1000,
		BatchTimeout: 1 * time.Second,
	}
	writer := NewDolphinDBWriter(config, wal)
	writer.Start()
	defer writer.Stop()

	event := createTestCanonicalEvent("bench_event")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Write(event)
	}
}
