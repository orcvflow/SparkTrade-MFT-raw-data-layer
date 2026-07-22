package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"raw-data-layer/pkg/canonicalizer"
)

// createTestWAL creates a WAL in temp directory for testing
func createTestWAL(t *testing.T) (*WAL, string) {
	tempDir := t.TempDir()
	
	config := WALConfig{
		Directory:      tempDir,
		MaxFileSize:    1024, // 1KB for testing
		MaxMessages:    10,
		RotateInterval: 100 * time.Millisecond,
	}
	
	wal, err := NewWAL(config)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	
	return wal, tempDir
}

// createTestEvent creates a test canonical event
func createTestEvent(id string) *canonicalizer.CanonicalEvent {
	return &canonicalizer.CanonicalEvent{
		EventID:           id,
		Source:            "TEST",
		CanonicalSymbol:   "BTC/USD",
		ExchangeTimestamp: time.Now().UnixNano(),
		LocalHWTimestamp:  time.Now().UnixNano(),
		EventType:         "TRADE",
		Price:             50000.0,
		Size:              1.0,
		Side:              "BUY",
		RawPayload:        []byte("test"),
	}
}

// TestDefaultWALConfig tests default configuration
func TestDefaultWALConfig(t *testing.T) {
	config := DefaultWALConfig()
	
	if config.Directory != "/var/log/raw_data/wal" {
		t.Errorf("Expected directory /var/log/raw_data/wal, got %s", config.Directory)
	}
	
	if config.MaxFileSize != 100*1024*1024 {
		t.Errorf("Expected MaxFileSize=100MB, got %d", config.MaxFileSize)
	}
	
	if config.MaxMessages != 10000 {
		t.Errorf("Expected MaxMessages=10000, got %d", config.MaxMessages)
	}
}

// TestNewWAL tests WAL creation
func TestNewWAL(t *testing.T) {
	wal, _ := createTestWAL(t)
	
	if wal == nil {
		t.Fatal("Expected WAL to be created")
	}
	
	if wal.running.Load() {
		t.Error("Expected WAL to not be running initially")
	}
}

// TestWAL_Start tests WAL start
func TestWAL_Start(t *testing.T) {
	wal, tempDir := createTestWAL(t)
	
	if err := wal.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer wal.Stop()
	
	if !wal.running.Load() {
		t.Error("Expected WAL to be running")
	}
	
	// Check that file was created
	files, err := filepath.Glob(filepath.Join(tempDir, "wal_*.jsonl"))
	if err != nil {
		t.Fatalf("Failed to list files: %v", err)
	}
	
	if len(files) == 0 {
		t.Error("Expected at least one WAL file to be created")
	}
}

// TestWAL_Write tests writing events
func TestWAL_Write(t *testing.T) {
	wal, _ := createTestWAL(t)
	wal.Start()
	defer wal.Stop()
	
	event := createTestEvent("evt_test_123")
	
	if err := wal.Write(event); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	
	stats := wal.Stats()
	if stats.TotalWritten != 1 {
		t.Errorf("Expected TotalWritten=1, got %d", stats.TotalWritten)
	}
	
	if stats.CurrentMessages != 1 {
		t.Errorf("Expected CurrentMessages=1, got %d", stats.CurrentMessages)
	}
}

// TestWAL_Write_Multiple tests writing multiple events
func TestWAL_Write_Multiple(t *testing.T) {
	wal, _ := createTestWAL(t)
	wal.Start()
	defer wal.Stop()
	
	// Write 5 events
	for i := 0; i < 5; i++ {
		event := createTestEvent(fmt.Sprintf("evt_%d", i))
		if err := wal.Write(event); err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}
	
	stats := wal.Stats()
	if stats.TotalWritten != 5 {
		t.Errorf("Expected TotalWritten=5, got %d", stats.TotalWritten)
	}
}

// TestWAL_Rotation_ByMessages tests rotation by message count
func TestWAL_Rotation_ByMessages(t *testing.T) {
	wal, tempDir := createTestWAL(t)
	wal.Start()
	defer wal.Stop()
	
	// Write 15 events (MaxMessages=10, should trigger rotation)
	for i := 0; i < 15; i++ {
		event := createTestEvent(fmt.Sprintf("evt_%d", i))
		wal.Write(event)
	}
	
	// Give time for rotation
	time.Sleep(200 * time.Millisecond)
	
	stats := wal.Stats()
	if stats.TotalRotations < 1 {
		t.Errorf("Expected at least 1 rotation, got %d", stats.TotalRotations)
	}
	
	// Check multiple files created
	files, _ := filepath.Glob(filepath.Join(tempDir, "wal_*.jsonl"))
	if len(files) < 2 {
		t.Errorf("Expected at least 2 WAL files, got %d", len(files))
	}
}

// TestWAL_Replay tests replaying events from WAL
func TestWAL_Replay(t *testing.T) {
	wal, _ := createTestWAL(t)
	wal.Start()
	
	// Write events
	events := []string{"evt_1", "evt_2", "evt_3"}
	for _, id := range events {
		event := createTestEvent(id)
		wal.Write(event)
	}
	
	wal.Stop()
	
	// Replay
	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	
	if len(replayed) != 3 {
		t.Errorf("Expected 3 replayed events, got %d", len(replayed))
	}
	
	// Verify event IDs
	for i, event := range replayed {
		expectedID := events[i]
		if event.EventID != expectedID {
			t.Errorf("Event %d: expected ID %s, got %s", i, expectedID, event.EventID)
		}
	}
}

// TestWAL_Replay_MultipleFiles tests replaying from multiple files
func TestWAL_Replay_MultipleFiles(t *testing.T) {
	wal, _ := createTestWAL(t)
	wal.Start()
	
	// Write events to trigger rotation
	for i := 0; i < 25; i++ {
		event := createTestEvent(fmt.Sprintf("evt_%d", i))
		wal.Write(event)
	}
	
	time.Sleep(200 * time.Millisecond)
	wal.Stop()
	
	// Replay all
	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	
	if len(replayed) != 25 {
		t.Errorf("Expected 25 replayed events, got %d", len(replayed))
	}
}

// TestWAL_CleanOld tests cleaning old files
func TestWAL_CleanOld(t *testing.T) {
	wal, tempDir := createTestWAL(t)
	wal.Start()
	
	// Write events
	for i := 0; i < 5; i++ {
		event := createTestEvent(fmt.Sprintf("evt_%d", i))
		wal.Write(event)
	}
	
	wal.Stop()
	
	// Check files exist
	filesBefore, _ := filepath.Glob(filepath.Join(tempDir, "wal_*.jsonl"))
	if len(filesBefore) == 0 {
		t.Fatal("Expected at least one WAL file")
	}
	
	// Clean files older than 10 seconds (should clean nothing)
	wal.CleanOld(10 * time.Second)
	
	filesAfter, _ := filepath.Glob(filepath.Join(tempDir, "wal_*.jsonl"))
	if len(filesAfter) != len(filesBefore) {
		t.Errorf("Expected %d files, got %d", len(filesBefore), len(filesAfter))
	}
	
	// Clean files older than 0 seconds (should clean all)
	wal.CleanOld(0)
	
	filesNone, _ := filepath.Glob(filepath.Join(tempDir, "wal_*.jsonl"))
	if len(filesNone) != 0 {
		t.Errorf("Expected 0 files after cleaning, got %d", len(filesNone))
	}
}

// TestWAL_Stop tests graceful shutdown
func TestWAL_Stop(t *testing.T) {
	wal, _ := createTestWAL(t)
	wal.Start()
	
	// Write events
	for i := 0; i < 5; i++ {
		event := createTestEvent(fmt.Sprintf("evt_%d", i))
		wal.Write(event)
	}
	
	// Stop should flush and close
	if err := wal.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
	
	if wal.running.Load() {
		t.Error("Expected WAL to not be running after Stop")
	}
}

// TestWAL_Stats tests statistics tracking
func TestWAL_Stats(t *testing.T) {
	wal, _ := createTestWAL(t)
	wal.Start()
	defer wal.Stop()
	
	// Write events
	for i := 0; i < 3; i++ {
		event := createTestEvent(fmt.Sprintf("evt_%d", i))
		wal.Write(event)
	}
	
	stats := wal.Stats()
	
	if !stats.Running {
		t.Error("Expected Running=true")
	}
	
	if stats.TotalWritten != 3 {
		t.Errorf("Expected TotalWritten=3, got %d", stats.TotalWritten)
	}
	
	if stats.CurrentFile == "" {
		t.Error("Expected CurrentFile to be set")
	}
	
	if stats.LastWrite.IsZero() {
		t.Error("Expected LastWrite to be set")
	}
}

// TestWALStats_IsHealthy tests health check
func TestWALStats_IsHealthy(t *testing.T) {
	tests := []struct {
		name     string
		stats    WALStats
		expected bool
	}{
		{
			name: "Healthy WAL",
			stats: WALStats{
				Running:      true,
				TotalWritten: 100,
				LastWrite:    time.Now(),
			},
			expected: true,
		},
		{
			name: "Not running",
			stats: WALStats{
				Running:      false,
				TotalWritten: 100,
			},
			expected: false,
		},
		{
			name: "No writes",
			stats: WALStats{
				Running:      true,
				TotalWritten: 0,
			},
			expected: false,
		},
		{
			name: "Stale writes (>1 minute)",
			stats: WALStats{
				Running:      true,
				TotalWritten: 100,
				LastWrite:    time.Now().Add(-2 * time.Minute),
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

// TestWAL_ConcurrentWrites tests thread-safety
func TestWAL_ConcurrentWrites(t *testing.T) {
	wal, _ := createTestWAL(t)
	wal.Start()
	defer wal.Stop()
	
	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 5; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				event := createTestEvent(fmt.Sprintf("evt_%d_%d", id, j))
				wal.Write(event)
			}
			done <- true
		}(i)
	}
	
	// Wait for all goroutines
	for i := 0; i < 5; i++ {
		<-done
	}
	
	stats := wal.Stats()
	if stats.TotalWritten != 50 {
		t.Errorf("Expected TotalWritten=50, got %d", stats.TotalWritten)
	}
}

// BenchmarkWAL_Write benchmarks write performance
func BenchmarkWAL_Write(b *testing.B) {
	tempDir := b.TempDir()
	
	config := WALConfig{
		Directory:      tempDir,
		MaxFileSize:    100 * 1024 * 1024,
		MaxMessages:    100000,
		RotateInterval: 1 * time.Hour,
	}
	
	wal, _ := NewWAL(config)
	wal.Start()
	defer wal.Stop()
	
	event := createTestEvent("evt_bench")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(event)
	}
}
