package storage

import (
	"testing"
	"time"

	"raw-data-layer/pkg/canonicalizer"
)

// TestBatchedWAL_WriteReplay verifies the core contract: events written through
// the async (deferred-fsync) path are NOT lost when the WAL is stopped cleanly
// (Stop drains the final batch). This is the correctness gate before trusting
// BatchedWAL in the E1 benchmark.
func TestBatchedWAL_WriteReplay(t *testing.T) {
	dir := t.TempDir()
	cfg := WALConfig{
		Directory:      dir,
		MaxFileSize:     1 << 20, // 1 MiB
		MaxMessages:     1000,
		RotateInterval:  time.Minute,
	}
	w, err := NewBatchedWAL(cfg, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("NewBatchedWAL: %v", err)
	}
	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const n = 500
	for i := 0; i < n; i++ {
		ev := &canonicalizer.CanonicalEvent{
			EventID:         "evt",
			Source:          "BINANCE",
			CanonicalSymbol: "BTC/USD",
			Price:           float64(i),
			Size:            1,
			Side:            "BUY",
		}
		if err := w.Write(ev); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}

	// Stop MUST drain the final un-fsynced batch (else data loss).
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	got, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != n {
		t.Fatalf("expected %d events, got %d (data lost in final batch?)", n, len(got))
	}
	// At least one flush should have happened (Stop's flushLocked).
	if w.TotalFlushes() == 0 {
		t.Fatalf("expected ≥1 flush, got 0")
	}
}

// TestBatchedWAL_NeverPanics sends a nil event (json.Marshal(nil) → "null\n",
// not a panic) and an event with nil slices — BatchedWAL must not crash.
func TestBatchedWAL_NeverPanics(t *testing.T) {
	dir := t.TempDir()
	cfg := WALConfig{Directory: dir, MaxFileSize: 1 << 20, MaxMessages: 1000, RotateInterval: time.Minute}
	w, _ := NewBatchedWAL(cfg, 10*time.Millisecond)
	_ = w.Start()

	// nil event: json.Marshal(nil) succeeds ("null"); Write must not panic.
	_ = w.Write(nil)

	// normal event after nil — pipeline keeps going.
	ev := &canonicalizer.CanonicalEvent{EventID: "x", Source: "BINANCE"}
	_ = w.Write(ev)

	_ = w.Stop()
	got, _ := w.Replay()
	if len(got) < 1 {
		t.Fatalf("expected ≥1 replayable event, got %d", len(got))
	}
}
