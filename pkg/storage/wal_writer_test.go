package storage

import (
	"testing"
	"time"

	"raw-data-layer/pkg/canonicalizer"
)

// TestNewWALWriter_ModeSelection locks the Addım F durability-mode contract:
// sync/empty/unknown → batched (production default); "sync" → *WAL.
func TestNewWALWriter_ModeSelection(t *testing.T) {
	cfg := WALConfig{Directory: t.TempDir(), MaxFileSize: 1 << 20, MaxMessages: 100, RotateInterval: time.Minute}

	cases := []struct {
		mode string
		wantBatched bool
	}{
		{"batched", true},
		{"", true},        // empty → default (batched), never crash
		{"BATCHED", true}, // case-insensitive
		{"unknown", true}, // unknown → default (batched), never crash
		{"sync", false},
		{"fsync", false},
		{"durable", false},
	}
	for _, c := range cases {
		w, err := NewWALWriter(cfg, c.mode, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("NewWALWriter(mode=%q): %v", c.mode, err)
		}
		if err := w.Start(); err != nil {
			t.Fatalf("Start(mode=%q): %v", c.mode, err)
		}
		// Write one event to confirm the chosen sink is functional.
		ev := &canonicalizer.CanonicalEvent{EventID: "x", Source: "BINANCE", CanonicalSymbol: "BTC/USD"}
		if err := w.Write(ev); err != nil {
			t.Fatalf("Write(mode=%q): %v", c.mode, err)
		}
		if _, err := w.Replay(); err != nil {
			t.Fatalf("Replay(mode=%q): %v", c.mode, err)
		}
		if err := w.Stop(); err != nil {
			t.Fatalf("Stop(mode=%q): %v", c.mode, err)
		}
		// Type assertion: batched → *BatchedWAL; sync → *WAL.
		_, isBatched := w.(*BatchedWAL)
		_, isSync := w.(*WAL)
		if c.wantBatched && !isBatched {
			t.Errorf("mode=%q: expected *BatchedWAL, got %T", c.mode, w)
		}
		if !c.wantBatched && !isSync {
			t.Errorf("mode=%q: expected *WAL, got %T", c.mode, w)
		}
	}
}
