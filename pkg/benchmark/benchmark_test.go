package benchmark

import (
	"testing"
)

// TestRunBoth_Smoke runs a small benchmark (no live deps, no Binance, no
// DolphinDB) and verifies the report is well-formed: non-zero throughput, a
// numeric p99, and the verdict fields populated. This is the in-process analog
// of the CLI acceptance check (./bin/adapter --benchmark).
func TestRunBoth_Smoke(t *testing.T) {
	// Small counts: the sync WAL fsyncs per message, which on a slow/overlay-FS
	// disk can be ~tens of ms each — keep the smoke test fast while still
	// exercising both paths end-to-end.
	const messages, warmup = 50, 5
	out, err := RunBoth(messages, warmup)
	if err != nil {
		t.Fatalf("RunBoth: %v", err)
	}
	if out == nil {
		t.Fatal("nil output")
	}
	if out.ThroughputMsgsPerSec <= 0 {
		t.Fatalf("sync throughput must be > 0, got %v", out.ThroughputMsgsPerSec)
	}
	if out.LatencyP99Ns <= 0 {
		t.Fatalf("sync p99 must be > 0, got %d", out.LatencyP99Ns)
	}
	if out.Sync.WALMode != "sync" {
		t.Fatalf("Sync.WALMode = %q, want sync", out.Sync.WALMode)
	}
	if out.Batched.WALMode != "batched" {
		t.Fatalf("Batched.WALMode = %q, want batched", out.Batched.WALMode)
	}
	if out.SpecTargetMsgPerSec != 100000 {
		t.Fatalf("spec target = %d, want 100000", out.SpecTargetMsgPerSec)
	}
	// Batched must be at least as fast as sync (no per-msg fsync). Honest
	// invariant: deferring fsync cannot make Write slower.
	if out.Batched.ThroughputMsgsPerSec < out.Sync.ThroughputMsgsPerSec {
		t.Fatalf("batched (%v) < sync (%v) — deferring fsync should not slow Write",
			out.Batched.ThroughputMsgsPerSec, out.Sync.ThroughputMsgsPerSec)
	}
	// Each run must have written ALL messages to WAL (warmup + measured), proving
	// no loss in the steady path. The WAL counter is cumulative across warmup.
	wantWritten := uint64(messages + warmup)
	if out.Sync.WALTotalWritten != wantWritten {
		t.Fatalf("sync WALTotalWritten = %d, want %d", out.Sync.WALTotalWritten, wantWritten)
	}
	if out.Batched.WALTotalWritten != wantWritten {
		t.Fatalf("batched WALTotalWritten = %d, want %d", out.Batched.WALTotalWritten, wantWritten)
	}
	// Batched must have flushed at least once (Stop's final drain).
	if out.Batched.WALTotalFlushes == 0 {
		t.Fatalf("batched WALTotalFlushes = 0, want ≥1")
	}
}
