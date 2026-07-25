package storage

import (
	"strings"
	"time"

	"raw-data-layer/pkg/canonicalizer"
)

// WALWriter is the durability sink used by DolphinDBWriter and the benchmark.
// Both *WAL (per-message fsync, durable) and *BatchedWAL (deferred fsync,
// throughput) satisfy it, so callers pick a durability mode without the
// DolphinDBWriter needing to know which concrete type it holds.
//
// Addım F: the production default is now "batched" (see config.go / config.yaml),
// because the Addım E E1 benchmark measured sync WAL as fsync-bound (~20 msg/s,
// p99 ~104ms) while batched WAL hit the spec targets (148K msg/s, p99 26µs).
// sync remains available where zero in-flight loss on crash is non-negotiable.
type WALWriter interface {
	// Start opens the initial file and spawns background goroutines. Never panics.
	Start() error
	// Write appends one canonical event. Never panics; returns error/default.
	Write(event *canonicalizer.CanonicalEvent) error
	// Stop drains (fsync) and closes. Never panics.
	Stop() error
	// Stats returns a snapshot of counters. Never panics.
	Stats() WALStats
	// Replay reads all durable events back (for DB recovery). Never panics;
	// malformed lines are skipped.
	Replay() ([]canonicalizer.CanonicalEvent, error)
}

// NewWALWriter selects a WAL implementation by durability mode.
//   - "sync"     → *WAL (per-message fsync; zero in-flight loss on crash)
//   - "batched"  → *BatchedWAL (deferred fsync every batchTimeout; ~4500× faster,
//     but can lose up to batchTimeout of un-fsynced messages on a hard crash)
//
// Empty/unknown mode resolves to "batched" (the production default per Addım F).
// Never panics; a bad mode never silently degrades to a crash — it falls back to
// batched and the caller proceeds. batchTimeout <= 0 → 50ms default.
func NewWALWriter(cfg WALConfig, mode string, batchTimeout time.Duration) (WALWriter, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "sync", "fsync", "durable":
		return NewWAL(cfg)
	default: // "batched", "", or anything else → the production default
		return NewBatchedWAL(cfg, batchTimeout)
	}
}
