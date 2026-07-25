// Package benchmark measures the system-level throughput/latency of the
// raw-data-layer hot path (Addım E, Task E1).
//
// It runs an IN-PROCESS pipeline: a single producer feeds Binance-shaped
// RawMessages through canonicalizer.Process → WAL.Write, measuring per-message
// latency (p50/p95/p99/max), throughput (msg/s), GC pause, and heap. It runs
// TWICE — once with the production sync WAL (per-message fsync) and once with a
// batched/async WAL variant (deferred fsync) — so the fsync bottleneck is
// directly quantified, which is the whole point of E1: the spec's system-level
// targets (100K msg/s, <500µs p99, <100ms GC, <2GB) become MEASURED numbers.
//
// What this measures vs. what it does NOT (honest scope):
//   - MEASURED: canonicalize (Sonic parse + sanitize + symbol-map) + WAL.Write
//     (json.Marshal + bufio.Write +, for sync, per-message fsync).
//   - NOT MEASURED: UDS codec (EncodeRaw/DecodeCanonical), ZMQ pub, live
//     DolphinDB, network I/O. These are << the WAL fsync cost (Addım C notes),
//     so the in-process number is an honest upper bound on the producer-side
//     hot path; a full 4-process UDS run would be slightly LOWER (codec +
//     socket overhead), not higher.
//
// The single-producer loop is honest for the WAL-bound path: sync WAL serializes
// on fsync under w.mu, so extra workers would NOT raise sync-WAL throughput;
// batched WAL likewise serializes bufio.Write under w.mu. Multi-worker scaling
// helps only upstream of the WAL (canonicalize), which is not the bottleneck.
//
// Paranoid: never panics (defer/recover on every entry), uses temp dirs (cleaned
// up), bounded work, no external dependencies (no Binance testnet, no live
// DolphinDB) — fully CI-reproducible.
package benchmark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/mapper"
	"raw-data-layer/pkg/storage"
)

// specTarget is CLAUDE.md's system throughput target (msg/s).
const specTarget = 100000

// Config controls one benchmark run (one WAL mode).
type Config struct {
	Messages int    // messages to measure (after warmup)
	Warmup   int    // warmup messages (discarded)
	WALMode  string // "sync" | "batched"
	BatchMs  int    // batched flush interval (ms); only used when WALMode=="batched"
}

// Result is one run's measurements.
type Result struct {
	WALMode              string  `json:"wal_mode"`
	Messages             int     `json:"messages"`
	Warmup               int     `json:"warmup"`
	ThroughputMsgsPerSec float64 `json:"throughput_msgs_per_sec"`
	LatencyP50Ns         int64   `json:"latency_p50_ns"`
	LatencyP95Ns         int64   `json:"latency_p95_ns"`
	LatencyP99Ns         int64   `json:"latency_p99_ns"`
	LatencyMaxNs         int64   `json:"latency_max_ns"`
	GCPauseTotalNs       uint64  `json:"gc_pause_total_ns"`
	GCPauseMaxNs         uint64  `json:"gc_pause_max_ns"`
	GCNumGC              uint32  `json:"gc_num_gc"`
	HeapAllocMB          float64 `json:"heap_alloc_mb"`
	SysMB                float64 `json:"sys_mb"`
	Goroutines           int     `json:"goroutines"`
	WALFiles             int     `json:"wal_files"`
	WALTotalWritten      uint64  `json:"wal_total_written"`
	WALTotalFlushes      uint64  `json:"wal_total_flushes"` // batched only; 0 for sync
}

// Output is the full report emitted by RunBoth. Top-level throughput/latency
// mirror the sync (production-default) run; the batched run is nested. This
// makes the jq acceptance check (top-level throughput + p99 > 0) validate the
// production number, while the nested objects carry the full delta.
type Output struct {
	ThroughputMsgsPerSec float64 `json:"throughput_msgs_per_sec"` // sync (production default)
	LatencyP99Ns         int64   `json:"latency_p99_ns"`          // sync
	SpecTargetMsgPerSec  int     `json:"spec_target_msg_per_sec"`
	MeetsTarget          bool    `json:"meets_target"`    // batched throughput >= spec target
	PipelineMode         string  `json:"pipeline_mode"`   // "in-process (canonicalize→WAL)"
	Sync                 Result  `json:"sync"`
	Batched              Result  `json:"batched"`
}

// benchWAL is the subset of WAL/BatchedWAL the benchmark drives.
type benchWAL interface {
	Write(*canonicalizer.CanonicalEvent) error
	Stop() error
	Stats() storage.WALStats
}

// RunBoth runs the benchmark twice (sync then batched) and returns the report.
func RunBoth(messages, warmup int) (*Output, error) {
	if messages <= 0 {
		messages = 100000
	}
	if warmup < 0 {
		warmup = 0
	}
	syncRes, err := Run(Config{Messages: messages, Warmup: warmup, WALMode: "sync"})
	if err != nil {
		return nil, fmt.Errorf("sync run: %w", err)
	}
	batchedRes, err := Run(Config{Messages: messages, Warmup: warmup, WALMode: "batched", BatchMs: 50})
	if err != nil {
		return nil, fmt.Errorf("batched run: %w", err)
	}
	return &Output{
		ThroughputMsgsPerSec: syncRes.ThroughputMsgsPerSec,
		LatencyP99Ns:        syncRes.LatencyP99Ns,
		SpecTargetMsgPerSec: specTarget,
		MeetsTarget:         batchedRes.ThroughputMsgsPerSec >= float64(specTarget),
		PipelineMode:        "in-process (canonicalize→WAL)",
		Sync:                *syncRes,
		Batched:             *batchedRes,
	}, nil
}

// Run executes one benchmark run. Never panics.
func Run(cfg Config) (res *Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("benchmark panic: %v", r)
		}
	}()
	if cfg.Messages <= 0 {
		cfg.Messages = 100000
	}

	// Stub mapper in a temp dir (self-contained — no dependency on repo mappings/).
	mapDir, err := os.MkdirTemp("", "rdl-bench-mappings-*")
	if err != nil {
		return nil, fmt.Errorf("mapper tempdir: %w", err)
	}
	defer os.RemoveAll(mapDir)
	if err := os.WriteFile(filepath.Join(mapDir, "binance.json"),
		[]byte(`{"BTCUSDT":"BTC/USD"}`), 0644); err != nil {
		return nil, fmt.Errorf("stub mapping: %w", err)
	}
	sm, err := mapper.NewSymbolMapper(mapDir)
	if err != nil {
		return nil, fmt.Errorf("mapper: %w", err)
	}
	canon := canonicalizer.NewCanonicalizer(sm)

	// WAL in a temp dir.
	walDir, err := os.MkdirTemp("", "rdl-bench-wal-*")
	if err != nil {
		return nil, fmt.Errorf("wal tempdir: %w", err)
	}
	defer os.RemoveAll(walDir)
	walCfg := storage.WALConfig{
		Directory:     walDir,
		MaxFileSize:    100 * 1024 * 1024,
		MaxMessages:    10000,
		RotateInterval: time.Minute,
	}

	var wal benchWAL
	switch cfg.WALMode {
	case "sync", "":
		w, e := storage.NewWAL(walCfg)
		if e != nil {
			return nil, fmt.Errorf("newwal: %w", e)
		}
		if e := w.Start(); e != nil {
			return nil, fmt.Errorf("wal start: %w", e)
		}
		wal = w
		defer func() { _ = w.Stop() }()
	case "batched":
		batchTO := time.Duration(cfg.BatchMs) * time.Millisecond
		if batchTO <= 0 {
			batchTO = 50 * time.Millisecond
		}
		w, e := storage.NewBatchedWAL(walCfg, batchTO)
		if e != nil {
			return nil, fmt.Errorf("newbatchedwal: %w", e)
		}
		if e := w.Start(); e != nil {
			return nil, fmt.Errorf("batchedwal start: %w", e)
		}
		wal = w
		// Defer runs after the return value (res) is assigned but before it is
		// returned, so it can patch res with the post-Stop flush count. Stop
		// happens AFTER ReadMemStats is captured (below), so GC/heap reflect the
		// loop only — the final fsync does not count against throughput.
		defer func() {
			_ = w.Stop()
			if res != nil {
				res.WALTotalFlushes = w.TotalFlushes()
			}
		}()
	default:
		return nil, fmt.Errorf("unknown wal_mode: %q (want sync|batched)", cfg.WALMode)
	}

	// Fixed Binance aggTrade payload matching parser.Trade JSON tags. Reused
	// verbatim per message (no Sprintf allocation) — isolates canon.Process +
	// WAL.Write cost from payload-construction cost. (A real adapter builds the
	// payload once off-wire; it does not Sprintf per message either.)
	payload := []byte(`{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":1,"p":"50000.00","q":"0.5","T":1700000000000,"m":false}`)
	ctx := context.Background()

	// Warmup (discarded) — prime JIT, pool, bufio, page cache.
	for i := 0; i < cfg.Warmup; i++ {
		raw := adapter.RawMessage{Source: "BINANCE", Payload: payload, ReceivedAt: time.Now().UnixNano(), SequenceNum: uint64(i)}
		pm, _ := canon.Process(ctx, raw)
		if ev, ok := pm.Canonical.(*canonicalizer.CanonicalEvent); ok && ev != nil {
			_ = wal.Write(ev)
			canonicalizer.ReleaseEvent(ev)
		}
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	latencies := make([]int64, 0, cfg.Messages)
	loopStart := time.Now()
	for i := 0; i < cfg.Messages; i++ {
		raw := adapter.RawMessage{Source: "BINANCE", Payload: payload, ReceivedAt: time.Now().UnixNano(), SequenceNum: uint64(i)}
		t0 := time.Now()
		pm, _ := canon.Process(ctx, raw)
		ev, ok := pm.Canonical.(*canonicalizer.CanonicalEvent)
		if !ok || ev == nil {
			return nil, fmt.Errorf("canon.Process returned non-*CanonicalEvent at msg %d (pool contract violated)", i)
		}
		if e := wal.Write(ev); e != nil {
			return nil, fmt.Errorf("wal write[%d]: %w", i, e)
		}
		t1 := time.Now()
		canonicalizer.ReleaseEvent(ev)
		latencies = append(latencies, t1.Sub(t0).Nanoseconds())
	}
	loopEnd := time.Now()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	// WAL Stop runs via defer AFTER this point — but MemStats is already
	// captured, so GC/heap reflect the measured loop only (the Stop's final
	// fsync does not count against throughput or memory).

	// Percentiles.
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) int64 {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(float64(len(latencies)) * p / 100)
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		return latencies[idx]
	}
	var latMax int64
	if len(latencies) > 0 {
		latMax = latencies[len(latencies)-1]
	}

	// GC pause stats. PauseNs[256] is circular; only the last 256 GC pauses are
	// retained, so the max is reliable only when the loop triggered ≤256 GCs.
	gcTotal := after.PauseTotalNs - before.PauseTotalNs
	gcNum := after.NumGC - before.NumGC
	var gcMax uint64
	if gcNum <= 256 {
		for g := before.NumGC; g < after.NumGC; g++ {
			if p := after.PauseNs[g%256]; p > gcMax {
				gcMax = p
			}
		}
	} // else gcMax stays 0 (unavailable — honest)

	elapsed := loopEnd.Sub(loopStart).Seconds()
	if elapsed <= 0 {
		elapsed = 1e-9
	}
	stats := wal.Stats()
	files, _ := filepath.Glob(filepath.Join(walDir, "wal_*.jsonl"))

	return &Result{
		WALMode:              resolvedMode(cfg.WALMode),
		Messages:             cfg.Messages,
		Warmup:               cfg.Warmup,
		ThroughputMsgsPerSec: float64(cfg.Messages) / elapsed,
		LatencyP50Ns:         pct(50),
		LatencyP95Ns:         pct(95),
		LatencyP99Ns:         pct(99),
		LatencyMaxNs:         latMax,
		GCPauseTotalNs:       gcTotal,
		GCPauseMaxNs:         gcMax,
		GCNumGC:              gcNum,
		HeapAllocMB:          float64(after.HeapAlloc) / 1e6,
		SysMB:                float64(after.Sys) / 1e6,
		Goroutines:           runtime.NumGoroutine(),
		WALFiles:             len(files),
		WALTotalWritten:      stats.TotalWritten,
		WALTotalFlushes:      0, // batched: patched by deferred Stop above
	}, nil
}

func resolvedMode(m string) string {
	if m == "" {
		return "sync"
	}
	return m
}
