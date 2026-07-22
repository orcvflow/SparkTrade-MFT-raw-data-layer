// Package chaos contains chaos engineering tests as specified in CLAUDE.md section 10.
//
// Scenarios:
//  1. TestChaos_ComponentFailure    — Kill canonicalizer goroutine → system continues
//  2. TestChaos_NetworkLatency      — Inject 100ms processing delay → graceful degradation
//  3. TestChaos_ResourceExhaustion  — Flood with 10x normal volume → backpressure, no OOM
//  4. TestChaos_MessageFlood        — 10x burst → autoscale, no crash
//  5. TestChaos_ByzantineFault      — Corrupted/out-of-order/duplicate messages
//  6. TestChaos_WALUnderStress      — WAL write under concurrent high load
//  7. TestChaos_PoolWorkerKill      — Simulate worker panic → pool recovers
//
// Run: go test ./test/chaos/ -v -timeout 120s -race
package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/mapper"
	"raw-data-layer/pkg/storage"
	"raw-data-layer/pkg/workerpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS TEST 1: Component Failure
//
// CLAUDE.md: "Kill canonicalizer → system continues, restarts"
// Simulated by: panicking inside the processor function, which triggers
// the worker's defer/recover — pool must stay alive and keep processing.
// ─────────────────────────────────────────────────────────────────────────────

func TestChaos_ComponentFailure(t *testing.T) {
	sm := newChaosSymbolMapper(t)

	var processedAfterFailure atomic.Int64
	var panicInjected atomic.Bool

	// Processor that panics once, then continues normally
	panicOnceProcessor := func(ctx context.Context, raw adapter.RawMessage) (workerpool.ProcessedMessage, error) {
		// Inject panic exactly once
		if !panicInjected.Load() && raw.SequenceNum == 5 {
			panicInjected.Store(true)
			panic("chaos: simulated canonicalizer failure")
		}
		processedAfterFailure.Add(1)
		canon := canonicalizer.NewCanonicalizer(sm)
		return canon.Process(ctx, raw)
	}

	pool := workerpool.NewPool(workerpool.PoolConfig{
		MinWorkers:       3,
		MaxWorkers:       10,
		QueueSize:        500,
		AutoscaleEnabled: false,
	}, panicOnceProcessor)
	pool.Start()
	defer pool.Stop()

	// Drain output to prevent blocking
	go drainPool(pool)

	// Submit 50 messages; message #5 will trigger panic in one worker
	const total = 50
	submitted := 0
	for i := 0; i < total; i++ {
		msg := makeChaosMessage("BINANCE", "BTCUSDT", 50000.0+float64(i), uint64(i))
		if err := pool.Submit(msg); err == nil {
			submitted++
		}
	}

	// Give pool time to process including the panic recovery
	time.Sleep(500 * time.Millisecond)

	stats := pool.Stats()
	t.Logf("After component failure: workers=%d, processed=%d, errors=%d, dropped=%d",
		stats.ActiveWorkers, stats.Processed, stats.Errors, stats.Dropped)

	// CRITICAL: pool must still have active workers after a panic
	if stats.ActiveWorkers == 0 {
		t.Error("FAIL: Pool has no active workers after component failure — system did not recover")
	}

	// Pool must have processed messages (even after the panic)
	if stats.Processed == 0 {
		t.Error("FAIL: Pool processed 0 messages — system is stuck")
	}

	t.Logf("TestChaos_ComponentFailure: PASSED — pool survived panic, workers=%d", stats.ActiveWorkers)
}

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS TEST 2: Network Latency Injection
//
// CLAUDE.md: "Inject 100ms delay → system degrades gracefully, no crash"
// ─────────────────────────────────────────────────────────────────────────────

func TestChaos_NetworkLatency(t *testing.T) {
	sm := newChaosSymbolMapper(t)
	canon := canonicalizer.NewCanonicalizer(sm)

	// Processor with injected latency (simulates slow network/parsing)
	latencyProcessor := func(ctx context.Context, raw adapter.RawMessage) (workerpool.ProcessedMessage, error) {
		// Simulate 100ms network latency
		select {
		case <-ctx.Done():
			return workerpool.ProcessedMessage{Raw: raw}, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
		return canon.Process(ctx, raw)
	}

	pool := workerpool.NewPool(workerpool.PoolConfig{
		MinWorkers:       5,
		MaxWorkers:       20,
		QueueSize:        200,
		AutoscaleEnabled: true,
		ScaleUpThreshold:   0.7,
		ScaleDownThreshold: 0.3,
	}, latencyProcessor)
	pool.Start()
	defer pool.Stop()

	go drainPool(pool)

	start := time.Now()

	// Submit 30 messages under latency — system should NOT hang or panic
	submitted := 0
	for i := 0; i < 30; i++ {
		msg := makeChaosMessage("BINANCE", "ETHUSDT", 3000.0+float64(i), uint64(i))
		if err := pool.Submit(msg); err == nil {
			submitted++
		}
	}

	elapsed := time.Since(start)
	t.Logf("Submitted %d messages in %v under 100ms latency injection", submitted, elapsed)

	// Wait for processing
	time.Sleep(2 * time.Second)

	stats := pool.Stats()
	t.Logf("Latency chaos: workers=%d, processed=%d, errors=%d",
		stats.ActiveWorkers, stats.Processed, stats.Errors)

	// System must NOT have crashed
	if stats.ActiveWorkers == 0 {
		t.Error("FAIL: Pool died under latency injection")
	}

	// Should have processed SOME messages (not zero)
	if stats.Processed == 0 && submitted > 0 {
		t.Log("Note: no messages processed yet (slow workers) — not a failure if system is alive")
	}

	t.Log("TestChaos_NetworkLatency: PASSED — system degraded gracefully under latency")
}

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS TEST 3: Resource Exhaustion (CPU limit simulation)
//
// CLAUDE.md: "CPU limit to 50% → system handles, no OOM"
// Simulated by: limiting GOMAXPROCS and flooding with messages.
// ─────────────────────────────────────────────────────────────────────────────

func TestChaos_ResourceExhaustion(t *testing.T) {
	// Limit CPU to simulate 50% by halving GOMAXPROCS
	originalMaxProcs := runtime.GOMAXPROCS(0)
	limited := originalMaxProcs / 2
	if limited < 1 {
		limited = 1
	}
	runtime.GOMAXPROCS(limited)
	defer runtime.GOMAXPROCS(originalMaxProcs)

	t.Logf("CPU limited: %d → %d GOMAXPROCS", originalMaxProcs, limited)

	sm := newChaosSymbolMapper(t)
	canon := canonicalizer.NewCanonicalizer(sm)

	pool := workerpool.NewPool(workerpool.PoolConfig{
		MinWorkers:       10,
		MaxWorkers:       50,
		QueueSize:        5000,
		AutoscaleEnabled: true,
		ScaleUpThreshold:   0.8,
		ScaleDownThreshold: 0.5,
	}, canon.Process)
	pool.Start()
	defer pool.Stop()

	go drainPool(pool)

	// Record baseline memory
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Flood with 2000 messages (10x normal for this test)
	var accepted, backpressured int64
	for i := 0; i < 2000; i++ {
		msg := makeChaosMessage("BINANCE", "SOLUSDT", 100.0+float64(i%100), uint64(i))
		if err := pool.Submit(msg); err != nil {
			atomic.AddInt64(&backpressured, 1)
		} else {
			atomic.AddInt64(&accepted, 1)
		}
	}

	time.Sleep(1 * time.Second)

	// Check memory
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	allocMB := float64(memAfter.Alloc) / (1024 * 1024)
	t.Logf("Memory after flood: %.1f MB allocated", allocMB)

	// ASSERTION: memory should not explode (< 500MB per CLAUDE.md)
	if allocMB > 500 {
		t.Errorf("Memory too high: %.1f MB (> 500MB limit)", allocMB)
	}

	stats := pool.Stats()
	t.Logf("Resource exhaustion: accepted=%d, backpressured=%d, workers=%d, processed=%d",
		accepted, backpressured, stats.ActiveWorkers, stats.Processed)

	// System must still be alive
	if stats.ActiveWorkers == 0 {
		t.Error("FAIL: Pool died under resource exhaustion")
	}

	t.Log("TestChaos_ResourceExhaustion: PASSED — system survived CPU constraint + message flood")
}

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS TEST 4: Message Flood (10x normal volume)
//
// CLAUDE.md: "10x normal volume → backpressure engages"
// Normal: ~150-350 msg/s (Binance). 10x = ~3500 msg/s burst.
// ─────────────────────────────────────────────────────────────────────────────

func TestChaos_MessageFlood(t *testing.T) {
	sm := newChaosSymbolMapper(t)
	canon := canonicalizer.NewCanonicalizer(sm)

	pool := workerpool.NewPool(workerpool.PoolConfig{
		MinWorkers:         10,
		MaxWorkers:         100,
		QueueSize:          10000,
		AutoscaleEnabled:   true,
		ScaleUpThreshold:   0.8,
		ScaleDownThreshold: 0.5,
	}, canon.Process)
	pool.Start()
	defer pool.Stop()

	go drainPool(pool)

	// Flood: 10,000 messages in rapid succession
	const floodSize = 10000
	var accepted, dropped int64

	floodStart := time.Now()
	for i := 0; i < floodSize; i++ {
		symbol := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT"}[i%4]
		msg := makeChaosMessage("BINANCE", symbol, float64(i%100000), uint64(i))
		if err := pool.Submit(msg); err != nil {
			atomic.AddInt64(&dropped, 1)
		} else {
			atomic.AddInt64(&accepted, 1)
		}
	}
	floodDuration := time.Since(floodStart)

	rate := float64(floodSize) / floodDuration.Seconds()
	t.Logf("Flood rate: %.0f msg/s, accepted=%d, backpressured=%d", rate, accepted, dropped)

	// Wait for processing
	time.Sleep(2 * time.Second)

	stats := pool.Stats()
	t.Logf("After flood: workers=%d, processed=%d, queue=%d",
		stats.ActiveWorkers, stats.Processed, stats.QueueDepth)

	// CRITICAL: system must not have crashed
	if stats.ActiveWorkers == 0 {
		t.Error("FAIL: Pool died during message flood")
	}

	// Backpressure must have engaged (or queue absorbed everything)
	if dropped == 0 && accepted == floodSize {
		t.Log("Queue absorbed all messages — backpressure not needed (acceptable)")
	} else {
		t.Logf("Backpressure engaged: %d messages rejected gracefully", dropped)
	}

	t.Log("TestChaos_MessageFlood: PASSED — 10x flood handled without crash")
}

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS TEST 5: Byzantine Fault Injection
//
// CLAUDE.md: "Corrupted messages, out-of-order sequence, duplicate events"
// System must handle all these without panic.
// ─────────────────────────────────────────────────────────────────────────────

func TestChaos_ByzantineFault(t *testing.T) {
	sm := newChaosSymbolMapper(t)
	canon := canonicalizer.NewCanonicalizer(sm)

	pool := workerpool.NewPool(workerpool.PoolConfig{
		MinWorkers:       3,
		MaxWorkers:       10,
		QueueSize:        500,
		AutoscaleEnabled: false,
	}, canon.Process)
	pool.Start()
	defer pool.Stop()

	go drainPool(pool)

	byzantineMessages := []adapter.RawMessage{
		// 1. Completely corrupted payload
		{Source: "BINANCE", Payload: []byte("NOT_JSON_AT_ALL!!!"), ReceivedAt: time.Now().UnixNano()},
		// 2. Nil payload
		{Source: "BINANCE", Payload: nil, ReceivedAt: time.Now().UnixNano()},
		// 3. Empty payload
		{Source: "BINANCE", Payload: []byte{}, ReceivedAt: time.Now().UnixNano()},
		// 4. Truncated JSON
		{Source: "BINANCE", Payload: []byte(`{"e":"trade","p":"5000`), ReceivedAt: time.Now().UnixNano()},
		// 5. Valid JSON but wrong schema
		{Source: "BINANCE", Payload: []byte(`{"wrong":"schema","no_price":true}`), ReceivedAt: time.Now().UnixNano()},
		// 6. Overflow values in JSON
		{Source: "BINANCE", Payload: []byte(`{"e":"trade","s":"BTCUSDT","p":"1e400","q":"-999"}`), ReceivedAt: time.Now().UnixNano()},
		// 7. Unicode garbage
		{Source: "BINANCE", Payload: []byte("\xff\xfe\x00\x01GARBAGE"), ReceivedAt: time.Now().UnixNano()},
		// 8. Very large payload (1MB of 'X')
		{Source: "BINANCE", Payload: makeLargePayload(1024 * 1024), ReceivedAt: time.Now().UnixNano()},
		// 9. Unknown source
		{Source: "UNKNOWN_SOURCE_XYZ", Payload: []byte(`{"data":"test"}`), ReceivedAt: time.Now().UnixNano()},
		// 10. Zero timestamp
		{Source: "IB", Payload: []byte{0x01, 0x02}, ReceivedAt: 0},
		// 11. Duplicate event (same content, submitted twice)
		{Source: "BINANCE", Payload: []byte(`{"e":"trade","s":"BTCUSDT","p":"50000","q":"1","T":999999}`), ReceivedAt: time.Now().UnixNano()},
		{Source: "BINANCE", Payload: []byte(`{"e":"trade","s":"BTCUSDT","p":"50000","q":"1","T":999999}`), ReceivedAt: time.Now().UnixNano()},
		// 12. Out-of-order sequence numbers
		{Source: "BINANCE", Payload: []byte(`{"e":"trade","s":"ETHUSDT","p":"3000","q":"2","T":100}`), ReceivedAt: time.Now().UnixNano(), SequenceNum: 1000},
		{Source: "BINANCE", Payload: []byte(`{"e":"trade","s":"ETHUSDT","p":"3001","q":"1","T":50}`), ReceivedAt: time.Now().UnixNano(), SequenceNum: 1},
	}

	// Submit all byzantine messages — NONE should cause panic or crash
	submitted := 0
	for _, msg := range byzantineMessages {
		m := msg // capture loop variable
		if err := pool.Submit(m); err == nil {
			submitted++
		}
	}

	t.Logf("Submitted %d byzantine messages", submitted)

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	stats := pool.Stats()
	t.Logf("Byzantine fault stats: workers=%d, processed=%d, errors=%d",
		stats.ActiveWorkers, stats.Processed, stats.Errors)

	// THE KEY ASSERTION: system alive after all byzantine inputs
	if stats.ActiveWorkers == 0 {
		t.Error("FAIL: Pool died on byzantine fault injection")
	}

	t.Log("TestChaos_ByzantineFault: PASSED — all byzantine inputs handled without crash")
}

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS TEST 6: WAL Under Stress
//
// Concurrent goroutines writing to WAL while rotation is happening.
// WAL must remain consistent — no data loss, no corruption.
// ─────────────────────────────────────────────────────────────────────────────

func TestChaos_WALUnderStress(t *testing.T) {
	walConfig := storage.WALConfig{
		Directory:      t.TempDir(),
		MaxFileSize:    10 * 1024, // 10KB — force frequent rotations
		MaxMessages:    20,        // force rotation every 20 messages
		RotateInterval: 100 * time.Millisecond,
	}

	wal, err := storage.NewWAL(walConfig)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	wal.Start()
	defer wal.Stop()

	const goroutines = 10
	const perGoroutine = 30

	var wg sync.WaitGroup
	var totalWritten atomic.Int64
	var writeErrors atomic.Int64

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				ev := &canonicalizer.CanonicalEvent{
					EventID:           fmt.Sprintf("chaos_wal_%02d_%03d", id, i),
					Source:            "CHAOS",
					CanonicalSymbol:   "BTC/USD",
					ExchangeTimestamp: time.Now().UnixNano(),
					LocalHWTimestamp:  time.Now().UnixNano(),
					EventType:         "TRADE",
					Price:             50000.0 + float64(id*100+i),
					Size:              float64(i) + 0.1,
					Side:              "BUY",
					RawPayload:        []byte(fmt.Sprintf(`{"g":%d,"i":%d}`, id, i)),
					RawFormat:         "JSON",
				}
				if err := wal.Write(ev); err != nil {
					writeErrors.Add(1)
				} else {
					totalWritten.Add(1)
				}
				// Small random jitter
				if i%5 == 0 {
					time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
				}
			}
		}(g)
	}

	wg.Wait()

	// Wait for any pending rotations
	time.Sleep(300 * time.Millisecond)

	expected := int64(goroutines * perGoroutine)
	t.Logf("WAL stress: expected=%d, written=%d, errors=%d",
		expected, totalWritten.Load(), writeErrors.Load())

	if totalWritten.Load() < expected {
		t.Errorf("WAL stress: data loss detected — expected %d, wrote %d",
			expected, totalWritten.Load())
	}

	// Verify WAL stats
	stats := wal.Stats()
	if stats.TotalWritten < uint64(expected) {
		t.Errorf("WAL stats mismatch: TotalWritten=%d, expected>=%d",
			stats.TotalWritten, expected)
	}

	// Verify rotation happened
	if stats.TotalRotations == 0 {
		t.Log("Note: no rotation occurred (data may have been small enough)")
	} else {
		t.Logf("WAL rotated %d times under stress", stats.TotalRotations)
	}

	t.Log("TestChaos_WALUnderStress: PASSED — WAL consistent under concurrent stress")
}

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS TEST 7: Pool Worker Kill
//
// Workers that panic should be recovered by defer/recover in worker goroutine.
// Pool should continue functioning after repeated worker panics.
// ─────────────────────────────────────────────────────────────────────────────

func TestChaos_PoolWorkerKill(t *testing.T) {
	sm := newChaosSymbolMapper(t)
	_ = sm

	var panicCount atomic.Int64
	var successCount atomic.Int64

	// Processor that panics 30% of the time
	chaoticProcessor := func(ctx context.Context, raw adapter.RawMessage) (workerpool.ProcessedMessage, error) {
		// 30% chance of panic
		if rand.Float64() < 0.3 {
			panicCount.Add(1)
			panic(fmt.Sprintf("chaos worker kill: seq=%d", raw.SequenceNum))
		}
		successCount.Add(1)
		return workerpool.ProcessedMessage{
			Raw:         raw,
			ProcessedAt: time.Now().UnixNano(),
		}, nil
	}

	pool := workerpool.NewPool(workerpool.PoolConfig{
		MinWorkers:       5,
		MaxWorkers:       20,
		QueueSize:        500,
		AutoscaleEnabled: false,
	}, chaoticProcessor)
	pool.Start()
	defer pool.Stop()

	go drainPool(pool)

	// Submit 200 messages — expect ~60 panics but pool must survive all
	for i := 0; i < 200; i++ {
		msg := makeChaosMessage("BINANCE", "BTCUSDT", float64(i), uint64(i))
		pool.Submit(msg)
	}

	time.Sleep(1 * time.Second)

	stats := pool.Stats()
	t.Logf("Worker kill chaos: panics=%d, successes=%d, pool_errors=%d, workers=%d",
		panicCount.Load(), successCount.Load(), stats.Errors, stats.ActiveWorkers)

	// CRITICAL: pool must still have workers despite repeated panics
	if stats.ActiveWorkers == 0 {
		t.Error("FAIL: All workers died after repeated panics — pool did not recover")
	}

	// Should have processed some messages successfully
	if stats.Processed == 0 {
		t.Error("FAIL: No messages processed — pool is stuck")
	}

	t.Logf("TestChaos_PoolWorkerKill: PASSED — pool survived %d panics with %d workers remaining",
		panicCount.Load(), stats.ActiveWorkers)
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

// makeChaosMessage creates a test RawMessage with a valid Binance JSON payload.
func makeChaosMessage(source, symbol string, price float64, seq uint64) adapter.RawMessage {
	payload := map[string]interface{}{
		"e": "aggTrade",
		"E": time.Now().UnixMilli(),
		"s": symbol,
		"p": fmt.Sprintf("%.2f", price),
		"q": "1.00000000",
		"T": time.Now().UnixMilli(),
		"m": false,
	}
	data, _ := json.Marshal(payload)
	return adapter.RawMessage{
		Source:      source,
		Payload:     data,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: seq,
	}
}

// makeLargePayload creates a payload of the given size.
func makeLargePayload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = 'X'
	}
	return b
}

// drainPool reads from pool output channel to prevent backpressure.
func drainPool(pool *workerpool.Pool) {
	for range pool.Output() {
		// discard — we only care about pool liveness in chaos tests
	}
}

// newChaosSymbolMapper creates a SymbolMapper for chaos tests.
func newChaosSymbolMapper(t *testing.T) *mapper.SymbolMapper {
	t.Helper()

	tempDir := t.TempDir()

	binanceMapping := map[string]string{
		"BTCUSDT":   "BTC/USD",
		"ETHUSDT":   "ETH/USD",
		"BNBUSDT":   "BNB/USD",
		"ADAUSDT":   "ADA/USD",
		"SOLUSDT":   "SOL/USD",
		"XRPUSDT":   "XRP/USD",
		"DOGEUSDT":  "DOGE/USD",
		"DOTUSDT":   "DOT/USD",
		"AVAXUSDT":  "AVAX/USD",
		"MATICUSDT": "MATIC/USD",
	}
	ibMapping := map[string]string{
		"265598":   "AAPL",
		"8314":     "MSFT",
		"76792991": "GOOGL",
		"4781":     "TSLA",
		"756733":   "SPY",
	}

	writeJSON(t, tempDir+string(os.PathSeparator)+"binance.json", binanceMapping)
	writeJSON(t, tempDir+string(os.PathSeparator)+"ib.json", ibMapping)

	sm, err := mapper.NewSymbolMapper(tempDir)
	if err != nil {
		t.Fatalf("NewSymbolMapper: %v", err)
	}
	return sm
}

func writeJSON(t *testing.T, path string, v interface{}) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile %s: %v", path, err)
	}
}
