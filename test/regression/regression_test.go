//go:build regression

// Package regression contains Addım D performance regression guards.
//
// These tests are MACHINE-CHECKED RELATIVE invariants, not absolute marketing
// numbers. The spec's "p99 < 500µs / throughput > 200K / GC < 100ms / memory <
// 2GB" targets are hardware- and deployment-dependent (measured by the
// production benchmark, Addım D Task 9). Asserting absolute numbers in a unit
// test would be flaky: they pass on a fast box and fail on a slow CI runner for
// reasons unrelated to the code.
//
// Each test instead asserts that the Addım D optimization still BEATS its
// baseline on the SAME machine, so the ratio is stable across hardware:
//
//   - Sonic parse   < encoding/json parse   (ns + allocs)
//   - Sonic parse   < old map[string]any     (ns, ≥1.5×)
//   - mmap ITCH     < bufio ITCH            (ns)
//   - sync.Pool     < new()                 (allocs, deterministic)
//   - pooled Process < non-pooled Process   (allocs, deterministic)
//   - lock-free OB  < mutex OB             (ns, single-threaded)
//   - pool reuse    preserves raw_payload  (no use-after-reset, value check)
//
// If any optimization is reverted (Sonic→encoding/json, mmap→bufio, pool→new,
// atomic→mutex), the corresponding test FAILS. That is the regression guard.
//
// RUN WITHOUT -race: these are performance tests. The -race detector distorts
// timing ~20-30× (it instruments every memory access, which penalizes Sonic's
// JIT and mmap's per-byte At() access so heavily that the mmap/bufio ratio even
// inverts). Race-safety is covered separately by the main `go test -race ./...`
// suite (pkg/orderbook's TestLockFreeOrderBook_Concurrent). So:
//
//	go test -tags=regression ./test/regression/... -v
//
// (Alloc-count and correctness tests here are -race-safe and would also pass
// under -race; only the ns comparisons require the no-race run.)
package regression

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/mapper"
	"raw-data-layer/pkg/orderbook"
	"raw-data-layer/pkg/parser"
)

const regressionPayload = `{"e":"aggTrade","E":1234567890,"s":"BTCUSDT","a":12345,"p":"50000.00","q":"0.5","T":1234567890,"m":false}`

// timed runs f N times and returns total elapsed. For relative ns comparisons
// only (absolute ns/op would be hardware-dependent).
func timed(N int, f func()) time.Duration {
	start := time.Now()
	for i := 0; i < N; i++ {
		f()
	}
	return time.Since(start)
}

// sink forces its argument to escape the closure so the compiler does not
// stack-allocate it (which would make alloc comparisons meaningless).
var sink any

// TestRegression_SonicFasterThanStdlib: Sonic must beat encoding/json on the
// same typed Trade struct (same struct, only the decoder differs).
func TestRegression_SonicFasterThanStdlib(t *testing.T) {
	data := []byte(regressionPayload)
	p := parser.NewSonicParser()
	var tr parser.Trade

	const N = 20000
	sonicDur := timed(N, func() { _ = p.ParseTradeInto(data, &tr) })
	stdDur := timed(N, func() { _, _ = parser.ParseTradeStd(data) })

	if sonicDur >= stdDur {
		t.Errorf("Sonic ns regression: sonic=%v >= stdlib=%v", sonicDur, stdDur)
	}

	sonicAllocs := testing.AllocsPerRun(100, func() { _ = p.ParseTradeInto(data, &tr) })
	stdAllocs := testing.AllocsPerRun(100, func() { _, _ = parser.ParseTradeStd(data) })
	if sonicAllocs > stdAllocs {
		t.Errorf("Sonic alloc regression: sonic=%v > stdlib=%v", sonicAllocs, stdAllocs)
	}
	t.Logf("Sonic %v/%.1f allocs vs stdlib %v/%.1f allocs", sonicDur, sonicAllocs, stdDur, stdAllocs)
}

// TestRegression_SonicFasterThanMap: Sonic-into-struct must beat the OLD
// canonicalizer path (encoding/json into map[string]any) by ≥1.5×. This is the
// guard that catches a Sonic→map revert (a revert makes them ~equal → fail).
func TestRegression_SonicFasterThanMap(t *testing.T) {
	data := []byte(regressionPayload)
	p := parser.NewSonicParser()
	var tr parser.Trade

	const N = 20000
	sonicDur := timed(N, func() { _ = p.ParseTradeInto(data, &tr) })
	mapDur := timed(N, func() { _, _ = parser.ParseTradeMapStd(data) })

	if sonicDur*3 >= mapDur*2 { // sonic must be ≥1.5× faster (measured ~5×)
		t.Errorf("Sonic-vs-map regression: sonic=%v, map=%v (need ≥1.5×)", sonicDur, mapDur)
	}
	t.Logf("Sonic %v vs map %v (%.1fx)", sonicDur, mapDur, float64(mapDur)/float64(sonicDur))
}

// TestRegression_MMapFasterThanBufio: the mmap zero-copy ITCH parser must beat
// the bufio baseline on the same fixture. Each iteration re-opens the file
// (matching the benchmark) — bufio.Reader is stateful, so reusing one reader
// across iterations would read-to-EOF once and then no-op, making the
// comparison meaningless.
func TestRegression_MMapFasterThanBufio(t *testing.T) {
	path := writeITCHFixtureReg(t, 50000)

	mDur := timed(20, func() {
		mp, err := parser.NewITCHParser(path)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = mp.CountAll()
		_ = mp.Close()
	})
	bDur := timed(20, func() {
		br, err := parser.NewITCHBufioReader(path)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = br.CountAll()
		_ = br.Close()
	})

	if mDur >= bDur {
		t.Errorf("mmap regression: mmap=%v >= bufio=%v", mDur, bDur)
	}
	t.Logf("mmap %v vs bufio %v (%.2fx)", mDur, bDur, float64(bDur)/float64(mDur))
}

// TestRegression_PoolFewerAllocs: sync.Pool must allocate less than new() for
// the real pooled object (canonicalizer.CanonicalEvent), which escapes (stored
// as `any` in ProcessedMessage) and so heap-allocates when not pooled. This is
// the GC/memory proxy: fewer allocs ⇒ less GC pressure ⇒ lower memory.
func TestRegression_PoolFewerAllocs(t *testing.T) {
	poolAllocs := testing.AllocsPerRun(100, func() {
		ev := canonicalizer.AcquireEvent()
		ev.Price = 1.0
		canonicalizer.ReleaseEvent(ev)
	})
	newAllocs := testing.AllocsPerRun(100, func() {
		ev := &canonicalizer.CanonicalEvent{}
		ev.Price = 1.0
		sink = ev // force heap escape (mirrors ProcessedMessage.Canonical any)
	})
	if poolAllocs >= newAllocs {
		t.Errorf("pool alloc regression: pool=%.2f >= new=%.2f", poolAllocs, newAllocs)
	}
	t.Logf("pool %.2f allocs vs new %.2f allocs", poolAllocs, newAllocs)
}

// TestRegression_CanonicalizerPooledFewerAllocs: the pooled Process lifecycle
// (Acquire→Process→Release) must allocate less than the non-pooled Process
// (Acquire without Release). Deterministic alloc comparison — no timing.
func TestRegression_CanonicalizerPooledFewerAllocs(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "binance.json"), []byte(`{"BTCUSDT":"BTC/USD"}`), 0644); err != nil {
		t.Fatal(err)
	}
	m, err := mapper.NewSymbolMapper(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := canonicalizer.NewCanonicalizer(m)
	ctx := context.Background()
	raw := adapter.RawMessage{Source: "BINANCE", Payload: []byte(regressionPayload), ReceivedAt: 1}

	pooledAllocs := testing.AllocsPerRun(100, func() {
		pm, _ := c.Process(ctx, raw)
		if ev, ok := pm.Canonical.(*canonicalizer.CanonicalEvent); ok {
			canonicalizer.ReleaseEvent(ev)
		}
	})
	nonPooledAllocs := testing.AllocsPerRun(100, func() {
		pm, _ := c.Process(ctx, raw)
		// deliberately do NOT release → the event is not recycled
		_ = pm
	})
	if pooledAllocs >= nonPooledAllocs {
		t.Errorf("pooled Process alloc regression: pooled=%.2f >= non-pooled=%.2f", pooledAllocs, nonPooledAllocs)
	}
	t.Logf("pooled %.2f allocs vs non-pooled %.2f allocs", pooledAllocs, nonPooledAllocs)
}

// TestRegression_LockFreeFasterThanMutex: lock-free reads must beat the RWMutex
// baseline single-threaded (mutex path copies the snapshot under RLock; lock-free
// returns an immutable slice with no copy and no lock).
func TestRegression_LockFreeFasterThanMutex(t *testing.T) {
	levels := make([]orderbook.Level, 64)
	for i := range levels {
		levels[i] = orderbook.Level{Price: float64(i), Size: 1, OrderID: int64(i)}
	}
	lf := orderbook.NewLockFreeOrderBook()
	lf.SetBids(levels)
	mu := orderbook.NewMutexOrderBook()
	mu.SetBids(levels)

	const N = 100000
	lfDur := timed(N, func() { _ = lf.Bids() })
	muDur := timed(N, func() { _ = mu.Bids() })

	if lfDur >= muDur {
		t.Errorf("lock-free regression: lockfree=%v >= mutex=%v", lfDur, muDur)
	}
	t.Logf("lock-free %v vs mutex %v (%.1fx)", lfDur, muDur, float64(muDur)/float64(lfDur))
}

// TestRegression_PoolRawPayloadIntegrity: the Addım D pool integration must not
// introduce use-after-reset. Acquire → fill msg1 → Release → Acquire (may get
// the same event) → fill msg2 → msg2's price + raw_payload must equal msg2's
// inputs, not msg1's. Uses a MAPPED symbol so the canonical symbol is stable
// (unmapped symbols collapse to "UNKNOWN" by design — that is not a pool bug).
func TestRegression_PoolRawPayloadIntegrity(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "binance.json"), []byte(`{"BTCUSDT":"BTC/USD"}`), 0644); err != nil {
		t.Fatal(err)
	}
	m, err := mapper.NewSymbolMapper(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := canonicalizer.NewCanonicalizer(m)
	ctx := context.Background()

	// Both payloads use BTCUSDT (mapped → BTC/USD) so the symbol is stable; the
	// distinguishing fields are price + raw_payload, which Reset must clear.
	payload1 := []byte(`{"e":"trade","s":"BTCUSDT","p":"50000.0","q":"1.0","T":1}`)
	payload2 := []byte(`{"e":"trade","s":"BTCUSDT","p":"3000.0","q":"2.0","T":2}`)
	raw1 := adapter.RawMessage{Source: "BINANCE", Payload: payload1, ReceivedAt: 1}
	raw2 := adapter.RawMessage{Source: "BINANCE", Payload: payload2, ReceivedAt: 2}

	pm1, _ := c.Process(ctx, raw1)
	ev1 := pm1.Canonical.(*canonicalizer.CanonicalEvent)
	canonicalizer.ReleaseEvent(ev1)

	pm2, _ := c.Process(ctx, raw2)
	ev2 := pm2.Canonical.(*canonicalizer.CanonicalEvent)
	defer canonicalizer.ReleaseEvent(ev2)

	if ev2.Price != 3000.0 {
		t.Errorf("use-after-reset: ev2.Price=%v, want 3000 (Reset must clear before reuse)", ev2.Price)
	}
	if ev2.Size != 2.0 {
		t.Errorf("use-after-reset: ev2.Size=%v, want 2", ev2.Size)
	}
	if !bytes.Equal(ev2.RawPayload, payload2) {
		t.Errorf("use-after-reset: ev2.RawPayload=%q, want %q", ev2.RawPayload, payload2)
	}
}

// writeITCHFixtureReg writes n ITCH-like records to a temp file (self-contained
// under the regression build tag). Layout matches pkg/parser exactly:
//
//	'A'/'T': [1 type][8 id][8 ts][1 side][8 price][8 qty][1 symLen=4][4 sym] = 39
//	'M':     [1 type][8 id][8 ts][8 price][8 qty][1 symLen=4][4 sym]       = 38
//	'S':     [1 type][8 ts][1 code]                                         = 10
func writeITCHFixtureReg(t *testing.T, n int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "itch.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var buf [40]byte
	for i := 0; i < n; i++ {
		switch {
		case i%100 < 60:
			buf[0] = 'A'
			buf[34] = 4
			copy(buf[35:39], []byte("AAPL"))
			f.Write(buf[:39])
		case i%100 < 85:
			buf[0] = 'T'
			buf[34] = 4
			copy(buf[35:39], []byte("MSFT"))
			f.Write(buf[:39])
		case i%100 < 95:
			buf[0] = 'M'
			buf[33] = 4
			copy(buf[34:38], []byte("GOOG"))
			f.Write(buf[:38])
		default:
			buf[0] = 'S'
			f.Write(buf[:10])
		}
	}
	return path
}
