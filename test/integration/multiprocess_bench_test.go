//go:build integration

// Package integration — Addım F F4: real 4-process UDS pipeline benchmark.
//
// STEP-E §9 item 2 asked to verify the in-process E1 upper bound (148K msg/s
// batched) against the REAL 4-process UDS pipeline. This test stands up the real
// canonicalizer + publisher + storage binaries over UDS+Protobuf, acts as the
// adapter (test-side IPC client), injects N raw Binance frames, and measures:
//   - THROUGHPUT (settled-to-WAL msg/s): frames that traversed
//     adapter→canon→publisher→storage and landed durably in the WAL per second.
//   - SETTLED LATENCY (send→WAL-appear): per-frame, with the batched-WAL caveat
//     that it is dominated by the ≤50ms flush interval, NOT the pipeline cost
//     (the codec/socket overhead is sub-µs per Addım C).
//
// HONEST SCOPE:
//   - WAL is the production-default BATCHED mode (50ms flush). So settled
//     latency ≈ pipeline + ≤50ms; throughput is bounded by canonicalize+IPC,
//     not fsync (batched amortizes it). This mirrors the E1 batched number.
//   - The test is the adapter — it does not measure Binance ingest (that's F3).
//   - Build-tagged `integration` because it spawns 3 child processes.
//
// Run: go test -tags=integration ./test/integration/ -run TestMultiprocess_PipelineBenchmark -v -timeout 180s
package integration

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestMultiprocess_PipelineBenchmark(t *testing.T) {
	env := mpNewEnv(t, false, "")

	mpStart(t, env, "canonicalizer")
	mpStart(t, env, "publisher")
	mpStart(t, env, "storage")
	mpWaitLive(t, env, 30*time.Second, "canonicalizer", "publisher", "storage")

	cl := mpDial(t, env.sockets.adapterCanon)
	defer cl.Stop()

	// Shared prefix so mpWaitForEventCount counts all injected frames.
	prefix := []byte(`{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":`)
	mkTrade := func(i int) []byte {
		return []byte(fmt.Sprintf(`{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":%d,"p":"50000.00","q":"0.5","T":1700000000000,"m":false}`, i))
	}

	// Warmup: prime the IPC clients, pool, bufio, and WAL file.
	const warmup = 200
	for i := 0; i < warmup; i++ {
		mpSendRaw(t, cl, "BINANCE", mkTrade(i))
	}
	if c := mpWaitForEventCount(t, env, prefix, warmup, 30*time.Second); c < warmup {
		t.Fatalf("warmup: only %d/%d events settled", c, warmup)
	}

	// ── Throughput: N frames, time(first send → all settled in WAL) ────────────
	const N = 1000
	t0 := time.Now()
	for i := 0; i < N; i++ {
		mpSendRaw(t, cl, "BINANCE", mkTrade(warmup+i))
	}
	want := warmup + N
	// Poll until all N settle. Capture tFirst (first MEASURED frame appears in
	// WAL, i.e. count first exceeds warmup) and tLast (all settled). These give
	// an honest first-frame settled latency and the full burst drain time without
	// fragile single-frame post-burst sends (which race the spool drain).
	deadline := time.Now().Add(60 * time.Second)
	got := -1
	var tFirst, tLast time.Time
	for time.Now().Before(deadline) {
		got = mpWaitForEventCountQuiet(t, env, prefix)
		if tFirst.IsZero() && got > warmup {
			tFirst = time.Now()
		}
		if got >= want {
			tLast = time.Now()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got < want {
		var diag strings.Builder
		for _, n := range []string{"canonicalizer", "publisher", "storage"} {
			h := mpProcHealth(env, n)
			fmt.Fprintf(&diag, "[%s running=%v healthy=%v restarts=%d exit=%d err=%s] ", n, h.Running, h.Healthy, h.Restarts, h.LastExitCode, h.Error)
		}
		t.Fatalf("throughput: only %d/%d events settled in 60s — %s", got, want, diag.String())
	}
	if tFirst.IsZero() {
		tFirst = tLast // degenerate: all settled in one poll tick
	}
	drainSec := tLast.Sub(t0).Seconds()
	throughput := float64(N) / drainSec
	firstLatency := tFirst.Sub(t0) // first measured frame: send → WAL-appear (incl. ≤50ms flush)

	// ── Assert + report ────────────────────────────────────────────────────────
	// Honest floor: 3 processes, 2 IPC hops, batched WAL. The pipeline must
	// sustain ≥1K msg/s settled (canonicalize+codec is the bound, not fsync).
	const throughputFloor = 1000.0
	if throughput < throughputFloor {
		t.Errorf("4-process throughput %.0f msg/s < floor %.0f", throughput, throughputFloor)
	}

	t.Logf("F4 4-process UDS pipeline benchmark:")
	t.Logf("  topology:       adapter(test) → canon → publisher → storage → WAL (UDS+Protobuf, 2 hops)")
	t.Logf("  WAL mode:       batched (production default, 50ms flush)")
	t.Logf("  throughput:     %.0f msg/s settled (%d frames in %.2fs drain)", throughput, N, drainSec)
	t.Logf("  first-frame:    send→WAL-appear %s (incl. ≤50ms batched flush; codec/socket is sub-µs)", firstLatency)
	t.Logf("  burst drain:    %s for %d frames (canonicalize+IPC is the bound, not fsync)", tLast.Sub(t0), N)

	// Cross-check: compare to E1 in-process batched upper bound (148K). The
	// 4-process number should be LOWER (codec+socket overhead) but same order.
	t.Logf("  E1 in-process batched upper bound: ~148000 msg/s (canonicalize→WAL, no IPC)")
}

// mpWaitForEventCountQuiet is a non-fatal, single-shot version of
// mpWaitForEventCount: it reads the WAL once and returns the count of events
// whose RawPayload starts with prefix. Used by the benchmark's polling loop so a
// timeout surfaces diagnostics instead of t.Fatalf inside the helper.
func mpWaitForEventCountQuiet(t *testing.T, env *mpEnv, prefix []byte) int {
	t.Helper()
	c := 0
	for _, ev := range mpReadWAL(t, env) {
		if bytes.HasPrefix(ev.RawPayload, prefix) {
			c++
		}
	}
	return c
}
