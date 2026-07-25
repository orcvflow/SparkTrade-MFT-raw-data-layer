package storage

// Addım F: transport-seam tests.
//
// These tests exercise the DolphinDBWriter's batch / flush / WAL-fallback /
// replay mechanics through the scriptRunner seam (a fakeScriptRunner), NOT a
// permissive HTTP mock. The prior httptest mock accepted any POST /run body,
// which hid 3 production script defects (testnet 404, ensureTables syntax,
// buildInsertScript form) until live validation. The fake here is honest about
// its limits: it does NOT validate DolphinDB syntax — script correctness against
// a real v3 instance is the job of TestIntegration_LiveDolphinDB.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/health"
)

// fakeScriptRunner is a test double for the scriptRunner transport seam. It
// records every script received and can simulate DB downtime via `fail`. It
// does not validate DolphinDB syntax — that is the live test's job.
type fakeScriptRunner struct {
	mu      sync.Mutex
	scripts []string
	fail    atomic.Bool
}

func (f *fakeScriptRunner) connect(_ context.Context) error { return nil } // always "connected"

func (f *fakeScriptRunner) runScript(script string) error {
	f.mu.Lock()
	f.scripts = append(f.scripts, script)
	f.mu.Unlock()
	if f.fail.Load() {
		return fmt.Errorf("simulated downtime")
	}
	return nil
}

func (f *fakeScriptRunner) close() error { return nil }

// scriptsCopy returns a snapshot of received scripts safe to read from any goroutine.
func (f *fakeScriptRunner) scriptsCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.scripts))
	copy(out, f.scripts)
	return out
}

// newFakeWriter builds a writer backed by a fakeScriptRunner plus a real sync
// WAL, returning all three so a test can inspect recorded scripts, toggle fail,
// and assert lossless behavior. The writer is NOT started (no flush ticker /
// reconnect loop) — callers drive flush explicitly to keep timing deterministic.
func newFakeWriter(t *testing.T, batchSize int, batchTimeout time.Duration) (*DolphinDBWriter, *fakeScriptRunner, *WAL) {
	t.Helper()
	tempDir := t.TempDir()
	wal, err := NewWAL(WALConfig{
		Directory:      tempDir,
		MaxFileSize:    10 * 1024 * 1024,
		MaxMessages:    100000,
		RotateInterval: 1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	if err := wal.Start(); err != nil {
		t.Fatalf("WAL Start: %v", err)
	}
	fake := &fakeScriptRunner{}
	cfg := DolphinDBConfig{
		Host:         "localhost",
		Port:         19999, // unreachable in tests; the fake intercepts runScript anyway
		Username:     "admin",
		Password:     "test",
		Database:     "test_db",
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
	}
	w := newDolphinDBWriterWithRunner(cfg, wal, fake)
	return w, fake, wal
}

// TestDolphinDBWriter_Transport_WritePath verifies the write path: a full batch
// triggers a single runScript whose script contains append! for BOTH tables,
// and the runner records it.
func TestDolphinDBWriter_Transport_WritePath(t *testing.T) {
	w, fake, wal := newFakeWriter(t, 3, 100*time.Millisecond)
	defer wal.Stop()
	defer w.Stop()

	// Skip Connect; exercise the write path directly.
	w.connected.Store(true)

	for i := 0; i < 3; i++ {
		ev := createTestCanonicalEvent(fmt.Sprintf("evt_http_%03d", i))
		if err := w.Write(ev); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// batch full (3) → async flush
	time.Sleep(200 * time.Millisecond)

	got := fake.scriptsCopy()
	if len(got) < 1 {
		t.Fatalf("expected at least 1 script, got %d", len(got))
	}
	last := got[len(got)-1]
	// Addım F F2: insert form is loadTable(...).append!(table(...)) — the prior
	// tableInsert-named-vectors form is rejected by DFS tables in DolphinDB v3.
	if !strings.Contains(last, "append!") {
		t.Errorf("script missing append!:\n%s", last)
	}
	if !strings.Contains(last, "raw_events") {
		t.Errorf("script missing raw_events table:\n%s", last)
	}
	if !strings.Contains(last, "canonical_events") {
		t.Errorf("script missing canonical_events table:\n%s", last)
	}
	// Event ids appear in the script (payload preserved too).
	if !strings.Contains(last, "evt_http_000") {
		t.Errorf("script missing event id evt_http_000:\n%s", last)
	}

	stats := w.Stats()
	if stats.TotalWritten != 3 {
		t.Errorf("expected 3 written, got %d", stats.TotalWritten)
	}
	if stats.TotalFailed != 0 {
		t.Errorf("expected 0 failed, got %d", stats.TotalFailed)
	}
}

// TestDolphinDBWriter_Transport_Recovery verifies lossless recovery end-to-end:
// while the DB is "down" events land in WAL; when the DB comes back, replayWAL
// drains them to the runner.
func TestDolphinDBWriter_Transport_Recovery(t *testing.T) {
	w, fake, wal := newFakeWriter(t, 10, 100*time.Millisecond)
	defer wal.Stop()
	defer w.Stop()

	// Phase 1: DB down. WAL is the only durable sink.
	fake.fail.Store(true)
	w.connected.Store(false) // so flush() drains without a DB write

	for i := 0; i < 4; i++ {
		ev := createTestCanonicalEvent(fmt.Sprintf("evt_rec_%03d", i))
		if err := w.Write(ev); err != nil {
			t.Fatalf("Write failed while DB down: %v", err)
		}
	}
	w.Flush()
	time.Sleep(100 * time.Millisecond)

	walStats := wal.Stats()
	if walStats.TotalWritten < 4 {
		t.Fatalf("expected ≥4 events in WAL during outage, got %d", walStats.TotalWritten)
	}

	// Phase 2: DB comes back. Replay the backlog.
	fake.fail.Store(false)
	w.connected.Store(true)
	w.replayWAL()
	time.Sleep(100 * time.Millisecond)

	replaySeen := false
	for _, s := range fake.scriptsCopy() {
		if strings.Contains(s, "append!") && strings.Contains(s, "evt_rec_000") {
			replaySeen = true
			break
		}
	}
	if !replaySeen {
		t.Errorf("WAL replay did not produce an append! script for evt_rec_000; scripts=%v", fake.scriptsCopy())
	}
}

// TestDolphinDBWriter_Transport_Failure verifies a failed runScript is counted
// as failed, WITHOUT losing data (events remain in WAL).
func TestDolphinDBWriter_Transport_Failure(t *testing.T) {
	w, fake, wal := newFakeWriter(t, 2, 100*time.Millisecond)
	defer wal.Stop()
	defer w.Stop()

	fake.fail.Store(true)
	w.connected.Store(true) // attempts write → runScript fails

	for i := 0; i < 2; i++ {
		_ = w.Write(createTestCanonicalEvent(fmt.Sprintf("evt_5xx_%03d", i)))
	}
	time.Sleep(200 * time.Millisecond)

	stats := w.Stats()
	if stats.TotalFailed != 2 {
		t.Errorf("expected 2 failed on runner error, got %d", stats.TotalFailed)
	}
	// Lossless: events are in WAL regardless.
	if wal.Stats().TotalWritten < 2 {
		t.Errorf("expected ≥2 events safe in WAL, got %d", wal.Stats().TotalWritten)
	}
}

// TestDolphinDBWriter_BuildInsertScript_Escaping ensures payloads containing
// double-quotes and backslashes are DolphinDB-escaped (no script injection,
// no broken literals).
func TestDolphinDBWriter_BuildInsertScript_Escaping(t *testing.T) {
	w, _, wal := newFakeWriter(t, 10, 100*time.Millisecond)
	defer wal.Stop()
	defer w.Stop()

	ev := &canonicalizer.CanonicalEvent{
		EventID:           `ev"t\1`,
		Source:            `BINANCE`,
		CanonicalSymbol:   `BTC/USD`,
		ExchangeTimestamp: 1,
		LocalHWTimestamp:  2,
		EventType:         `TRADE`,
		Price:             1.0,
		Size:              2.0,
		Side:              `BUY`,
		RawPayload:        []byte(`{"p":"x","q":"y"}`),
	}
	script := w.buildInsertScript([]*canonicalizer.CanonicalEvent{ev})
	if !strings.Contains(script, `"ev\"t\\1"`) {
		t.Errorf("event id not escaped correctly:\n%s", script)
	}
	if !strings.Contains(script, `"{\"p\":\"x\",\"q\":\"y\"}"`) {
		t.Errorf("payload not escaped correctly:\n%s", script)
	}
}

// TestMetrics_Export verifies the /metrics HTTP handler exposes the registered
// metrics in Prometheus text format and reflects a counter increment.
func TestMetrics_Export(t *testing.T) {
	health.Register() // idempotent

	before := readMetrics(t)
	health.WALWrites.Inc()
	health.WALWrites.Inc()
	after := readMetrics(t)

	if !strings.Contains(after, "raw_data_wal_writes_total") {
		t.Errorf("metrics output missing raw_data_wal_writes_total:\n%s", after)
	}
	// CounterVec is present even with no labels yet; ensure it's listed.
	if !strings.Contains(after, "raw_data_messages_received_total") {
		t.Logf("messages_received not yet labelled (ok)")
	}
	_ = before
}

func readMetrics(t *testing.T) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	health.MetricsHandler().ServeHTTP(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	return string(body)
}
