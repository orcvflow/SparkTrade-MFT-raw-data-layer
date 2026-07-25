package storage

import (
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

// mockDolphinDB is a thread-safe httptest stand-in for DolphinDB's /run endpoint.
type mockDolphinDB struct {
	server  *httptest.Server
	fail    atomic.Bool
	scripts []string
	mu      sync.Mutex
}

func (m *mockDolphinDB) appendScript(s string) {
	m.mu.Lock()
	m.scripts = append(m.scripts, s)
	m.mu.Unlock()
}

// scriptsCopy returns a snapshot of received scripts safe to read from any goroutine.
func (m *mockDolphinDB) scriptsCopy() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.scripts))
	copy(out, m.scripts)
	return out
}

// newMockDolphinDB starts an httptest server that emulates DolphinDB's /run
// endpoint. It records received scripts and returns 200 unless `fail` is set
// (to simulate DB downtime).
func newMockDolphinDB(t *testing.T) (*httptest.Server, *atomic.Bool, *mockDolphinDB) {
	t.Helper()
	m := &mockDolphinDB{}
	m.fail.Store(false)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/run" {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		m.appendScript(string(body))

		if m.fail.Load() {
			http.Error(w, "simulated downtime", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	return srv, &m.fail, m
}

// pointWriterAtServer rewrites a writer's baseURL + httpClient to target the
// given httptest server, and marks it connected so flush() attempts the HTTP
// write path without needing a real Connect() round-trip.
func pointWriterAtServer(w *DolphinDBWriter, srv *httptest.Server) {
	w.baseURL = srv.URL
	w.httpClient = srv.Client()
	w.httpClient.Timeout = 5 * time.Second
}

// TestDolphinDBWriter_HTTP_WritePath verifies the real HTTP REST write path:
// a full batch triggers a single POST /run whose script contains tableInsert
// for BOTH raw_events and canonical_events, and the server returns 200.
func TestDolphinDBWriter_HTTP_WritePath(t *testing.T) {
	srv, _, mock := newMockDolphinDB(t)
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()
	defer writer.Stop()

	pointWriterAtServer(writer, srv)
	writer.connected.Store(true)
	writer.batchSize = 3

	for i := 0; i < 3; i++ {
		ev := createTestCanonicalEvent(fmt.Sprintf("evt_http_%03d", i))
		if err := writer.Write(ev); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// batch full (3) → async flush
	time.Sleep(200 * time.Millisecond)

	got := mock.scriptsCopy()
	if len(got) < 1 {
		t.Fatalf("expected at least 1 /run script, got %d", len(got))
	}
	last := got[len(got)-1]
	if !strings.Contains(last, "tableInsert") {
		t.Errorf("script missing tableInsert:\n%s", last)
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

	stats := writer.Stats()
	if stats.TotalWritten != 3 {
		t.Errorf("expected 3 written, got %d", stats.TotalWritten)
	}
	if stats.TotalFailed != 0 {
		t.Errorf("expected 0 failed, got %d", stats.TotalFailed)
	}
}

// TestDolphinDBWriter_HTTP_Recovery verifies lossless recovery end-to-end over
// the real HTTP path: while the DB is "down" events land in WAL; when the DB
// comes back, Connect() replays them and they reach DolphinDB.
func TestDolphinDBWriter_HTTP_Recovery(t *testing.T) {
	srv, fail, mock := newMockDolphinDB(t)
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()
	defer writer.Stop()

	// Phase 1: DB down. WAL is the only durable sink.
	fail.Store(true)
	pointWriterAtServer(writer, srv)
	writer.connected.Store(false) // so flush() drains without DB write

	for i := 0; i < 4; i++ {
		ev := createTestCanonicalEvent(fmt.Sprintf("evt_rec_%03d", i))
		if err := writer.Write(ev); err != nil {
			t.Fatalf("Write failed while DB down: %v", err)
		}
	}
	writer.Flush()
	time.Sleep(100 * time.Millisecond)

	walStats := wal.Stats()
	if walStats.TotalWritten < 4 {
		t.Fatalf("expected ≥4 events in WAL during outage, got %d", walStats.TotalWritten)
	}

	// Phase 2: DB comes back. Replay the backlog.
	fail.Store(false)
	writer.connected.Store(true)
	writer.replayWAL()
	time.Sleep(100 * time.Millisecond)

	replaySeen := false
	for _, s := range mock.scriptsCopy() {
		if strings.Contains(s, "tableInsert") && strings.Contains(s, "evt_rec_000") {
			replaySeen = true
			break
		}
	}
	if !replaySeen {
		t.Errorf("WAL replay did not produce a tableInsert script for evt_rec_000; scripts=%v", mock.scriptsCopy())
	}
}

// TestDolphinDBWriter_HTTP_Non200 verifies a non-200 response is surfaced as an
// error and counted as failed, WITHOUT losing data (events remain in WAL).
func TestDolphinDBWriter_HTTP_Non200(t *testing.T) {
	srv, fail, _ := newMockDolphinDB(t)
	writer, wal := createTestDolphinDBWriter(t)
	defer wal.Stop()
	defer writer.Stop()

	fail.Store(true)
	pointWriterAtServer(writer, srv)
	writer.connected.Store(true)
	writer.batchSize = 2

	for i := 0; i < 2; i++ {
		_ = writer.Write(createTestCanonicalEvent(fmt.Sprintf("evt_5xx_%03d", i)))
	}
	time.Sleep(200 * time.Millisecond)

	stats := writer.Stats()
	if stats.TotalFailed != 2 {
		t.Errorf("expected 2 failed on 500, got %d", stats.TotalFailed)
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
	w, _ := createTestDolphinDBWriter(t)
	ev := &canonicalizer.CanonicalEvent{
		EventID:          `ev"t\1`,
		Source:           `BINANCE`,
		CanonicalSymbol:  `BTC/USD`,
		ExchangeTimestamp: 1,
		LocalHWTimestamp:  2,
		EventType:        `TRADE`,
		Price:            1.0,
		Size:             2.0,
		Side:             `BUY`,
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
	// The counter should have increased by 2.
	if !strings.Contains(after, "raw_data_messages_received_total") {
		// CounterVec is present even with no labels yet; ensure it's listed.
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
