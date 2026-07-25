package storage

// DolphinDB Batch Writer — HTTP REST API implementation
//
// Evidence: DolphinDB exposes an HTTP REST endpoint (POST /run, body = script).
// The official Go API (github.com/dolphindb/api-go) is NOT a database/sql driver
// (see https://go.dev/wiki/SQLDrivers — no dolphindb entry), so the previous
// database/sql approach could never bind a driver and never actually connected.
// This implementation talks to DolphinDB over its real, documented HTTP REST API:
//   POST http://host:port/run?user=...&password=...  body: "<DolphinDB script>"
//   200 OK ⇒ success.  Script uses loadTable() + tableInsert() (see
//   https://docs.dolphindb.com/en/javadoc/data_writing/ddb_writing_methods.html).
//
// Lossless design (never lose data) — preserved from the prior version:
//  1. Every event is written to WAL synchronously on arrival.
//  2. The event is also appended to the in-memory batch for a DB write.
//  3. flush() writes the batch to DolphinDB when connected; if the DB is down
//     or the write fails, nothing is lost — WAL already holds the event
//     (flush does NOT re-write to WAL, avoiding duplicates).
//  4. Connect() replays the WAL on startup; reconnectLoop() replays on recovery.
//
// Rules: Never panic, never lose data, bounded queues.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/health"
)

// DolphinDBWriter writes canonical events to DolphinDB over HTTP REST in batches.
// Fallback: if DB unavailable, events are safe in WAL.
type DolphinDBWriter struct {
	// Connection config
	host     string
	port     int
	username string
	password string
	database string // DolphinDB DFS path, e.g. "dfs://raw_data"

	// HTTP
	baseURL    string
	httpClient *http.Client

	// Batch config
	batchSize    int
	batchTimeout time.Duration

	// Internal state
	batch     []*canonicalizer.CanonicalEvent
	batchMu   sync.Mutex
	connected atomic.Bool

	// WAL fallback
	wal *WAL

	// Stats
	totalWritten  atomic.Uint64
	totalFailed   atomic.Uint64
	totalBatches  atomic.Uint64
	lastWriteTime atomic.Value // time.Time
	lastErrorTime atomic.Value // time.Time

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Errors
	errors   []error
	errorsMu sync.RWMutex
}

// DolphinDBConfig holds DolphinDB connection configuration
type DolphinDBConfig struct {
	Host         string
	Port         int
	Username     string
	Password     string
	Database     string // DFS path, e.g. "dfs://raw_data"
	BatchSize    int
	BatchTimeout time.Duration
}

// DefaultDolphinDBConfig returns default configuration per CLAUDE.md
func DefaultDolphinDBConfig() DolphinDBConfig {
	return DolphinDBConfig{
		Host:         "localhost",
		Port:         8848,
		Username:     "admin",
		Password:     "123456",
		Database:     "dfs://raw_data",
		BatchSize:    1000,
		BatchTimeout: 1 * time.Second,
	}
}

// NewDolphinDBWriter creates a new DolphinDB batch writer.
// wal parameter is optional (nil = no WAL fallback).
func NewDolphinDBWriter(config DolphinDBConfig, wal *WAL) *DolphinDBWriter {
	ctx, cancel := context.WithCancel(context.Background())

	if config.Database == "" {
		config.Database = "dfs://raw_data"
	}

	return &DolphinDBWriter{
		host:         config.Host,
		port:         config.Port,
		username:     config.Username,
		password:     config.Password,
		database:     config.Database,
		baseURL:      fmt.Sprintf("http://%s:%d", config.Host, config.Port),
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		batchSize:    config.BatchSize,
		batchTimeout: config.BatchTimeout,
		batch:        make([]*canonicalizer.CanonicalEvent, 0, config.BatchSize),
		wal:          wal,
		ctx:          ctx,
		cancel:       cancel,
		errors:       make([]error, 0),
	}
}

// Connect establishes a connection to DolphinDB by running a trivial script
// (1+1) over the HTTP REST API. On success it marks the writer connected and
// replays any WAL events that accumulated while the DB was unavailable.
// Returns error if the connection fails — caller decides whether to continue
// with WAL-only mode.
func (w *DolphinDBWriter) Connect() error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in Connect: %v", r))
		}
	}()

	if err := w.runScript("1+1"); err != nil {
		w.addError(fmt.Errorf("connect ping failed: %w", err))
		return fmt.Errorf("DolphinDB connect failed: %w", err)
	}

	w.connected.Store(true)

	// Ensure tables exist before replaying into them.
	if err := w.ensureTables(); err != nil {
		// Non-fatal: continue with WAL-only mode for writes; replay will retry.
		w.addError(fmt.Errorf("ensureTables warning: %w", err))
	}

	// Replay WAL on startup so events accumulated while the DB was down are
	// not lost. This runs synchronously — Connect() does not return until the
	// backlog is drained (best-effort: failed batches stay in WAL).
	w.replayWAL()

	return nil
}

// Start begins the batch writer background flush loop. If not connected, it
// also starts the reconnect loop.
func (w *DolphinDBWriter) Start() error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in Start: %v", r))
		}
	}()

	// Start batch flush ticker
	w.wg.Add(1)
	go w.flushLoop()

	// Start reconnect loop if DB not connected
	if !w.connected.Load() {
		w.wg.Add(1)
		go w.reconnectLoop()
	}

	return nil
}

// Write queues a canonical event for batch writing. Thread-safe; never blocks.
func (w *DolphinDBWriter) Write(event *canonicalizer.CanonicalEvent) error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in Write: %v", r))
		}
	}()

	if event == nil {
		return fmt.Errorf("nil event")
	}

	// CRITICAL: ALWAYS write to WAL first (sync, lossless guarantee).
	if w.wal != nil {
		if err := w.wal.Write(event); err != nil {
			w.addError(fmt.Errorf("WAL write failed for %s: %w", event.EventID, err))
			return fmt.Errorf("wal write failed: %w", err)
		}
	}

	// Append to batch regardless of connection state.
	w.batchMu.Lock()
	w.batch = append(w.batch, event)
	batchLen := len(w.batch)
	w.batchMu.Unlock()

	// Trigger flush if batch is full
	if batchLen >= w.batchSize {
		go w.flush() // async flush — safe because WAL already has the event
	}

	return nil
}

// flushLoop is the background goroutine that flushes on timeout
func (w *DolphinDBWriter) flushLoop() {
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in flushLoop: %v", r))
		}
		w.wg.Done()
	}()

	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			// Final flush on shutdown
			w.flush()
			return
		case <-ticker.C:
			w.flush()
		}
	}
}

// flush drains the current batch and writes to DolphinDB. Every event in the
// batch was already written to WAL synchronously in Write(), so on any failure
// path we do NOT re-write to WAL — that would only create duplicates. WAL
// remains the durable source of truth; DolphinDB is a best-effort fast path.
func (w *DolphinDBWriter) flush() {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in flush: %v", r))
		}
	}()

	// Drain batch
	w.batchMu.Lock()
	if len(w.batch) == 0 {
		w.batchMu.Unlock()
		return
	}
	toWrite := make([]*canonicalizer.CanonicalEvent, len(w.batch))
	copy(toWrite, w.batch)
	w.batch = w.batch[:0]
	w.batchMu.Unlock()

	if w.connected.Load() {
		if err := w.writeBatch(toWrite); err != nil {
			w.addError(fmt.Errorf("writeBatch failed: %w", err))
			w.totalFailed.Add(uint64(len(toWrite)))
			health.DolphinDBWriteErrors.Add(float64(len(toWrite)))
			w.lastErrorTime.Store(time.Now())
			// Events are safe in WAL (written in Write()) — no re-write needed.
			return
		}
	}
	// When not connected: batch is drained, events live in WAL. Nothing to do.

	w.totalWritten.Add(uint64(len(toWrite)))
	w.totalBatches.Add(1)
	health.DolphinDBWrites.Add(float64(len(toWrite)))
	w.lastWriteTime.Store(time.Now())
}

// writeBatch writes a batch to DolphinDB (both tables) over HTTP REST.
func (w *DolphinDBWriter) writeBatch(events []*canonicalizer.CanonicalEvent) error {
	script := w.buildInsertScript(events)
	if script == "" {
		return nil
	}
	if err := w.runScript(script); err != nil {
		return fmt.Errorf("http write: %w", err)
	}
	return nil
}

// buildInsertScript builds a DolphinDB script that bulk-inserts the batch into
// both raw_events and canonical_events via tableInsert(loadTable(...), vectors).
// Each batch is a single /run call. Strings are DolphinDB-escaped.
func (w *DolphinDBWriter) buildInsertScript(events []*canonicalizer.CanonicalEvent) string {
	if len(events) == 0 {
		return ""
	}

	rawIDs := make([]string, 0, len(events))
	rawSources := make([]string, 0, len(events))
	rawPayloads := make([]string, 0, len(events))
	rawReceivedAts := make([]string, 0, len(events))
	rawSeqs := make([]string, 0, len(events))

	canIDs := make([]string, 0, len(events))
	canSymbols := make([]string, 0, len(events))
	canExchTS := make([]string, 0, len(events))
	canLocalTS := make([]string, 0, len(events))
	canTypes := make([]string, 0, len(events))
	canPrices := make([]string, 0, len(events))
	canSizes := make([]string, 0, len(events))
	canSides := make([]string, 0, len(events))
	canRawIDs := make([]string, 0, len(events))

	for _, ev := range events {
		if ev == nil {
			continue
		}
		rawIDs = append(rawIDs, ddbString(ev.EventID))
		rawSources = append(rawSources, ddbString(ev.Source))
		rawPayloads = append(rawPayloads, ddbString(string(ev.RawPayload)))
		rawReceivedAts = append(rawReceivedAts, strconv.FormatInt(ev.LocalHWTimestamp, 10))
		rawSeqs = append(rawSeqs, "0")

		canIDs = append(canIDs, ddbString(ev.EventID))
		canSymbols = append(canSymbols, ddbString(ev.CanonicalSymbol))
		canExchTS = append(canExchTS, strconv.FormatInt(ev.ExchangeTimestamp, 10))
		canLocalTS = append(canLocalTS, strconv.FormatInt(ev.LocalHWTimestamp, 10))
		canTypes = append(canTypes, ddbString(ev.EventType))
		canPrices = append(canPrices, strconv.FormatFloat(ev.Price, 'f', -1, 64))
		canSizes = append(canSizes, strconv.FormatFloat(ev.Size, 'f', -1, 64))
		canSides = append(canSides, ddbString(ev.Side))
		canRawIDs = append(canRawIDs, ddbString(ev.EventID))
	}

	var b strings.Builder
	b.WriteString("try{ ")
	b.WriteString("rt = loadTable(\"" + w.database + "\", \"raw_events\"); ")
	b.WriteString("tableInsert(rt, [" + strings.Join(rawIDs, ",") + "] as event_id, ")
	b.WriteString("[" + strings.Join(rawSources, ",") + "] as source, ")
	b.WriteString("[" + strings.Join(rawPayloads, ",") + "] as payload, ")
	b.WriteString("[" + strings.Join(rawReceivedAts, ",") + "] as received_at, ")
	b.WriteString("[" + strings.Join(rawSeqs, ",") + "] as sequence_num); ")
	b.WriteString("ct = loadTable(\"" + w.database + "\", \"canonical_events\"); ")
	b.WriteString("tableInsert(ct, [" + strings.Join(canIDs, ",") + "] as event_id, ")
	b.WriteString("[" + strings.Join(canSymbols, ",") + "] as symbol, ")
	b.WriteString("[" + strings.Join(canExchTS, ",") + "] as exchange_ts, ")
	b.WriteString("[" + strings.Join(canLocalTS, ",") + "] as local_ts, ")
	b.WriteString("[" + strings.Join(canPrices, ",") + "] as price, ")
	b.WriteString("[" + strings.Join(canSizes, ",") + "] as size, ")
	b.WriteString("[" + strings.Join(canSides, ",") + "] as side, ")
	b.WriteString("[" + strings.Join(canTypes, ",") + "] as event_type); ")
	b.WriteString("}catch(ex){ throw ex };")
	return b.String()
}

// ddbString returns a DolphinDB double-quoted string literal with the contents
// escaped (backslash and double-quote). Non-printable bytes are preserved as-is;
// WAL holds the byte-for-byte original — DolphinDB stores a STRING copy here.
func ddbString(s string) string {
	r := strings.NewReplacer("\\", "\\\\", "\"", "\\\"")
	return "\"" + r.Replace(s) + "\""
}

// runScript sends a DolphinDB script to the /run endpoint over HTTP POST.
// Returns nil only on HTTP 200.
func (w *DolphinDBWriter) runScript(script string) error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in runScript: %v", r))
		}
	}()

	if w.httpClient == nil {
		return fmt.Errorf("http client not initialized")
	}

	endpoint := fmt.Sprintf("%s/run?user=%s&password=%s",
		w.baseURL,
		w.username,
		w.password)

	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, endpoint, bytes.NewReader([]byte(script)))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("dolphindb status %d: %s", resp.StatusCode, string(body))
	}
	// Drain body so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return nil
}

// ensureTables creates the DolphinDB tables if they don't exist.
// Best-effort: a failure is logged but does not block writes (WAL is the
// durable fallback).
func (w *DolphinDBWriter) ensureTables() error {
	script := `
if(!existsDatabase("` + w.database + `")){
	db = database("` + w.database + `", VALUE, ` + "`source`" + `, ` + "`BTC/USD`" + `);
};
if(!existsTable("` + w.database + `", "raw_events")){
	tableInsert(createTable(database("` + w.database + `"), "raw_events", event_id SYMBOL, source SYMBOL, payload STRING, received_at LONG, sequence_num LONG));
};
if(!existsTable("` + w.database + `", "canonical_events")){
	tableInsert(createTable(database("` + w.database + `), "canonical_events", event_id SYMBOL, symbol SYMBOL, exchange_ts LONG, local_ts LONG, price DOUBLE, size DOUBLE, side SYMBOL, event_type SYMBOL));
};`
	return w.runScript(script)
}

// reconnectLoop attempts to reconnect to DolphinDB with exponential backoff.
// Evidence: IB Gateway pattern — 1s, 2s, 4s, 8s, 16s, max 30s.
func (w *DolphinDBWriter) reconnectLoop() {
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in reconnectLoop: %v", r))
		}
		w.wg.Done()
	}()

	backoff := []time.Duration{1, 2, 4, 8, 16, 30}
	attempt := 0

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		if w.connected.Load() {
			// Already connected — check every 30s
			select {
			case <-w.ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
			continue
		}

		// Wait before reconnect attempt
		delay := backoff[attempt%len(backoff)]
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(delay * time.Second):
		}

		// Attempt reconnect
		if err := w.Connect(); err != nil {
			w.addError(fmt.Errorf("reconnect attempt %d failed: %w", attempt+1, err))
			attempt++
			continue
		}

		// Connected — Connect() already replayed WAL.
		attempt = 0
	}
}

// replayWAL replays WAL events to DolphinDB after recovery.
// Evidence: CLAUDE.md — "WAL replayed to DB on recovery"
func (w *DolphinDBWriter) replayWAL() {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in replayWAL: %v", r))
		}
	}()

	if w.wal == nil {
		return
	}

	events, err := w.wal.Replay()
	if err != nil {
		w.addError(fmt.Errorf("WAL replay read failed: %w", err))
		return
	}

	if len(events) == 0 {
		return
	}

	// Replay in batches
	for i := 0; i < len(events); i += w.batchSize {
		end := i + w.batchSize
		if end > len(events) {
			end = len(events)
		}

		batch := events[i:end]
		ptrs := make([]*canonicalizer.CanonicalEvent, len(batch))
		for j := range batch {
			ptrs[j] = &batch[j]
		}

		if err := w.writeBatch(ptrs); err != nil {
			w.addError(fmt.Errorf("WAL replay batch %d-%d failed: %w", i, end, err))
			return // stop replay; remaining events stay in WAL for next attempt
		}
	}
}

// Flush forces an immediate flush of the current batch.
// Useful for graceful shutdown.
func (w *DolphinDBWriter) Flush() error {
	w.flush()
	return nil
}

// Stop gracefully stops the writer, flushing any pending events.
func (w *DolphinDBWriter) Stop() error {
	// Cancel context (triggers flushLoop final flush)
	w.cancel()

	// Wait for goroutines
	w.wg.Wait()

	w.connected.Store(false)

	return nil
}

// Stats returns current writer statistics
func (w *DolphinDBWriter) Stats() DolphinDBStats {
	w.batchMu.Lock()
	pendingBatch := len(w.batch)
	w.batchMu.Unlock()

	w.errorsMu.RLock()
	errors := make([]error, len(w.errors))
	copy(errors, w.errors)
	w.errorsMu.RUnlock()

	lastWrite := time.Time{}
	if v := w.lastWriteTime.Load(); v != nil {
		lastWrite = v.(time.Time)
	}

	lastError := time.Time{}
	if v := w.lastErrorTime.Load(); v != nil {
		lastError = v.(time.Time)
	}

	return DolphinDBStats{
		Connected:    w.connected.Load(),
		TotalWritten: w.totalWritten.Load(),
		TotalFailed:  w.totalFailed.Load(),
		TotalBatches: w.totalBatches.Load(),
		PendingBatch: pendingBatch,
		BatchSize:    w.batchSize,
		LastWrite:    lastWrite,
		LastError:    lastError,
		Errors:       errors,
	}
}

// DolphinDBStats holds writer statistics
type DolphinDBStats struct {
	Connected    bool
	TotalWritten uint64
	TotalFailed  uint64
	TotalBatches uint64
	PendingBatch int
	BatchSize    int
	LastWrite    time.Time
	LastError    time.Time
	Errors       []error
}

// IsHealthy returns true if writer is in good shape
func (s DolphinDBStats) IsHealthy() bool {
	// Healthy if connected and no recent errors
	if !s.Connected {
		return false
	}

	// Recent error in last 30 seconds is concerning
	if !s.LastError.IsZero() && time.Since(s.LastError) < 30*time.Second {
		return false
	}

	return true
}

// addError adds an error to the list (capped at 10)
func (w *DolphinDBWriter) addError(err error) {
	w.errorsMu.Lock()
	defer w.errorsMu.Unlock()

	w.errors = append(w.errors, err)
	if len(w.errors) > 10 {
		w.errors = w.errors[1:]
	}
}
