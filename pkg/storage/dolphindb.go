package storage

// DolphinDB Batch Writer — official api-go (native protocol) implementation.
//
// Addım F: the prior implementation talked to DolphinDB over HTTP POST /run.
// Against a LIVE DolphinDB v3 container that path is rejected ("Unsupport http
// request") — v3's HTTP API is a JSON function-call API, not a raw-script POST.
// The correct transport is the official Go client (github.com/dolphindb/api-go),
// which speaks DolphinDB's native binary protocol on :8848. This file uses it.
//
// Transport seam: every DolphinDB round-trip goes through the scriptRunner
// interface. The production implementation is apiGoRunner (api-go); tests inject
// a fakeScriptRunner (see dolphindb_transport_test.go). The interface exists
// specifically so the unit test layer does NOT depend on a permissive HTTP mock
// that accepted any POST /run body — the bug class that hid 3 production
// script defects (testnet 404, ensureTables syntax, buildInsertScript form)
// until live validation in Addım F.
//
// Lossless design (never lose data) — unchanged by the transport migration:
//  1. Every event is written to WAL synchronously on arrival.
//  2. The event is also appended to the in-memory batch for a DB write.
//  3. flush() writes the batch to DolphinDB when connected; if the DB is down
//     or the write fails, nothing is lost — WAL already holds the event
//     (flush does NOT re-write to WAL, avoiding duplicates).
//  4. Connect() replays the WAL on startup; reconnectLoop() replays on recovery.
//
// Rules: Never panic, never lose data, bounded queues.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/dialer"

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

	// Transport seam (Addım F): api-go native protocol on :8848. Swappable for
	// tests via newDolphinDBWriterWithRunner. nil only between NewDolphinDBWriter
	// and the first Connect(); production always sets it in the constructor.
	runner scriptRunner

	// Batch config
	batchSize    int
	batchTimeout time.Duration

	// Internal state
	batch     []*canonicalizer.CanonicalEvent
	batchMu   sync.Mutex
	connected atomic.Bool

	// WAL fallback — held as the WALWriter interface so a caller can plug in
	// either *WAL (sync, durable) or *BatchedWAL (deferred fsync, throughput)
	// without DolphinDBWriter caring which. Addım F: storage process now selects
	// the WAL mode from config; this field is the seam.
	//
	// TYPED-NIL NOTE: never assign a typed nil (e.g. `var w *WAL; NewDolphinDBWriter(cfg, w)`)
	// — a non-nil interface wrapping a nil pointer would defeat the `w.wal != nil`
	// guard below and panic on Write. All call sites pass either a real, non-nil
	// *WAL/*BatchedWAL from New*WAL or the literal nil. See NewWALWriter.
	wal WALWriter

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

// scriptRunner is the transport seam between DolphinDBWriter and DolphinDB.
// The production implementation (apiGoRunner) uses the official api-go client
// over the native binary protocol on :8848; tests inject a fakeScriptRunner
// (see dolphindb_transport_test.go) so the lossless/recovery paths can be
// exercised without a permissive HTTP mock — the bug class that hid 3 script
// defects (testnet 404, ensureTables syntax, buildInsertScript form) until live
// validation in Addım F.
//
// connect/close may race with runScript across reconnects, so implementations
// must guard their own state (apiGoRunner uses a mutex).
type scriptRunner interface {
	// connect establishes the transport session (TCP dial + login). Idempotent
	// across reconnects: a prior session, if any, is closed first. Returns error
	// on failure; the writer treats this as "DB down" and falls back to WAL.
	// Never panics.
	connect(ctx context.Context) error
	// runScript sends a DolphinDB script. Returns nil only on success. A nil
	// session (not connected) returns an error, never a panic.
	runScript(script string) error
	// close tears down the transport session. Safe to call when not connected.
	close() error
}

// apiGoRunner is the production scriptRunner: it talks to DolphinDB via the
// official api-go client (native protocol on :8848). NewDolphinDBClient only
// allocates (dialer.NewConn does NOT dial — the TCP dial happens in Connect()),
// so constructing a runner is free of network I/O; the dial occurs in connect().
type apiGoRunner struct {
	addr string // "host:port"
	user string
	pass string

	mu sync.Mutex
	db api.DolphinDB // nil until connect() succeeds; nil on close() or connect failure
}

// newAPIGoRunner builds a production runner. No network I/O here.
func newAPIGoRunner(host string, port int, user, pass string) *apiGoRunner {
	return &apiGoRunner{
		addr: fmt.Sprintf("%s:%d", host, port),
		user: user,
		pass: pass,
	}
}

func (r *apiGoRunner) connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Tear down any prior session before opening a new one (reconnect path).
	if r.db != nil {
		_ = r.db.Close()
		r.db = nil
	}

	db, err := api.NewDolphinDBClient(ctx, r.addr, &dialer.BehaviorOptions{})
	if err != nil {
		return fmt.Errorf("new client %s: %w", r.addr, err)
	}
	if err := db.Connect(); err != nil {
		_ = db.Close()
		return fmt.Errorf("connect %s: %w", r.addr, err)
	}
	if err := db.Login(&api.LoginRequest{UserID: r.user, Password: r.pass}); err != nil {
		_ = db.Close()
		return fmt.Errorf("login: %w", err)
	}
	r.db = db
	return nil
}

func (r *apiGoRunner) runScript(script string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return fmt.Errorf("dolphindb not connected")
	}
	if _, err := r.db.RunScript(script); err != nil {
		return err
	}
	return nil
}

func (r *apiGoRunner) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return nil
	}
	err := r.db.Close()
	r.db = nil
	return err
}

// NewDolphinDBWriter creates a new DolphinDB batch writer backed by the api-go
// native client. wal is optional (nil = no WAL fallback). Accepts the WALWriter
// interface so either *WAL (sync) or *BatchedWAL (deferred) can back it.
// Constructor does NO network I/O — connect happens lazily in Connect().
func NewDolphinDBWriter(config DolphinDBConfig, wal WALWriter) *DolphinDBWriter {
	return newDolphinDBWriterWithRunner(config, wal,
		newAPIGoRunner(config.Host, config.Port, config.Username, config.Password))
}

// newDolphinDBWriterWithRunner builds a writer with an injected scriptRunner.
// Production callers use NewDolphinDBWriter (apiGoRunner); tests inject a fake
// to exercise write/recovery paths without a live DB.
func newDolphinDBWriterWithRunner(config DolphinDBConfig, wal WALWriter, runner scriptRunner) *DolphinDBWriter {
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
		runner:       runner,
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

	// Addım F: api-go native transport. connect() does TCP dial + login; a
	// refused/unreachable host returns an error here (never panics) and the
	// writer falls back to WAL. Login already executes a script, so the prior
	// HTTP "1+1" ping is redundant and dropped.
	if err := w.runner.connect(w.ctx); err != nil {
		w.addError(fmt.Errorf("connect failed: %w", err))
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
// both raw_events and canonical_events via loadTable(...).append!(table(...)).
// Each batch is a single RunScript call. Strings are DolphinDB-escaped.
//
// Addım F F2: the prior form `tableInsert(loadTable(...), [vec] as col, ...)`
// fails on a DFS table in v3 ("Can only append a table to a DFS/disk table") —
// the named-vector tableInsert form is rejected. append!(table(...)) is the
// verified-working form (live probe, Addım F F2).
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
	}

	var b strings.Builder
	b.WriteString("try{ ")
	b.WriteString("loadTable(\"" + w.database + "\", \"raw_events\").append!(table(")
	b.WriteString("[" + strings.Join(rawIDs, ",") + "] as event_id, ")
	b.WriteString("[" + strings.Join(rawSources, ",") + "] as source, ")
	b.WriteString("[" + strings.Join(rawPayloads, ",") + "] as payload, ")
	b.WriteString("[" + strings.Join(rawReceivedAts, ",") + "] as received_at, ")
	b.WriteString("[" + strings.Join(rawSeqs, ",") + "] as sequence_num)); ")
	b.WriteString("loadTable(\"" + w.database + "\", \"canonical_events\").append!(table(")
	b.WriteString("[" + strings.Join(canIDs, ",") + "] as event_id, ")
	b.WriteString("[" + strings.Join(canSymbols, ",") + "] as symbol, ")
	b.WriteString("[" + strings.Join(canExchTS, ",") + "] as exchange_ts, ")
	b.WriteString("[" + strings.Join(canLocalTS, ",") + "] as local_ts, ")
	b.WriteString("[" + strings.Join(canPrices, ",") + "] as price, ")
	b.WriteString("[" + strings.Join(canSizes, ",") + "] as size, ")
	b.WriteString("[" + strings.Join(canSides, ",") + "] as side, ")
	b.WriteString("[" + strings.Join(canTypes, ",") + "] as event_type)); ")
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

// runScript sends a DolphinDB script over the api-go native transport. Returns
// nil only on success. Never panics. The runner guards its own session state.
func (w *DolphinDBWriter) runScript(script string) error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in runScript: %v", r))
		}
	}()

	return w.runner.runScript(script)
}

// ensureTablesScript returns the DolphinDB script that creates the raw_events
// and canonical_events tables (and the DFS database) if they do not yet exist.
// Extracted so a live integration test (and the api-go migration) can run the
// SAME script the production path uses, without going through the HTTP transport.
func (w *DolphinDBWriter) ensureTablesScript() string {
	// Correct DolphinDB v3 DFS table creation. The prior script (VALUE partition
	// with bare symbol literals + createTable on a DFS path) was malformed and
	// never validated against a live instance — the httptest mock accepted any
	// POST /run body so the syntax error went uncaught (Addım F F2 live test).
	//
	// Fix: partition the database by HASH on a SYMBOL column (event_id is present
	// in BOTH tables), so a single DB-level scheme serves both. createPartitionedTable
	// (not createTable) is the correct primitive for a DFS partitioned table.
	return `
if(!existsDatabase("` + w.database + `")){
	database("` + w.database + `", HASH, [SYMBOL, 4]);
};
db = database("` + w.database + `");
if(!existsTable("` + w.database + `", "raw_events")){
	s = table(1:0, ` + "`event_id`source`payload`received_at`sequence_num" + `, [SYMBOL,SYMBOL,STRING,LONG,LONG]);
	createPartitionedTable(db, s, "raw_events", "event_id");
};
if(!existsTable("` + w.database + `", "canonical_events")){
	s2 = table(1:0, ` + "`event_id`symbol`exchange_ts`local_ts`price`size`side`event_type" + `, [SYMBOL,SYMBOL,LONG,LONG,DOUBLE,DOUBLE,SYMBOL,SYMBOL]);
	createPartitionedTable(db, s2, "canonical_events", "event_id");
};`
}

// ensureTables creates the DolphinDB tables if they don't exist.
// Best-effort: a failure is logged but does not block writes (WAL is the
// durable fallback).
func (w *DolphinDBWriter) ensureTables() error {
	return w.runScript(w.ensureTablesScript())
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

	// Addım F: tear down the api-go session. Safe if never connected.
	_ = w.runner.close()

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
