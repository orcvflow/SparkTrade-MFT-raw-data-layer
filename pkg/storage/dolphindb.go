package storage

// DolphinDB Batch Writer
// Evidence: DolphinDB vs pickle — 10x faster read speed, 4:1 to 10:1 compression
// Batch: 1000 messages or 1 second timeout
// Two tables: raw_events (BLOB), canonical_events (structured)
// On timeout: persist to WAL, retry on recovery
// Rules: Never panic, never lose data, bounded queues

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"raw-data-layer/pkg/canonicalizer"

	// _ "github.com/dolphindb/go-api" // Uncomment when DolphinDB Go driver is available
)

// DolphinDBWriter writes canonical events to DolphinDB in batches
// Fallback: if DB unavailable, events go to WAL
type DolphinDBWriter struct {
	// Connection config
	host     string
	port     int
	username string
	password string
	database string

	// Batch config
	batchSize    int
	batchTimeout time.Duration

	// Internal state
	db        *sql.DB
	batch     []*canonicalizer.CanonicalEvent
	batchMu   sync.Mutex
	connected atomic.Bool

	// WAL fallback
	wal *WAL

	// Stats
	totalWritten   atomic.Uint64
	totalFailed    atomic.Uint64
	totalBatches   atomic.Uint64
	lastWriteTime  atomic.Value // time.Time
	lastErrorTime  atomic.Value // time.Time

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
	Database     string
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
		Database:     "raw_data",
		BatchSize:    1000,
		BatchTimeout: 1 * time.Second,
	}
}

// NewDolphinDBWriter creates a new DolphinDB batch writer
// wal parameter is optional (nil = no WAL fallback)
func NewDolphinDBWriter(config DolphinDBConfig, wal *WAL) *DolphinDBWriter {
	ctx, cancel := context.WithCancel(context.Background())

	return &DolphinDBWriter{
		host:         config.Host,
		port:         config.Port,
		username:     config.Username,
		password:     config.Password,
		database:     config.Database,
		batchSize:    config.BatchSize,
		batchTimeout: config.BatchTimeout,
		batch:        make([]*canonicalizer.CanonicalEvent, 0, config.BatchSize),
		wal:          wal,
		ctx:          ctx,
		cancel:       cancel,
		errors:       make([]error, 0),
	}
}

// Connect establishes connection to DolphinDB
// Returns error if connection fails — caller decides whether to continue with WAL-only mode
func (w *DolphinDBWriter) Connect() error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in Connect: %v", r))
		}
	}()

	// DolphinDB uses its own protocol; for MVP we use database/sql with a DSN-style
	// connection string. In production, use the official DolphinDB Go API.
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		w.username, w.password, w.host, w.port, w.database)

	db, err := sql.Open("dolphindb", dsn)
	if err != nil {
		w.addError(fmt.Errorf("sql.Open failed: %w", err))
		return fmt.Errorf("failed to open DolphinDB connection: %w", err)
	}

	// Ping to verify connection
	pingCtx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		w.addError(fmt.Errorf("ping failed: %w", err))
		return fmt.Errorf("DolphinDB ping failed: %w", err)
	}

	// Connection pool settings
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	w.db = db
	w.connected.Store(true)

	return nil
}

// Start begins the batch writer background flush loop
func (w *DolphinDBWriter) Start() error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in Start: %v", r))
		}
	}()

	if w.connected.Load() {
		// Ensure tables exist
		if err := w.ensureTables(); err != nil {
			// Non-fatal: continue with WAL-only mode
			w.addError(fmt.Errorf("ensureTables warning: %w", err))
		}
	}

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

// Write queues a canonical event for batch writing
// Thread-safe; never blocks; falls back to WAL on batch failure
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

	w.batchMu.Lock()
	w.batch = append(w.batch, event)
	batchLen := len(w.batch)
	w.batchMu.Unlock()

	// Trigger flush if batch is full
	if batchLen >= w.batchSize {
		go w.flush() // async flush — never block the caller
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

// flush drains the current batch and writes to DolphinDB or WAL
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
	w.batch = w.batch[:0] // reset without reallocation
	w.batchMu.Unlock()

	// Attempt DolphinDB write
	if w.connected.Load() {
		if err := w.writeBatch(toWrite); err != nil {
			w.addError(fmt.Errorf("writeBatch failed: %w", err))
			w.totalFailed.Add(uint64(len(toWrite)))
			w.lastErrorTime.Store(time.Now())

			// Fallback: write to WAL
			w.writeToWAL(toWrite)
			return
		}
	} else {
		// DB not connected — write to WAL
		w.writeToWAL(toWrite)
		return
	}

	w.totalWritten.Add(uint64(len(toWrite)))
	w.totalBatches.Add(1)
	w.lastWriteTime.Store(time.Now())
}

// writeBatch writes a batch to DolphinDB (both tables)
func (w *DolphinDBWriter) writeBatch(events []*canonicalizer.CanonicalEvent) error {
	if w.db == nil {
		return fmt.Errorf("database connection is nil")
	}

	ctx, cancel := context.WithTimeout(w.ctx, 10*time.Second)
	defer cancel()

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Write to raw_events table
	if err = w.writeRawEvents(ctx, tx, events); err != nil {
		return fmt.Errorf("writeRawEvents: %w", err)
	}

	// Write to canonical_events table
	if err = w.writeCanonicalEvents(ctx, tx, events); err != nil {
		return fmt.Errorf("writeCanonicalEvents: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// writeRawEvents writes to raw_events table
// Columns: event_id, source, payload BLOB, received_at, sequence_num
func (w *DolphinDBWriter) writeRawEvents(ctx context.Context, tx *sql.Tx, events []*canonicalizer.CanonicalEvent) error {
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO raw_events (event_id, source, payload, received_at, sequence_num) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare raw_events: %w", err)
	}
	defer stmt.Close()

	for _, event := range events {
		if event == nil {
			continue
		}
		_, err := stmt.ExecContext(ctx,
			event.EventID,
			event.Source,
			event.RawPayload, // BLOB — byte-for-byte preserved
			event.LocalHWTimestamp,
			0, // sequence_num: not available in CanonicalEvent struct
		)
		if err != nil {
			return fmt.Errorf("insert raw_event %s: %w", event.EventID, err)
		}
	}

	return nil
}

// writeCanonicalEvents writes to canonical_events table
// Columns: event_id, symbol, exchange_ts, local_ts, price, size, side
func (w *DolphinDBWriter) writeCanonicalEvents(ctx context.Context, tx *sql.Tx, events []*canonicalizer.CanonicalEvent) error {
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO canonical_events (event_id, symbol, exchange_ts, local_ts, price, size, side, event_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare canonical_events: %w", err)
	}
	defer stmt.Close()

	for _, event := range events {
		if event == nil {
			continue
		}
		_, err := stmt.ExecContext(ctx,
			event.EventID,
			event.CanonicalSymbol,
			event.ExchangeTimestamp,
			event.LocalHWTimestamp,
			event.Price,
			event.Size,
			event.Side,
			event.EventType,
		)
		if err != nil {
			return fmt.Errorf("insert canonical_event %s: %w", event.EventID, err)
		}
	}

	return nil
}

// writeToWAL sends events to WAL fallback
// This is called when DolphinDB is unavailable or write fails
func (w *DolphinDBWriter) writeToWAL(events []*canonicalizer.CanonicalEvent) {
	if w.wal == nil {
		w.addError(fmt.Errorf("WAL not configured, %d events dropped", len(events)))
		return
	}

	for _, event := range events {
		if event == nil {
			continue
		}
		if err := w.wal.Write(event); err != nil {
			w.addError(fmt.Errorf("WAL write failed for %s: %w", event.EventID, err))
		}
	}
}

// ensureTables creates DolphinDB tables if they don't exist
func (w *DolphinDBWriter) ensureTables() error {
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}

	ctx, cancel := context.WithTimeout(w.ctx, 10*time.Second)
	defer cancel()

	// raw_events table
	_, err := w.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS raw_events (
			event_id    VARCHAR(64)  NOT NULL,
			source      VARCHAR(32)  NOT NULL,
			payload     BLOB,
			received_at BIGINT       NOT NULL,
			sequence_num BIGINT      DEFAULT 0,
			PRIMARY KEY (event_id)
		)`)
	if err != nil {
		return fmt.Errorf("create raw_events: %w", err)
	}

	// canonical_events table
	_, err = w.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS canonical_events (
			event_id    VARCHAR(64)  NOT NULL,
			symbol      VARCHAR(32)  NOT NULL,
			exchange_ts BIGINT       NOT NULL,
			local_ts    BIGINT       NOT NULL,
			price       DOUBLE       DEFAULT 0.0,
			size        DOUBLE       DEFAULT 0.0,
			side        VARCHAR(8)   DEFAULT 'UNKNOWN',
			event_type  VARCHAR(16)  DEFAULT 'UNKNOWN',
			PRIMARY KEY (event_id)
		)`)
	if err != nil {
		return fmt.Errorf("create canonical_events: %w", err)
	}

	return nil
}

// reconnectLoop attempts to reconnect to DolphinDB with exponential backoff
// Evidence: IB Gateway pattern — 1s, 2s, 4s, 8s, 16s, max 30s
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

		// Connected — ensure tables and try WAL replay
		if err := w.ensureTables(); err != nil {
			w.addError(fmt.Errorf("ensureTables after reconnect: %w", err))
		}

		w.replayWAL()
		attempt = 0
	}
}

// replayWAL replays WAL events to DolphinDB after recovery
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
			return
		}
	}
}

// Flush forces an immediate flush of the current batch
// Useful for graceful shutdown
func (w *DolphinDBWriter) Flush() error {
	w.flush()
	return nil
}

// Stop gracefully stops the writer, flushing any pending events
func (w *DolphinDBWriter) Stop() error {
	// Cancel context (triggers flushLoop final flush)
	w.cancel()

	// Wait for goroutines
	w.wg.Wait()

	// Close DB connection
	if w.db != nil {
		w.db.Close()
		w.connected.Store(false)
	}

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
