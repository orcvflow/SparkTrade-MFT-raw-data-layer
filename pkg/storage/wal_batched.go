package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/health"
)

// BatchedWAL is a Write-Ahead Log variant that defers fsync: Write appends to a
// bufio buffer and returns immediately, and a background goroutine calls
// Flush()+Sync() at most every batchTimeout. This amortizes the fsync cost
// across many messages — the per-message fsync of WAL is the documented
// throughput bottleneck (CLAUDE.md / PROGRESS.md Step C: "WAL fsync per-message
// is a known bottleneck").
//
// DURABILITY TRADE-OFF (honest): unlike WAL (which fsyncs every message, so zero
// in-flight loss on crash), BatchedWAL can lose up to batchTimeout worth of
// un-fsynced messages on a hard crash. It is therefore NOT the production
// default. It exists to (a) quantify the fsync bottleneck in the Addım E E1
// benchmark and (b) serve as a candidate for a configurable durability mode
// where callers accept bounded in-flight loss in exchange for throughput.
//
// File format + naming are identical to WAL (wal_YYYYMMDD_HHMMSS_NNNNNN.jsonl,
// JSON Lines), so Replay() reuses the same reader. Never panics — every public
// method has defer/recover and returns error/default.
type BatchedWAL struct {
	directory      string
	maxFileSize    int64
	maxMessages    int64
	rotateInterval time.Duration
	batchTimeout   time.Duration

	currentFile     *os.File
	currentWriter   *bufio.Writer
	currentFilePath string
	currentSize     atomic.Int64
	currentMessages atomic.Int64
	rotationSeq     atomic.Uint64
	pendingMessages atomic.Int64 // un-fsynced in-flight count (informational)

	running        atomic.Bool
	totalWritten   atomic.Uint64
	totalFlushes   atomic.Uint64
	totalRotations atomic.Uint64
	lastWrite      atomic.Value // time.Time

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	errors   []error
	errorsMu sync.RWMutex
}

// NewBatchedWAL creates a batched WAL. batchTimeout is the max delay between
// fsyncs (default 50ms if <=0). Rotation reuses WALConfig.MaxFileSize/MaxMessages.
func NewBatchedWAL(config WALConfig, batchTimeout time.Duration) (*BatchedWAL, error) {
	if batchTimeout <= 0 {
		batchTimeout = 50 * time.Millisecond
	}
	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create BatchedWAL directory: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &BatchedWAL{
		directory:      config.Directory,
		maxFileSize:    config.MaxFileSize,
		maxMessages:    config.MaxMessages,
		rotateInterval: config.RotateInterval,
		batchTimeout:   batchTimeout,
		ctx:            ctx,
		cancel:         cancel,
		errors:         make([]error, 0),
	}, nil
}

// Start opens the initial file and spawns the flush + rotation goroutines.
func (w *BatchedWAL) Start() error {
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in BatchedWAL.Start: %v", r))
		}
	}()
	if err := w.rotate(); err != nil {
		return err
	}
	w.running.Store(true)
	w.wg.Add(1)
	go w.flushLoop()
	w.wg.Add(1)
	go w.rotationChecker()
	return nil
}

// Write appends an event to the bufio buffer WITHOUT fsync. The background
// flushLoop performs the fsync. Returns immediately; thread-safe; never panics.
func (w *BatchedWAL) Write(event *canonicalizer.CanonicalEvent) error {
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in BatchedWAL.Write: %v", r))
		}
	}()
	if !w.running.Load() {
		return fmt.Errorf("BatchedWAL not running")
	}
	data, err := json.Marshal(event)
	if err != nil {
		w.addError(fmt.Errorf("json marshal error: %w", err))
		return err
	}
	data = append(data, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentWriter == nil {
		return fmt.Errorf("BatchedWAL writer not initialized")
	}
	n, err := w.currentWriter.Write(data)
	if err != nil {
		w.addError(fmt.Errorf("write error: %w", err))
		return err
	}
	w.currentSize.Add(int64(n))
	w.currentMessages.Add(1)
	w.pendingMessages.Add(1)
	w.totalWritten.Add(1)
	health.WALWrites.Inc()
	w.lastWrite.Store(time.Now())
	if w.shouldRotate() {
		w.rotateLocked()
	}
	return nil
}

// Flush flushes the bufio buffer and fsyncs the file. Called by flushLoop and
// available for explicit drain (e.g. at benchmark end, before Stop).
func (w *BatchedWAL) Flush() error {
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in BatchedWAL.Flush: %v", r))
		}
	}()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

// flushLocked performs the flush+fsync assuming the caller holds w.mu.
func (w *BatchedWAL) flushLocked() error {
	if w.currentWriter == nil || w.currentFile == nil {
		return nil
	}
	if err := w.currentWriter.Flush(); err != nil {
		w.addError(fmt.Errorf("flush error: %w", err))
		return err
	}
	if err := w.currentFile.Sync(); err != nil {
		w.addError(fmt.Errorf("sync error: %w", err))
		return err
	}
	w.pendingMessages.Store(0)
	w.totalFlushes.Add(1)
	return nil
}

func (w *BatchedWAL) flushLoop() {
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in BatchedWAL.flushLoop: %v", r))
		}
		w.wg.Done()
	}()
	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			_ = w.Flush()
		}
	}
}

func (w *BatchedWAL) shouldRotate() bool {
	return w.currentSize.Load() >= w.maxFileSize || w.currentMessages.Load() >= w.maxMessages
}

func (w *BatchedWAL) rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
}

// rotateLocked performs rotation assuming the caller holds w.mu. It fsyncs the
// outgoing file first so no un-fsynced data straddles a file boundary.
func (w *BatchedWAL) rotateLocked() error {
	_ = w.flushLocked()
	if w.currentWriter != nil {
		w.currentWriter.Flush()
	}
	if w.currentFile != nil {
		w.currentFile.Close()
	}
	w.rotationSeq.Add(1)
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("wal_%s_%06d.jsonl", timestamp, w.rotationSeq.Load()%1000000)
	fp := filepath.Join(w.directory, filename)
	file, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		w.addError(fmt.Errorf("failed to create BatchedWAL file: %w", err))
		return err
	}
	w.currentFile = file
	w.currentWriter = bufio.NewWriter(file)
	w.currentFilePath = fp
	w.currentSize.Store(0)
	w.currentMessages.Store(0)
	w.totalRotations.Add(1)
	return nil
}

func (w *BatchedWAL) rotationChecker() {
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in BatchedWAL.rotationChecker: %v", r))
		}
		w.wg.Done()
	}()
	ticker := time.NewTicker(w.rotateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if w.shouldRotate() {
				_ = w.rotate()
			}
		}
	}
}

// Stop drains the final batch (fsync), stops goroutines, and closes the file.
func (w *BatchedWAL) Stop() error {
	w.running.Store(false)
	w.cancel()
	w.wg.Wait()
	w.mu.Lock()
	_ = w.flushLocked()
	if w.currentWriter != nil {
		w.currentWriter.Flush()
	}
	if w.currentFile != nil {
		w.currentFile.Close()
	}
	w.mu.Unlock()
	return nil
}

// Stats returns WALStats (same shape as WAL) for familiarity.
func (w *BatchedWAL) Stats() WALStats {
	w.errorsMu.RLock()
	errs := make([]error, len(w.errors))
	copy(errs, w.errors)
	w.errorsMu.RUnlock()
	lastWrite := time.Time{}
	if v := w.lastWrite.Load(); v != nil {
		lastWrite = v.(time.Time)
	}
	w.mu.Lock()
	cur := w.currentFilePath
	w.mu.Unlock()
	return WALStats{
		Running:         w.running.Load(),
		CurrentFile:     cur,
		CurrentSize:     w.currentSize.Load(),
		CurrentMessages: w.currentMessages.Load(),
		TotalWritten:    w.totalWritten.Load(),
		TotalRotations:  w.totalRotations.Load(),
		LastWrite:       lastWrite,
		Errors:          errs,
	}
}

// PendingMessages returns the count of un-fsynced messages (informational).
func (w *BatchedWAL) PendingMessages() int64 {
	return w.pendingMessages.Load()
}

// TotalFlushes returns the number of fsync batches performed.
func (w *BatchedWAL) TotalFlushes() uint64 {
	return w.totalFlushes.Load()
}

// Replay reads all wal_*.jsonl files in the directory. Format is identical to
// WAL, so this mirrors WAL.Replay. Never panics; malformed lines are skipped.
func (w *BatchedWAL) Replay() ([]canonicalizer.CanonicalEvent, error) {
	files, err := filepath.Glob(filepath.Join(w.directory, "wal_*.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("failed to list BatchedWAL files: %w", err)
	}
	var events []canonicalizer.CanonicalEvent
	for _, filePath := range files {
		fileEvents, err := readWALFile(filePath)
		if err != nil {
			w.addError(fmt.Errorf("failed to read %s: %w", filePath, err))
			continue
		}
		events = append(events, fileEvents...)
	}
	return events, nil
}

// readWALFile reads a single wal_*.jsonl file and returns the decoded events.
// The file format is identical for WAL and BatchedWAL (JSON Lines), so this
// standalone helper is shared. Never panics; malformed lines are skipped.
func readWALFile(filePath string) ([]canonicalizer.CanonicalEvent, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var events []canonicalizer.CanonicalEvent
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024) // 1 MB initial buffer
	scanner.Buffer(buf, 10*1024*1024)  // 10 MB max line

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var event canonicalizer.CanonicalEvent
		if err := json.Unmarshal(line, &event); err != nil {
			continue // skip malformed line (never panic)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return events, fmt.Errorf("scanner error: %w", err)
	}
	return events, nil
}

// addError appends an error (max 10), mirroring WAL.
func (w *BatchedWAL) addError(err error) {
	w.errorsMu.Lock()
	defer w.errorsMu.Unlock()
	w.errors = append(w.errors, err)
	if len(w.errors) > 10 {
		w.errors = w.errors[1:]
	}
}
