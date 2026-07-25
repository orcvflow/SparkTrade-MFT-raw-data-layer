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

// WAL (Write-Ahead Log) provides durable storage before DolphinDB
// Format: JSON Lines (/var/log/raw_data/wal/YYYY-MM-DD.jsonl)
// Rotation: Every 100MB or 10,000 messages
// Purpose: Prevent data loss if DolphinDB is unavailable
type WAL struct {
	// Configuration
	directory      string
	maxFileSize    int64
	maxMessages    int64
	rotateInterval time.Duration
	
	// Current file
	currentFile     *os.File
	currentWriter   *bufio.Writer
	currentFilePath string
	currentSize     atomic.Int64
	currentMessages atomic.Int64
	rotationSeq     atomic.Uint64 // monotonic counter for unique filenames
	
	// State
	running        atomic.Bool
	totalWritten   atomic.Uint64
	totalRotations atomic.Uint64
	lastWrite      atomic.Value // time.Time
	
	// Synchronization
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	
	// Errors
	errors   []error
	errorsMu sync.RWMutex
}

// WALConfig holds WAL configuration
type WALConfig struct {
	Directory      string
	MaxFileSize    int64         // Bytes
	MaxMessages    int64         // Messages per file
	RotateInterval time.Duration // Check rotation every X
}

// DefaultWALConfig returns default configuration
func DefaultWALConfig() WALConfig {
	return WALConfig{
		Directory:      "/var/log/raw_data/wal",
		MaxFileSize:    100 * 1024 * 1024, // 100MB
		MaxMessages:    10000,
		RotateInterval: 1 * time.Minute,
	}
}

// NewWAL creates a new Write-Ahead Log
func NewWAL(config WALConfig) (*WAL, error) {
	// Create directory if not exists
	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	return &WAL{
		directory:      config.Directory,
		maxFileSize:    config.MaxFileSize,
		maxMessages:    config.MaxMessages,
		rotateInterval: config.RotateInterval,
		ctx:            ctx,
		cancel:         cancel,
		errors:         make([]error, 0),
	}, nil
}

// Start initializes the WAL
func (w *WAL) Start() error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in Start: %v", r))
		}
	}()
	
	// Create initial file
	if err := w.rotate(); err != nil {
		return err
	}
	
	w.running.Store(true)
	
	// Start rotation checker
	w.wg.Add(1)
	go w.rotationChecker()
	
	return nil
}

// Write writes a canonical event to WAL
func (w *WAL) Write(event *canonicalizer.CanonicalEvent) error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in Write: %v", r))
		}
	}()
	
	if !w.running.Load() {
		return fmt.Errorf("WAL not running")
	}
	
	// Serialize to JSON
	data, err := json.Marshal(event)
	if err != nil {
		w.addError(fmt.Errorf("json marshal error: %w", err))
		return err
	}
	
	// Add newline (JSON Lines format)
	data = append(data, '\n')
	
	// Write to file (thread-safe)
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.currentWriter == nil {
		return fmt.Errorf("WAL writer not initialized")
	}
	
	n, err := w.currentWriter.Write(data)
	if err != nil {
		w.addError(fmt.Errorf("write error: %w", err))
		return err
	}
	
	// Flush immediately (durability)
	if err := w.currentWriter.Flush(); err != nil {
		w.addError(fmt.Errorf("flush error: %w", err))
		return err
	}
	
	// Sync to disk (fsync)
	if err := w.currentFile.Sync(); err != nil {
		w.addError(fmt.Errorf("sync error: %w", err))
		return err
	}
	
	// Update counters
	w.currentSize.Add(int64(n))
	w.currentMessages.Add(1)
	w.totalWritten.Add(1)
	health.WALWrites.Inc()
	w.lastWrite.Store(time.Now())
	
	// Check if rotation needed — rotate synchronously (still under w.mu).
	// Previously this was `go w.rotate()`, which raced: multiple concurrent
	// writers each spawning a rotation goroutine, all re-opening the same
	// second-precision filename and closing each other's freshly opened file,
	// losing data and producing a single file with N spurious rotations.
	if w.shouldRotate() {
		w.rotateLocked() // sync, no new goroutine, no race
	}

	return nil
}

// shouldRotate checks if file should be rotated
func (w *WAL) shouldRotate() bool {
	size := w.currentSize.Load()
	messages := w.currentMessages.Load()
	
	return size >= w.maxFileSize || messages >= w.maxMessages
}

// rotate creates a new WAL file. Public entry point acquires the mutex.
func (w *WAL) rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
}

// rotateLocked performs rotation assuming the caller already holds w.mu.
// The filename embeds a monotonic counter so multiple rotations within the
// same second never collide on the same path (previously they reopened the
// same file via O_APPEND, losing data and inflating the rotation count).
func (w *WAL) rotateLocked() error {
	// Close current file
	if w.currentWriter != nil {
		w.currentWriter.Flush()
	}
	if w.currentFile != nil {
		w.currentFile.Close()
	}

	// Unique filename: compact timestamp + monotonic sequence. This replaces
	// second-only precision, which collided for rapid rotations.
	w.rotationSeq.Add(1)
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("wal_%s_%06d.jsonl", timestamp, w.rotationSeq.Load()%1000000)
	filepath := filepath.Join(w.directory, filename)

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		w.addError(fmt.Errorf("failed to create WAL file: %w", err))
		return err
	}

	w.currentFile = file
	w.currentWriter = bufio.NewWriter(file)
	w.currentFilePath = filepath
	w.currentSize.Store(0)
	w.currentMessages.Store(0)
	w.totalRotations.Add(1)
	
	return nil
}

// rotationChecker periodically checks if rotation is needed
func (w *WAL) rotationChecker() {
	defer func() {
		if r := recover(); r != nil {
			w.addError(fmt.Errorf("panic in rotationChecker: %v", r))
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
				w.rotate()
			}
		}
	}
}

// Replay reads all WAL files and returns events
func (w *WAL) Replay() ([]canonicalizer.CanonicalEvent, error) {
	files, err := filepath.Glob(filepath.Join(w.directory, "wal_*.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}
	
	var events []canonicalizer.CanonicalEvent
	
	for _, filePath := range files {
		fileEvents, err := w.readFile(filePath)
		if err != nil {
			w.addError(fmt.Errorf("failed to read %s: %w", filePath, err))
			continue
		}
		events = append(events, fileEvents...)
	}
	
	return events, nil
}

// readFile reads a single WAL file
func (w *WAL) readFile(filePath string) ([]canonicalizer.CanonicalEvent, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	var events []canonicalizer.CanonicalEvent
	scanner := bufio.NewScanner(file)
	
	// Increase buffer size for large lines
	buf := make([]byte, 0, 1024*1024) // 1MB buffer
	scanner.Buffer(buf, 10*1024*1024) // 10MB max line
	
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		
		if len(line) == 0 {
			continue
		}
		
		var event canonicalizer.CanonicalEvent
		if err := json.Unmarshal(line, &event); err != nil {
			w.addError(fmt.Errorf("line %d: json unmarshal error: %w", lineNum, err))
			continue
		}
		
		events = append(events, event)
	}
	
	if err := scanner.Err(); err != nil {
		return events, fmt.Errorf("scanner error: %w", err)
	}
	
	return events, nil
}

// CleanOld removes WAL files older than specified duration
func (w *WAL) CleanOld(olderThan time.Duration) error {
	files, err := filepath.Glob(filepath.Join(w.directory, "wal_*.jsonl"))
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}
	
	cutoff := time.Now().Add(-olderThan)
	cleaned := 0
	
	for _, filePath := range files {
		info, err := os.Stat(filePath)
		if err != nil {
			continue
		}
		
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(filePath); err != nil {
				w.addError(fmt.Errorf("failed to remove %s: %w", filePath, err))
				continue
			}
			cleaned++
		}
	}
	
	return nil
}

// Stop gracefully stops the WAL
func (w *WAL) Stop() error {
	w.running.Store(false)
	w.cancel()
	w.wg.Wait()
	
	// Flush and close current file
	w.mu.Lock()
	if w.currentWriter != nil {
		w.currentWriter.Flush()
	}
	if w.currentFile != nil {
		w.currentFile.Close()
	}
	w.mu.Unlock()
	
	return nil
}

// Stats returns WAL statistics
func (w *WAL) Stats() WALStats {
	w.errorsMu.RLock()
	errors := make([]error, len(w.errors))
	copy(errors, w.errors)
	w.errorsMu.RUnlock()

	lastWrite := time.Time{}
	if v := w.lastWrite.Load(); v != nil {
		lastWrite = v.(time.Time)
	}

	// currentFilePath is written under w.mu in rotateLocked(); read it under the
	// same mutex so a concurrent rotation (rotationChecker tick or Write's
	// shouldRotate→rotateLocked) can't race with Stats. The atomic fields are
	// already safe; currentFilePath is the one plain field.
	w.mu.Lock()
	currentFile := w.currentFilePath
	w.mu.Unlock()

	return WALStats{
		Running:          w.running.Load(),
		CurrentFile:      currentFile,
		CurrentSize:      w.currentSize.Load(),
		CurrentMessages:  w.currentMessages.Load(),
		TotalWritten:     w.totalWritten.Load(),
		TotalRotations:   w.totalRotations.Load(),
		LastWrite:         lastWrite,
		Errors:            errors,
	}
}

// WALStats holds WAL statistics
type WALStats struct {
	Running         bool
	CurrentFile     string
	CurrentSize     int64
	CurrentMessages int64
	TotalWritten    uint64
	TotalRotations  uint64
	LastWrite       time.Time
	Errors          []error
}

// IsHealthy returns true if WAL is healthy
func (s WALStats) IsHealthy() bool {
	// WAL is healthy if:
	// - Running
	// - Written at least once
	// - Last write is recent (<1 minute)
	
	if !s.Running {
		return false
	}
	
	if s.TotalWritten == 0 {
		return false
	}
	
	if !s.LastWrite.IsZero() && time.Since(s.LastWrite) > 1*time.Minute {
		return false
	}
	
	return true
}

// addError adds an error to the error list (max 10)
func (w *WAL) addError(err error) {
	w.errorsMu.Lock()
	defer w.errorsMu.Unlock()
	
	w.errors = append(w.errors, err)
	if len(w.errors) > 10 {
		w.errors = w.errors[1:]
	}
}
