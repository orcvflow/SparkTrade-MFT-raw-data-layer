package ipc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// ErrSpoolFull is returned when the spool file has reached its size cap. The
// caller (Phase 4 wiring) decides whether to apply hard backpressure.
var ErrSpoolFull = errors.New("ipc: spool full")

const (
	// defaultMaxSpoolBytes bounds the on-disk spool. During a downstream outage
	// the in-memory queue overflows to this file; once it fills, Send fails with
	// ErrSpoolFull rather than growing unbounded (OOM defense).
	defaultMaxSpoolBytes = 256 * 1024 * 1024 // 256 MiB
	spoolRecHeader       = 4
)

// spool is a bounded, append-only FIFO of marshaled IPCMessage bodies. It is
// the lossless overflow sink for the client when the downstream is down or the
// in-memory queue is full.
//
// Drain semantics (lossless, crash-safe for process restarts):
//   - drain reads every complete record into memory, sends them via the
//     caller's send func, and only truncates the file once ALL sends succeed.
//   - If send fails mid-way, the file is left intact; already-sent records are
//     re-sent on the next drain (duplicate delivery). Duplicates are acceptable
//     for market-data consumers that dedupe by event_id/seq; a LOSS is never
//     introduced.
//   - A trailing partial record (host crash mid-append) is ignored, not
//     replayed — it was never acknowledged.
//
// The authoritative lossless guarantee for the whole system lives in the
// Storage process's WAL (pkg/storage/wal.go); this spool is a best-effort
// smoothing buffer for process-level outages (which is exactly what the chaos
// tests simulate).
type spool struct {
	path     string
	maxBytes int64
	curBytes atomic.Int64

	mu sync.Mutex
	f  *os.File
}

func newSpool(path string, maxBytes int64) (*spool, error) {
	if maxBytes <= 0 {
		maxBytes = defaultMaxSpoolBytes
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("ipc: create spool dir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("ipc: open spool %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("ipc: stat spool: %w", err)
	}
	s := &spool{path: path, f: f, maxBytes: maxBytes}
	s.curBytes.Store(info.Size())
	return s, nil
}

// append writes one marshaled body to the spool. Never panics.
func (s *spool) append(body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	recLen := int64(spoolRecHeader + len(body))
	if s.curBytes.Load()+recLen > s.maxBytes {
		return ErrSpoolFull
	}
	var hdr [spoolRecHeader]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := s.f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.f.Write(body); err != nil {
		return err
	}
	s.curBytes.Add(recLen)
	return nil
}

// drain replays spooled records via send. Returns the number successfully sent.
// On full success the spool is truncated. On a send error the spool is left
// intact (records will be retried, with possible duplicates). Never panics.
func (s *spool) drain(send func(body []byte) error) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	br := bufio.NewReader(s.f)

	type rec struct{ body []byte }
	var recs []rec
	for {
		var hdr [spoolRecHeader]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			// EOF or trailing partial record (host crash mid-append): stop.
			break
		}
		size := binary.BigEndian.Uint32(hdr[:])
		if size == 0 {
			continue
		}
		if size > maxFrameSize {
			return 0, fmt.Errorf("%w: spool record %d > %d", ErrFrameTooLarge, size, maxFrameSize)
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(br, body); err != nil {
			break // trailing partial — ignore
		}
		recs = append(recs, rec{body: body})
	}

	sent := 0
	for _, r := range recs {
		if err := send(r.body); err != nil {
			return sent, err // spool stays intact → retry with duplicates next drain
		}
		sent++
	}
	if err := s.f.Truncate(0); err != nil {
		return sent, err
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return sent, err
	}
	s.curBytes.Store(0)
	return sent, nil
}

func (s *spool) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

func (s *spool) size() int64 { return s.curBytes.Load() }
