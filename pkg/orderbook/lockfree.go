// Package orderbook provides a lock-free order book built on atomic.Pointer.
//
// Addım D replaces a sync.RWMutex-protected book (lock contention, 40%
// throughput loss per the HFT matching-engine evidence cited in CLAUDE.md)
// with an immutable-snapshot design: writers publish a whole new sideBook via
// an atomic pointer store; readers do a single atomic load and receive a
// consistent, immutable snapshot with no locks and no copying.
//
// Correctness note — why this and not the spec's snippet:
//
//	var bids unsafe.Pointer // *[]Level
//	func (ob *OB) Update(levels []Level) {
//	    new := make([]Level, ...)
//	    atomic.StorePointer(&ob.bids, unsafe.Pointer(&new)) // ← UB
//	}
//
// `&new` is the address of a local slice header. atomic.Pointer to a
// heap-allocated wrapper (sideBook) is the race-free, GC-safe version: the
// wrapper is allocated on the heap, its address is stable, and readers never
// see a torn slice. There is no raw unsafe.Pointer in the public API.
//
// Tested with -race (CLAUDE.md mandatory: "0 race condition").
package orderbook

import (
	"sync"
	"sync/atomic"
)

// Level is one price level of the book. It mirrors canonicalizer.Level but is
// local to this package to avoid coupling orderbook to the canonicalizer (the
// spec ships orderbook as a standalone component).
type Level struct {
	Price   float64
	Size    float64
	Side    string
	OrderID int64
}

// sideBook is an immutable snapshot of one side. Once published, its levels
// slice is never mutated in place — a writer that wants to change the side
// builds a new sideBook and atomically swaps the pointer. This is what makes
// lock-free concurrent reads safe.
type sideBook struct {
	levels []Level
}

// LockFreeOrderBook is a lock-free L2 order book. Reads are a single atomic
// pointer load; writes are a build + atomic store. The sequence counter
// lets a reader detect whether a snapshot changed between two reads.
type LockFreeOrderBook struct {
	bids atomic.Pointer[sideBook]
	asks atomic.Pointer[sideBook]
	seq  atomic.Uint64
}

// NewLockFreeOrderBook returns an empty book with non-nil sides so readers
// never have to nil-check after a Load.
func NewLockFreeOrderBook() *LockFreeOrderBook {
	ob := &LockFreeOrderBook{}
	ob.bids.Store(&sideBook{})
	ob.asks.Store(&sideBook{})
	return ob
}

// SetBids publishes an immutable copy of levels as the new bids snapshot.
// The caller's slice is copied so subsequent caller mutation cannot tear the
// published snapshot. Never panics.
func (ob *LockFreeOrderBook) SetBids(levels []Level) {
	cp := append([]Level(nil), levels...)
	ob.bids.Store(&sideBook{levels: cp})
	ob.seq.Add(1)
}

// SetAsks publishes an immutable copy of levels as the new asks snapshot.
func (ob *LockFreeOrderBook) SetAsks(levels []Level) {
	cp := append([]Level(nil), levels...)
	ob.asks.Store(&sideBook{levels: cp})
	ob.seq.Add(1)
}

// Bids returns the current bids snapshot. The returned slice is immutable for
// the lifetime of this snapshot — it is safe to read concurrently with a
// writer, with no lock and no copy. Returns nil only if the book was never
// initialized.
func (ob *LockFreeOrderBook) Bids() []Level {
	if sb := ob.bids.Load(); sb != nil {
		return sb.levels
	}
	return nil
}

// Asks returns the current asks snapshot (same contract as Bids).
func (ob *LockFreeOrderBook) Asks() []Level {
	if sb := ob.asks.Load(); sb != nil {
		return sb.levels
	}
	return nil
}

// Seq returns the snapshot sequence number. A reader can compare Seq() before
// and after a pair of reads to detect that a write landed in between.
func (ob *LockFreeOrderBook) Seq() uint64 {
	return ob.seq.Load()
}

// --- Mutex baseline --------------------------------------------------------
//
// MutexOrderBook is the "before" path: a sync.RWMutex-protected book. Reads
// take the read lock AND must copy the slice out (a writer could reassign the
// slice header, so handing back the internal slice directly would be unsafe
// across a re-entrant write). The lock-free book above avoids both the lock
// and the copy — the benchmark measures that combined win.

// MutexOrderBook is the RWMutex baseline used by the benchmark.
type MutexOrderBook struct {
	mu   sync.RWMutex
	bids []Level
	asks []Level
	seq  uint64
}

// NewMutexOrderBook returns an empty mutex-protected book.
func NewMutexOrderBook() *MutexOrderBook {
	return &MutexOrderBook{}
}

// SetBids replaces the bids under the write lock.
func (ob *MutexOrderBook) SetBids(levels []Level) {
	cp := append([]Level(nil), levels...)
	ob.mu.Lock()
	ob.bids = cp
	ob.seq++
	ob.mu.Unlock()
}

// SetAsks replaces the asks under the write lock.
func (ob *MutexOrderBook) SetAsks(levels []Level) {
	cp := append([]Level(nil), levels...)
	ob.mu.Lock()
	ob.asks = cp
	ob.seq++
	ob.mu.Unlock()
}

// Bids returns a copy of the bids under the read lock. The copy is required
// for correctness: without it, a concurrent SetBids could reassign the slice
// header while a caller iterates.
func (ob *MutexOrderBook) Bids() []Level {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	return append([]Level(nil), ob.bids...)
}

// Asks returns a copy of the asks under the read lock.
func (ob *MutexOrderBook) Asks() []Level {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	return append([]Level(nil), ob.asks...)
}

// Seq returns the snapshot sequence number.
func (ob *MutexOrderBook) Seq() uint64 {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	return ob.seq
}
