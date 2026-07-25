// Package allocator provides a generic, allocation-reducing object pool built
// on sync.Pool.
//
// Addım D uses it to recycle CanonicalEvent objects on the canonicalizer hot
// path: each parsed message no longer allocates a fresh event — it acquires one
// from the pool, fills it, and releases it back after the downstream marshal
// (which copies the bytes out) is done.
//
// Design note — why generic and not the spec's concrete
// `var EventPool = sync.Pool{New: func() any { return &canonicalizer.CanonicalEvent{} }}`:
// that snippet creates an import cycle (allocator → canonicalizer for the type,
// canonicalizer → allocator to use the pool). A generic Pool[T] breaks the
// cycle: allocator knows nothing about canonicalizer; canonicalizer
// instantiates `allocator.Pool[*CanonicalEvent]`. The Reset hook is a function
// the caller supplies, so allocator also does not depend on a Reset() method
// existing on the plain CanonicalEvent struct (though one is added anyway for
// convenience).
//
// Safety contract (CLAUDE.md "never lose data"): an object acquired from the
// pool MUST NOT be released until every consumer downstream of the Acquire is
// finished reading it. The canonicalizer process owns the event end-to-end
// (Acquire in Process → marshal copies bytes out → Release in the output loop),
// so no async reference survives the Release. Pooling is therefore use-after-
// reset free. See cmd/canonicalizer/main.go's output loop.
package allocator

import "sync"

// Pool is a generic object pool. The New function constructs fresh objects when
// the pool is empty; the Reset function (optional) clears a returned object so
// stale fields do not leak into the next Acquire. T is typically a pointer type.
type Pool[T any] struct {
	pool  sync.Pool
	reset func(T)
}

// NewPool builds a Pool. newFn must be non-nil and return a zeroed/fresh T;
// resetFn may be nil (in which case Put is a no-op cleanup — callers then reset
// the object themselves before reuse). Never panics.
func NewPool[T any](newFn func() T, resetFn func(T)) *Pool[T] {
	p := &Pool[T]{reset: resetFn}
	// sync.Pool.New returns any; box the typed value from newFn.
	p.pool.New = func() any {
		return newFn()
	}
	return p
}

// Get returns a T from the pool, allocating via New if the pool is empty.
func (p *Pool[T]) Get() T {
	v := p.pool.Get()
	// The cast is always safe: the pool only ever holds T (from New or Put).
	return v.(T)
}

// Put returns x to the pool. If a Reset hook is set, x is reset first so the
// next Acquire gets a clean object. Put with a nil pool is a no-op.
func (p *Pool[T]) Put(x T) {
	if p == nil {
		return
	}
	if p.reset != nil {
		p.reset(x)
	}
	p.pool.Put(x)
}
