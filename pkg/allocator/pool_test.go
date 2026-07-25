package allocator

import (
	"sync/atomic"
	"testing"
)

// benchEvent is a stand-in for canonicalizer.CanonicalEvent: a struct with a
// few scalars + a slice that must be reset (not just dropped) on reuse.
type benchEvent struct {
	ID    int64
	Price float64
	Size  float64
	Side  string
	Data  []byte
}

func (e *benchEvent) Reset() {
	e.ID = 0
	e.Price = 0
	e.Size = 0
	e.Side = ""
	e.Data = e.Data[:0] // keep cap, drop length
}

// TestPool_GetPut_Roundtrip: Get returns a usable object; Put+Get recycles.
func TestPool_GetPut_Roundtrip(t *testing.T) {
	var resets atomic.Int32
	p := NewPool(
		func() *benchEvent { return &benchEvent{} },
		func(e *benchEvent) { resets.Add(1); e.Reset() },
	)
	e := p.Get()
	if e == nil {
		t.Fatal("Get returned nil")
	}
	e.ID = 42
	e.Price = 100.0
	e.Data = append(e.Data, 1, 2, 3)
	p.Put(e)
	if resets.Load() != 1 {
		t.Errorf("Reset called %d times, want 1", resets.Load())
	}
	// Re-acquire — most likely the same object, now reset.
	e2 := p.Get()
	if e2.ID != 0 || e2.Price != 0 || len(e2.Data) != 0 {
		t.Errorf("recycled object not reset: %+v", e2)
	}
}

// TestPool_NewOnEmpty: when the pool is empty (no Puts yet), Get must invoke
// New and never panic.
func TestPool_NewOnEmpty(t *testing.T) {
	p := NewPool(func() *benchEvent { return &benchEvent{ID: 7} }, nil)
	e := p.Get()
	if e.ID != 7 {
		t.Errorf("Get from empty pool: ID=%d, want 7 (from New)", e.ID)
	}
}

// TestPool_NilResetOK: a nil reset hook must not panic on Put.
func TestPool_NilResetOK(t *testing.T) {
	p := NewPool(func() *benchEvent { return &benchEvent{} }, nil)
	e := p.Get()
	e.ID = 99
	p.Put(e) // must not panic with nil reset
}

// --- Benchmark: pool (recycled) vs new() (allocate each time) --------------
//
// BenchmarkPoolAlloc/Pool: Get → fill → Put (steady-state recycling)
// BenchmarkPoolAlloc/New:  new()  → fill        (allocate every iteration)
//
// The spec cites "85% fewer allocs". In steady state sync.Pool recycles to ~0
// allocs/op (modulo GC pool eviction), so the real measured number on this
// machine is reported directly — not the spec's marketing figure.

func BenchmarkPoolAlloc(b *testing.B) {
	pool := NewPool(func() *benchEvent { return &benchEvent{} }, func(e *benchEvent) { e.Reset() })

	b.Run("Pool", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e := pool.Get()
			e.ID = int64(i)
			e.Price = 100.0
			e.Size = 1.0
			e.Side = "BUY"
			e.Data = append(e.Data, byte(i))
			pool.Put(e)
		}
	})

	b.Run("New", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e := &benchEvent{}
			e.ID = int64(i)
			e.Price = 100.0
			e.Size = 1.0
			e.Side = "BUY"
			e.Data = append(e.Data, byte(i))
			_ = e
		}
	})
}
