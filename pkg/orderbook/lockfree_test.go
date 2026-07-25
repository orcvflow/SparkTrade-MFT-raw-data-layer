package orderbook

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestLockFreeOrderBook_SetGet(t *testing.T) {
	ob := NewLockFreeOrderBook()
	bids := []Level{{Price: 100.0, Size: 5, Side: "BID", OrderID: 1}, {Price: 99.0, Size: 3, Side: "BID", OrderID: 2}}
	ob.SetBids(bids)

	got := ob.Bids()
	if len(got) != 2 || got[0].Price != 100.0 || got[1].Price != 99.0 {
		t.Errorf("Bids = %+v, want 2 levels 100/99", got)
	}
	// Mutate the caller's slice AFTER SetBids: the published snapshot must be
	// untouched (SetBids copies). This is the core invariant.
	bids[0].Price = 0
	got = ob.Bids()
	if got[0].Price != 100.0 {
		t.Errorf("published snapshot mutated by caller: got[0].Price=%v, want 100", got[0].Price)
	}
	if ob.Seq() != 1 {
		t.Errorf("Seq=%d, want 1", ob.Seq())
	}
}

// TestLockFreeOrderBook_Concurrent is the -race mandatory test: one writer
// publishing snapshots concurrently with many readers. The lock-free book must
// be race-free and every reader must see a consistent (non-torn) snapshot.
func TestLockFreeOrderBook_Concurrent(t *testing.T) {
	ob := NewLockFreeOrderBook()
	// Build deterministic snapshots where bids all share a tag; readers check
	// every level carries the same tag, so a torn read (mixing two snapshots)
	// is detectable.
	snapshot := func(tag int) []Level {
		ls := make([]Level, 16)
		for i := range ls {
			ls[i] = Level{Price: float64(tag)*100 + float64(i), Size: float64(tag), Side: "BID", OrderID: int64(tag)}
		}
		return ls
	}

	var stop atomic.Bool
	var writersDone sync.WaitGroup
	writersDone.Add(1)
	go func() {
		defer writersDone.Done()
		for tag := 1; !stop.Load(); tag++ {
			ob.SetBids(snapshot(tag))
		}
	}()

	// 8 readers, each checking snapshot consistency.
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				got := ob.Bids()
				if len(got) == 0 {
					// The legitimate initial empty snapshot (before the writer's
					// first publish) — NOT a torn read. With atomic.Pointer a
					// torn read is impossible: Store/Load of *sideBook is atomic,
					// and sideBook is never mutated after publish.
					continue
				}
				if len(got) != 16 {
					t.Errorf("unexpected snapshot len %d (want 0 or 16)", len(got))
					return
				}
				// Every level in a single snapshot carries the same OrderID
				// (tag). A mix would mean two snapshots were spliced together.
				tag := got[0].OrderID
				for _, l := range got {
					if l.OrderID != tag {
						t.Errorf("torn read: tags %d vs %d (snapshot mixed two writes)", tag, l.OrderID)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	stop.Store(true)
	writersDone.Wait()
}

func TestMutexOrderBook_SetGet(t *testing.T) {
	ob := NewMutexOrderBook()
	ob.SetAsks([]Level{{Price: 101.0, Size: 2, Side: "ASK"}})
	if got := ob.Asks(); len(got) != 1 || got[0].Price != 101.0 {
		t.Errorf("Asks = %+v", got)
	}
}

// --- Benchmark: lock-free (atomic.Pointer) vs mutex (RWMutex) --------------
//
// A read-heavy workload: a background writer publishes fresh snapshots while
// b.RunParallel readers each read a snapshot per iteration. The lock-free book
// wins by avoiding both the RWMutex read-lock contention AND the copy that the
// mutex book must do to return a safe snapshot. Real measured ratio is
// reported (spec cites "5×"; this machine's number is in the bench output).

func benchmarkOrderBookReads(b *testing.B, reader func() []Level, writer func([]Level), seq func() uint64) {
	levels := make([]Level, 64)
	for i := range levels {
		levels[i] = Level{Price: float64(i), Size: 1, OrderID: int64(i)}
	}
	writer(levels)
	_ = seq()

	var stop atomic.Bool
	var writersDone sync.WaitGroup
	writersDone.Add(1)
	go func() {
		defer writersDone.Done()
		for i := 0; !stop.Load(); i++ {
			l := make([]Level, 64)
			for j := range l {
				l[j] = Level{Price: float64(i + j), Size: 1, OrderID: int64(i)}
			}
			writer(l)
		}
	}()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			got := reader()
			if len(got) == 0 {
				b.Fatal("empty snapshot")
			}
		}
	})
	b.StopTimer()
	stop.Store(true)
	writersDone.Wait()
}

func BenchmarkLockFree(b *testing.B) {
	ob := NewLockFreeOrderBook()
	b.ReportAllocs()
	benchmarkOrderBookReads(b, ob.Bids, ob.SetBids, ob.Seq)
}

func BenchmarkMutexOB(b *testing.B) {
	ob := NewMutexOrderBook()
	b.ReportAllocs()
	benchmarkOrderBookReads(b, ob.Asks, ob.SetAsks, ob.Seq)
}
