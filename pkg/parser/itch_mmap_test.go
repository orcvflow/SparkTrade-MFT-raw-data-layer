package parser

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// writeITCHFixture writes n ITCH-like records to a temp file and returns its
// path. The mix is roughly: 60% AddOrder, 25% Trade, 10% Modify, 5% System —
// an order-book-shaped workload. Each AddOrder/Trade/Modify is 39 bytes for a
// 4-byte symbol; 100k records ≈ 3.6 MB, enough to make the mmap/bufio
// difference visible without making the test slow.
func writeITCHFixture(t *testing.T, n int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "itch.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create fixture: %v", err)
	}
	defer f.Close()

	var buf [40]byte
	for i := 0; i < n; i++ {
		switch {
		case i%100 < 60: // AddOrder
			buf[0] = 'A'
			be.PutUint64(buf[1:], uint64(i+1))         // orderID
			be.PutUint64(buf[9:], uint64(i)*1000)        // ts ns
			buf[17] = 'B'                                // side
			be.PutUint64(buf[18:], uint64(100000+i))    // price
			be.PutUint64(buf[26:], uint64(10+i%50))     // qty
			buf[34] = 4                                  // symLen
			sym := []byte("AAPL")
			copy(buf[35:39], sym)
			if _, err := f.Write(buf[:39]); err != nil {
				t.Fatalf("write add: %v", err)
			}
		case i%100 < 85: // Trade
			buf[0] = 'T'
			be.PutUint64(buf[1:], uint64(i+1))
			be.PutUint64(buf[9:], uint64(i)*1000)
			buf[17] = 'S'
			be.PutUint64(buf[18:], uint64(100000+i))
			be.PutUint64(buf[26:], uint64(5+i%50))
			buf[34] = 4
			copy(buf[35:39], []byte("MSFT"))
			if _, err := f.Write(buf[:39]); err != nil {
				t.Fatalf("write trade: %v", err)
			}
		case i%100 < 95: // Modify
			buf[0] = 'M'
			be.PutUint64(buf[1:], uint64(i+1))
			be.PutUint64(buf[9:], uint64(i)*1000)
			be.PutUint64(buf[17:], uint64(100000+i))
			be.PutUint64(buf[25:], uint64(20+i%50))
			buf[33] = 4
			copy(buf[34:38], []byte("GOOG"))
			if _, err := f.Write(buf[:38]); err != nil {
				t.Fatalf("write modify: %v", err)
			}
		default: // System
			buf[0] = 'S'
			be.PutUint64(buf[1:], uint64(i)*1000)
			buf[9] = byte(i % 10)
			if _, err := f.Write(buf[:10]); err != nil {
				t.Fatalf("write system: %v", err)
			}
		}
	}
	return path
}

func TestITCHParser_CountAll(t *testing.T) {
	const n = 1000
	path := writeITCHFixture(t, n)

	p, err := NewITCHParser(path)
	if err != nil {
		t.Fatalf("NewITCHParser: %v", err)
	}
	defer p.Close()

	got, err := p.CountAll()
	if err != nil {
		t.Fatalf("CountAll: %v", err)
	}
	if got != n {
		t.Errorf("CountAll = %d, want %d", got, n)
	}
}

func TestITCHParser_ParseAll_Fields(t *testing.T) {
	// Small deterministic fixture: one AddOrder, one Trade, one Modify, one
	// System. Verify field values round-trip exactly through the zero-copy
	// reader.
	dir := t.TempDir()
	path := filepath.Join(dir, "itch.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	add := []byte{'A'}
	add = binary.BigEndian.AppendUint64(add, 42)   // orderID
	add = binary.BigEndian.AppendUint64(add, 1234) // ts
	add = append(add, 'B')                         // side
	add = binary.BigEndian.AppendUint64(add, 50000)
	add = binary.BigEndian.AppendUint64(add, 10)
	add = append(add, 4)               // symLen
	add = append(add, []byte("AAPL")...) // symbol
	tr := []byte{'T'}
	tr = binary.BigEndian.AppendUint64(tr, 7)
	tr = binary.BigEndian.AppendUint64(tr, 99)
	tr = append(tr, 'S')
	tr = binary.BigEndian.AppendUint64(tr, 3000)
	tr = binary.BigEndian.AppendUint64(tr, 3)
	tr = append(tr, 4)
	tr = append(tr, []byte("MSFT")...)
	mo := []byte{'M'}
	mo = binary.BigEndian.AppendUint64(mo, 42)
	mo = binary.BigEndian.AppendUint64(mo, 5555)
	mo = binary.BigEndian.AppendUint64(mo, 51000)
	mo = binary.BigEndian.AppendUint64(mo, 11)
	mo = append(mo, 4)
	mo = append(mo, []byte("GOOG")...)
	sy := []byte{'S'}
	sy = binary.BigEndian.AppendUint64(sy, 7777)
	sy = append(sy, byte(1))
	if _, err := f.Write(append(append(append(add, tr...), mo...), sy...)); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.Close()

	p, err := NewITCHParser(path)
	if err != nil {
		t.Fatalf("NewITCHParser: %v", err)
	}
	defer p.Close()

	msgs, err := p.ParseAll()
	if err != nil {
		t.Fatalf("ParseAll: %v", err)
	}
	if len(msgs) != 4 {
		t.Fatalf("got %d msgs, want 4", len(msgs))
	}
	if msgs[0].Type != 'A' || msgs[0].OrderID != 42 || msgs[0].Side != 'B' || msgs[0].Symbol != "AAPL" || msgs[0].Price != 50000 || msgs[0].Quantity != 10 {
		t.Errorf("AddOrder mismatch: %+v", msgs[0])
	}
	if msgs[1].Type != 'T' || msgs[1].Side != 'S' || msgs[1].Symbol != "MSFT" || msgs[1].Price != 3000 {
		t.Errorf("Trade mismatch: %+v", msgs[1])
	}
	if msgs[2].Type != 'M' || msgs[2].OrderID != 42 || msgs[2].Price != 51000 || msgs[2].Quantity != 11 || msgs[2].Symbol != "GOOG" {
		t.Errorf("Modify mismatch: %+v", msgs[2])
	}
	if msgs[3].Type != 'S' || msgs[3].TS != 7777 || msgs[3].Code != 1 {
		t.Errorf("System mismatch: %+v", msgs[3])
	}
}

// TestITCHParser_MMapEqBufio is the cross-check: the zero-copy mmap parser and
// the bufio parser must agree on the record count for a realistic fixture.
func TestITCHParser_MMapEqBufio(t *testing.T) {
	const n = 5000
	path := writeITCHFixture(t, n)

	mp, err := NewITCHParser(path)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	defer mp.Close()
	mCount, err := mp.CountAll()
	if err != nil {
		t.Fatalf("mmap CountAll: %v", err)
	}

	br, err := NewITCHBufioReader(path)
	if err != nil {
		t.Fatalf("bufio: %v", err)
	}
	defer br.Close()
	bCount, err := br.CountAll()
	if err != nil {
		t.Fatalf("bufio CountAll: %v", err)
	}

	if mCount != bCount {
		t.Errorf("mmap=%d bufio=%d (must agree)", mCount, bCount)
	}
	if mCount != n {
		t.Errorf("count=%d, want %d", mCount, n)
	}
}

func TestITCHParser_Malformed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.bin")
	// Truncated AddOrder (type + 4 bytes, needs 38 more).
	if err := os.WriteFile(path, []byte{'A', 1, 2, 3, 4}, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	p, err := NewITCHParser(path)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	defer p.Close()
	if _, err := p.CountAll(); err == nil {
		t.Error("expected error on truncated record, got nil")
	}
}

func TestITCHParser_UnknownType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.bin")
	if err := os.WriteFile(path, []byte{'Z'}, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	p, err := NewITCHParser(path)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	defer p.Close()
	if _, err := p.CountAll(); err == nil {
		t.Error("expected error on unknown message type, got nil")
	}
}

// --- Benchmarks: mmap (zero-copy) vs bufio ----------------------------------

func BenchmarkITCH_MMAP(b *testing.B) {
	path := writeITCHFixtureB(b, 100_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, err := NewITCHParser(path)
		if err != nil {
			b.Fatal(err)
		}
		c, err := p.CountAll()
		if err != nil {
			b.Fatal(err)
		}
		if c != 100_000 {
			b.Fatalf("count=%d", c)
		}
		p.Close()
	}
}

func BenchmarkITCH_Bufio(b *testing.B) {
	path := writeITCHFixtureB(b, 100_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := NewITCHBufioReader(path)
		if err != nil {
			b.Fatal(err)
		}
		c, err := r.CountAll()
		if err != nil {
			b.Fatal(err)
		}
		if c != 100_000 {
			b.Fatalf("count=%d", c)
		}
		r.Close()
	}
}

// writeITCHFixtureB is the benchmark variant of writeITCHFixture (same layout)
// — kept separate so the benchmark's *testing.B does not depend on a *testing.T
// helper signature.
func writeITCHFixtureB(b *testing.B, n int) string {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "itch_bench.bin")
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("create fixture: %v", err)
	}
	defer f.Close()
	var buf [40]byte
	for i := 0; i < n; i++ {
		switch {
		case i%100 < 60:
			buf[0] = 'A'
			binary.BigEndian.PutUint64(buf[1:], uint64(i+1))
			binary.BigEndian.PutUint64(buf[9:], uint64(i)*1000)
			buf[17] = 'B'
			binary.BigEndian.PutUint64(buf[18:], uint64(100000+i))
			binary.BigEndian.PutUint64(buf[26:], uint64(10+i%50))
			buf[34] = 4
			copy(buf[35:39], []byte("AAPL"))
			f.Write(buf[:39])
		case i%100 < 85:
			buf[0] = 'T'
			binary.BigEndian.PutUint64(buf[1:], uint64(i+1))
			binary.BigEndian.PutUint64(buf[9:], uint64(i)*1000)
			buf[17] = 'S'
			binary.BigEndian.PutUint64(buf[18:], uint64(100000+i))
			binary.BigEndian.PutUint64(buf[26:], uint64(5+i%50))
			buf[34] = 4
			copy(buf[35:39], []byte("MSFT"))
			f.Write(buf[:39])
		case i%100 < 95:
			buf[0] = 'M'
			binary.BigEndian.PutUint64(buf[1:], uint64(i+1))
			binary.BigEndian.PutUint64(buf[9:], uint64(i)*1000)
			binary.BigEndian.PutUint64(buf[17:], uint64(100000+i))
			binary.BigEndian.PutUint64(buf[25:], uint64(20+i%50))
			buf[33] = 4
			copy(buf[34:38], []byte("GOOG"))
			f.Write(buf[:38])
		default:
			buf[0] = 'S'
			binary.BigEndian.PutUint64(buf[1:], uint64(i)*1000)
			buf[9] = byte(i % 10)
			f.Write(buf[:10])
		}
	}
	return path
}
