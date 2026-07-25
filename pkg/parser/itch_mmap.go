package parser

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"golang.org/x/exp/mmap"
)

// ITCHMessage is a parsed ITCH-like record. This is a minimal subset of NASDAQ
// ITCH (AddOrder / Trade / Modify / System) sufficient to exercise the
// zero-copy parser and benchmark it against a bufio baseline. A full ITCH
// implementation is a future extension (CLAUDE.md §21) — Addım D's scope is the
// parsing mechanism (mmap zero-copy), not protocol completeness.
type ITCHMessage struct {
	Type     byte   // 'A' | 'T' | 'M' | 'S'
	OrderID  uint64 // AddOrder/Modify order reference; Trade trade id
	TS       int64  // nanosecond timestamp
	Side     byte   // 'B' | 'S' (AddOrder/Trade); 0 for Modify/System
	Price    uint64 // fixed-point price
	Quantity uint64 // quantity
	Symbol   string // symbol (AddOrder/Trade/Modify)
	Code     byte   // System event code
}

// ITCHParser parses an ITCH-like binary file from a memory map. Fields are read
// directly off the mapped pages via At(i) — there is no full-file copy into a
// heap []byte (vs os.ReadFile) and no per-read buffer copy (vs bufio). This is
// the zero-copy hot path: the kernel maps the file's pages into the process
// address space and the parser indexes them directly.
//
// Message layout (big-endian, length-prefixed symbol):
//
//	'A' AddOrder: [1 type][8 orderID][8 ts][1 side][8 price][8 qty][1 symLen][sym..]
//	'T' Trade:    [1 type][8 tradeID][8 ts][1 side][8 price][8 qty][1 symLen][sym..]
//	'M' Modify:   [1 type][8 orderID][8 ts][8 price][8 qty][1 symLen][sym..]
//	'S' System:   [1 type][8 ts][1 code]
//
// Never panics: every read is bounds-checked before At() (which itself panics
// out-of-bounds); a truncated record yields an error and the parse stops.
type ITCHParser struct {
	data *mmap.ReaderAt // the memory map; At(i)/Len() drive zero-copy reads
}

// NewITCHParser memory-maps the file at path. The caller must call Close to
// release the mapping. Never panics.
func NewITCHParser(path string) (*ITCHParser, error) {
	mm, err := mmap.Open(path)
	if err != nil {
		return nil, fmt.Errorf("itch: mmap open %s: %w", path, err)
	}
	return &ITCHParser{data: mm}, nil
}

// Close releases the memory map. Idempotent and never panics.
func (p *ITCHParser) Close() error {
	if p == nil || p.data == nil {
		return nil
	}
	err := p.data.Close()
	p.data = nil
	return err
}

// Len returns the mapped file size in bytes.
func (p *ITCHParser) Len() int {
	if p == nil || p.data == nil {
		return 0
	}
	return p.data.Len()
}

// ParseAll walks the whole map and returns every parsed message. Field
// extraction is zero-copy off the mapped pages; the only allocation is the
// result slice (and the symbol strings). Never panics — on a truncated record
// it returns the messages parsed so far plus an error.
func (p *ITCHParser) ParseAll() ([]ITCHMessage, error) {
	if p == nil || p.data == nil {
		return nil, fmt.Errorf("itch: parser closed")
	}
	n := p.data.Len()
	out := make([]ITCHMessage, 0, n/40)
	off := 0
	for off < n {
		msg, next, err := p.parseOne(off)
		if err != nil {
			return out, fmt.Errorf("itch: parse at offset %d: %w", off, err)
		}
		out = append(out, msg)
		off = next
	}
	return out, nil
}

// CountAll walks the map counting records without allocating a result slice —
// the throughput benchmark uses this so allocation numbers reflect parsing
// only, not slice growth.
func (p *ITCHParser) CountAll() (int, error) {
	if p == nil || p.data == nil {
		return 0, fmt.Errorf("itch: parser closed")
	}
	n := p.data.Len()
	off, count := 0, 0
	for off < n {
		_, next, err := p.parseOne(off)
		if err != nil {
			return count, err
		}
		count++
		off = next
	}
	return count, nil
}

// parseOne parses the record starting at off. Returns the message, the offset
// of the next record, and an error if the record is truncated/malformed.
func (p *ITCHParser) parseOne(off int) (ITCHMessage, int, error) {
	n := p.data.Len()
	if off >= n {
		return ITCHMessage{}, off, fmt.Errorf("truncated: no type byte")
	}
	mt := p.data.At(off)
	m := ITCHMessage{Type: mt}
	pos := off + 1

	switch mt {
	case 'A', 'T': // [8 id][8 ts][1 side][8 price][8 qty][1 symLen][sym]
		id, ok := p.u64(pos)
		if !ok {
			return m, off, fmt.Errorf("truncated: id")
		}
		m.OrderID = id
		pos += 8
		ts, ok := p.i64(pos)
		if !ok {
			return m, off, fmt.Errorf("truncated: ts")
		}
		m.TS = ts
		pos += 8
		if pos >= n {
			return m, off, fmt.Errorf("truncated: side")
		}
		m.Side = p.data.At(pos)
		pos++
		price, ok := p.u64(pos)
		if !ok {
			return m, off, fmt.Errorf("truncated: price")
		}
		m.Price = price
		pos += 8
		qty, ok := p.u64(pos)
		if !ok {
			return m, off, fmt.Errorf("truncated: qty")
		}
		m.Quantity = qty
		pos += 8
		if pos >= n {
			return m, off, fmt.Errorf("truncated: symLen")
		}
		symLen := int(p.data.At(pos))
		pos++
		sym, ok := p.symbol(pos, symLen)
		if !ok {
			return m, off, fmt.Errorf("truncated: sym")
		}
		m.Symbol = sym
		pos += symLen

	case 'M': // [8 id][8 ts][8 price][8 qty][1 symLen][sym]
		id, ok := p.u64(pos)
		if !ok {
			return m, off, fmt.Errorf("truncated: id")
		}
		m.OrderID = id
		pos += 8
		ts, ok := p.i64(pos)
		if !ok {
			return m, off, fmt.Errorf("truncated: ts")
		}
		m.TS = ts
		pos += 8
		price, ok := p.u64(pos)
		if !ok {
			return m, off, fmt.Errorf("truncated: price")
		}
		m.Price = price
		pos += 8
		qty, ok := p.u64(pos)
		if !ok {
			return m, off, fmt.Errorf("truncated: qty")
		}
		m.Quantity = qty
		pos += 8
		if pos >= n {
			return m, off, fmt.Errorf("truncated: symLen")
		}
		symLen := int(p.data.At(pos))
		pos++
		sym, ok := p.symbol(pos, symLen)
		if !ok {
			return m, off, fmt.Errorf("truncated: sym")
		}
		m.Symbol = sym
		pos += symLen

	case 'S': // [8 ts][1 code]
		ts, ok := p.i64(pos)
		if !ok {
			return m, off, fmt.Errorf("truncated: ts")
		}
		m.TS = ts
		pos += 8
		if pos >= n {
			return m, off, fmt.Errorf("truncated: code")
		}
		m.Code = p.data.At(pos)
		pos++

	default:
		return m, off, fmt.Errorf("unknown message type 0x%02x", mt)
	}

	return m, pos, nil
}

// u64 reads 8 big-endian bytes at off as a uint64, directly off the mapped
// page (no intermediate buffer). Bounds-checked.
func (p *ITCHParser) u64(off int) (uint64, bool) {
	n := p.data.Len()
	if off < 0 || off+8 > n {
		return 0, false
	}
	return uint64(p.data.At(off))<<56 |
			uint64(p.data.At(off+1))<<48 |
			uint64(p.data.At(off+2))<<40 |
			uint64(p.data.At(off+3))<<32 |
			uint64(p.data.At(off+4))<<24 |
			uint64(p.data.At(off+5))<<16 |
			uint64(p.data.At(off+6))<<8 |
			uint64(p.data.At(off+7)),
		true
}

// i64 reads 8 big-endian bytes at off as a signed int64 via int64(uint64),
// giving correct two's-complement sign extension for negative timestamps.
func (p *ITCHParser) i64(off int) (int64, bool) {
	u, ok := p.u64(off)
	return int64(u), ok
}

// symbol reads symLen bytes at off and materializes them into a string. (The
// string materialization is the one unavoidable copy; numeric fields above are
// genuinely zero-copy.) Bounds-checked.
func (p *ITCHParser) symbol(off, symLen int) (string, bool) {
	n := p.data.Len()
	if symLen < 0 || off < 0 || off+symLen > n {
		return "", false
	}
	b := make([]byte, symLen)
	for i := 0; i < symLen; i++ {
		b[i] = p.data.At(off + i)
	}
	return string(b), true
}

// --- Bufio baseline --------------------------------------------------------
//
// ITCHBufioReader parses the SAME binary format from a bufio.Reader over a
// plain *os.File. This is the "before" path: every byte is copied through the
// bufio buffer and again into per-field scratch buffers. The mmap parser above
// avoids both copies. The benchmark compares them apples-to-apples.

// ITCHBufioReader parses ITCH-like records from a buffered file reader.
type ITCHBufioReader struct {
	f  *os.File
	br *bufio.Reader
}

// NewITCHBufioReader opens path as a buffered file reader. Caller must Close.
func NewITCHBufioReader(path string) (*ITCHBufioReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("itch-bufio: open %s: %w", path, err)
	}
	return &ITCHBufioReader{f: f, br: bufio.NewReaderSize(f, 64<<10)}, nil
}

// CountAll parses the whole stream counting records — same semantics as
// ITCHParser.CountAll so the two benchmarks measure identical work.
func (r *ITCHBufioReader) CountAll() (int, error) {
	count := 0
	var t [1]byte
	for {
		if _, err := io.ReadFull(r.br, t[:]); err != nil {
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				return count, fmt.Errorf("truncated type: %w", err)
			}
			return count, err
		}
		mt := t[0]
		switch mt {
		case 'A', 'T':
			var buf [34]byte // 8 id + 8 ts + 1 side + 8 price + 8 qty + 1 symLen
			if _, err := io.ReadFull(r.br, buf[:]); err != nil {
				return count, fmt.Errorf("truncated body: %w", err)
			}
			symLen := int(buf[33])
			if symLen > 0 {
				tmp := make([]byte, symLen)
				if _, err := io.ReadFull(r.br, tmp); err != nil {
					return count, fmt.Errorf("truncated sym: %w", err)
				}
			}
		case 'M':
			var buf [33]byte // 8 id + 8 ts + 8 price + 8 qty + 1 symLen
			if _, err := io.ReadFull(r.br, buf[:]); err != nil {
				return count, fmt.Errorf("truncated body: %w", err)
			}
			symLen := int(buf[32])
			if symLen > 0 {
				tmp := make([]byte, symLen)
				if _, err := io.ReadFull(r.br, tmp); err != nil {
					return count, fmt.Errorf("truncated sym: %w", err)
				}
			}
		case 'S':
			var buf [9]byte // 8 ts + 1 code
			if _, err := io.ReadFull(r.br, buf[:]); err != nil {
				return count, fmt.Errorf("truncated body: %w", err)
			}
		default:
			return count, fmt.Errorf("unknown message type 0x%02x", mt)
		}
		count++
	}
	return count, nil
}

// Close closes the underlying file. Never panics.
func (r *ITCHBufioReader) Close() error {
	if r == nil {
		return nil
	}
	if r.f != nil {
		return r.f.Close()
	}
	return nil
}

// be is re-exported for the test fixture generator so it does not need its own
// encoding/binary import line.
var be = binary.BigEndian
