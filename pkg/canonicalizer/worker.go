package canonicalizer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/allocator"
	"raw-data-layer/pkg/axiom"
	"raw-data-layer/pkg/mapper"
	"raw-data-layer/pkg/parser"
	"raw-data-layer/pkg/workerpool"
)

// Canonicalizer converts raw messages to canonical format.
// Key principles:
// - Never panic ("Garbage In, Canonical Out")
// - Preserve raw_payload byte-for-byte
// - Sanitize all numeric fields (Axle-Axiom)
// - Map symbols to canonical form
//
// Addım D hot-path changes:
//   - JSON decode uses Sonic (SIMD + JIT) via pkg/parser into a typed Trade
//     struct, replacing reflection-based encoding/json + map[string]any. The
//     map path allocated per field; the typed struct allocates ~once and zero
//     times when the Trade is reused (ParseTradeInto).
//   - CanonicalEvents are recycled through allocator.Pool so each parsed
//     message no longer allocates a fresh event (Acquire in Process, Release
//     in the downstream consumer after the marshal copies the bytes out).
//   - parseFloat uses strconv.ParseFloat instead of fmt.Sscanf (no fmt-format
//     allocation, no reflection).
type Canonicalizer struct {
	symbolMapper *mapper.SymbolMapper
	sanitizer    *axiom.MathSanitizer
	sonic        *parser.SonicParser // stateless SIMD JSON decoder; safe for concurrent use
}

// NewCanonicalizer creates a new canonicalizer.
func NewCanonicalizer(symbolMapper *mapper.SymbolMapper) *Canonicalizer {
	return &Canonicalizer{
		symbolMapper: symbolMapper,
		sanitizer:    axiom.NewMathSanitizer(),
		sonic:        parser.NewSonicParser(),
	}
}

// eventPool recycles CanonicalEvent objects on the hot path. A recycled event
// is Reset before reuse (see Reset). Safety contract: an acquired event is
// released ONLY by the canonicalizer's own consumer (cmd/canonicalizer's
// output loop, after EncodeCanonical copies the bytes out) — no async
// reference survives the Release, so there is no use-after-reset. See the
// package doc in pkg/allocator.
var eventPool = allocator.NewPool(
	func() *CanonicalEvent { return &CanonicalEvent{} },
	func(e *CanonicalEvent) { e.Reset() },
)

// AcquireEvent returns a clean CanonicalEvent from the pool (allocating on
// first use). The caller MUST eventually call ReleaseEvent on the returned
// pointer once every downstream reader is done with it.
func AcquireEvent() *CanonicalEvent { return eventPool.Get() }

// ReleaseEvent returns an event to the pool, Resetting it first. Nil-safe.
func ReleaseEvent(ev *CanonicalEvent) {
	if ev != nil {
		eventPool.Put(ev)
	}
}

// Process is the main processing function for worker pool.
// Signature matches workerpool.ProcessorFunc.
//
// The returned ProcessedMessage.Canonical is a pooled *CanonicalEvent. The
// worker pool's consumer must Release it after marshaling (which copies the
// bytes out). Never panics — a panic is converted to an error via named
// returns + recover, and a lossless fallback event carrying the raw payload is
// always attached so the pipeline never loses data.
func (c *Canonicalizer) Process(ctx context.Context, raw adapter.RawMessage) (pm workerpool.ProcessedMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in Process: %v", r)
			if pm.Canonical == nil {
				ev := AcquireEvent()
				fillUnknown(ev, raw, raw.Source, "UNKNOWN")
				pm.Canonical = ev
			}
			pm.ProcessedAt = time.Now().UnixNano()
		}
	}()

	ev := AcquireEvent()
	pm = workerpool.ProcessedMessage{
		Raw:         raw,
		Canonical:   ev,
		ProcessedAt: time.Now().UnixNano(),
	}

	switch raw.Source {
	case "BINANCE":
		err = c.parseBinanceInto(raw, ev)
	case "IB":
		err = c.parseIBInto(raw, ev)
	default:
		fillUnknown(ev, raw, raw.Source, "UNKNOWN")
		err = fmt.Errorf("unknown source: %s", raw.Source)
	}
	return pm, err
}

// CanonicalEvent represents the unified market data format.
// This matches proto/canonical.proto structure.
type CanonicalEvent struct {
	EventID           string
	Source            string
	CanonicalSymbol   string
	ExchangeTimestamp int64
	LocalHWTimestamp  int64
	EventType         string
	Price             float64
	Size              float64
	Side              string
	Levels            []Level
	RawPayload        []byte
	RawFormat         string

	// Asset-specific metadata
	ForexMetadata   *ForexMetadata
	FuturesMetadata *FuturesMetadata
	CryptoMetadata  *CryptoMetadata
	EquityMetadata   *EquityMetadata
}

// Reset zeros the event for reuse via allocator.Pool. It preserves the Levels
// slice's capacity (Levels[:0]) so repeated reuse does not reallocate the
// backing array; all pointer metadata is dropped so prior allocations can be
// GC'd. Required by the pool's Reset hook — never panics.
func (e *CanonicalEvent) Reset() {
	e.EventID = ""
	e.Source = ""
	e.CanonicalSymbol = ""
	e.ExchangeTimestamp = 0
	e.LocalHWTimestamp = 0
	e.EventType = ""
	e.Price = 0
	e.Size = 0
	e.Side = ""
	e.Levels = e.Levels[:0]
	e.RawPayload = nil
	e.RawFormat = ""
	e.ForexMetadata = nil
	e.FuturesMetadata = nil
	e.CryptoMetadata = nil
	e.EquityMetadata = nil
}

// Level represents order book level
type Level struct {
	Price   float64
	Size    float64
	Side    string
	OrderID int64
}

// Metadata types
type ForexMetadata struct {
	CurrencyPair string
	Bid          float64
	Ask          float64
	Spread       float64
}

type FuturesMetadata struct {
	ContractMonth   string
	OpenInterest    float64
	SettlementPrice float64
}

type CryptoMetadata struct {
	ExchangeSpecific map[string]interface{}
}

type EquityMetadata struct {
	Exchange       string
	MIC            string
	ConditionCodes []string
}

// fillUnknown fills ev with a lossless "UNKNOWN" fallback that preserves the
// raw payload. Used on parse error or unknown source. The event is Reset first
// so a pooled ev never leaks stale fields from a prior message.
func fillUnknown(ev *CanonicalEvent, raw adapter.RawMessage, source, format string) {
	ev.Reset()
	ev.EventID = generateEventID()
	ev.Source = source
	ev.CanonicalSymbol = "UNKNOWN"
	ev.ExchangeTimestamp = time.Now().UnixNano()
	ev.LocalHWTimestamp = raw.ReceivedAt
	ev.EventType = "UNKNOWN"
	ev.Price = 0.0
	ev.Size = 0.0
	ev.Side = "UNKNOWN"
	ev.RawPayload = raw.Payload
	ev.RawFormat = format
}

// parseBinanceInto decodes a Binance WebSocket JSON message into ev using Sonic
// (SIMD + JIT) into a typed parser.Trade, then maps/sanitizes fields. Never
// panics; on malformed JSON it fills ev with an UNKNOWN fallback (raw payload
// preserved) and returns the decode error. The caller owns ev (it is acquired
// from the pool in Process).
//
// Binance trade/aggTrade shape:
//
//	{"e":"trade","E":<event_ms>,"s":"BTCUSDT","p":"50000.00","q":"0.5","T":<trade_ms>,"m":false}
//
// Note: the OLD path stashed the entire parsed map into CryptoMetadata for
// "exchange-specific data". For a plain trade there is no extra data beyond the
// top-level fields already captured, so CryptoMetadata is left nil — the
// canonical event is lossless without duplicating fields into a map. The proto
// bridge still supports CryptoMetadata for any caller that sets it.
func (c *Canonicalizer) parseBinanceInto(raw adapter.RawMessage, ev *CanonicalEvent) error {
	var t parser.Trade
	if err := c.sonic.ParseTradeInto(raw.Payload, &t); err != nil {
		fillUnknown(ev, raw, "BINANCE", "JSON")
		return fmt.Errorf("sonic parse error: %w", err)
	}

	canonicalSymbol := c.symbolMapper.ToCanonical("BINANCE", t.Symbol)
	if canonicalSymbol == "" {
		canonicalSymbol = t.Symbol
	}

	price := c.sanitizer.SanitizePrice(c.parseFloat(t.Price))
	size := c.sanitizer.SanitizeSize(c.parseFloat(t.Quantity))

	exchangeTS := t.TradeTime
	if exchangeTS == 0 {
		exchangeTS = t.Time
	}

	// Binance "m"=true means the buyer is the maker → the aggressor sold → SELL.
	side := "BUY"
	if t.IsBuyer {
		side = "SELL"
	}

	ev.EventID = generateEventID()
	ev.Source = "BINANCE"
	ev.CanonicalSymbol = canonicalSymbol
	ev.ExchangeTimestamp = exchangeTS * 1000000 // ms → ns
	ev.LocalHWTimestamp = raw.ReceivedAt
	ev.EventType = "TRADE"
	ev.Price = price
	ev.Size = size
	ev.Side = side
	ev.RawPayload = raw.Payload // UNTOUCHED — byte-for-byte
	ev.RawFormat = "JSON"
	return nil
}

// parseBinance is the non-mutating wrapper kept for the existing tests/benches:
// it acquires an event, fills it, and returns it (the caller inspects but does
// not Release — the pool simply allocates fresh on the next Acquire).
func (c *Canonicalizer) parseBinance(raw adapter.RawMessage) (*CanonicalEvent, error) {
	ev := AcquireEvent()
	err := c.parseBinanceInto(raw, ev)
	return ev, err
}

// parseIBInto parses an IB Gateway binary message into ev. IB binary protocol is
// complex (CLAUDE.md treats it as opaque for the MVP); this carries the raw
// payload losslessly with UNKNOWN canonical fields. Never panics.
func (c *Canonicalizer) parseIBInto(raw adapter.RawMessage, ev *CanonicalEvent) error {
	ev.EventID = generateEventID()
	ev.Source = "IB"
	ev.CanonicalSymbol = "UNKNOWN"
	ev.ExchangeTimestamp = time.Now().UnixNano()
	ev.LocalHWTimestamp = raw.ReceivedAt
	ev.EventType = "TRADE"
	ev.Price = 0.0
	ev.Size = 0.0
	ev.Side = "UNKNOWN"
	ev.RawPayload = raw.Payload // UNTOUCHED
	ev.RawFormat = "BINARY"
	ev.EquityMetadata = &EquityMetadata{Exchange: "IB"}
	return nil
}

// parseIB is the non-mutating wrapper kept for tests (mirrors parseBinance).
func (c *Canonicalizer) parseIB(raw adapter.RawMessage) (*CanonicalEvent, error) {
	ev := AcquireEvent()
	_ = c.parseIBInto(raw, ev)
	return ev, nil
}

// Helper functions for safe extraction (kept for backward compatibility with
// tests that call them directly; parseBinanceInto no longer uses them since it
// decodes into a typed struct instead of a map).

func (c *Canonicalizer) extractString(msg map[string]interface{}, key string) string {
	if v, ok := msg[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (c *Canonicalizer) extractInt64(msg map[string]interface{}, key string) int64 {
	if v, ok := msg[key]; ok {
		switch val := v.(type) {
		case float64:
			return int64(val)
		case int64:
			return val
		case int:
			return int64(val)
		}
	}
	return 0
}

// parseFloat parses a decimal string into float64. Addım D: strconv.ParseFloat
// replaces fmt.Sscanf — no fmt-format allocation, no reflection. Returns 0.0
// for empty/invalid input (same behavior as the old Sscanf path, verified by
// TestCanonicalizer_SanitizePrice and TestCanonicalizer_ExtractHelpers).
func (c *Canonicalizer) parseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0.0
	}
	return f
}

// generateEventID generates a unique event ID.
// Simple timestamp-based ID for MVP; production would use UUID v4 or ULID.
func generateEventID() string {
	return fmt.Sprintf("evt_%d", time.Now().UnixNano())
}
