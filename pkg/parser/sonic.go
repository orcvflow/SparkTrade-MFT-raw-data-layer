// Package parser provides SIMD-accelerated and zero-copy parsers for the raw
// data layer's hot path.
//
// Addım D introduces two specialized parsers that replace the two slowest
// steps in the pipeline:
//
//   - SonicParser: JIT + SIMD JSON decoding (github.com/bytedance/sonic) for
//     Binance trade messages, replacing reflection-based encoding/json.
//     Sonic generates native code at runtime and vectorizes scanning with
//     SSE4/AVX2 on amd64 (ByteDance production: 3–17× faster, ~197× fewer
//     allocations than encoding/json on typed structs).
//   - ITCHParser: zero-copy binary parsing over a memory-mapped file, reading
//     fields directly off mapped pages with At(i) (no full-file copy into a
//     heap []byte, no per-read buffer copy).
//
// Both follow CLAUDE.md paranoid rules: never panic (every entry point has
// defer/recover and returns an error), preserve raw payload, no hidden
// allocations on the hot path (ParseTradeInto fills a caller-provided struct).
package parser

import (
	"encoding/json"
	"fmt"

	"github.com/bytedance/sonic"
)

// Trade is the typed Binance aggTrade/trade message.
//
// Decoding into this struct instead of map[string]interface{} is the single
// largest allocation win on the canonicalizer hot path: a map allocates per
// field (one map + one boxing alloc per value), a typed struct allocates once
// (or zero times when reused via ParseTradeInto).
//
// Price and Quantity stay strings because Binance sends them as decimal
// strings — float parsing is the canonicalizer's job (axiom sanitizer), not the
// parser's. Keeping them as strings also preserves precision for replay.
type Trade struct {
	Event     string `json:"e"` // "aggTrade" / "trade"
	Time      int64  `json:"E"` // event time (ms)
	Symbol    string `json:"s"` // e.g. "BTCUSDT"
	AggID     int64  `json:"a"` // aggregated trade id
	Price     string `json:"p"` // price (string)
	Quantity  string `json:"q"` // quantity (string)
	TradeTime int64  `json:"T"` // trade time (ms)
	IsBuyer   bool   `json:"m"` // m=true → buyer is the maker → side SELL
}

// SonicParser decodes JSON using ByteDance Sonic (JIT + SIMD). On amd64 it
// generates native code at runtime and vectorizes scanning; on other arches
// sonic falls back to a fast scalar path. Either way it avoids
// encoding/json's reflection. The parser is stateless and safe for concurrent
// use — sonic.Marshal/Unmarshal manage their own internal state.
type SonicParser struct{}

// NewSonicParser returns a stateless SonicParser.
func NewSonicParser() *SonicParser { return &SonicParser{} }

// ParseTrade decodes a Binance trade/aggTrade JSON message into a typed Trade.
// Never panics: returns an error on malformed input (and converts any internal
// sonic panic into an error via named returns + recover, per CLAUDE.md). The
// returned *Trade is heap-allocated by sonic; callers that recycle Trade
// values should use ParseTradeInto to avoid the per-call allocation entirely.
func (p *SonicParser) ParseTrade(data []byte) (t *Trade, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sonic: panic in ParseTrade: %v", r)
		}
	}()
	if len(data) == 0 {
		return nil, fmt.Errorf("sonic: empty payload")
	}
	var trade Trade
	if e := sonic.Unmarshal(data, &trade); e != nil {
		return nil, e
	}
	return &trade, nil
}

// ParseTradeInto decodes into a caller-provided Trade, avoiding the per-call
// allocation. The trade is Reset before decode so no stale fields leak across
// reuse (a recycled Trade would otherwise carry the previous message's strings
// for any field absent from the new payload). Never panics.
func (p *SonicParser) ParseTradeInto(data []byte, t *Trade) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sonic: panic in ParseTradeInto: %v", r)
		}
	}()
	// Reset before decode — sonic fills only the fields present in the JSON;
	// absent fields would otherwise keep their previous value across reuse.
	*t = Trade{}
	if len(data) == 0 {
		return fmt.Errorf("sonic: empty payload")
	}
	return sonic.Unmarshal(data, t)
}

// ParseTradeStd decodes with encoding/json into the same typed struct. It exists
// as a benchmark baseline so the Sonic vs stdlib comparison is apples-to-apples
// (only the decoder differs), not confounded by the old map[string]any path.
func ParseTradeStd(data []byte) (*Trade, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("stdlib: empty payload")
	}
	var t Trade
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// ParseTradeMapStd decodes into map[string]interface{} using encoding/json.
// This is the OLD canonicalizer hot path, preserved as a benchmark baseline so
// the map-vs-typed-struct allocation cliff is directly measurable.
func ParseTradeMapStd(data []byte) (map[string]interface{}, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("stdlib: empty payload")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}
