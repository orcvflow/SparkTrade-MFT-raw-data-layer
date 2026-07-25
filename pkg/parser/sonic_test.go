package parser

import (
	"encoding/json"
	"math"
	"testing"
)

// canonicalPayload is a realistic Binance aggTrade payload (the same shape used
// by the canonicalizer benchmarks), used to drive correctness + comparative
// benchmarks.
const canonicalPayload = `{"e":"aggTrade","E":1234567890,"s":"BTCUSDT","a":12345,"p":"50000.00","q":"0.5","T":1234567890,"m":false}`

func TestSonicParser_ParseTrade_Correct(t *testing.T) {
	p := NewSonicParser()
	tr, err := p.ParseTrade([]byte(canonicalPayload))
	if err != nil {
		t.Fatalf("ParseTrade: %v", err)
	}
	if tr.Symbol != "BTCUSDT" {
		t.Errorf("Symbol = %q, want BTCUSDT", tr.Symbol)
	}
	if tr.Price != "50000.00" {
		t.Errorf("Price = %q, want 50000.00", tr.Price)
	}
	if tr.Quantity != "0.5" {
		t.Errorf("Quantity = %q, want 0.5", tr.Quantity)
	}
	if tr.TradeTime != 1234567890 {
		t.Errorf("TradeTime = %d, want 1234567890", tr.TradeTime)
	}
	if tr.IsBuyer != false {
		t.Errorf("IsBuyer = %v, want false (→ side BUY)", tr.IsBuyer)
	}
}

func TestSonicParser_ParseTradeInto_Reuse(t *testing.T) {
	p := NewSonicParser()
	var tr Trade
	// First decode.
	if err := p.ParseTradeInto([]byte(canonicalPayload), &tr); err != nil {
		t.Fatalf("ParseTradeInto #1: %v", err)
	}
	if tr.Symbol != "BTCUSDT" {
		t.Fatalf("Symbol #1 = %q", tr.Symbol)
	}
	// Second decode of a payload MISSING some fields — the reused Trade must
	// NOT carry stale values from the first decode.
	const partial = `{"e":"aggTrade","s":"ETHUSDT","p":"3000.00","q":"1.0","T":99}`
	if err := p.ParseTradeInto([]byte(partial), &tr); err != nil {
		t.Fatalf("ParseTradeInto #2: %v", err)
	}
	if tr.Symbol != "ETHUSDT" {
		t.Errorf("Symbol #2 = %q, want ETHUSDT", tr.Symbol)
	}
	if tr.Price != "3000.00" {
		t.Errorf("Price #2 = %q, want 3000.00", tr.Price)
	}
	// AggID was absent in the second payload → must be 0, not the stale 12345.
	if tr.AggID != 0 {
		t.Errorf("AggID = %d, want 0 (stale value leaked across reuse)", tr.AggID)
	}
	if tr.TradeTime != 99 {
		t.Errorf("TradeTime = %d, want 99", tr.TradeTime)
	}
}

func TestSonicParser_ParseTrade_Malformed(t *testing.T) {
	p := NewSonicParser()
	// Never panic, return error.
	if _, err := p.ParseTrade([]byte(`{"invalid json`)); err == nil {
		t.Error("expected error for malformed JSON")
	}
	// Empty payload → error, no panic.
	if _, err := p.ParseTrade(nil); err == nil {
		t.Error("expected error for empty payload")
	}
}

func TestSonicParser_ParseTradeInto_Malformed(t *testing.T) {
	p := NewSonicParser()
	var tr Trade
	if err := p.ParseTradeInto([]byte(`{"invalid json`), &tr); err == nil {
		t.Error("expected error for malformed JSON")
	}
	if err := p.ParseTradeInto(nil, &tr); err == nil {
		t.Error("expected error for empty payload")
	}
}

// TestSonicParser_EquivalentToStdlib proves Sonic and encoding/json decode the
// same payload to identical field values — a regression guard: if Sonic's
// semantics drift, the canonicalizer would silently change behavior.
func TestSonicParser_EquivalentToStdlib(t *testing.T) {
	data := []byte(canonicalPayload)
	var gotSonic Trade
	if err := NewSonicParser().ParseTradeInto(data, &gotSonic); err != nil {
		t.Fatalf("sonic: %v", err)
	}
	std, err := ParseTradeStd(data)
	if err != nil {
		t.Fatalf("stdlib: %v", err)
	}
	if gotSonic != *std {
		t.Errorf("sonic != stdlib:\n sonic=%+v\n std  =%+v", gotSonic, *std)
	}
}

// --- Benchmarks (apples-to-apples) ------------------------------------------
//
// Three decoders, same payload, same typed struct (except the map variant):
//
//   - BenchmarkParseJSON_Map:     encoding/json → map[string]any  (OLD hot path)
//   - BenchmarkParseJSON_Std:     encoding/json → typed Trade     (stdlib, typed)
//   - BenchmarkParseJSON_Sonic:   sonic          → typed Trade    (NEW hot path)
//   - BenchmarkParseJSON_SonicInto: sonic → reused Trade          (zero-alloc)
//
// The "3× faster / 197× fewer allocs" claim is measured here as
// BenchmarkParseJSON_Sonic vs BenchmarkParseJSON_Std (same struct, decoder
// only differs). The map variant is reported for context — it shows the real
// reason the old path was slow (reflection + per-field boxing).

func BenchmarkParseJSON_Map(b *testing.B) {
	data := []byte(canonicalPayload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseTradeMapStd(data)
	}
}

func BenchmarkParseJSON_Std(b *testing.B) {
	data := []byte(canonicalPayload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseTradeStd(data)
	}
}

func BenchmarkParseJSON_Sonic(b *testing.B) {
	data := []byte(canonicalPayload)
	p := NewSonicParser()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.ParseTrade(data)
	}
}

func BenchmarkParseJSON_SonicInto(b *testing.B) {
	data := []byte(canonicalPayload)
	p := NewSonicParser()
	var tr Trade
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.ParseTradeInto(data, &tr)
	}
	// Sink to prevent the compiler from eliding the loop.
	if tr.Price == "" && math.IsNaN(0) {
		b.Fatal("unreachable")
	}
}

// BenchmarkParseJSON_E2E_Compare runs all four side-by-side for a quick human
// readout (go test -bench=BenchmarkParseJSON_E2E -benchmem).
func BenchmarkParseJSON_E2E_Compare(b *testing.B) {
	data := []byte(canonicalPayload)
	p := NewSonicParser()
	var tr Trade
	b.Run("Map_stdlib", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = ParseTradeMapStd(data)
		}
	})
	b.Run("Struct_stdlib", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = ParseTradeStd(data)
		}
	})
	b.Run("Struct_sonic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = p.ParseTrade(data)
		}
	})
	b.Run("Struct_sonic_reuse", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = p.ParseTradeInto(data, &tr)
		}
	})
	// keep tr alive
	if tr.Price == "" {
		_, _ = json.Marshal(tr)
	}
}
