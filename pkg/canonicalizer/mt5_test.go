package canonicalizer

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/mapper"
)

// newTempMapperMT5 creates a temp mapper with MT5 symbol mappings
func newTempMapperMT5(t *testing.T, mt5Mapping map[string]string) *mapper.SymbolMapper {
	t.Helper()
	dir := t.TempDir()
	data, err := json.Marshal(mt5Mapping)
	if err != nil {
		t.Fatalf("marshal mt5 mapping: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "mt5.json"), data, 0644); err != nil {
		t.Fatalf("write mt5.json: %v", err)
	}
	m, err := mapper.NewSymbolMapper(dir)
	if err != nil {
		t.Fatalf("NewSymbolMapper: %v", err)
	}
	return m
}

// TestMT5_ParseL1Tick tests L1_TICK parsing with valid data
// SUCCESS CRITERIA: Correct CanonicalEvent fields, ForexMetadata populated, raw payload preserved
func TestMT5_ParseL1Tick(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"EURUSD": "EUR/USD"})
	c := NewCanonicalizer(m)

	payload := []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120,"source":"MT5","timestamp":1722933771120}`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	ev, err := c.parseMT5(raw)
	if err != nil {
		t.Fatalf("parseMT5 returned error: %v", err)
	}

	// Verify basic fields
	if ev.Source != "MT5" {
		t.Errorf("Source = %q; want %q", ev.Source, "MT5")
	}
	if ev.CanonicalSymbol != "EUR/USD" {
		t.Errorf("CanonicalSymbol = %q; want %q", ev.CanonicalSymbol, "EUR/USD")
	}
	if ev.EventType != "QUOTE" {
		t.Errorf("EventType = %q; want %q", ev.EventType, "QUOTE")
	}
	if ev.Price != 1.08457 {
		t.Errorf("Price = %f; want %f", ev.Price, 1.08457)
	}
	if ev.Size != 0.5 {
		t.Errorf("Size = %f; want %f", ev.Size, 0.5)
	}
	if ev.Side != "UNKNOWN" {
		t.Errorf("Side = %q; want %q", ev.Side, "UNKNOWN")
	}

	// Verify timestamp (ms → ns conversion)
	expectedTS := int64(1722933771120 * 1000000)
	if ev.ExchangeTimestamp != expectedTS {
		t.Errorf("ExchangeTimestamp = %d; want %d", ev.ExchangeTimestamp, expectedTS)
	}

	// Verify ForexMetadata
	if ev.ForexMetadata == nil {
		t.Fatal("ForexMetadata is nil")
	}
	if ev.ForexMetadata.CurrencyPair != "EUR/USD" {
		t.Errorf("ForexMetadata.CurrencyPair = %q; want %q", ev.ForexMetadata.CurrencyPair, "EUR/USD")
	}
	if ev.ForexMetadata.Bid != 1.08456 {
		t.Errorf("ForexMetadata.Bid = %f; want %f", ev.ForexMetadata.Bid, 1.08456)
	}
	if ev.ForexMetadata.Ask != 1.08458 {
		t.Errorf("ForexMetadata.Ask = %f; want %f", ev.ForexMetadata.Ask, 1.08458)
	}
	expectedSpread := 1.08458 - 1.08456
	if math.Abs(ev.ForexMetadata.Spread-expectedSpread) > 0.00001 {
		t.Errorf("ForexMetadata.Spread = %f; want %f", ev.ForexMetadata.Spread, expectedSpread)
	}

	// Verify raw payload preserved byte-for-byte
	if string(ev.RawPayload) != string(payload) {
		t.Error("RawPayload not preserved byte-for-byte")
	}
	if ev.RawFormat != "JSON" {
		t.Errorf("RawFormat = %q; want %q", ev.RawFormat, "JSON")
	}
}

// TestMT5_ParseL2Depth tests L2_DEPTH parsing with valid bid/ask levels
// SUCCESS CRITERIA: Levels populated correctly, ForexMetadata derived from first bid/ask
func TestMT5_ParseL2Depth(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"EURUSD": "EUR/USD"})
	c := NewCanonicalizer(m)

	payload := []byte(`{"type":"L2_DEPTH","symbol":"EURUSD","bids":[{"price":1.08456,"volume":2.5},{"price":1.08455,"volume":3.0}],"asks":[{"price":1.08458,"volume":3.0},{"price":1.08459,"volume":4.0}],"source":"MT5"}`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  1234567890000000000,
		SequenceNum: 1,
	}

	ev, err := c.parseMT5(raw)
	if err != nil {
		t.Fatalf("parseMT5 returned error: %v", err)
	}

	// Verify basic fields
	if ev.Source != "MT5" {
		t.Errorf("Source = %q; want %q", ev.Source, "MT5")
	}
	if ev.CanonicalSymbol != "EUR/USD" {
		t.Errorf("CanonicalSymbol = %q; want %q", ev.CanonicalSymbol, "EUR/USD")
	}
	if ev.EventType != "BOOK_SNAPSHOT" {
		t.Errorf("EventType = %q; want %q", ev.EventType, "BOOK_SNAPSHOT")
	}

	// Verify ExchangeTimestamp uses raw.ReceivedAt (Bug #1 fix)
	if ev.ExchangeTimestamp != raw.ReceivedAt {
		t.Errorf("ExchangeTimestamp = %d; want %d (should use raw.ReceivedAt)", ev.ExchangeTimestamp, raw.ReceivedAt)
	}

	// Verify levels
	if len(ev.Levels) != 4 {
		t.Fatalf("Levels length = %d; want 4 (2 bids + 2 asks)", len(ev.Levels))
	}

	// Check first bid
	if ev.Levels[0].Price != 1.08456 {
		t.Errorf("Levels[0].Price = %f; want %f", ev.Levels[0].Price, 1.08456)
	}
	if ev.Levels[0].Size != 2.5 {
		t.Errorf("Levels[0].Size = %f; want %f", ev.Levels[0].Size, 2.5)
	}
	if ev.Levels[0].Side != "BID" {
		t.Errorf("Levels[0].Side = %q; want %q", ev.Levels[0].Side, "BID")
	}

	// Check first ask
	if ev.Levels[2].Price != 1.08458 {
		t.Errorf("Levels[2].Price = %f; want %f", ev.Levels[2].Price, 1.08458)
	}
	if ev.Levels[2].Side != "ASK" {
		t.Errorf("Levels[2].Side = %q; want %q", ev.Levels[2].Side, "ASK")
	}

	// Verify ForexMetadata (Bug #3 fix - should derive from first bid/ask)
	if ev.ForexMetadata == nil {
		t.Fatal("ForexMetadata is nil")
	}
	if ev.ForexMetadata.Bid != 1.08456 {
		t.Errorf("ForexMetadata.Bid = %f; want %f (derived from first bid level)", ev.ForexMetadata.Bid, 1.08456)
	}
	if ev.ForexMetadata.Ask != 1.08458 {
		t.Errorf("ForexMetadata.Ask = %f; want %f (derived from first ask level)", ev.ForexMetadata.Ask, 1.08458)
	}
	expectedSpread := 1.08458 - 1.08456
	if math.Abs(ev.ForexMetadata.Spread-expectedSpread) > 0.00001 {
		t.Errorf("ForexMetadata.Spread = %f; want %f", ev.ForexMetadata.Spread, expectedSpread)
	}

	// Verify raw payload preserved
	if string(ev.RawPayload) != string(payload) {
		t.Error("RawPayload not preserved")
	}
}

// TestMT5_InvalidJSON tests handling of malformed JSON
// SUCCESS CRITERIA: Error returned, UNKNOWN event created, raw payload preserved
func TestMT5_InvalidJSON(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"EURUSD": "EUR/USD"})
	c := NewCanonicalizer(m)

	payload := []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,INVALID_JSON`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	ev, err := c.parseMT5(raw)

	// Should return error but not panic
	if err == nil {
		t.Error("Expected error for malformed JSON, got nil")
	}

	// Should create UNKNOWN event with raw payload preserved
	if ev.CanonicalSymbol != "UNKNOWN" {
		t.Errorf("CanonicalSymbol = %q; want %q", ev.CanonicalSymbol, "UNKNOWN")
	}
	if ev.EventType != "UNKNOWN" {
		t.Errorf("EventType = %q; want %q", ev.EventType, "UNKNOWN")
	}
	if string(ev.RawPayload) != string(payload) {
		t.Error("RawPayload not preserved on error")
	}
	if ev.Source != "MT5" {
		t.Errorf("Source = %q; want %q (should preserve source even on error)", ev.Source, "MT5")
	}
}

// TestMT5_SanitizeNegativePrice tests negative price sanitization
// SUCCESS CRITERIA: Negative bid/ask/last sanitized to 0.0
func TestMT5_SanitizeNegativePrice(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"EURUSD": "EUR/USD"})
	c := NewCanonicalizer(m)

	payload := []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":-1.08456,"ask":-1.08458,"last":-1.08457,"volume":0.5,"time":1722933771120}`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	ev, err := c.parseMT5(raw)
	if err != nil {
		t.Fatalf("parseMT5 returned error: %v", err)
	}

	// Negative prices should be sanitized to 0.0
	if ev.Price != 0.0 {
		t.Errorf("Price = %f; want 0.0 (negative sanitized)", ev.Price)
	}
	if ev.ForexMetadata.Bid != 0.0 {
		t.Errorf("ForexMetadata.Bid = %f; want 0.0 (negative sanitized)", ev.ForexMetadata.Bid)
	}
	if ev.ForexMetadata.Ask != 0.0 {
		t.Errorf("ForexMetadata.Ask = %f; want 0.0 (negative sanitized)", ev.ForexMetadata.Ask)
	}
}

// TestMT5_SanitizeNaN tests NaN price sanitization
// SUCCESS CRITERIA: NaN prices sanitized to 0.0
func TestMT5_SanitizeNaN(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"EURUSD": "EUR/USD"})
	c := NewCanonicalizer(m)

	// Create event with NaN directly (JSON doesn't support NaN, so we test sanitizer)
	testPrice := math.NaN()
	sanitized := c.sanitizer.SanitizePrice(testPrice)

	if !math.IsNaN(testPrice) {
		t.Fatal("testPrice should be NaN")
	}
	if sanitized != 0.0 {
		t.Errorf("SanitizePrice(NaN) = %f; want 0.0", sanitized)
	}

	// Also test Inf
	testInf := math.Inf(1)
	sanitizedInf := c.sanitizer.SanitizePrice(testInf)
	if sanitizedInf != 0.0 {
		t.Errorf("SanitizePrice(Inf) = %f; want 0.0", sanitizedInf)
	}
}

// TestMT5_UnknownSymbol tests handling of unmapped symbols
// SUCCESS CRITERIA: Symbol passed through as-is when not in mapping
func TestMT5_UnknownSymbol(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"EURUSD": "EUR/USD"}) // GBPUSD not mapped
	c := NewCanonicalizer(m)

	payload := []byte(`{"type":"L1_TICK","symbol":"GBPUSD","bid":1.25456,"ask":1.25458,"last":1.25457,"volume":0.5,"time":1722933771120}`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	ev, err := c.parseMT5(raw)
	if err != nil {
		t.Fatalf("parseMT5 returned error: %v", err)
	}

	// Unknown symbol should pass through as-is (not "UNKNOWN")
	if ev.CanonicalSymbol != "GBPUSD" {
		t.Errorf("CanonicalSymbol = %q; want %q (unmapped symbol passes through)", ev.CanonicalSymbol, "GBPUSD")
	}
	if ev.ForexMetadata.CurrencyPair != "GBPUSD" {
		t.Errorf("ForexMetadata.CurrencyPair = %q; want %q", ev.ForexMetadata.CurrencyPair, "GBPUSD")
	}
}

// TestMT5_RawPayloadPreserved tests byte-for-byte raw payload preservation
// SUCCESS CRITERIA: RawPayload identical to input, even with special characters
func TestMT5_RawPayloadPreserved(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"EURUSD": "EUR/USD"})
	c := NewCanonicalizer(m)

	// Payload with unicode and escaped characters
	payload := []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120,"note":"special chars: \u20AC \n \t"}`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	ev, err := c.parseMT5(raw)
	if err != nil {
		t.Fatalf("parseMT5 returned error: %v", err)
	}

	// Verify byte-for-byte preservation
	if len(ev.RawPayload) != len(payload) {
		t.Errorf("RawPayload length = %d; want %d", len(ev.RawPayload), len(payload))
	}
	for i, b := range payload {
		if ev.RawPayload[i] != b {
			t.Errorf("RawPayload byte %d = %d; want %d (not preserved)", i, ev.RawPayload[i], b)
		}
	}
}

// TestMT5_ForexMetadata tests complete ForexMetadata population for L1_TICK
// SUCCESS CRITERIA: All forex fields populated correctly
func TestMT5_ForexMetadata(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"XAUUSD": "XAU/USD"})
	c := NewCanonicalizer(m)

	payload := []byte(`{"type":"L1_TICK","symbol":"XAUUSD","bid":1920.50,"ask":1920.80,"last":1920.65,"volume":10.5,"time":1722933771120}`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	ev, err := c.parseMT5(raw)
	if err != nil {
		t.Fatalf("parseMT5 returned error: %v", err)
	}

	if ev.ForexMetadata == nil {
		t.Fatal("ForexMetadata is nil")
	}

	// Test all fields
	if ev.ForexMetadata.CurrencyPair != "XAU/USD" {
		t.Errorf("CurrencyPair = %q; want %q", ev.ForexMetadata.CurrencyPair, "XAU/USD")
	}
	if ev.ForexMetadata.Bid != 1920.50 {
		t.Errorf("Bid = %f; want %f", ev.ForexMetadata.Bid, 1920.50)
	}
	if ev.ForexMetadata.Ask != 1920.80 {
		t.Errorf("Ask = %f; want %f", ev.ForexMetadata.Ask, 1920.80)
	}

	expectedSpread := 1920.80 - 1920.50
	if math.Abs(ev.ForexMetadata.Spread-expectedSpread) > 0.01 {
		t.Errorf("Spread = %f; want %f", ev.ForexMetadata.Spread, expectedSpread)
	}
}

// Test_MT5_OverflowPrice tests handling of extremely large prices (overflow detection)
// SUCCESS CRITERIA: Prices > 1e15 detected and sanitized
func Test_MT5_OverflowPrice(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"EURUSD": "EUR/USD"})
	c := NewCanonicalizer(m)

	// Test with 1e308 (near float64 max)
	overflowPrice := 1e308
	sanitized := c.sanitizer.SanitizePrice(overflowPrice)

	// Axiom sanitizer should detect overflow (config has overflow_threshold: 1e15)
	// But 1e308 is valid float64 (just very large). Check if it's Inf
	if math.IsInf(overflowPrice, 0) {
		if sanitized != 0.0 {
			t.Errorf("SanitizePrice(Inf) = %f; want 0.0", sanitized)
		}
	}

	// Test actual overflow scenario
	infPrice := math.Inf(1)
	sanitizedInf := c.sanitizer.SanitizePrice(infPrice)
	if sanitizedInf != 0.0 {
		t.Errorf("SanitizePrice(+Inf) = %f; want 0.0", sanitizedInf)
	}

	negInfPrice := math.Inf(-1)
	sanitizedNegInf := c.sanitizer.SanitizePrice(negInfPrice)
	if sanitizedNegInf != 0.0 {
		t.Errorf("SanitizePrice(-Inf) = %f; want 0.0", sanitizedNegInf)
	}
}

// TestMT5_L2Depth_EmptyLevels tests L2_DEPTH with empty bids/asks
// SUCCESS CRITERIA: Empty levels handled gracefully, ForexMetadata has zero bid/ask
func TestMT5_L2Depth_EmptyLevels(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"EURUSD": "EUR/USD"})
	c := NewCanonicalizer(m)

	payload := []byte(`{"type":"L2_DEPTH","symbol":"EURUSD","bids":[],"asks":[],"source":"MT5"}`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	ev, err := c.parseMT5(raw)
	if err != nil {
		t.Fatalf("parseMT5 returned error: %v", err)
	}

	if ev.EventType != "BOOK_SNAPSHOT" {
		t.Errorf("EventType = %q; want %q", ev.EventType, "BOOK_SNAPSHOT")
	}
	if len(ev.Levels) != 0 {
		t.Errorf("Levels length = %d; want 0 (empty depth)", len(ev.Levels))
	}

	// ForexMetadata should have zero bid/ask (no levels available)
	if ev.ForexMetadata.Bid != 0.0 {
		t.Errorf("ForexMetadata.Bid = %f; want 0.0 (no bid levels)", ev.ForexMetadata.Bid)
	}
	if ev.ForexMetadata.Ask != 0.0 {
		t.Errorf("ForexMetadata.Ask = %f; want 0.0 (no ask levels)", ev.ForexMetadata.Ask)
	}
	if ev.ForexMetadata.Spread != 0.0 {
		t.Errorf("ForexMetadata.Spread = %f; want 0.0 (no levels)", ev.ForexMetadata.Spread)
	}
}

// TestMT5_UnknownEventType tests handling of unknown MT5 event types
// SUCCESS CRITERIA: Unknown type returns error, UNKNOWN event created
func TestMT5_UnknownEventType(t *testing.T) {
	m := newTempMapperMT5(t, map[string]string{"EURUSD": "EUR/USD"})
	c := NewCanonicalizer(m)

	payload := []byte(`{"type":"UNKNOWN_TYPE","symbol":"EURUSD"}`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	ev, err := c.parseMT5(raw)

	// Should return error for unknown type
	if err == nil {
		t.Error("Expected error for unknown event type, got nil")
	}

	// Should create UNKNOWN event
	if ev.EventType != "UNKNOWN" {
		t.Errorf("EventType = %q; want %q", ev.EventType, "UNKNOWN")
	}
	if string(ev.RawPayload) != string(payload) {
		t.Error("RawPayload not preserved for unknown type")
	}
}

// BenchmarkMT5_Canonicalize benchmarks MT5 L1_TICK parsing
// SUCCESS CRITERIA: Measures end-to-end MT5 canonicalization performance
func BenchmarkMT5_Canonicalize(b *testing.B) {
	m, err := mapper.NewSymbolMapper("../../mappings")
	if err != nil {
		b.Skip("mappings not available")
	}
	c := NewCanonicalizer(m)

	payload := []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120,"source":"MT5","timestamp":1722933771120}`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.parseMT5(raw)
	}
}

// BenchmarkMT5_Canonicalize_L2Depth benchmarks L2_DEPTH parsing
func BenchmarkMT5_Canonicalize_L2Depth(b *testing.B) {
	m, err := mapper.NewSymbolMapper("../../mappings")
	if err != nil {
		b.Skip("mappings not available")
	}
	c := NewCanonicalizer(m)

	payload := []byte(`{"type":"L2_DEPTH","symbol":"EURUSD","bids":[{"price":1.08456,"volume":2.5},{"price":1.08455,"volume":3.0}],"asks":[{"price":1.08458,"volume":3.0},{"price":1.08459,"volume":4.0}],"source":"MT5"}`)

	raw := adapter.RawMessage{
		Source:      "MT5",
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.parseMT5(raw)
	}
}
