package canonicalizer

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/mapper"
)

// createTestMapper creates a mapper for testing
func createTestMapper(t *testing.T) *mapper.SymbolMapper {
	m, err := mapper.NewSymbolMapper("../../mappings")
	if err != nil {
		t.Logf("Warning: could not load real mappings: %v", err)
		// Create mock mapper
		m = &mapper.SymbolMapper{}
	}
	return m
}

// TestNewCanonicalizer tests canonicalizer creation
func TestNewCanonicalizer(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	
	if c == nil {
		t.Fatal("Expected canonicalizer to be created")
	}
	
	if c.symbolMapper == nil {
		t.Error("Expected symbolMapper to be set")
	}
	
	if c.sanitizer == nil {
		t.Error("Expected sanitizer to be set")
	}
}

// TestCanonicalizer_ParseBinance tests Binance message parsing
func TestCanonicalizer_ParseBinance(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	
	// Valid Binance trade message
	payload := []byte(`{"e":"trade","E":1234567890,"s":"BTCUSDT","t":12345,"p":"50000.00","q":"0.5","T":1234567890,"m":false}`)
	
	raw := adapter.RawMessage{
		Source:     "BINANCE",
		Payload:    payload,
		ReceivedAt: time.Now().UnixNano(),
	}
	
	canonical, err := c.parseBinance(raw)
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if canonical.Source != "BINANCE" {
		t.Errorf("Expected source BINANCE, got %s", canonical.Source)
	}
	
	if canonical.EventType != "TRADE" {
		t.Errorf("Expected event type TRADE, got %s", canonical.EventType)
	}
	
	if canonical.Price != 50000.0 {
		t.Errorf("Expected price 50000.0, got %f", canonical.Price)
	}
	
	if canonical.Size != 0.5 {
		t.Errorf("Expected size 0.5, got %f", canonical.Size)
	}
	
	if canonical.Side != "BUY" {
		t.Errorf("Expected side BUY, got %s", canonical.Side)
	}
	
	// Verify raw payload is preserved byte-for-byte
	if string(canonical.RawPayload) != string(payload) {
		t.Error("Raw payload not preserved")
	}
}

// TestCanonicalizer_ParseBinance_MalformedJSON tests malformed JSON handling
// This is a mandatory test (CLAUDE.md: "Garbage In, Canonical Out")
func TestCanonicalizer_ParseBinance_MalformedJSON(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	
	// Malformed JSON
	payload := []byte(`{"invalid json`)
	
	raw := adapter.RawMessage{
		Source:     "BINANCE",
		Payload:    payload,
		ReceivedAt: time.Now().UnixNano(),
	}
	
	canonical, err := c.parseBinance(raw)
	
	// Should return error but not panic
	if err == nil {
		t.Error("Expected error for malformed JSON")
	}
	
	// Should still preserve raw payload
	if string(canonical.RawPayload) != string(payload) {
		t.Error("Raw payload not preserved even with error")
	}
	
	// Should have UNKNOWN fields
	if canonical.CanonicalSymbol != "UNKNOWN" {
		t.Errorf("Expected UNKNOWN symbol, got %s", canonical.CanonicalSymbol)
	}
	
	if canonical.EventType != "UNKNOWN" {
		t.Errorf("Expected UNKNOWN event type, got %s", canonical.EventType)
	}
}

// TestCanonicalizer_SanitizePrice tests price sanitization
// This verifies Axle-Axiom integration
func TestCanonicalizer_SanitizePrice(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	
	tests := []struct {
		name     string
		price    string
		expected float64
	}{
		{"Valid price", "100.50", 100.50},
		{"Zero price", "0.0", 0.0},
		{"Large price", "999999.99", 999999.99},
		{"Invalid string", "invalid", 0.0},
		{"Empty string", "", 0.0},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			price := c.parseFloat(tt.price)
			sanitized := c.sanitizer.SanitizePrice(price)
			
			if sanitized != tt.expected {
				t.Errorf("Expected %f, got %f", tt.expected, sanitized)
			}
		})
	}
}

// TestCanonicalizer_SanitizePrice_Negative tests negative price handling
// This is a mandatory test (CLAUDE.md: "price < 0 → 0.0")
func TestCanonicalizer_SanitizePrice_Negative(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	
	// Negative price
	payload := []byte(`{"e":"trade","s":"BTCUSDT","p":"-100.0","q":"1.0","T":1234567890}`)
	
	raw := adapter.RawMessage{
		Source:     "BINANCE",
		Payload:    payload,
		ReceivedAt: time.Now().UnixNano(),
	}
	
	canonical, _ := c.parseBinance(raw)
	
	// Negative price should be sanitized to 0.0
	if canonical.Price != 0.0 {
		t.Errorf("Expected price 0.0 (sanitized), got %f", canonical.Price)
	}
}

// TestCanonicalizer_SanitizePrice_Overflow tests overflow handling
// This is a mandatory test (CLAUDE.md: Test_OverflowPrice)
func TestCanonicalizer_SanitizePrice_Overflow(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	
	// Create price with 1e308 (near float64 max)
	price := 1e308
	sanitized := c.sanitizer.SanitizePrice(price)
	
	// Should detect as infinity and return 0.0
	if !math.IsInf(price, 0) && sanitized != 0.0 {
		t.Errorf("Expected overflow to be sanitized to 0.0, got %f", sanitized)
	}
}

// TestCanonicalizer_SanitizeSize_Negative tests negative size handling
func TestCanonicalizer_SanitizeSize_Negative(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	
	// Negative size
	payload := []byte(`{"e":"trade","s":"BTCUSDT","p":"100.0","q":"-1.0","T":1234567890}`)
	
	raw := adapter.RawMessage{
		Source:     "BINANCE",
		Payload:    payload,
		ReceivedAt: time.Now().UnixNano(),
	}
	
	canonical, _ := c.parseBinance(raw)
	
	// Negative size should be sanitized to 0.0
	if canonical.Size != 0.0 {
		t.Errorf("Expected size 0.0 (sanitized), got %f", canonical.Size)
	}
}

// TestCanonicalizer_ParseIB tests IB message parsing
func TestCanonicalizer_ParseIB(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	
	// IB binary message (simplified for MVP)
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	
	raw := adapter.RawMessage{
		Source:     "IB",
		Payload:    payload,
		ReceivedAt: time.Now().UnixNano(),
	}
	
	canonical, err := c.parseIB(raw)
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if canonical.Source != "IB" {
		t.Errorf("Expected source IB, got %s", canonical.Source)
	}
	
	if canonical.RawFormat != "BINARY" {
		t.Errorf("Expected format BINARY, got %s", canonical.RawFormat)
	}
	
	// Verify raw payload is preserved
	if string(canonical.RawPayload) != string(payload) {
		t.Error("Raw payload not preserved")
	}
}

// TestCanonicalizer_Process tests the main Process function
func TestCanonicalizer_Process(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	ctx := context.Background()
	
	payload := []byte(`{"e":"trade","s":"BTCUSDT","p":"50000.0","q":"1.0","T":1234567890}`)
	
	raw := adapter.RawMessage{
		Source:     "BINANCE",
		Payload:    payload,
		ReceivedAt: time.Now().UnixNano(),
	}
	
	processed, err := c.Process(ctx, raw)
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if processed.ProcessedAt == 0 {
		t.Error("Expected ProcessedAt to be set")
	}
	
	// Verify raw message is preserved
	if string(processed.Raw.Payload) != string(payload) {
		t.Error("Raw message not preserved in ProcessedMessage")
	}
}

// TestCanonicalizer_Process_UnknownSource tests unknown source handling
func TestCanonicalizer_Process_UnknownSource(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	ctx := context.Background()
	
	raw := adapter.RawMessage{
		Source:     "UNKNOWN_EXCHANGE",
		Payload:    []byte("test"),
		ReceivedAt: time.Now().UnixNano(),
	}
	
	processed, err := c.Process(ctx, raw)
	
	// Should return error but not panic
	if err == nil {
		t.Error("Expected error for unknown source")
	}
	
	// Should still have processed message
	if processed.ProcessedAt == 0 {
		t.Error("Expected ProcessedAt to be set")
	}
}

// TestCanonicalizer_RawPayloadPreservation tests raw payload immutability
// This is a mandatory test (CLAUDE.md: "raw_payload byte-for-byte preserved")
func TestCanonicalizer_RawPayloadPreservation(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	
	originalPayload := []byte(`{"e":"trade","s":"BTCUSDT","p":"50000.0","q":"1.0"}`)
	
	raw := adapter.RawMessage{
		Source:     "BINANCE",
		Payload:    originalPayload,
		ReceivedAt: time.Now().UnixNano(),
	}
	
	canonical, _ := c.parseBinance(raw)
	
	// Verify byte-for-byte identity
	if len(canonical.RawPayload) != len(originalPayload) {
		t.Errorf("Raw payload length changed: expected %d, got %d", 
			len(originalPayload), len(canonical.RawPayload))
	}
	
	for i, b := range originalPayload {
		if canonical.RawPayload[i] != b {
			t.Errorf("Raw payload modified at index %d: expected %d, got %d", 
				i, b, canonical.RawPayload[i])
		}
	}
}

// TestCanonicalizer_EventIDGeneration tests event ID generation
func TestCanonicalizer_EventIDGeneration(t *testing.T) {
	id1 := generateEventID()
	time.Sleep(1 * time.Millisecond)
	id2 := generateEventID()
	
	if id1 == id2 {
		t.Error("Expected unique event IDs")
	}
	
	if id1 == "" || id2 == "" {
		t.Error("Expected non-empty event IDs")
	}
}

// TestCanonicalizer_ExtractHelpers tests helper functions
func TestCanonicalizer_ExtractHelpers(t *testing.T) {
	m := createTestMapper(t)
	c := NewCanonicalizer(m)
	
	msg := map[string]interface{}{
		"string_field": "test",
		"int_field":    42.0,
		"float_field":  3.14,
	}
	
	// Test extractString
	if s := c.extractString(msg, "string_field"); s != "test" {
		t.Errorf("Expected 'test', got %s", s)
	}
	
	if s := c.extractString(msg, "missing_field"); s != "" {
		t.Errorf("Expected empty string for missing field, got %s", s)
	}
	
	// Test extractInt64
	if i := c.extractInt64(msg, "int_field"); i != 42 {
		t.Errorf("Expected 42, got %d", i)
	}
	
	if i := c.extractInt64(msg, "missing_field"); i != 0 {
		t.Errorf("Expected 0 for missing field, got %d", i)
	}
	
	// Test parseFloat
	if f := c.parseFloat("3.14"); f != 3.14 {
		t.Errorf("Expected 3.14, got %f", f)
	}
	
	if f := c.parseFloat("invalid"); f != 0.0 {
		t.Errorf("Expected 0.0 for invalid string, got %f", f)
	}
}

// TestCanonicalEvent_JSONMarshaling tests JSON serialization
func TestCanonicalEvent_JSONMarshaling(t *testing.T) {
	event := &CanonicalEvent{
		EventID:           "evt_123",
		Source:            "BINANCE",
		CanonicalSymbol:   "BTC/USD",
		ExchangeTimestamp: 1234567890,
		LocalHWTimestamp:  1234567890,
		EventType:         "TRADE",
		Price:             50000.0,
		Size:              1.0,
		Side:              "BUY",
		RawPayload:        []byte("test"),
		RawFormat:         "JSON",
	}
	
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("JSON marshal failed: %v", err)
	}
	
	var decoded CanonicalEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}
	
	if decoded.EventID != event.EventID {
		t.Errorf("EventID mismatch: expected %s, got %s", event.EventID, decoded.EventID)
	}
	
	if decoded.Price != event.Price {
		t.Errorf("Price mismatch: expected %f, got %f", event.Price, decoded.Price)
	}
}

// BenchmarkCanonicalizer_ParseBinance benchmarks Binance parsing
func BenchmarkCanonicalizer_ParseBinance(b *testing.B) {
	m, _ := mapper.NewSymbolMapper("../../mappings")
	c := NewCanonicalizer(m)
	
	payload := []byte(`{"e":"trade","E":1234567890,"s":"BTCUSDT","p":"50000.00","q":"0.5","T":1234567890,"m":false}`)
	
	raw := adapter.RawMessage{
		Source:     "BINANCE",
		Payload:    payload,
		ReceivedAt: time.Now().UnixNano(),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.parseBinance(raw)
	}
}

// BenchmarkCanonicalizer_Process benchmarks full processing
func BenchmarkCanonicalizer_Process(b *testing.B) {
	m, _ := mapper.NewSymbolMapper("../../mappings")
	c := NewCanonicalizer(m)
	ctx := context.Background()
	
	payload := []byte(`{"e":"trade","s":"BTCUSDT","p":"50000.0","q":"1.0","T":1234567890}`)
	
	raw := adapter.RawMessage{
		Source:     "BINANCE",
		Payload:    payload,
		ReceivedAt: time.Now().UnixNano(),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Process(ctx, raw)
	}
}
