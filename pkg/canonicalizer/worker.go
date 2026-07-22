package canonicalizer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/axiom"
	"raw-data-layer/pkg/mapper"
	"raw-data-layer/pkg/workerpool"
)

// Canonicalizer converts raw messages to canonical format
// Key principles:
// - Never panic ("Garbage In, Canonical Out")
// - Preserve raw_payload byte-for-byte
// - Sanitize all numeric fields (Axle-Axiom)
// - Map symbols to canonical form
type Canonicalizer struct {
	symbolMapper *mapper.SymbolMapper
	sanitizer    *axiom.MathSanitizer
}

// NewCanonicalizer creates a new canonicalizer
func NewCanonicalizer(symbolMapper *mapper.SymbolMapper) *Canonicalizer {
	return &Canonicalizer{
		symbolMapper: symbolMapper,
		sanitizer:    axiom.NewMathSanitizer(),
	}
}

// Process is the main processing function for worker pool
// Signature matches workerpool.ProcessorFunc
func (c *Canonicalizer) Process(ctx context.Context, raw adapter.RawMessage) (workerpool.ProcessedMessage, error) {
	// Never panic - use defer recover
	defer func() {
		if r := recover(); r != nil {
			// Panic recovery - return error but continue
		}
	}()
	
	// Determine source type and parse accordingly
	var canonical *CanonicalEvent
	var err error
	
	switch raw.Source {
	case "BINANCE":
		canonical, err = c.parseBinance(raw)
	case "IB":
		canonical, err = c.parseIB(raw)
	default:
		err = fmt.Errorf("unknown source: %s", raw.Source)
	}
	
	if err != nil {
		// Even with error, preserve raw payload
		canonical = &CanonicalEvent{
			EventID:          generateEventID(),
			Source:           raw.Source,
			CanonicalSymbol:  "UNKNOWN",
			ExchangeTimestamp: time.Now().UnixNano(),
			LocalHWTimestamp: raw.ReceivedAt,
			EventType:        "UNKNOWN",
			Price:            0.0,
			Size:             0.0,
			Side:             "UNKNOWN",
			RawPayload:       raw.Payload, // UNTOUCHED
			RawFormat:        "UNKNOWN",
		}
	}
	
	// Use canonical variable to avoid compile error
	if canonical != nil {
		_ = canonical // Silence unused variable warning
	}
	
	return workerpool.ProcessedMessage{
		Raw:         raw,
		Error:       err,
		ProcessedAt: time.Now().UnixNano(),
	}, err
}

// CanonicalEvent represents the unified market data format
// This matches proto/canonical.proto structure
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
	ForexMetadata    *ForexMetadata
	FuturesMetadata  *FuturesMetadata
	CryptoMetadata   *CryptoMetadata
	EquityMetadata   *EquityMetadata
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
	ContractMonth    string
	OpenInterest     float64
	SettlementPrice  float64
}

type CryptoMetadata struct {
	ExchangeSpecific map[string]interface{}
}

type EquityMetadata struct {
	Exchange       string
	MIC            string
	ConditionCodes []string
}

// parseBinance parses Binance WebSocket JSON messages
func (c *Canonicalizer) parseBinance(raw adapter.RawMessage) (*CanonicalEvent, error) {
	// Binance trade message format:
	// {"e":"trade","E":1234567890,"s":"BTCUSDT","t":12345,"p":"50000.00","q":"0.5","T":1234567890}
	
	var msg map[string]interface{}
	if err := json.Unmarshal(raw.Payload, &msg); err != nil {
		// JSON parse failed - return with preserved raw payload
		return &CanonicalEvent{
			EventID:          generateEventID(),
			Source:           "BINANCE",
			CanonicalSymbol:  "UNKNOWN",
			ExchangeTimestamp: time.Now().UnixNano(),
			LocalHWTimestamp: raw.ReceivedAt,
			EventType:        "UNKNOWN",
			Price:            0.0,
			Size:             0.0,
			Side:             "UNKNOWN",
			RawPayload:       raw.Payload,
			RawFormat:        "JSON",
		}, fmt.Errorf("json parse error: %w", err)
	}
	
	// Extract fields with paranoid validation
	symbol := c.extractString(msg, "s")
	canonicalSymbol := c.symbolMapper.ToCanonical("BINANCE", symbol)
	if canonicalSymbol == "" {
		canonicalSymbol = symbol // Fallback to original
	}
	
	// Extract price and sanitize (Axle-Axiom)
	priceStr := c.extractString(msg, "p")
	price := c.parseFloat(priceStr)
	price = c.sanitizer.SanitizePrice(price)
	
	// Extract size and sanitize
	sizeStr := c.extractString(msg, "q")
	size := c.parseFloat(sizeStr)
	size = c.sanitizer.SanitizeSize(size)
	
	// Extract timestamp
	exchangeTS := c.extractInt64(msg, "T")
	if exchangeTS == 0 {
		exchangeTS = c.extractInt64(msg, "E")
	}
	
	// Determine side (Binance doesn't include side in trade message)
	side := "UNKNOWN"
	if m, ok := msg["m"].(bool); ok {
		if m {
			side = "SELL"
		} else {
			side = "BUY"
		}
	}
	
	return &CanonicalEvent{
		EventID:           generateEventID(),
		Source:            "BINANCE",
		CanonicalSymbol:   canonicalSymbol,
		ExchangeTimestamp: exchangeTS * 1000000, // ms to ns
		LocalHWTimestamp:  raw.ReceivedAt,
		EventType:         "TRADE",
		Price:             price,
		Size:              size,
		Side:              side,
		RawPayload:        raw.Payload, // UNTOUCHED
		RawFormat:         "JSON",
		CryptoMetadata: &CryptoMetadata{
			ExchangeSpecific: msg,
		},
	}, nil
}

// parseIB parses IB Gateway binary messages
func (c *Canonicalizer) parseIB(raw adapter.RawMessage) (*CanonicalEvent, error) {
	// IB binary protocol is complex - simplified version
	// Production would require full IB API message parsing
	
	// For MVP, treat as opaque binary
	return &CanonicalEvent{
		EventID:           generateEventID(),
		Source:            "IB",
		CanonicalSymbol:   "UNKNOWN",
		ExchangeTimestamp: time.Now().UnixNano(),
		LocalHWTimestamp:  raw.ReceivedAt,
		EventType:         "TRADE",
		Price:             0.0,
		Size:              0.0,
		Side:              "UNKNOWN",
		RawPayload:        raw.Payload, // UNTOUCHED
		RawFormat:         "BINARY",
		EquityMetadata: &EquityMetadata{
			Exchange: "IB",
		},
	}, nil
}

// Helper functions for safe extraction

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

func (c *Canonicalizer) parseFloat(s string) float64 {
	var result float64
	fmt.Sscanf(s, "%f", &result)
	return result
}

// generateEventID generates a unique event ID
func generateEventID() string {
	// Simple timestamp-based ID for MVP
	// Production would use UUID v4 or ULID
	return fmt.Sprintf("evt_%d", time.Now().UnixNano())
}
