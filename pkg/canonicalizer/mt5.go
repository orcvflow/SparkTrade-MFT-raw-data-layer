package canonicalizer

import (
	"encoding/json"
	"fmt"

	"raw-data-layer/pkg/adapter"
)

// parseMT5Into parses MT5 ZeroMQ JSON (L1 tick or L2 depth) into ev.
// MT5 sends two event types via ZeroMQ:
//   - L1_TICK: {"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120}
//   - L2_DEPTH: {"type":"L2_DEPTH","symbol":"EURUSD","bids":[{"price":1.08456,"volume":2.5}],"asks":[...]}
//
// Never panics; on malformed JSON it fills ev with UNKNOWN fallback (raw payload preserved).
func (c *Canonicalizer) parseMT5Into(raw adapter.RawMessage, ev *CanonicalEvent) error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			fillUnknown(ev, raw, "MT5", "JSON")
		}
	}()

	// Detect message type first
	var header struct {
		Type   string `json:"type"`
		Symbol string `json:"symbol"`
	}
	if err := json.Unmarshal(raw.Payload, &header); err != nil {
		fillUnknown(ev, raw, "MT5", "JSON")
		return fmt.Errorf("mt5 header parse: %w", err)
	}

	// Map symbol (if not mapped, use original symbol as-is)
	canonicalSymbol := c.symbolMapper.ToCanonical("MT5", header.Symbol)
	if canonicalSymbol == "" || canonicalSymbol == "UNKNOWN" {
		canonicalSymbol = header.Symbol // Pass through unmapped symbols
	}

	switch header.Type {
	case "L1_TICK":
		return c.parseMT5TickInto(raw, canonicalSymbol, ev)
	case "L2_DEPTH":
		return c.parseMT5DepthInto(raw, canonicalSymbol, ev)
	default:
		fillUnknown(ev, raw, "MT5", "JSON")
		return fmt.Errorf("unknown mt5 event type: %s", header.Type)
	}
}

// parseMT5TickInto parses L1 tick (forex quote) into ev.
func (c *Canonicalizer) parseMT5TickInto(raw adapter.RawMessage, canonicalSymbol string, ev *CanonicalEvent) error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			fillUnknown(ev, raw, "MT5", "JSON")
		}
	}()

	var tick struct {
		Type      string  `json:"type"`
		Symbol    string  `json:"symbol"`
		Bid       float64 `json:"bid"`
		Ask       float64 `json:"ask"`
		Last      float64 `json:"last"`
		Volume    float64 `json:"volume"`
		Time      int64   `json:"time"`
		Source    string  `json:"source"`
		Timestamp int64   `json:"timestamp"`
	}

	if err := json.Unmarshal(raw.Payload, &tick); err != nil {
		fillUnknown(ev, raw, "MT5", "JSON")
		return fmt.Errorf("mt5 tick parse: %w", err)
	}

	// Sanitize all floats (paranoid principle from CLAUDE.md)
	bid := c.sanitizer.SanitizePrice(tick.Bid)
	ask := c.sanitizer.SanitizePrice(tick.Ask)
	last := c.sanitizer.SanitizePrice(tick.Last)
	volume := c.sanitizer.SanitizeSize(tick.Volume)

	// Calculate spread (forex-specific)
	spread := 0.0
	if bid > 0 && ask > 0 {
		spread = ask - bid
	}

	ev.EventID = generateEventID()
	ev.Source = "MT5"
	ev.CanonicalSymbol = canonicalSymbol
	ev.ExchangeTimestamp = tick.Time * 1000000 // ms → ns (MT5 uses milliseconds)
	ev.LocalHWTimestamp = raw.ReceivedAt
	ev.EventType = "QUOTE" // Forex tick = quote
	ev.Price = last
	ev.Size = volume
	ev.Side = "UNKNOWN"         // Tick has no aggressor side
	ev.RawPayload = raw.Payload // UNTOUCHED — byte-for-byte preservation
	ev.RawFormat = "JSON"
	ev.ForexMetadata = &ForexMetadata{
		CurrencyPair: canonicalSymbol,
		Bid:          bid,
		Ask:          ask,
		Spread:       spread,
	}
	return nil
}

// parseMT5DepthInto parses L2 depth (order book snapshot) into ev.
func (c *Canonicalizer) parseMT5DepthInto(raw adapter.RawMessage, canonicalSymbol string, ev *CanonicalEvent) error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			fillUnknown(ev, raw, "MT5", "JSON")
		}
	}()

	var depth struct {
		Type   string `json:"type"`
		Symbol string `json:"symbol"`
		Bids   []struct {
			Price  float64 `json:"price"`
			Volume float64 `json:"volume"`
		} `json:"bids"`
		Asks []struct {
			Price  float64 `json:"price"`
			Volume float64 `json:"volume"`
		} `json:"asks"`
		Source string `json:"source"`
	}

	if err := json.Unmarshal(raw.Payload, &depth); err != nil {
		fillUnknown(ev, raw, "MT5", "JSON")
		return fmt.Errorf("mt5 depth parse: %w", err)
	}

	// Build levels with sanitization
	levels := make([]Level, 0, len(depth.Bids)+len(depth.Asks))

	// Bids
	for _, b := range depth.Bids {
		price := c.sanitizer.SanitizePrice(b.Price)
		size := c.sanitizer.SanitizeSize(b.Volume)
		if price > 0 { // Skip invalid levels
			levels = append(levels, Level{
				Price: price,
				Size:  size,
				Side:  "BID",
			})
		}
	}

	// Asks
	for _, a := range depth.Asks {
		price := c.sanitizer.SanitizePrice(a.Price)
		size := c.sanitizer.SanitizeSize(a.Volume)
		if price > 0 { // Skip invalid levels
			levels = append(levels, Level{
				Price: price,
				Size:  size,
				Side:  "ASK",
			})
		}
	}

	// Calculate best bid/ask from levels (first bid/ask levels)
	bid := 0.0
	ask := 0.0
	if len(depth.Bids) > 0 {
		bid = c.sanitizer.SanitizePrice(depth.Bids[0].Price)
	}
	if len(depth.Asks) > 0 {
		ask = c.sanitizer.SanitizePrice(depth.Asks[0].Price)
	}
	spread := 0.0
	if bid > 0 && ask > 0 {
		spread = ask - bid
	}

	ev.EventID = generateEventID()
	ev.Source = "MT5"
	ev.CanonicalSymbol = canonicalSymbol
	ev.ExchangeTimestamp = raw.ReceivedAt // L2 depth has no timestamp in MT5 message, use adapter ReceivedAt
	ev.LocalHWTimestamp = raw.ReceivedAt
	ev.EventType = "BOOK_SNAPSHOT"
	ev.Price = 0.0 // Not applicable for order book
	ev.Size = 0.0
	ev.Side = "UNKNOWN"
	ev.Levels = levels
	ev.RawPayload = raw.Payload // UNTOUCHED
	ev.RawFormat = "JSON"
	ev.ForexMetadata = &ForexMetadata{
		CurrencyPair: canonicalSymbol,
		Bid:          bid,
		Ask:          ask,
		Spread:       spread,
	}
	return nil
}

// parseMT5 is the non-mutating wrapper for tests.
func (c *Canonicalizer) parseMT5(raw adapter.RawMessage) (*CanonicalEvent, error) {
	ev := AcquireEvent()
	err := c.parseMT5Into(raw, ev)
	return ev, err
}
