package canonicalizer

import (
	"bytes"
	"testing"

	gen "raw-data-layer/proto/gen"
)

func TestProto_RoundTripPreservesRawPayload(t *testing.T) {
	raw := []byte(`{"e":"trade","s":"BTCUSDT","p":"50000.00","q":"0.5","T":1234567890}`)
	ev := &CanonicalEvent{
		EventID:           "evt_1",
		Source:            "BINANCE",
		CanonicalSymbol:   "BTC/USD",
		ExchangeTimestamp: 1234567890000000000,
		LocalHWTimestamp:  1234567891000000000,
		EventType:         "TRADE",
		Price:             50000.0,
		Size:              0.5,
		Side:              "BUY",
		RawPayload:        raw,
		RawFormat:         "JSON",
		CryptoMetadata:    &CryptoMetadata{ExchangeSpecific: map[string]interface{}{"s": "BTCUSDT", "p": "50000.00"}},
	}

	data, err := MarshalProto(ev)
	if err != nil {
		t.Fatalf("MarshalProto: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("empty marshaled data")
	}

	back, err := UnmarshalProto(data)
	if err != nil {
		t.Fatalf("UnmarshalProto: %v", err)
	}
	if !bytes.Equal(back.RawPayload, raw) {
		t.Errorf("raw_payload not preserved: got %q want %q", back.RawPayload, raw)
	}
	if back.EventID != ev.EventID || back.Source != ev.Source || back.CanonicalSymbol != ev.CanonicalSymbol {
		t.Errorf("identity fields lost: %+v", back)
	}
	if back.Price != ev.Price || back.Size != ev.Size || back.Side != ev.Side {
		t.Errorf("trade fields lost: price=%v size=%v side=%v", back.Price, back.Size, back.Side)
	}
	if back.EventType != "TRADE" {
		t.Errorf("event type lost: %q", back.EventType)
	}
	if back.CryptoMetadata == nil || back.CryptoMetadata.ExchangeSpecific["s"] != "BTCUSDT" {
		t.Errorf("crypto metadata lost: %+v", back.CryptoMetadata)
	}
}

func TestProto_NilSafe(t *testing.T) {
	if _, err := MarshalProto(nil); err == nil {
		t.Error("MarshalProto(nil) should error")
	}
	if _, err := UnmarshalProto(nil); err == nil {
		t.Error("UnmarshalProto(nil) should error")
	}
	if p := ToProto(nil); p != nil {
		t.Error("ToProto(nil) should return nil")
	}
	if p := FromProto(nil); p != nil {
		t.Error("FromProto(nil) should return nil")
	}
}

func TestProto_EventTypeMapping(t *testing.T) {
	cases := []string{"TRADE", "QUOTE", "BOOK_UPDATE", "BOOK_SNAPSHOT", "UNKNOWN", "HEARTBEAT"}
	for _, c := range cases {
		got := eventTypeFromProto(eventTypeToProto(c))
		if c == "HEARTBEAT" {
			// unmapped → collapses to UNKNOWN (never panic)
			if got != "UNKNOWN" {
				t.Errorf("HEARTBEAT should map to UNKNOWN, got %q", got)
			}
			continue
		}
		if got != c {
			t.Errorf("round-trip event type %q → %q", c, got)
		}
	}
}

func TestProto_RawPayloadNoAlias(t *testing.T) {
	// Mutating the proto buffer after FromProto must not corrupt the plain copy.
	ev := &CanonicalEvent{EventID: "x", RawPayload: []byte("hello")}
	p := ToProto(ev)
	p.RawPayload[0] = 'X' // mutate proto copy
	if string(ev.RawPayload) != "hello" {
		t.Errorf("plain RawPayload aliased proto buffer: %q", ev.RawPayload)
	}
	_ = gen.EventType_UNKNOWN
}

func TestProto_FullMetadataRoundTrip(t *testing.T) {
	ev := &CanonicalEvent{
		EventID:         "evt_full",
		Source:          "IB",
		CanonicalSymbol: "AAPL",
		EventType:       "BOOK_SNAPSHOT",
		Price:           150.25,
		Size:            100,
		Side:            "SELL",
		RawPayload:      []byte("raw-ib-bytes"),
		RawFormat:       "BINARY",
		Levels: []Level{
			{Price: 150.25, Size: 100, Side: "BID", OrderID: 1},
			{Price: 150.30, Size: 200, Side: "ASK", OrderID: 2},
		},
		ForexMetadata: &ForexMetadata{CurrencyPair: "EUR/USD", Bid: 1.1, Ask: 1.2, Spread: 0.1},
		FuturesMetadata: &FuturesMetadata{ContractMonth: "2025-03", OpenInterest: 12345, SettlementPrice: 99.5},
		EquityMetadata:  &EquityMetadata{Exchange: "NASDAQ", MIC: "XNGS", ConditionCodes: []string{"A", "B"}},
	}
	data, err := MarshalProto(ev)
	if err != nil {
		t.Fatalf("MarshalProto: %v", err)
	}
	back, err := UnmarshalProto(data)
	if err != nil {
		t.Fatalf("UnmarshalProto: %v", err)
	}
	if back.EventType != "BOOK_SNAPSHOT" {
		t.Errorf("event type: %q", back.EventType)
	}
	if len(back.Levels) != 2 || back.Levels[0].OrderID != 1 || back.Levels[1].Side != "ASK" {
		t.Errorf("levels lost: %+v", back.Levels)
	}
	if back.ForexMetadata == nil || back.ForexMetadata.CurrencyPair != "EUR/USD" || back.ForexMetadata.Spread != 0.1 {
		t.Errorf("forex metadata lost: %+v", back.ForexMetadata)
	}
	if back.FuturesMetadata == nil || back.FuturesMetadata.ContractMonth != "2025-03" {
		t.Errorf("futures metadata lost: %+v", back.FuturesMetadata)
	}
	if back.EquityMetadata == nil || back.EquityMetadata.MIC != "XNGS" || len(back.EquityMetadata.ConditionCodes) != 2 {
		t.Errorf("equity metadata lost: %+v", back.EquityMetadata)
	}
	if string(back.RawPayload) != "raw-ib-bytes" {
		t.Errorf("raw payload lost: %q", back.RawPayload)
	}
}
