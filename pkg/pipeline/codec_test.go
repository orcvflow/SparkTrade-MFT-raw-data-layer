package pipeline

import (
	"bytes"
	"testing"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/ipc"
)

func TestCodec_RawRoundTrip(t *testing.T) {
	raw := []byte(`{"e":"trade","s":"BTCUSDT","p":"50000.00"}`)
	rm := adapter.RawMessage{Source: "BINANCE", Payload: raw, ReceivedAt: 123, SequenceNum: 7}

	msg, err := EncodeRaw(rm)
	if err != nil {
		t.Fatalf("EncodeRaw: %v", err)
	}
	if msg.Type != ipc.TypeRaw {
		t.Errorf("type = %q, want %q", msg.Type, ipc.TypeRaw)
	}
	if msg.Seq == 0 {
		t.Error("seq not assigned")
	}

	back, err := DecodeRaw(msg)
	if err != nil {
		t.Fatalf("DecodeRaw: %v", err)
	}
	if !bytes.Equal(back.Payload, raw) {
		t.Errorf("payload not preserved: %q vs %q", back.Payload, raw)
	}
	if back.Source != "BINANCE" || back.ReceivedAt != 123 || back.SequenceNum != 7 {
		t.Errorf("fields lost: %+v", back)
	}
}

func TestCodec_RawPayloadNoAlias(t *testing.T) {
	raw := []byte("hello")
	msg, _ := EncodeRaw(adapter.RawMessage{Source: "X", Payload: raw})
	back, _ := DecodeRaw(msg)
	back.Payload[0] = 'X' // mutate decoded copy
	if raw[0] != 'h' {
		t.Errorf("decoded payload aliased source slice: %q", raw)
	}
}

func TestCodec_CanonicalRoundTrip(t *testing.T) {
	ev := &canonicalizer.CanonicalEvent{
		EventID:         "evt_42",
		Source:          "BINANCE",
		CanonicalSymbol: "BTC/USD",
		Price:           50000.5,
		Size:            0.25,
		Side:            "BUY",
		EventType:       "TRADE",
		RawPayload:      []byte(`{"p":"50000.5"}`),
		RawFormat:       "JSON",
	}
	msg, err := EncodeCanonical(ev)
	if err != nil {
		t.Fatalf("EncodeCanonical: %v", err)
	}
	if msg.Type != ipc.TypeCanonical {
		t.Errorf("type = %q, want %q", msg.Type, ipc.TypeCanonical)
	}
	back, err := DecodeCanonical(msg)
	if err != nil {
		t.Fatalf("DecodeCanonical: %v", err)
	}
	if back.EventID != "evt_42" || back.Price != 50000.5 || !bytes.Equal(back.RawPayload, ev.RawPayload) {
		t.Errorf("round-trip lost fields: %+v", back)
	}
}

func TestCodec_DecodeNilSafe(t *testing.T) {
	if _, err := DecodeRaw(nil); err == nil {
		t.Error("DecodeRaw(nil) should error")
	}
	if _, err := DecodeCanonical(nil); err == nil {
		t.Error("DecodeCanonical(nil) should error")
	}
}
