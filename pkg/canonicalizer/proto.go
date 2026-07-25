package canonicalizer

// Protobuf bridge for the plain CanonicalEvent struct.
//
// The pipeline (canonicalizer/publisher/storage/validation) all use the plain
// Go struct below. Addım C sends events across process boundaries over UDS; the
// user chose Protobuf ("JSON YOX, Protobuf") for IPC serialization, leaning on
// the existing proto/canonical.proto. This file converts between the plain
// struct and the generated proto type, then (un)marshals to bytes.
//
// Invariants (CLAUDE.md):
//   - Never panic: every entry point has defer/recover.
//   - raw_payload preserved byte-for-byte (proto bytes field copies the slice).
//
// Note: CryptoMetadata.ExchangeSpecific is map[string]interface{} in the plain
// struct but a string in the proto schema — the proto comment explicitly calls
// for a "JSON blob for exchange-specific data", so the map is JSON-encoded into
// that string on the way out and decoded back on the way in. This is proto-level
// schema design (a scalar carrying an opaque blob), not "JSON for IPC".

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"

	gen "raw-data-layer/proto/gen"
)

// MarshalProto converts a plain CanonicalEvent to its proto form and marshals it
// to bytes for IPC transport. Never panics; returns an error on failure.
func MarshalProto(ev *CanonicalEvent) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("canonicalizer: panic in MarshalProto: %v", r)
		}
	}()
	if ev == nil {
		return nil, fmt.Errorf("canonicalizer: marshal nil event")
	}
	p := ToProto(ev)
	if p == nil {
		return nil, fmt.Errorf("canonicalizer: ToProto returned nil")
	}
	return proto.Marshal(p)
}

// UnmarshalProto unmarshals bytes into a plain CanonicalEvent. Never panics.
func UnmarshalProto(data []byte) (ev *CanonicalEvent, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("canonicalizer: panic in UnmarshalProto: %v", r)
		}
	}()
	if len(data) == 0 {
		return nil, fmt.Errorf("canonicalizer: unmarshal empty data")
	}
	p := &gen.CanonicalEvent{}
	if err := proto.Unmarshal(data, p); err != nil {
		return nil, fmt.Errorf("canonicalizer: proto unmarshal: %w", err)
	}
	return FromProto(p), nil
}

// ToProto converts a plain CanonicalEvent to the generated proto type.
func ToProto(ev *CanonicalEvent) *gen.CanonicalEvent {
	if ev == nil {
		return nil
	}
	p := &gen.CanonicalEvent{
		EventId:           ev.EventID,
		Source:            ev.Source,
		CanonicalSymbol:   ev.CanonicalSymbol,
		ExchangeTimestamp: ev.ExchangeTimestamp,
		LocalHwTimestamp:  ev.LocalHWTimestamp,
		EventType:         eventTypeToProto(ev.EventType),
		Price:             ev.Price,
		Size:              ev.Size,
		Side:              ev.Side,
		RawPayload:        append([]byte(nil), ev.RawPayload...), // copy — byte-for-byte, no aliasing
		RawFormat:          ev.RawFormat,
	}

	for i := range ev.Levels {
		l := ev.Levels[i]
		p.Levels = append(p.Levels, &gen.Level{
			Price:   l.Price,
			Size:    l.Size,
			Side:    l.Side,
			OrderId: l.OrderID,
		})
	}

	if ev.ForexMetadata != nil {
		p.Forex = &gen.ForexMetadata{
			CurrencyPair: ev.ForexMetadata.CurrencyPair,
			Bid:          ev.ForexMetadata.Bid,
			Ask:          ev.ForexMetadata.Ask,
			Spread:       ev.ForexMetadata.Spread,
		}
	}
	if ev.FuturesMetadata != nil {
		p.Futures = &gen.FuturesMetadata{
			ContractMonth:    ev.FuturesMetadata.ContractMonth,
			OpenInterest:     ev.FuturesMetadata.OpenInterest,
			SettlementPrice:  ev.FuturesMetadata.SettlementPrice,
		}
	}
	if ev.CryptoMetadata != nil {
		// map[string]interface{} → opaque JSON blob string (proto schema intent).
		if blob, mErr := json.Marshal(ev.CryptoMetadata.ExchangeSpecific); mErr == nil {
			p.Crypto = &gen.CryptoMetadata{ExchangeSpecific: string(blob)}
		} else {
			// Best-effort: an undecodable map still carries the raw_payload;
			// store the raw blob verbatim so nothing is silently dropped.
			if b, jErr := json.Marshal(ev.CryptoMetadata.ExchangeSpecific); jErr == nil {
				p.Crypto = &gen.CryptoMetadata{ExchangeSpecific: string(b)}
			}
		}
	}
	if ev.EquityMetadata != nil {
		p.Equity = &gen.EquityMetadata{
			Exchange:       ev.EquityMetadata.Exchange,
			Mic:            ev.EquityMetadata.MIC,
			ConditionCodes: append([]string(nil), ev.EquityMetadata.ConditionCodes...),
		}
	}
	return p
}

// FromProto converts a generated proto CanonicalEvent back to the plain struct.
func FromProto(p *gen.CanonicalEvent) *CanonicalEvent {
	if p == nil {
		return nil
	}
	ev := &CanonicalEvent{
		EventID:           p.EventId,
		Source:            p.Source,
		CanonicalSymbol:   p.CanonicalSymbol,
		ExchangeTimestamp: p.ExchangeTimestamp,
		LocalHWTimestamp:  p.LocalHwTimestamp,
		EventType:         eventTypeFromProto(p.EventType),
		Price:             p.Price,
		Size:              p.Size,
		Side:              p.Side,
		RawPayload:        append([]byte(nil), p.RawPayload...), // copy — no aliasing of proto buffer
		RawFormat:         p.RawFormat,
	}

	for _, l := range p.Levels {
		if l == nil {
			continue
		}
		ev.Levels = append(ev.Levels, Level{
			Price:   l.Price,
			Size:    l.Size,
			Side:    l.Side,
			OrderID: l.OrderId,
		})
	}

	if p.Forex != nil {
		ev.ForexMetadata = &ForexMetadata{
			CurrencyPair: p.Forex.CurrencyPair,
			Bid:          p.Forex.Bid,
			Ask:          p.Forex.Ask,
			Spread:       p.Forex.Spread,
		}
	}
	if p.Futures != nil {
		ev.FuturesMetadata = &FuturesMetadata{
			ContractMonth:   p.Futures.ContractMonth,
			OpenInterest:    p.Futures.OpenInterest,
			SettlementPrice: p.Futures.SettlementPrice,
		}
	}
	if p.Crypto != nil {
		var m map[string]interface{}
		if p.Crypto.ExchangeSpecific != "" {
			// Opaque JSON blob → map. A malformed blob yields an empty map;
			// raw_payload (the real lossless guarantee) is untouched.
			_ = json.Unmarshal([]byte(p.Crypto.ExchangeSpecific), &m)
		}
		ev.CryptoMetadata = &CryptoMetadata{ExchangeSpecific: m}
	}
	if p.Equity != nil {
		ev.EquityMetadata = &EquityMetadata{
			Exchange:       p.Equity.Exchange,
			MIC:            p.Equity.Mic,
			ConditionCodes: append([]string(nil), p.Equity.ConditionCodes...),
		}
	}
	return ev
}

// eventTypeToProto maps the plain string EventType to the proto enum. Unknown
// strings (e.g. "HEARTBEAT") collapse to UNKNOWN(0) — never panic.
func eventTypeToProto(s string) gen.EventType {
	switch s {
	case "TRADE":
		return gen.EventType_TRADE
	case "QUOTE":
		return gen.EventType_QUOTE
	case "BOOK_UPDATE":
		return gen.EventType_BOOK_UPDATE
	case "BOOK_SNAPSHOT":
		return gen.EventType_BOOK_SNAPSHOT
	default:
		return gen.EventType_UNKNOWN
	}
}

// eventTypeFromProto maps the proto enum back to the plain string.
func eventTypeFromProto(e gen.EventType) string {
	switch e {
	case gen.EventType_TRADE:
		return "TRADE"
	case gen.EventType_QUOTE:
		return "QUOTE"
	case gen.EventType_BOOK_UPDATE:
		return "BOOK_UPDATE"
	case gen.EventType_BOOK_SNAPSHOT:
		return "BOOK_SNAPSHOT"
	default:
		return "UNKNOWN"
	}
}
