// Package pipeline is the glue layer for the multi-process split (Addım C):
// it encodes/decodes domain types (adapter.RawMessage, canonicalizer.CanonicalEvent)
// to/from the opaque payload of an ipc.IPCMessage. This keeps pkg/ipc a pure
// transport (no domain dependencies) while giving the 4 process binaries a
// single, typed encode/decode seam instead of re-implementing the mapping.
//
// Invariants (CLAUDE.md):
//   - Never panic (defer/recover on every entry point).
//   - raw_payload preserved byte-for-byte (decoded bytes are copied, not aliased
//     to the proto buffer).
package pipeline

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/ipc"
)

// EncodeRaw wraps an adapter.RawMessage in an IPCMessage (type="raw"). The raw
// Payload is serialized byte-for-byte into the RawFrame. Never panics.
func EncodeRaw(rm adapter.RawMessage) (m *ipc.IPCMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pipeline: panic in EncodeRaw: %v", r)
		}
	}()
	rf := &ipc.RawFrame{
		Source:      rm.Source,
		Payload:     rm.Payload, // proto.Marshal serializes (copies); no aliasing
		ReceivedAt:  rm.ReceivedAt,
		SequenceNum: rm.SequenceNum,
	}
	data, err := proto.Marshal(rf)
	if err != nil {
		return nil, fmt.Errorf("pipeline: encode raw: %w", err)
	}
	return ipc.NewMessage(ipc.TypeRaw, data, ipc.NextSeq()), nil
}

// DecodeRaw unwraps a type="raw" IPCMessage back into an adapter.RawMessage.
// The Payload is copied so callers may mutate rm.Payload freely. Never panics.
func DecodeRaw(m *ipc.IPCMessage) (adapter.RawMessage, error) {
	if m == nil {
		return adapter.RawMessage{}, fmt.Errorf("pipeline: decode raw: nil message")
	}
	rf := &ipc.RawFrame{}
	if err := proto.Unmarshal(m.Payload, rf); err != nil {
		return adapter.RawMessage{}, fmt.Errorf("pipeline: decode raw: %w", err)
	}
	return adapter.RawMessage{
		Source:      rf.Source,
		Payload:     append([]byte(nil), rf.Payload...), // copy — no proto-buffer aliasing
		ReceivedAt:  rf.ReceivedAt,
		SequenceNum: rf.SequenceNum,
	}, nil
}

// EncodeCanonical wraps a CanonicalEvent in an IPCMessage (type="canonical")
// using the protobuf bridge in pkg/canonicalizer. Never panics.
func EncodeCanonical(ev *canonicalizer.CanonicalEvent) (m *ipc.IPCMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pipeline: panic in EncodeCanonical: %v", r)
		}
	}()
	data, err := canonicalizer.MarshalProto(ev)
	if err != nil {
		return nil, fmt.Errorf("pipeline: encode canonical: %w", err)
	}
	return ipc.NewMessage(ipc.TypeCanonical, data, ipc.NextSeq()), nil
}

// DecodeCanonical unwraps a type="canonical" IPCMessage into a CanonicalEvent.
// Never panics.
func DecodeCanonical(m *ipc.IPCMessage) (*canonicalizer.CanonicalEvent, error) {
	if m == nil {
		return nil, fmt.Errorf("pipeline: decode canonical: nil message")
	}
	ev, err := canonicalizer.UnmarshalProto(m.Payload)
	if err != nil {
		return nil, fmt.Errorf("pipeline: decode canonical: %w", err)
	}
	return ev, nil
}
