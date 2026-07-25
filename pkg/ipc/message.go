package ipc

import (
	"errors"
	"fmt"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

// Common message types carried in IPCMessage.Type.
const (
	TypeRaw       = "raw"        // payload = adapter.RawMessage (Phase 4 encoding)
	TypeCanonical = "canonical"  // payload = canonicalizer.CanonicalEvent (Phase 4 encoding)
	TypeControl   = "control"    // payload = control signaling (e.g. flush, shutdown)
)

// Errors returned by (un)marshaling helpers.
var (
	ErrMarshal   = errors.New("ipc: marshal failed")
	ErrUnmarshal = errors.New("ipc: unmarshal failed")
)

// seqGen is a process-local monotonic sequence source. Each process assigns its
// own sequence numbers to frames it originates; receivers use them only for
// gap detection, not for global ordering (each producer has its own counter).
var seqGen atomic.Uint64

// NextSeq returns the next sequence number. Never panics.
func NextSeq() uint64 { return seqGen.Add(1) }

// NewMessage builds an IPCMessage. Never panics; never returns nil.
func NewMessage(typ string, payload []byte, seq uint64) *IPCMessage {
	return &IPCMessage{Type: typ, Payload: payload, Seq: seq}
}

// Marshal serializes an IPCMessage into a pooled buffer. The returned slice
// shares the pooled backing array — the caller MUST PutBuf(out) when finished
// (typically right after a synchronous WriteFrame). On error, the pool buffer
// is recycled internally. Never panics.
func Marshal(m *IPCMessage) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrMarshal, r)
		}
	}()
	if m == nil {
		return nil, fmt.Errorf("%w: nil message", ErrMarshal)
	}
	buf := GetBuf()
	out, err = (proto.MarshalOptions{}).MarshalAppend(buf, m)
	if err != nil {
		PutBuf(buf)
		return nil, fmt.Errorf("%w: %v", ErrMarshal, err)
	}
	return out, nil
}

// Unmarshal deserializes an IPCMessage from data. The returned message is a
// fresh allocation (proto.Unmarshal does not retain data). Never panics.
func Unmarshal(data []byte) (m *IPCMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrUnmarshal, r)
		}
	}()
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty data", ErrUnmarshal)
	}
	m = &IPCMessage{}
	if err = proto.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnmarshal, err)
	}
	return m, nil
}
