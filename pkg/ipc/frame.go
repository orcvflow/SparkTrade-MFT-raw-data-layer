// Package ipc implements the inter-process transport for Addım C: length-prefixed
// Protobuf messages over Unix Domain Sockets, with process isolation in mind.
//
// Design principles (CLAUDE.md §0 Core Design):
//   - Never panic: every function recovers and returns an error.
//   - Never hang: reads/writes are deadline-bound; sends are non-blocking.
//   - Bounded: frames are capped; pooled buffers are size-capped.
//
// Wire format: [4-byte big-endian length][Protobuf-marshaled IPCMessage body].
package ipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// lenFieldSize is the number of bytes used to encode the frame length.
	lenFieldSize = 4
	// maxFrameSize caps a single frame's body. Defends against a malformed or
	// hostile length prefix that would request a multi-GB allocation.
	maxFrameSize = 4 * 1024 * 1024 // 4 MiB
)

// ErrFrameTooLarge is returned when a frame's declared length exceeds maxFrameSize.
var ErrFrameTooLarge = errors.New("ipc: frame too large")

// ReadFrame reads one length-prefixed frame into buf (grown as needed; reused
// across calls when the caller passes the same *[]byte). It never panics.
// Returns the number of body bytes read.
func ReadFrame(r io.Reader, buf *[]byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ipc: panic in ReadFrame: %v", r)
		}
	}()

	var hdr [lenFieldSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, err
	}

	size := binary.BigEndian.Uint32(hdr[:])
	if size == 0 {
		return 0, nil
	}
	if size > maxFrameSize {
		return 0, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, size, maxFrameSize)
	}

	// Reuse buf if it is large enough, otherwise reallocate.
	if cap(*buf) < int(size) {
		*buf = make([]byte, size)
	} else {
		*buf = (*buf)[:size]
	}
	if _, err := io.ReadFull(r, *buf); err != nil {
		return 0, err
	}
	return int(size), nil
}

// WriteFrame writes a length-prefixed frame to w. It never panics.
func WriteFrame(w io.Writer, data []byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ipc: panic in WriteFrame: %v", r)
		}
	}()

	if len(data) > maxFrameSize {
		return 0, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, len(data), maxFrameSize)
	}

	var hdr [lenFieldSize]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))

	if _, err := w.Write(hdr[:]); err != nil {
		return 0, err
	}
	// A zero-length body frame is just the header; skip the second write so
	// we never issue a 0-byte Write (which blocks on some transports like
	// net.Pipe and is a no-op on real sockets anyway).
	if len(data) == 0 {
		return lenFieldSize, nil
	}
	nw, err := w.Write(data)
	return lenFieldSize + nw, err
}
