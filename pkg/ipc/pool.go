package ipc

import "sync"

// maxPooledBuf caps the capacity of buffers returned to the pool. Buffers that
// grew beyond this are dropped to GC instead of pooled. This defends against
// the documented sync.Pool bloat-trap: under high QPS, a few oversized packets
// can make every pooled buffer huge, retaining megabytes that never get reused
// while still pinning memory (V2EX t/956136, bytedance/sonic #614,
// golang/go #23199). Drop oversized; pool the common case.
const maxPooledBuf = 16 * 1024 // 16 KiB

var bufPool = sync.Pool{
	New: func() any { b := make([]byte, 0, 1024); return b },
}

// GetBuf returns a zero-length buffer (cap ≥ 1 KiB) from the pool.
func GetBuf() []byte { return bufPool.Get().([]byte)[:0] }

// PutBuf returns a buffer to the pool. Buffers with cap > maxPooledBuf are
// intentionally not pooled (they go to GC) to prevent unbounded retention.
func PutBuf(b []byte) {
	if b == nil {
		return
	}
	if cap(b) > maxPooledBuf {
		return // let GC reclaim oversized buffers
	}
	bufPool.Put(b[:0])
}
