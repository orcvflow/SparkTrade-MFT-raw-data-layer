package ipc

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

func tmpSock(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "raw.sock")
}

// fastBackoff: first miss instant, then 1s — keeps reconnect snappy without a
// busy-loop while the downstream is down.
func fastBackoff() []int { return []int{0, 1} }

func waitFor(t *testing.T, deadline time.Duration, fn func() bool) bool {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if fn() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// recorder is a thread-safe Handler that captures every received message.
type recorder struct {
	mu   sync.Mutex
	msgs []*IPCMessage
}

func (r *recorder) handle(m *IPCMessage) *IPCMessage {
	r.mu.Lock()
	r.msgs = append(r.msgs, m)
	r.mu.Unlock()
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func (r *recorder) snapshot() []*IPCMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*IPCMessage, len(r.msgs))
	copy(out, r.msgs)
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// Frame tests
// ─────────────────────────────────────────────────────────────────────────────

func TestFrame_ReadWrite(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	payload := []byte("the quick brown fox")
	go func() {
		body, err := Marshal(NewMessage("raw", payload, 7))
		if err != nil {
			t.Errorf("Marshal: %v", err)
			return
		}
		if _, err := WriteFrame(a, body); err != nil {
			t.Errorf("WriteFrame: %v", err)
		}
	}()

	var buf []byte
	n, err := ReadFrame(b, &buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("n=%d len(buf)=%d", n, len(buf))
	}
	m, err := Unmarshal(buf[:n])
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if m.Type != "raw" || string(m.Payload) != string(payload) || m.Seq != 7 {
		t.Errorf("round-trip mismatch: %+v", m)
	}
}

func TestFrame_TooLarge(t *testing.T) {
	huge := make([]byte, maxFrameSize+1)
	// WriteFrame checks size BEFORE any I/O, so a blocking writer is fine.
	if _, err := WriteFrame(&bytes.Buffer{}, huge); err == nil || !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("expected ErrFrameTooLarge, got %v", err)
	}
}

func TestFrame_NilPayloadNoPanic(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := WriteFrame(a, nil); err != nil {
			t.Errorf("WriteFrame(nil): %v", err)
		}
	}()
	var buf []byte
	n, err := ReadFrame(b, &buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 body bytes for nil payload, got %d", n)
	}
	<-done
}

// ─────────────────────────────────────────────────────────────────────────────
// Message / pool tests
// ─────────────────────────────────────────────────────────────────────────────

func TestMessage_MarshalUnmarshal(t *testing.T) {
	src := NewMessage("canonical", []byte{0x01, 0x02, 0x03}, 99)
	body, err := Marshal(src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	PutBuf(body)

	// marshalFresh returns a non-pooled slice.
	fresh, err := marshalFresh(src)
	if err != nil {
		t.Fatalf("marshalFresh: %v", err)
	}
	dst, err := Unmarshal(fresh)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if dst.Type != "canonical" || dst.Seq != 99 || len(dst.Payload) != 3 || dst.Payload[0] != 0x01 {
		t.Errorf("mismatch: %+v", dst)
	}
}

func TestMessage_NilNoPanic(t *testing.T) {
	if _, err := Marshal(nil); err == nil {
		t.Error("Marshal(nil) should error")
	}
	if _, err := marshalFresh(nil); err == nil {
		t.Error("marshalFresh(nil) should error")
	}
	if _, err := Unmarshal(nil); err == nil {
		t.Error("Unmarshal(nil) should error")
	}
	if _, err := Unmarshal([]byte{}); err == nil {
		t.Error("Unmarshal(empty) should error")
	}
}

func TestPool_PutBufOversizedDropped(t *testing.T) {
	// A pooled (small) buffer is recycled; an oversized one is dropped (no panic).
	small := GetBuf()
	small = append(small, "x"...)
	PutBuf(small)

	oversized := make([]byte, maxPooledBuf+1)
	PutBuf(oversized) // must not panic; dropped to GC

	// Re-get should still return a valid (small) buffer.
	b := GetBuf()
	if cap(b) < 1024 {
		t.Errorf("GetBuf cap=%d < 1024", cap(b))
	}
	PutBuf(b)
}

// ─────────────────────────────────────────────────────────────────────────────
// Server tests
// ─────────────────────────────────────────────────────────────────────────────

func TestUDSServer_Start(t *testing.T) {
	sock := tmpSock(t)
	srv, err := Listen(sock, func(m *IPCMessage) *IPCMessage {
		return m // echo
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Stop()

	conn, err := net.DialTimeout("unix", sock, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	msg := NewMessage("raw", []byte("hello"), 42)
	body, err := marshalFresh(msg)
	if err != nil {
		t.Fatalf("marshalFresh: %v", err)
	}
	if _, err := WriteFrame(conn, body); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	var buf []byte
	n, err := ReadFrame(conn, &buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if n == 0 {
		t.Fatal("no echo")
	}
	reply, err := Unmarshal(buf[:n])
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if reply.Type != "raw" || string(reply.Payload) != "hello" || reply.Seq != 42 {
		t.Errorf("echo mismatch: %+v", reply)
	}

	if !waitFor(t, 1*time.Second, func() bool {
		st := srv.Stats()
		return st.Received >= 1 && st.Sent >= 1
	}) {
		t.Errorf("stats not updated: %+v", srv.Stats())
	}
}

func TestUDSServer_StopRemovesSocket(t *testing.T) {
	sock := tmpSock(t)
	srv, err := Listen(sock, func(*IPCMessage) *IPCMessage { return nil })
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if _, err := os.Stat(sock); err != nil {
		t.Fatalf("socket not created: %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Errorf("socket not removed after Stop: %v", err)
	}
}

func TestUDSServer_HandlerPanicRecovered(t *testing.T) {
	sock := tmpSock(t)
	srv, err := Listen(sock, func(m *IPCMessage) *IPCMessage {
		panic("boom")
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Stop()

	conn, err := net.DialTimeout("unix", sock, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	body, _ := marshalFresh(NewMessage("raw", []byte("x"), 1))
	if _, err := WriteFrame(conn, body); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	// The panic must be recovered (error recorded), not crash the process.
	if !waitFor(t, 1*time.Second, func() bool {
		st := srv.Stats()
		return st.Received >= 1 && len(st.Errors) >= 1
	}) {
		t.Errorf("panic not recovered: %+v", srv.Stats())
	}

	// Server must still accept new connections.
	conn2, err := net.DialTimeout("unix", sock, 2*time.Second)
	if err != nil {
		t.Fatalf("server died after handler panic: %v", err)
	}
	conn2.Close()
}

// ─────────────────────────────────────────────────────────────────────────────
// Client tests
// ─────────────────────────────────────────────────────────────────────────────

func TestUDSClient_Send(t *testing.T) {
	sock := tmpSock(t)
	rec := &recorder{}
	srv, err := Listen(sock, rec.handle)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Stop()

	client, err := NewClient(sock, ClientConfig{Backoff: fastBackoff()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Stop()
	client.Start()

	if !waitFor(t, 3*time.Second, client.Connected) {
		t.Fatalf("client did not connect")
	}

	if err := client.Send(NewMessage("canonical", []byte("evt1"), 1)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if !waitFor(t, 3*time.Second, func() bool { return rec.count() >= 1 }) {
		t.Fatalf("server did not receive: %d", rec.count())
	}
	if st := client.Stats(); st.Sent < 1 {
		t.Errorf("client.Sent=%d", st.Sent)
	}
}

func TestUDSClient_SendAfterStopClosed(t *testing.T) {
	sock := tmpSock(t)
	client, err := NewClient(sock, ClientConfig{Backoff: fastBackoff()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.Start()
	if err := client.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := client.Send(NewMessage("raw", []byte("x"), 1)); !errors.Is(err, ErrClientClosed) {
		t.Errorf("expected ErrClientClosed, got %v", err)
	}
}

func TestUDSClient_Reconnect(t *testing.T) {
	sock := tmpSock(t)
	rec := &recorder{}

	srv, err := Listen(sock, rec.handle)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	client, err := NewClient(sock, ClientConfig{Backoff: fastBackoff()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Stop()
	client.Start()

	if !waitFor(t, 3*time.Second, client.Connected) {
		t.Fatalf("client did not connect initially")
	}
	if err := client.Send(NewMessage("canonical", []byte("m1"), 1)); err != nil {
		t.Fatalf("Send m1: %v", err)
	}
	if !waitFor(t, 3*time.Second, func() bool { return rec.count() >= 1 }) {
		t.Fatalf("m1 not received: %d", rec.count())
	}

	// Kill the downstream.
	if err := srv.Stop(); err != nil {
		t.Fatalf("srv.Stop: %v", err)
	}
	if !waitFor(t, 3*time.Second, func() bool { return !client.Connected() }) {
		t.Fatalf("client did not detect the drop: %+v", client.Stats())
	}

	// Send while down → spooled.
	if err := client.Send(NewMessage("canonical", []byte("m2"), 2)); err != nil {
		t.Fatalf("Send m2 (down): %v", err)
	}

	// Revive the downstream on the same path.
	srv2, err := Listen(sock, rec.handle)
	if err != nil {
		t.Fatalf("Listen (revive): %v", err)
	}
	defer srv2.Stop()

	// m2 must be delivered after reconnect (lossless).
	if !waitFor(t, 5*time.Second, func() bool { return rec.count() >= 2 }) {
		t.Fatalf("m2 not delivered after reconnect: %d", rec.count())
	}
	if st := client.Stats(); st.Reconnects < 1 {
		t.Errorf("reconnects=%d", st.Reconnects)
	}
}

// TestUDS_SpoolOnDownstream verifies the lossless + FIFO property: messages
// sent while the downstream is DOWN are all delivered, in order, once the
// downstream returns.
func TestUDS_SpoolOnDownstream(t *testing.T) {
	sock := tmpSock(t)
	rec := &recorder{}

	// Server NOT started yet — the client has nowhere to send.
	client, err := NewClient(sock, ClientConfig{Backoff: fastBackoff()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Stop()
	client.Start()

	// Let the client attempt (and fail) to connect before we flood it.
	time.Sleep(100 * time.Millisecond)

	const N = 10
	for i := 0; i < N; i++ {
		m := NewMessage("canonical", []byte(fmt.Sprintf("evt-%d", i)), uint64(i))
		if err := client.Send(m); err != nil {
			t.Fatalf("Send[%d]: %v", i, err)
		}
	}
	// All N are buffered in the on-disk spool (conn down).
	if st := client.Stats(); st.Spooled != N {
		t.Fatalf("expected Spooled=%d, got %d", N, st.Spooled)
	}

	// Bring the downstream up.
	srv, err := Listen(sock, rec.handle)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Stop()

	// Lossless: every message delivered.
	if !waitFor(t, 5*time.Second, func() bool { return rec.count() >= N }) {
		t.Fatalf("lossless FAILED: delivered %d of %d", rec.count(), N)
	}

	// FIFO: delivered in send order (seq 0..N-1).
	snap := rec.snapshot()
	if len(snap) < N {
		t.Fatalf("snapshot too short: %d", len(snap))
	}
	for i := 0; i < N; i++ {
		if snap[i].Seq != uint64(i) {
			t.Errorf("FIFO violated at pos %d: seq=%d (want %d)", i, snap[i].Seq, i)
			break
		}
	}

	// Spool fully drained after replay.
	if st := client.Stats(); st.SpoolBytes != 0 {
		t.Errorf("spool not drained: %d bytes remain", st.SpoolBytes)
	}
}

// TestUDS_Nonblock_Backpressure verifies Send never blocks (even when the
// spool fills) and returns ErrSpoolFull once the cap is reached.
func TestUDS_Nonblock_Backpressure(t *testing.T) {
	sock := tmpSock(t)
	// No server. Tiny spool so it fills quickly.
	client, err := NewClient(sock, ClientConfig{
		Backoff:       fastBackoff(),
		MaxSpoolBytes: 512,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Stop()
	client.Start()

	time.Sleep(50 * time.Millisecond) // let it fail to connect

	start := time.Now()
	spoolFull := 0
	const N = 200
	for i := 0; i < N; i++ {
		m := NewMessage("raw", make([]byte, 50), uint64(i))
		err := client.Send(m)
		switch {
		case err == nil:
			// appended to spool
		case errors.Is(err, ErrSpoolFull):
			spoolFull++
		default:
			t.Fatalf("Send[%d] unexpected err: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Errorf("Send blocked: %v for %d sends", elapsed, N)
	}
	if spoolFull == 0 {
		t.Errorf("expected some ErrSpoolFull, got 0 (spool=%d bytes)", client.Stats().SpoolBytes)
	}
	t.Logf("non-blocking: %d sends, %d spool-full, elapsed=%v, spool=%d bytes",
		N, spoolFull, elapsed, client.Stats().SpoolBytes)
}
