package adapter

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// MOCK IB GATEWAY TCP SERVER
// ─────────────────────────────────────────────────────────────────────────────

// mockIBServer is a local TCP server that emulates the simplified IB handshake
// used by IBAdapter: it reads the "v100..clientID\0" handshake and per-symbol
// "REQ_MKT_DATA:symbol\0" requests, then writes length-prefixed messages.
type mockIBServer struct {
	addr     string
	listener net.Listener

	// frames: ordered length-prefixed payloads to send after subscription.
	frames [][]byte
	// loop: keep resending the last frame until the client disconnects.
	loop bool

	// subscribed records the symbols the client requested.
	mu         sync.Mutex
	subscribed []string

	closed atomic.Bool
	wg      sync.WaitGroup
}

func newMockIBServer(t *testing.T, frames [][]byte, loop bool) *mockIBServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	m := &mockIBServer{
		addr:     ln.Addr().String(),
		listener: ln,
		frames:   frames,
		loop:     loop,
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go m.handle(conn)
		}
	}()
	t.Cleanup(m.close)
	return m
}

func (m *mockIBServer) host() string  { host, _, _ := net.SplitHostPort(m.addr); return host }
func (m *mockIBServer) port() int     { _, p, _ := net.SplitHostPort(m.addr); var port int; fmt.Sscanf(p, "%d", &port); return port }
func (m *mockIBServer) close() {
	if m.closed.Swap(true) {
		return
	}
	_ = m.listener.Close()
	m.wg.Wait()
}

func (m *mockIBServer) handle(conn net.Conn) {
	defer conn.Close()
	// Read until \0 for handshake, then per-symbol requests until EOF.
	buf := make([]byte, 256)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		// Record any REQ_MKT_DATA symbols seen.
		chunk := string(buf[:n])
		if sym := parseReqSymbol(chunk); sym != "" {
			m.mu.Lock()
			m.subscribed = append(m.subscribed, sym)
			m.mu.Unlock()
		}
		// Once we've seen at least one subscription request, start pushing frames.
		_ = chunk
		// Send frames if we have them and the client subscribed.
		m.mu.Lock()
		hasSub := len(m.subscribed) > 0
		m.mu.Unlock()
		if hasSub {
			for i, fr := range m.frames {
				if !writeFrame(conn, fr) {
					return
				}
				if m.loop && i == len(m.frames)-1 {
					for {
						if !writeFrame(conn, fr) {
							return
						}
						time.Sleep(5 * time.Millisecond)
					}
				}
			}
			// Keep the connection open so the client doesn't see a read error
			// and spuriously reconnect after consuming the frames. Block until
			// the client closes the socket (Stop / test cleanup).
			io.Copy(io.Discard, conn)
			return
		}
	}
}

// parseReqSymbol extracts the symbol from a "REQ_MKT_DATA:SYMBOL\0" chunk.
func parseReqSymbol(s string) string {
	const marker = "REQ_MKT_DATA:"
	idx := indexOf(s, marker)
	if idx < 0 {
		return ""
	}
	rest := s[idx+len(marker):]
	// up to next null or end.
	for i, c := range rest {
		if c == 0 {
			return rest[:i]
		}
	}
	return rest
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// writeFrame writes a 4-byte big-endian length prefix + payload.
func writeFrame(conn net.Conn, payload []byte) bool {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if _, err := conn.Write(header); err != nil {
		return false
	}
	if _, err := conn.Write(payload); err != nil {
		return false
	}
	return true
}

func ibTestConfig(attempts int) AdapterConfig {
	return AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: attempts,
		BackoffSeconds:    []int{0, 0, 0},
		Timeout:           2 * time.Second,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECT / HANDSHAKE / SUBSCRIBE
// ─────────────────────────────────────────────────────────────────────────────

// TestIBNet_Connect_OK verifies a real TCP connect + handshake + subscribe
// against a mock IB server.
func TestIBNet_Connect_OK(t *testing.T) {
	srv := newMockIBServer(t, nil, false)
	ib := NewIBAdapter(srv.host(), srv.port(), 1, []string{"AAPL", "MSFT"}, ibTestConfig(1))
	defer ib.Stop()

	if err := ib.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	if !ib.connected.Load() {
		t.Error("expected connected=true after Connect")
	}
	// The server records subscriptions asynchronously as it reads the client's
	// REQ_MKT_DATA writes; poll briefly for them.
	deadline := time.Now().Add(1 * time.Second)
	var subs int
	for time.Now().Before(deadline) {
		srv.mu.Lock()
		subs = len(srv.subscribed)
		srv.mu.Unlock()
		if subs >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if subs < 1 {
		t.Errorf("expected server to record ≥1 subscription, got %d", subs)
	}
}

// TestIBNet_Connect_AlreadyConnected covers the fast-path return.
func TestIBNet_Connect_AlreadyConnected(t *testing.T) {
	srv := newMockIBServer(t, nil, false)
	ib := NewIBAdapter(srv.host(), srv.port(), 1, []string{"AAPL"}, ibTestConfig(1))
	defer ib.Stop()
	ib.connected.Store(true)

	if err := ib.Connect(context.Background()); err != nil {
		t.Fatalf("Connect on already-connected should be no-op, got: %v", err)
	}
}

// TestIBNet_Connect_DialFail covers the connection-error branch.
func TestIBNet_Connect_DialFail(t *testing.T) {
	srv := newMockIBServer(t, nil, false)
	srv.close() // nothing to connect to

	ib := NewIBAdapter(srv.host(), srv.port(), 1, []string{"AAPL"}, ibTestConfig(1))
	defer ib.Stop()
	err := ib.Connect(context.Background())
	if err == nil {
		t.Fatal("expected Connect to fail against dead server")
	}
	if ae, ok := err.(*AdapterError); !ok || ae.Type != ErrorConnection {
		t.Errorf("expected ErrorConnection, got %v", err)
	}
}

// TestIBNet_Connect_NoSymbols covers the subscribeSymbols protocol-error branch.
func TestIBNet_Connect_NoSymbols(t *testing.T) {
	srv := newMockIBServer(t, nil, false)
	ib := NewIBAdapter(srv.host(), srv.port(), 1, []string{}, ibTestConfig(1))
	defer ib.Stop()

	err := ib.Connect(context.Background())
	if err == nil {
		t.Fatal("expected Connect to fail with no symbols")
	}
	if ae, ok := err.(*AdapterError); !ok || ae.Type != ErrorProtocol {
		t.Errorf("expected ErrorProtocol, got %v", err)
	}
}

// TestIBNet_sendHandshake_NoConnection covers the nil-conn branch.
func TestIBNet_sendHandshake_NoConnection(t *testing.T) {
	ib := NewIBAdapter("127.0.0.1", 1, 1, []string{"AAPL"}, ibTestConfig(1))
	defer ib.Stop()
	if err := ib.sendHandshake(); err == nil {
		t.Error("expected handshake error with no connection")
	}
}

// TestIBNet_subscribeSymbol_NoConnection covers the nil-conn branch.
func TestIBNet_subscribeSymbol_NoConnection(t *testing.T) {
	ib := NewIBAdapter("127.0.0.1", 1, 1, []string{"AAPL"}, ibTestConfig(1))
	defer ib.Stop()
	if err := ib.subscribeSymbol("AAPL"); err == nil {
		t.Error("expected subscribe error with no connection")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// START / RECEIVE LOOP / SAFE READ
// ─────────────────────────────────────────────────────────────────────────────

// TestIBNet_Start_ReceivesMessage verifies the full live path: Connect → Start
// → receiveLoop → safeRead delivers a length-prefixed payload intact.
func TestIBNet_Start_ReceivesMessage(t *testing.T) {
	payload := []byte("AAPL trade 500.0 x100")
	srv := newMockIBServer(t, [][]byte{payload}, false)
	ib := NewIBAdapter(srv.host(), srv.port(), 1, []string{"AAPL"}, ibTestConfig(1))
	defer ib.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := ib.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	out := make(chan RawMessage, 8)
	if err := ib.Start(ctx, out); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case msg := <-out:
		if msg.Source != "IB" {
			t.Errorf("expected source IB, got %s", msg.Source)
		}
		if string(msg.Payload) != string(payload) {
			t.Errorf("payload mismatch: got %q want %q", string(msg.Payload), string(payload))
		}
		if ib.messagesRecv.Load() < 1 {
			t.Errorf("expected messagesRecv>=1, got %d", ib.messagesRecv.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for IB message")
	}
}

// TestIBNet_SafeRead_Oversize covers the message-too-large protocol-error
// branch: a >1MB length prefix must be rejected without allocating a buffer.
func TestIBNet_SafeRead_Oversize(t *testing.T) {
	srv := newOversizeServer(t)
	defer srv.close()
	ib := NewIBAdapter(srv.host(), srv.port(), 1, []string{"AAPL"}, ibTestConfig(1))
	defer ib.Stop()

	if err := ib.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	out := make(chan RawMessage, 8)
	_ = ib.Start(context.Background(), out)

	// safeRead should reject the oversized message and set connected=false; the
	// loop then attempts reconnect (which fails — oversize server is one-shot).
	// We assert: no oversized payload delivered, no panic, error recorded.
	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		ib.mu.RLock()
		n := len(ib.errors)
		ib.mu.RUnlock()
		if n > 0 {
			return // protocol error recorded
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Error("expected a protocol error for the oversized message")
}

// oversizeServer accepts a connection, reads the handshake/subscription bytes,
// then writes a single 4-byte length prefix of 0x100000 (>1MB) and closes —
// forcing IBAdapter.safeRead down the message-too-large branch.
type oversizeServer struct {
	mockIBServer
}

func newOversizeServer(t *testing.T) *oversizeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	o := &oversizeServer{mockIBServer{addr: ln.Addr().String(), listener: ln}}
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			// Read the handshake + subscribe bytes (small), then send bad frame.
			buf := make([]byte, 256)
			_, _ = conn.Read(buf)
			// 4-byte big-endian length = 2MB (> 1MB cap).
			hdr := make([]byte, 4)
			binary.BigEndian.PutUint32(hdr, 2*1024*1024)
			_, _ = conn.Write(hdr)
			// Write nothing else — safeRead rejects on the length alone.
		}()
	}()
	return o
}

// TestIBNet_SafeRead_NoConnection covers the nil-conn branch.
func TestIBNet_SafeRead_NoConnection(t *testing.T) {
	ib := NewIBAdapter("127.0.0.1", 1, 1, []string{"AAPL"}, ibTestConfig(1))
	defer ib.Stop()
	msg, err := ib.safeRead()
	if msg != nil {
		t.Errorf("expected nil msg, got %+v", msg)
	}
	if err == nil {
		t.Error("expected error with no connection")
	}
	if ib.connected.Load() {
		t.Error("expected connected=false after nil-conn read")
	}
}

// TestIBNet_ReceiveLoop_ContextCancel verifies the receive loop exits on
// context cancellation without leaking or panicking.
func TestIBNet_ReceiveLoop_ContextCancel(t *testing.T) {
	srv := newMockIBServer(t, [][]byte{[]byte("AAPL trade")}, true)
	ib := NewIBAdapter(srv.host(), srv.port(), 1, []string{"AAPL"}, ibTestConfig(1))
	defer ib.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	if err := ib.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	out := make(chan RawMessage, 8)
	_ = ib.Start(ctx, out)

	select {
	case <-out:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first message")
	}
	cancel()
	time.Sleep(150 * time.Millisecond)
	if ib.running.Load() {
		t.Error("expected running=false after context cancel")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RECONNECT
// ─────────────────────────────────────────────────────────────────────────────

// TestIBNet_Reconnect_AlreadyConnected covers the success-return path.
func TestIBNet_Reconnect_AlreadyConnected(t *testing.T) {
	srv := newMockIBServer(t, nil, false)
	ib := NewIBAdapter(srv.host(), srv.port(), 1, []string{"AAPL"}, ibTestConfig(3))
	defer ib.Stop()
	ib.connected.Store(true)

	ib.reconnect(context.Background())
	if ib.reconnectCount.Load() != 1 {
		t.Errorf("expected reconnectCount=1, got %d", ib.reconnectCount.Load())
	}
}

// TestIBNet_Reconnect_FailExhausted covers the failure path.
func TestIBNet_Reconnect_FailExhausted(t *testing.T) {
	srv := newMockIBServer(t, nil, false)
	srv.close()

	ib := NewIBAdapter(srv.host(), srv.port(), 1, []string{"AAPL"}, ibTestConfig(2))
	defer ib.Stop()
	ib.reconnect(context.Background())

	ib.mu.RLock()
	errs := len(ib.errors)
	ib.mu.RUnlock()
	if errs == 0 {
		t.Error("expected reconnect errors after all attempts failed")
	}
}

// TestIBNet_Reconnect_ContextCancel covers the ctx.Done() branch.
func TestIBNet_Reconnect_ContextCancel(t *testing.T) {
	srv := newMockIBServer(t, nil, false)
	defer srv.close()
	ib := NewIBAdapter(srv.host(), srv.port(), 1, []string{"AAPL"}, ibTestConfig(5))
	defer ib.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	ib.reconnect(ctx)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("reconnect with cancelled ctx should return immediately, took %v", elapsed)
	}
}
