package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// ─────────────────────────────────────────────────────────────────────────────
// MOCK BINANCE WEBSOCKET SERVER
// ─────────────────────────────────────────────────────────────────────────────

// mockBinanceServer is a local WebSocket server that emulates Binance's stream
// endpoint: it accepts the SUBSCRIBE handshake, then pushes configurable
// frames (trades, ACKs, pings). It can be killed mid-stream to simulate a
// connection drop.
type mockBinanceServer struct {
	addr     string
	upgrader websocket.Upgrader

	// frames is the ordered list of frames to send after a client subscribes.
	// Each frame is sent once per connected client.
	frames []mockFrame

	// loop, when true, makes the server resend the last frame forever (so the
	// receive loop always has something to read — useful for output-block tests).
	loop bool

	// closed signals when the underlying listener has been shut down.
	closed atomic.Bool

	listener net.Listener
	srv      *http.Server
	wg       sync.WaitGroup
}

type mockFrame struct {
	msgType int // websocket.TextMessage | websocket.PingMessage
	data    []byte
}

func newMockBinanceServer(t *testing.T, frames []mockFrame, loop bool) *mockBinanceServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	m := &mockBinanceServer{
		addr:     ln.Addr().String(),
		listener: ln,
		frames:   frames,
		loop:     loop,
		upgrader: websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", m.handle)
	m.srv = &http.Server{Handler: mux}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		_ = m.srv.Serve(ln)
	}()
	t.Cleanup(m.close)

	return m
}

func (m *mockBinanceServer) url() string { return fmt.Sprintf("ws://%s/ws", m.addr) }

func (m *mockBinanceServer) handle(w http.ResponseWriter, r *http.Request) {
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Read the SUBSCRIBE handshake (ignore content; if the client sends one).
	_, _, _ = conn.ReadMessage()

	// Send configured frames.
	for i, f := range m.frames {
		if err := conn.WriteMessage(f.msgType, f.data); err != nil {
			return
		}
		// If looping, re-send the last frame until the client goes away.
		if m.loop && i == len(m.frames)-1 {
			for {
				if err := conn.WriteMessage(f.msgType, f.data); err != nil {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}

	// Keep the connection open (block reading) so the client doesn't see a
	// read error and spuriously reconnect after consuming the frames. The read
	// returns when the client closes the socket (Stop / test cleanup).
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (m *mockBinanceServer) close() {
	if m.closed.Swap(true) {
		return
	}
	_ = m.srv.Close()
	_ = m.listener.Close()
	m.wg.Wait()
}

// binanceTestConfig returns an AdapterConfig with instant backoff so reconnect
// tests don't waste wall-clock on real exponential delays.
func binanceTestConfig(attempts int) AdapterConfig {
	return AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: attempts,
		BackoffSeconds:    []int{0, 0, 0},
		Timeout:           2 * time.Second,
	}
}

func binanceTradePayload(symbol string, price float64) []byte {
	return []byte(fmt.Sprintf(`{"e":"aggTrade","s":"%s","p":"%v","q":"1.0","T":1700000000000}`, symbol, price))
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECT
// ─────────────────────────────────────────────────────────────────────────────

// TestBinanceNet_Connect_OK verifies a real WebSocket connect+subscribe against
// a mock server: the adapter ends up connected with no error.
func TestBinanceNet_Connect_OK(t *testing.T) {
	srv := newMockBinanceServer(t, nil, false)
	b := NewBinanceAdapter(srv.url(), []string{"btcusdt"}, binanceTestConfig(1))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	if !b.connected.Load() {
		t.Error("expected connected=true after Connect")
	}
	_ = b.Stop()
}

// TestBinanceNet_Connect_AlreadyConnected covers the fast-path early return
// when the adapter is already connected.
func TestBinanceNet_Connect_AlreadyConnected(t *testing.T) {
	srv := newMockBinanceServer(t, nil, false)
	b := NewBinanceAdapter(srv.url(), []string{"btcusdt"}, binanceTestConfig(1))
	b.connected.Store(true)

	if err := b.Connect(context.Background()); err != nil {
		t.Fatalf("Connect on already-connected should be no-op, got: %v", err)
	}
	_ = b.Stop()
}

// TestBinanceNet_Connect_DialFail covers the connection-error branch: the server
// is down, so DialContext fails and Connect returns an AdapterError.
func TestBinanceNet_Connect_DialFail(t *testing.T) {
	srv := newMockBinanceServer(t, nil, false)
	srv.close() // kill server before connecting

	b := NewBinanceAdapter(srv.url(), []string{"btcusdt"}, binanceTestConfig(1))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := b.Connect(ctx)
	if err == nil {
		t.Fatal("expected Connect to fail against dead server")
	}
	if ae, ok := err.(*AdapterError); !ok || ae.Type != ErrorConnection {
		t.Errorf("expected ErrorConnection, got %v", err)
	}
}

// TestBinanceNet_Connect_NoSymbols covers the subscribe() error branch (empty
// symbols) — Connect must close the socket and surface the protocol error.
func TestBinanceNet_Connect_NoSymbols(t *testing.T) {
	srv := newMockBinanceServer(t, nil, false)
	b := NewBinanceAdapter(srv.url(), []string{}, binanceTestConfig(1))

	err := b.Connect(context.Background())
	if err == nil {
		t.Fatal("expected Connect to fail with no symbols")
	}
	if ae, ok := err.(*AdapterError); !ok || ae.Type != ErrorProtocol {
		t.Errorf("expected ErrorProtocol, got %v", err)
	}
	if b.connected.Load() {
		t.Error("expected connected=false after subscribe failure")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// START / RECEIVE LOOP / SAFE READ
// ─────────────────────────────────────────────────────────────────────────────

// TestBinanceNet_Start_ReceivesTrade verifies the full live path: Connect →
// Start → receiveLoop → safeRead delivers a real trade to the output channel
// with the payload byte-for-byte preserved and the ACK frame filtered out.
func TestBinanceNet_Start_ReceivesTrade(t *testing.T) {
	frames := []mockFrame{
		{websocket.TextMessage, []byte(`{"result":null,"id":1}`)}, // ACK (filtered)
		{websocket.TextMessage, binanceTradePayload("BTCUSDT", 50000.0)},
	}
	srv := newMockBinanceServer(t, frames, false)
	b := NewBinanceAdapter(srv.url(), []string{"btcusdt"}, binanceTestConfig(1))
	defer b.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	out := make(chan RawMessage, 8)
	if err := b.Start(ctx, out); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case msg := <-out:
		if msg.Source != "BINANCE" {
			t.Errorf("expected source BINANCE, got %s", msg.Source)
		}
		// Payload must be the trade JSON (the ACK must NOT have been delivered).
		var parsed map[string]interface{}
		if err := json.Unmarshal(msg.Payload, &parsed); err != nil {
			t.Fatalf("payload not valid JSON: %v (payload=%s)", err, string(msg.Payload))
		}
		if parsed["e"] != "aggTrade" {
			t.Errorf("expected aggTrade event, got %v (payload=%s)", parsed["e"], string(msg.Payload))
		}
		if b.messagesRecv.Load() < 1 {
			t.Errorf("expected messagesRecv>=1, got %d", b.messagesRecv.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for trade message")
	}
}

// TestBinanceNet_SafeRead_NonTextSkipped verifies that non-text frames (e.g. a
// Ping) are silently skipped: safeRead returns (nil,nil) and nothing reaches
// the output channel.
func TestBinanceNet_SafeRead_NonTextSkipped(t *testing.T) {
	frames := []mockFrame{
		{websocket.PingMessage, []byte{}},
		{websocket.TextMessage, binanceTradePayload("BTCUSDT", 1.0)},
	}
	srv := newMockBinanceServer(t, frames, false)
	b := NewBinanceAdapter(srv.url(), []string{"btcusdt"}, binanceTestConfig(1))
	defer b.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	out := make(chan RawMessage, 8)
	_ = b.Start(ctx, out)

	select {
	case msg := <-out:
		// Must be the trade, not the ping.
		var parsed map[string]interface{}
		if err := json.Unmarshal(msg.Payload, &parsed); err != nil {
			t.Fatalf("expected trade payload, got: %s", string(msg.Payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout — ping should have been skipped, trade delivered")
	}
}

// TestBinanceNet_SafeRead_NoConnection covers the nil-conn branch of safeRead.
func TestBinanceNet_SafeRead_NoConnection(t *testing.T) {
	b := NewBinanceAdapter("ws://127.0.0.1:1/ws", []string{"btcusdt"}, binanceTestConfig(1))
	msg, err := b.safeRead()
	if msg != nil {
		t.Errorf("expected nil msg, got %+v", msg)
	}
	if err == nil {
		t.Error("expected error when no connection")
	}
	if b.connected.Load() {
		t.Error("expected connected=false after a nil-conn read")
	}
}

// TestBinanceNet_ReceiveLoop_ContextCancel verifies the receive loop exits
// cleanly when the context is cancelled (no goroutine leak, no panic).
func TestBinanceNet_ReceiveLoop_ContextCancel(t *testing.T) {
	srv := newMockBinanceServer(t, []mockFrame{
		{websocket.TextMessage, binanceTradePayload("BTCUSDT", 1.0)},
	}, true)
	b := NewBinanceAdapter(srv.url(), []string{"btcusdt"}, binanceTestConfig(1))
	defer b.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	out := make(chan RawMessage, 8)
	_ = b.Start(ctx, out)

	// Drain a message to confirm it's alive, then cancel.
	select {
	case <-out:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first message")
	}
	cancel()
	// Give the loop time to observe cancellation.
	time.Sleep(150 * time.Millisecond)
	if b.running.Load() {
		t.Error("expected running=false after context cancel")
	}
}

// TestBinanceNet_OutputBlock covers the blocked-output branch: with a tiny
// output buffer and no reader, a delivered message times out and is recorded
// as an "output channel blocked" error.
func TestBinanceNet_OutputBlock(t *testing.T) {
	srv := newMockBinanceServer(t, []mockFrame{
		{websocket.TextMessage, binanceTradePayload("BTCUSDT", 1.0)},
	}, true)
	b := NewBinanceAdapter(srv.url(), []string{"btcusdt"}, binanceTestConfig(1))
	defer b.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// Buffered size 1, no reader: the 2nd delivered message blocks the send.
	out := make(chan RawMessage, 1)
	_ = b.Start(ctx, out)

	// Wait long enough for the 1s blocked-output timeout to fire.
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		n := len(b.errors)
		b.mu.RUnlock()
		if n > 0 {
			return // blocked-output error recorded
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("expected an 'output channel blocked' error after 1s")
}

// ─────────────────────────────────────────────────────────────────────────────
// RECONNECT
// ─────────────────────────────────────────────────────────────────────────────

// TestBinanceNet_Reconnect_AlreadyConnected covers the success-return path of
// reconnect: when Connect reports already-connected, reconnect counts the
// reconnect and returns immediately.
func TestBinanceNet_Reconnect_AlreadyConnected(t *testing.T) {
	srv := newMockBinanceServer(t, nil, false)
	b := NewBinanceAdapter(srv.url(), []string{"btcusdt"}, binanceTestConfig(3))
	defer b.Stop()

	b.connected.Store(true) // simulate an already-good link
	b.reconnect(context.Background())

	if b.reconnectCount.Load() != 1 {
		t.Errorf("expected reconnectCount=1, got %d", b.reconnectCount.Load())
	}
}

// TestBinanceNet_Reconnect_FailExhausted covers the failure path: server down,
// all attempts fail, "max reconnect attempts reached" is recorded.
func TestBinanceNet_Reconnect_FailExhausted(t *testing.T) {
	srv := newMockBinanceServer(t, nil, false)
	srv.close() // no server

	b := NewBinanceAdapter(srv.url(), []string{"btcusdt"}, binanceTestConfig(2))
	defer b.Stop()

	b.reconnect(context.Background())

	b.mu.RLock()
	errs := len(b.errors)
	b.mu.RUnlock()
	if errs == 0 {
		t.Error("expected reconnect errors after all attempts failed")
	}
	if b.reconnectCount.Load() != 0 {
		t.Errorf("expected 0 successful reconnects, got %d", b.reconnectCount.Load())
	}
}

// TestBinanceNet_Reconnect_ContextCancel covers the ctx.Done() branch: when
// the context is already cancelled, reconnect returns without retrying.
func TestBinanceNet_Reconnect_ContextCancel(t *testing.T) {
	srv := newMockBinanceServer(t, nil, false)
	defer srv.close()
	b := NewBinanceAdapter(srv.url(), []string{"btcusdt"}, binanceTestConfig(5))
	defer b.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	b.reconnect(ctx)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("reconnect with cancelled ctx should return immediately, took %v", elapsed)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HEARTBEAT + SESSION ROTATION (loop exit paths)
// ─────────────────────────────────────────────────────────────────────────────

// TestBinanceNet_HeartbeatLoop_Exit verifies the heartbeat goroutine exits on
// context cancellation.
func TestBinanceNet_HeartbeatLoop_Exit(t *testing.T) {
	b := NewBinanceAdapter("ws://127.0.0.1:1/ws", []string{"btcusdt"}, binanceTestConfig(1))
	defer b.Stop()

	done := make(chan struct{})
	go func() {
		b.heartbeatLoop()
		close(done)
	}()
	// Cancel and expect the loop to exit promptly.
	b.cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("heartbeatLoop did not exit after context cancel")
	}
}

// TestBinanceNet_SessionRotationLoop_Exit verifies the session-rotation
// goroutine exits on context cancellation.
func TestBinanceNet_SessionRotationLoop_Exit(t *testing.T) {
	b := NewBinanceAdapter("ws://127.0.0.1:1/ws", []string{"btcusdt"}, binanceTestConfig(1))
	defer b.Stop()

	done := make(chan struct{})
	go func() {
		b.sessionRotationLoop()
		close(done)
	}()
	b.cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("sessionRotationLoop did not exit after context cancel")
	}
}

// TestBinanceNet_HeartbeatLoop_NoConnection covers the "not connected →
// continue" branch when the ticker fires but the adapter is disconnected.
func TestBinanceNet_HeartbeatLoop_NoConnection(t *testing.T) {
	b := NewBinanceAdapter("ws://127.0.0.1:1/ws", []string{"btcusdt"}, binanceTestConfig(1))
	// Make the heartbeat fire immediately so the connected=false branch runs.
	b.heartbeatInterval = 10 * time.Millisecond

	done := make(chan struct{})
	go func() {
		b.heartbeatLoop()
		close(done)
	}()
	// Let at least one tick fire while disconnected.
	time.Sleep(80 * time.Millisecond)
	b.cancel()
	<-done
	// No errors should have been added (the not-connected branch just continues).
	b.mu.RLock()
	errs := len(b.errors)
	b.mu.RUnlock()
	if errs != 0 {
		t.Errorf("expected no errors during disconnected heartbeat, got %d", errs)
	}
}
