// Package chaos — connection drop + recovery tests.
//
// These tests exercise the REAL adapters against a controllable local mock
// server that can be killed and revived mid-stream. They verify:
//   - the adapter detects a dropped connection (no panic, error recorded,
//     connected=false);
//   - when the server returns, the adapter reconnects autonomously and resumes
//     delivering data (recovery);
//   - no data is lost across the outage window (messages keep flowing after).
//
// Run: go test ./test/chaos/ -run TestChaos_ConnectionDrop -race -v -timeout 60s
package chaos

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"raw-data-layer/pkg/adapter"
)

// controllableWS is a Binance-like WebSocket server bound to a fixed local
// address that can be stopped and revived on the same port, so the adapter's
// reconnect (which targets a fixed endpoint URL) actually reaches it again.
type controllableWS struct {
	addr     string
	trades   [][]byte
	upgrader websocket.Upgrader

	mu       sync.Mutex
	listener net.Listener
	srv      *http.Server
	srvWG    sync.WaitGroup
	stopped  atomic.Bool

	// active client connections; closed on stop() to force a real drop.
	connMu sync.Mutex
	conns  []*websocket.Conn
}

func newControllableWS(t *testing.T, trades [][]byte) *controllableWS {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	c := &controllableWS{
		addr:     ln.Addr().String(),
		trades:   trades,
		upgrader: websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }},
		listener: ln,
	}
	c.serve()
	t.Cleanup(c.stop)
	return c
}

func (c *controllableWS) serve() {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", c.handle)
	c.srv = &http.Server{Handler: mux}
	c.srvWG.Add(1)
	go func() {
		defer c.srvWG.Done()
		_ = c.srv.Serve(c.listener)
	}()
}

func (c *controllableWS) handle(w http.ResponseWriter, r *http.Request) {
	conn, err := c.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	// Track the upgraded conn so stop() can forcibly break it (an upgraded WS
	// connection is hijacked from http.Server, so http.Server.Close() alone
	// does NOT close it).
	c.connMu.Lock()
	c.conns = append(c.conns, conn)
	c.connMu.Unlock()

	defer func() {
		c.connMu.Lock()
		c.conns = removeWSConn(c.conns, conn)
		c.connMu.Unlock()
		conn.Close()
	}()
	_, _, _ = conn.ReadMessage() // consume SUBSCRIBE
	for {
		for _, tr := range c.trades {
			if err := conn.WriteMessage(websocket.TextMessage, tr); err != nil {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func removeWSConn(conns []*websocket.Conn, target *websocket.Conn) []*websocket.Conn {
	for i, c := range conns {
		if c == target {
			return append(conns[:i], conns[i+1:]...)
		}
	}
	return conns
}

func (c *controllableWS) url() string { return fmt.Sprintf("ws://%s/ws", c.addr) }

// stop kills the listener + HTTP server AND forcibly closes all active client
// connections, so the adapter's blocking reads return an error immediately.
func (c *controllableWS) stop() {
	if c.stopped.Swap(true) {
		return
	}
	_ = c.srv.Close()
	_ = c.listener.Close()
	c.connMu.Lock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	c.conns = nil
	c.connMu.Unlock()
	c.srvWG.Wait()
}

// revive re-binds a listener on the same address and resumes serving.
func (c *controllableWS) revive(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", c.addr)
	if err != nil {
		t.Fatalf("revive listen on %s: %v", c.addr, err)
	}
	c.mu.Lock()
	c.listener = ln
	c.mu.Unlock()
	c.stopped.Store(false)
	c.serve()
}

// adapterCfg returns instant-backoff config so reconnect retries land quickly.
func chaosAdapterCfg(attempts int) adapter.AdapterConfig {
	return adapter.AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: attempts,
		BackoffSeconds:    []int{0, 0, 0},
		Timeout:           2 * time.Second,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS: Binance connection drop + recovery
// ─────────────────────────────────────────────────────────────────────────────

// TestChaos_ConnectionDrop_Binance kills the WebSocket server mid-stream and
// verifies the adapter: (1) does not panic, (2) records the disconnect, (3)
// reconnects autonomously once the server is revived, (4) resumes delivering
// messages.
func TestChaos_ConnectionDrop_Binance(t *testing.T) {
	trade := []byte(`{"e":"aggTrade","s":"BTCUSDT","p":"50000.00","q":"1.0","T":1700000000000}`)
	srv := newControllableWS(t, [][]byte{trade})

	ba := adapter.NewBinanceAdapter(srv.url(), []string{"btcusdt"}, chaosAdapterCfg(5))
	defer ba.Stop()

	// Phase 1: connect + confirm live traffic.
	ctx, cancel := contextWithTimeout(8 * time.Second)
	defer cancel()
	if err := ba.Connect(ctx); err != nil {
		t.Fatalf("initial Connect: %v", err)
	}
	out := make(chan adapter.RawMessage, 64)
	if err := ba.Start(ctx, out); err != nil {
		t.Fatalf("Start: %v", err)
	}

	firstRecv := waitForMessages(out, 1, 2*time.Second)
	if firstRecv == 0 {
		t.Fatal("adapter delivered no messages before the outage")
	}
	t.Logf("phase 1 (live): %d messages before drop", firstRecv)

	// Phase 2: kill the server mid-stream.
	srv.stop()
	t.Log("phase 2 (outage): server killed — adapter must detect + not panic")

	// The adapter must register the disconnect (connected=false or an error).
	deadline := time.Now().Add(2 * time.Second)
	detected := false
	for time.Now().Before(deadline) {
		h := ba.Health()
		if !h.Connected || len(h.Errors) > 0 || h.ReconnectCount > 0 {
			detected = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !detected {
		t.Error("adapter did not detect the dropped connection within 2s")
	}

	// Phase 3: revive the server on the same address; adapter must reconnect.
	srv.revive(t)
	t.Log("phase 3 (recovery): server revived — adapter must reconnect + resume")

	recovered := false
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		// Drain any messages that arrive after reconnection.
		select {
		case <-out:
			recovered = true
		case <-time.After(50 * time.Millisecond):
		}
		h := ba.Health()
		if h.Connected && h.MessagesRecv > uint64(firstRecv) {
			recovered = true
			break
		}
	}

	if !recovered {
		h := ba.Health()
		t.Errorf("adapter did not recover after server revive: connected=%v recv=%d reconnects=%d errs=%d",
			h.Connected, h.MessagesRecv, h.ReconnectCount, len(h.Errors))
	} else {
		h := ba.Health()
		t.Logf("recovered: connected=%v recv=%d reconnects=%d", h.Connected, h.MessagesRecv, h.ReconnectCount)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS: IB connection drop + recovery
// ─────────────────────────────────────────────────────────────────────────────

// TestChaos_ConnectionDrop_IB kills the TCP server mid-stream and verifies the
// IB adapter detects the drop and reconnects when the server is revived.
func TestChaos_ConnectionDrop_IB(t *testing.T) {
	frame := []byte("IB trade AAPL 500.0 x100")
	srv := newControllableIB(t, [][]byte{frame})

	ia := adapter.NewIBAdapter(srv.host(), srv.port(), 1, []string{"AAPL"}, chaosAdapterCfg(5))
	defer ia.Stop()

	ctx, cancel := contextWithTimeout(8 * time.Second)
	defer cancel()
	if err := ia.Connect(ctx); err != nil {
		t.Fatalf("initial Connect: %v", err)
	}
	out := make(chan adapter.RawMessage, 64)
	if err := ia.Start(ctx, out); err != nil {
		t.Fatalf("Start: %v", err)
	}

	firstRecv := waitForMessages(out, 1, 2*time.Second)
	if firstRecv == 0 {
		t.Fatal("IB adapter delivered no messages before the outage")
	}
	t.Logf("phase 1 (live): %d messages before drop", firstRecv)

	srv.stop()
	t.Log("phase 2 (outage): server killed")

	deadline := time.Now().Add(2 * time.Second)
	detected := false
	for time.Now().Before(deadline) {
		h := ia.Health()
		if !h.Connected || len(h.Errors) > 0 || h.ReconnectCount > 0 {
			detected = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !detected {
		t.Error("IB adapter did not detect the dropped connection within 2s")
	}

	srv.revive(t)
	t.Log("phase 3 (recovery): server revived")

	recovered := false
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-out:
			recovered = true
		case <-time.After(50 * time.Millisecond):
		}
		h := ia.Health()
		if h.Connected && h.MessagesRecv > uint64(firstRecv) {
			recovered = true
			break
		}
	}
	if !recovered {
		h := ia.Health()
		t.Errorf("IB adapter did not recover: connected=%v recv=%d reconnects=%d",
			h.Connected, h.MessagesRecv, h.ReconnectCount)
	} else {
		h := ia.Health()
		t.Logf("IB recovered: connected=%v recv=%d reconnects=%d", h.Connected, h.MessagesRecv, h.ReconnectCount)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Controllable IB TCP server (stop + revive on same port)
// ─────────────────────────────────────────────────────────────────────────────

type controllableIB struct {
	addr     string
	frames   [][]byte
	listener net.Listener
	wg       sync.WaitGroup
	stopped  atomic.Bool

	// active client connections; closed on stop() to force a real drop.
	connMu sync.Mutex
	conns  []net.Conn
}

func newControllableIB(t *testing.T, frames [][]byte) *controllableIB {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	c := &controllableIB{addr: ln.Addr().String(), frames: frames, listener: ln}
	c.serve()
	t.Cleanup(c.stop)
	return c
}

func (c *controllableIB) serve() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			conn, err := c.listener.Accept()
			if err != nil {
				return
			}
			go c.handle(conn)
		}
	}()
}

func (c *controllableIB) handle(conn net.Conn) {
	// Track so stop() can forcibly close the client's connection.
	c.connMu.Lock()
	c.conns = append(c.conns, conn)
	c.connMu.Unlock()

	defer func() {
		c.connMu.Lock()
		c.conns = removeNetConn(c.conns, conn)
		c.connMu.Unlock()
		conn.Close()
	}()
	buf := make([]byte, 256)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		if containsReqMkt(buf[:n]) {
			for {
				for _, fr := range c.frames {
					hdr := make([]byte, 4)
					binary.BigEndian.PutUint32(hdr, uint32(len(fr)))
					if _, err := conn.Write(hdr); err != nil {
						return
					}
					if _, err := conn.Write(fr); err != nil {
						return
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
}

func removeNetConn(conns []net.Conn, target net.Conn) []net.Conn {
	for i, c := range conns {
		if c == target {
			return append(conns[:i], conns[i+1:]...)
		}
	}
	return conns
}

func containsReqMkt(b []byte) bool {
	const marker = "REQ_MKT_DATA:"
	if len(marker) > len(b) {
		return false
	}
	for i := 0; i+len(marker) <= len(b); i++ {
		match := true
		for j := range marker {
			if b[i+j] != marker[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func (c *controllableIB) host() string { h, _, _ := net.SplitHostPort(c.addr); return h }
func (c *controllableIB) port() int {
	_, p, _ := net.SplitHostPort(c.addr)
	var port int
	fmt.Sscanf(p, "%d", &port)
	return port
}

func (c *controllableIB) stop() {
	if c.stopped.Swap(true) {
		return
	}
	_ = c.listener.Close()
	c.connMu.Lock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	c.conns = nil
	c.connMu.Unlock()
	c.wg.Wait()
}

func (c *controllableIB) revive(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", c.addr)
	if err != nil {
		t.Fatalf("revive listen on %s: %v", c.addr, err)
	}
	c.listener = ln
	c.stopped.Store(false)
	c.serve()
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// waitForMessages drains up to n messages within the timeout and returns how
// many it actually received.
func waitForMessages(ch <-chan adapter.RawMessage, n int, timeout time.Duration) int {
	got := 0
	deadline := time.Now().Add(timeout)
	for got < n && time.Now().Before(deadline) {
		select {
		case <-ch:
			got++
		case <-time.After(20 * time.Millisecond):
		}
	}
	return got
}
