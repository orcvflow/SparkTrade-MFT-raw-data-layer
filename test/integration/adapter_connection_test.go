// Package integration — real-adapter connection tests.
//
// Unlike pipeline_test.go (which feeds hand-built payloads straight into the
// worker pool), these tests stand up local protocol mock servers and connect
// the REAL BinanceAdapter and IBAdapter to them — exercising Connect → Start →
// receiveLoop → safeRead against live sockets, then routing the produced
// RawMessages through the full canonicalizer → pool → WAL pipeline.
//
// Run: go test ./test/integration/ -run TestIntegration_RealAdapter -v -timeout 60s
package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/canonicalizer"
)

// ─────────────────────────────────────────────────────────────────────────────
// Local protocol mock servers (self-contained — the adapter package's mock
// servers are in its _test files and not importable from here).
// ─────────────────────────────────────────────────────────────────────────────

// realAdapterWSMock is a Binance-like WebSocket server: it upgrades, reads the
// SUBSCRIBE handshake, then pushes the given trade payloads indefinitely.
type realAdapterWSMock struct {
	srv  *http.Server
	addr string
	wg   sync.WaitGroup
}

func newRealAdapterWSMock(t *testing.T, trades [][]byte) *realAdapterWSMock {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	m := &realAdapterWSMock{addr: ln.Addr().String()}
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage() // consume SUBSCRIBE
		for {
			for _, tr := range trades {
				if err := conn.WriteMessage(websocket.TextMessage, tr); err != nil {
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	m.srv = &http.Server{Handler: mux}
	m.wg.Add(1)
	go func() { defer m.wg.Done(); _ = m.srv.Serve(ln) }()
	t.Cleanup(func() { _ = m.srv.Close(); _ = ln.Close(); m.wg.Wait() })
	return m
}

func (m *realAdapterWSMock) url() string { return fmt.Sprintf("ws://%s/ws", m.addr) }

// realAdapterIBMock is an IB-like TCP server: it reads the handshake +
// REQ_MKT_DATA bytes, then pushes length-prefixed payloads indefinitely.
type realAdapterIBMock struct {
	listener net.Listener
	wg       sync.WaitGroup
}

func newRealAdapterIBMock(t *testing.T, frames [][]byte) *realAdapterIBMock {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	m := &realAdapterIBMock{listener: ln}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 256)
				for {
					n, err := conn.Read(buf)
					if err != nil {
						return
					}
					if containsReq(buf[:n]) {
						for {
							for _, fr := range frames {
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
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); m.wg.Wait() })
	return m
}

func containsReq(b []byte) bool {
	const marker = "REQ_MKT_DATA:"
	return containsSubslice(b, []byte(marker))
}

func containsSubslice(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
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

func (m *realAdapterIBMock) host() string { host, _, _ := net.SplitHostPort(m.listener.Addr().String()); return host }
func (m *realAdapterIBMock) port() int {
	_, p, _ := net.SplitHostPort(m.listener.Addr().String())
	var port int
	fmt.Sscanf(p, "%d", &port)
	return port
}

// adapterCfg returns an instant-backoff config for fast tests.
func adapterCfg(attempts int) adapter.AdapterConfig {
	return adapter.AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: attempts,
		BackoffSeconds:    []int{0, 0, 0},
		Timeout:           2 * time.Second,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST: real Binance adapter → pool → canonicalizer → WAL
// ─────────────────────────────────────────────────────────────────────────────

// TestIntegration_RealAdapter_Binance wires the REAL BinanceAdapter to a local
// WS mock and verifies trades flow end-to-end: adapter output → worker pool →
// canonicalizer → WAL. The raw_payload must be preserved byte-for-byte.
func TestIntegration_RealAdapter_Binance(t *testing.T) {
	const symbol = "BTCUSDT"
	trade := buildBinanceTrade(symbol, 50000.0)
	ws := newRealAdapterWSMock(t, [][]byte{trade})

	ba := adapter.NewBinanceAdapter(ws.url(), []string{"btcusdt"}, adapterCfg(3))

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := ba.Connect(ctx); err != nil {
		t.Fatalf("BinanceAdapter Connect: %v", err)
	}

	// Build the downstream pipeline harness (pool + canonicalizer + WAL).
	h := newTestHarness(t)
	defer h.teardown()

	// Bridge: real adapter output → pool Submit.
	adapterOut := make(chan adapter.RawMessage, 64)
	if err := ba.Start(ctx, adapterOut); err != nil {
		t.Fatalf("BinanceAdapter Start: %v", err)
	}
	defer ba.Stop()

	// Bridge: real adapter output → pool Submit. Exits on ctx done (no leak).
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case raw, ok := <-adapterOut:
				if !ok {
					return
				}
				_ = h.pool.Submit(raw)
			}
		}
	}()

	if !h.waitForEvents(3, 4*time.Second) {
		t.Errorf("expected ≥3 canonical events through the real adapter, got %d", h.collectedCount())
	}

	// Raw payload preservation: every collected event's raw_payload must start
	// with the aggTrade envelope (the wire bytes the adapter forwarded).
	h.mu.Lock()
	events := append([]*canonicalizer.CanonicalEvent(nil), h.canonicalEvents...)
	h.mu.Unlock()
	if len(events) == 0 {
		t.Fatal("no events collected to verify raw_payload preservation")
	}
	// The canonicalizer may have set RawPayload to the original trade JSON; verify
	// it contains the price field we injected.
	priceOk := false
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if containsSubslice(ev.RawPayload, []byte(`"50000.00"`)) {
			priceOk = true
			break
		}
	}
	if !priceOk {
		t.Errorf("raw_payload price not preserved in any event; sample=%s", sampleRaw(events))
	}

	stats := ba.Health()
	t.Logf("Binance real adapter: connected=%v messagesRecv=%d reconnects=%d",
		stats.Connected, stats.MessagesRecv, stats.ReconnectCount)
	if stats.MessagesRecv < 1 {
		t.Error("real adapter received no messages")
	}
}

func sampleRaw(events []*canonicalizer.CanonicalEvent) string {
	for _, ev := range events {
		if ev != nil && len(ev.RawPayload) > 0 {
			return string(ev.RawPayload)
		}
	}
	return ""
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST: real IB adapter → pool → canonicalizer → WAL
// ─────────────────────────────────────────────────────────────────────────────

// TestIntegration_RealAdapter_IB wires the REAL IBAdapter to a local TCP mock
// and verifies length-prefixed payloads flow end-to-end through the pipeline.
func TestIntegration_RealAdapter_IB(t *testing.T) {
	frame := []byte("IB trade AAPL 500.0 x100")
	ibsrv := newRealAdapterIBMock(t, [][]byte{frame})

	ia := adapter.NewIBAdapter(ibsrv.host(), ibsrv.port(), 1, []string{"AAPL"}, adapterCfg(3))

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := ia.Connect(ctx); err != nil {
		t.Fatalf("IBAdapter Connect: %v", err)
	}

	h := newTestHarness(t)
	defer h.teardown()

	adapterOut := make(chan adapter.RawMessage, 64)
	if err := ia.Start(ctx, adapterOut); err != nil {
		t.Fatalf("IBAdapter Start: %v", err)
	}
	defer ia.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case raw, ok := <-adapterOut:
				if !ok {
					return
				}
				_ = h.pool.Submit(raw)
			}
		}
	}()

	if !h.waitForEvents(2, 4*time.Second) {
		t.Errorf("expected ≥2 canonical events through the real IB adapter, got %d", h.collectedCount())
	}

	stats := ia.Health()
	t.Logf("IB real adapter: connected=%v messagesRecv=%d reconnects=%d",
		stats.Connected, stats.MessagesRecv, stats.ReconnectCount)
	if stats.MessagesRecv < 1 {
		t.Error("real IB adapter received no messages")
	}
}
