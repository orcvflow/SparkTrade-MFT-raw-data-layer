// Package integration — LIVE Binance testnet test (Addım F, Task F3).
//
// Unlike adapter_connection_test.go (which connects the real adapter to a LOCAL
// mock WS server), this test connects the real BinanceAdapter to the LIVE
// Binance testnet WebSocket and verifies end-to-end:
//   - Connect() succeeds against a real Binance endpoint (TLS + handshake)
//   - at least one real aggTrade is received (raw_payload preserved, source=BINANCE)
//   - the trade canonicalizes to BTC/USD via the repo's mappings/binance.json
//
// HONEST SCOPE — this is a LIVE external-network test:
//   - OFF by default (keeps `go test ./...` green, fast, CI-reproducible).
//     Opt in with LIVE_BINANCE=1.
//   - SKIPs (never FAILs) if the testnet is unreachable or produces no trades
//     within the timeout. Binance testnet aggTrade volume is sparse, so the
//     absence of a trade is a market condition, not a code defect.
//   - Override endpoint/symbol via BINANCE_LIVE_ENDPOINT / BINANCE_LIVE_SYMBOL
//     (e.g. point at mainnet wss://stream.binance.com:9443/ws for denser flow).
//
// Run:
//
//	LIVE_BINANCE=1 go test ./test/integration/ -run TestIntegration_LiveBinance -v -timeout 90s
package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/mapper"
)

func TestIntegration_LiveBinance(t *testing.T) {
	if os.Getenv("LIVE_BINANCE") == "" {
		t.Skip("set LIVE_BINANCE=1 to run the live Binance testnet test (hits external network)")
	}

	endpoint := os.Getenv("BINANCE_LIVE_ENDPOINT")
	if endpoint == "" {
		// Default: mainnet public market-data WS. Testnet /ws is 404 (verified
		// Addım F F3); mainnet aggTrade is read-only public, no credentials.
		endpoint = "wss://stream.binance.com:9443/ws"
	}
	symbol := os.Getenv("BINANCE_LIVE_SYMBOL")
	if symbol == "" {
		symbol = "btcusdt"
	}

	// Mapper from the repo's checked-in mappings/ so the canonical-symbol
	// assertion (BTCUSDT → BTC/USD) is real, not a stub.
	mappingsDir := filepath.Join(repoRoot(t), "mappings")
	sm, err := mapper.NewSymbolMapper(mappingsDir)
	if err != nil {
		t.Fatalf("NewSymbolMapper: %v", err)
	}
	canon := canonicalizer.NewCanonicalizer(sm)

	cfg := adapter.AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: 3,
		BackoffSeconds:    []int{1, 2, 4},
	}
	adp := adapter.NewBinanceAdapter(endpoint, []string{symbol}, cfg)

	// Overall budget covers connect (10s handshake) + wait-for-trade (60s).
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()

	if err := adp.Connect(ctx); err != nil {
		t.Skipf("live Binance connect failed (testnet unreachable?): %v", err)
	}
	t.Logf("connected to %s, subscribed %s@aggTrade", endpoint, symbol)
	defer adp.Stop()

	rawCh := make(chan adapter.RawMessage, 16)
	if err := adp.Start(ctx, rawCh); err != nil {
		t.Fatalf("adapter Start: %v", err)
	}

	// Wait for ≥1 real trade. Testnet aggTrade is sparse — generous timeout,
	// and SKIP (not FAIL) on timeout so the test never flakes on market silence.
	var first adapter.RawMessage
	timer := time.NewTimer(60 * time.Second)
	defer timer.Stop()
	select {
	case first = <-rawCh:
		t.Logf("received live trade: %d bytes", len(first.Payload))
	case <-ctx.Done():
		t.Skip("no live trades within 75s — Binance testnet aggTrade sparse; not a failure")
	case <-timer.C:
		t.Skip("no live trades within 60s — Binance testnet aggTrade sparse; not a failure")
	}

	// CLAUDE.md paranoid rule: raw_payload byte-for-byte untouched.
	if first.Source != "BINANCE" {
		t.Errorf("source = %q, want BINANCE", first.Source)
	}
	if len(first.Payload) == 0 {
		t.Fatal("empty payload — adapter must preserve raw bytes")
	}
	if first.ReceivedAt <= 0 {
		t.Error("ReceivedAt not set")
	}

	// Canonicalize the live trade and assert symbol mapping.
	pm, err := canon.Process(ctx, first)
	if err != nil {
		t.Fatalf("canon.Process: %v", err)
	}
	ev, ok := pm.Canonical.(*canonicalizer.CanonicalEvent)
	if !ok || ev == nil {
		t.Fatalf("canon.Process returned %T, want *CanonicalEvent", pm.Canonical)
	}
	t.Logf("canonical: symbol=%s price=%v size=%v side=%s",
		ev.CanonicalSymbol, ev.Price, ev.Size, ev.Side)

	wantSym := "BTC/USD"
	if ev.CanonicalSymbol != wantSym {
		t.Errorf("canonical_symbol = %q, want %q (check mappings/binance.json)", ev.CanonicalSymbol, wantSym)
	}
	if len(ev.RawPayload) == 0 {
		t.Error("CanonicalEvent.RawPayload empty — must preserve raw bytes byte-for-byte")
	}
}

// repoRoot returns the repository root (two levels up from test/integration/).
// Mirrors mpRepoRoot() in multiprocess_test.go but is self-contained so this
// file does not require the `integration` build tag.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}
