//go:build integration

// Package integration — Addım C multi-process pipeline tests.
//
// These tests stand up the REAL 4 cmd binaries (adapter/canonicalizer/publisher/
// storage) as isolated child processes over UDS+Protobuf IPC and verify the full
// pipeline end-to-end: raw frame → canonicalizer → publisher → storage → WAL,
// with raw_payload preserved byte-for-byte across every IPC hop. The lossless
// guarantee (on-disk spool + FIFO drain) of pkg/ipc is what makes "kill one
// process, no data lost" a real, testable property here rather than a vacuous
// in-process claim.
//
// Unlike adapter_connection_test.go (which wires the real adapter to an
// in-process pool+canonicalizer+WAL harness), these tests exercise the actual
// process boundaries that Addım C introduced.
//
// Run: go test -tags=integration ./test/integration/ -v -timeout 300s
package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/config"
	"raw-data-layer/pkg/ipc"
	"raw-data-layer/pkg/pipeline"
	"raw-data-layer/pkg/process"
)

// ─────────────────────────────────────────────────────────────────────────────
// Isolated topology for one test: temp sockets, free health ports, temp WAL dir,
// a generated config.yaml, and the 4 built binaries (built once per test binary).
// ─────────────────────────────────────────────────────────────────────────────

type mpSockets struct {
	adapterCanon, canonPublisher, publisherStorage string
}

type mpPorts struct {
	adapter, canon, publisher, storage int
}

type mpEnv struct {
	t        *testing.T
	tmpDir   string
	repoRoot string
	binDir   string
	cfgPath  string
	sockets  mpSockets
	ports    mpPorts
	walDir   string
	ctx      context.Context
	cancel   context.CancelFunc
	procs    []*process.Process
}

// mpBuildOnce builds the 4 cmd binaries once per test-binary run (shared across
// tests in this package). Building 4 binaries costs ~10-15s; we pay it once.
var mpBuildOnce sync.Once
var mpBinDir string
var mpBuildErr error

func mpBuildBinaries(t *testing.T) string {
	mpBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "rdl-mp-bin-*")
		if err != nil {
			mpBuildErr = err
			return
		}
		repo := mpRepoRoot()
		for _, name := range []string{"adapter", "canonicalizer", "publisher", "storage"} {
			out := filepath.Join(dir, name)
			cmd := exec.Command("go", "build", "-o", out, "./cmd/"+name)
			cmd.Dir = repo
			cmd.Env = os.Environ()
			if b, err := cmd.CombinedOutput(); err != nil {
				mpBuildErr = fmt.Errorf("go build %s: %w\n%s", name, err, b)
				return
			}
		}
		mpBinDir = dir
	})
	if mpBuildErr != nil {
		t.Fatalf("build 4 binaries: %v", mpBuildErr)
	}
	return mpBinDir
}

func mpRepoRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	// test/integration -> repo root.
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func mpFreePorts(t *testing.T, n int) []int {
	t.Helper()
	var ports []int
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("free port: %v", err)
		}
		addr := ln.Addr().(*net.TCPAddr)
		_ = ln.Close()
		ports = append(ports, addr.Port)
	}
	return ports
}

// mpNewEnv builds an isolated topology. If binanceEnabled, the adapter process
// will try to connect to binanceEndpoint (a local WS mock for the real-adapter
// test); otherwise both adapters are disabled.
func mpNewEnv(t *testing.T, binanceEnabled bool, binanceEndpoint string) *mpEnv {
	t.Helper()
	tmp := t.TempDir()
	ports := mpFreePorts(t, 4)
	env := &mpEnv{
		t:        t,
		tmpDir:   tmp,
		repoRoot: mpRepoRoot(),
		sockets: mpSockets{
			adapterCanon:     filepath.Join(tmp, "ac.sock"),
			canonPublisher:   filepath.Join(tmp, "cp.sock"),
			publisherStorage: filepath.Join(tmp, "ps.sock"),
		},
		ports:  mpPorts{adapter: ports[0], canon: ports[1], publisher: ports[2], storage: ports[3]},
		walDir: filepath.Join(tmp, "wal"),
	}
	env.ctx, env.cancel = context.WithCancel(context.Background())
	env.binDir = mpBuildBinaries(t)
	env.cfgPath = mpWriteConfig(t, env, binanceEnabled, binanceEndpoint)

	t.Cleanup(func() { env.shutdown() })
	return env
}

// mpWriteConfig generates an isolated config.yaml from DefaultConfig with the
// multi-process topology pointed at temp sockets/ports. Using DefaultConfig +
// yaml.Marshal (rather than a hand-written template) guarantees the yaml keys
// match pkg/config's struct tags exactly.
func mpWriteConfig(t *testing.T, env *mpEnv, binanceEnabled bool, binanceEndpoint string) string {
	t.Helper()
	cfg := config.DefaultConfig()
	mappings := filepath.Join(env.repoRoot, "mappings")

	cfg.MappingsDir = mappings
	cfg.Adapters.Binance.Enabled = binanceEnabled
	if binanceEndpoint != "" {
		cfg.Adapters.Binance.Endpoint = binanceEndpoint
	}
	cfg.Adapters.Binance.Symbols = []string{"btcusdt"}
	cfg.Adapters.Binance.Reconnect.MaxAttempts = 3
	cfg.Adapters.Binance.Reconnect.BackoffSeconds = []int{0, 1, 1} // fast IPC reconnect
	cfg.Adapters.IB.Enabled = false
	cfg.Adapters.IB.Reconnect.BackoffSeconds = []int{0, 1, 1}
	cfg.Adapters.IB.RequestTimeout = "2s"

	// Small pool → fast tests. Autoscale on so workers definitely spawn.
	cfg.WorkerPool.QueueSize = 1000
	cfg.WorkerPool.Autoscale.Enabled = true
	cfg.WorkerPool.Autoscale.MinWorkers = 4
	cfg.WorkerPool.Autoscale.MaxWorkers = 8
	cfg.WorkerPool.Autoscale.HighWaterMark = 0.8
	cfg.WorkerPool.Autoscale.LowWaterMark = 0.2

	// No ZMQ socket, no DolphinDB (WAL-only durable sink).
	cfg.Publisher.Zeromq.Enabled = false
	cfg.Storage.WAL.Directory = env.walDir
	cfg.Storage.DolphinDB.Enabled = false
	cfg.Storage.DolphinDB.Password = "" // disabled; don't duplicate the real secret

	cfg.IPC.AdapterToCanonicalizer = env.sockets.adapterCanon
	cfg.IPC.CanonicalizerToPublisher = env.sockets.canonPublisher
	cfg.IPC.PublisherToStorage = env.sockets.publisherStorage
	cfg.Processes.AdapterHealthPort = env.ports.adapter
	cfg.Processes.CanonicalizerHealthPort = env.ports.canon
	cfg.Processes.PublisherHealthPort = env.ports.publisher
	cfg.Processes.StorageHealthPort = env.ports.storage

	cfg.Logging.Level = "warn"
	cfg.Logging.Format = "text"
	cfg.Logging.Output = "stdout"

	data, err := yaml.Marshal(&cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	p := filepath.Join(env.tmpDir, "config.yaml")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

// mpStart launches one process binary with the shared config + MAPPINGS_DIR env.
func mpStart(t *testing.T, env *mpEnv, name string) *process.Process {
	t.Helper()
	bin := filepath.Join(env.binDir, name)
	port := 0
	switch name {
	case "adapter":
		port = env.ports.adapter
	case "canonicalizer":
		port = env.ports.canon
	case "publisher":
		port = env.ports.publisher
	case "storage":
		port = env.ports.storage
	default:
		t.Fatalf("unknown process %s", name)
	}
	p := process.New(name, bin, "-config", env.cfgPath)
	p.Env = []string{"MAPPINGS_DIR=" + filepath.Join(env.repoRoot, "mappings")}
	p.HealthURL = fmt.Sprintf("http://127.0.0.1:%d/live", port)
	p.HealthTimeout = 2 * time.Second
	p.GracefulTimeout = 4 * time.Second
	if err := p.Start(env.ctx); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	env.procs = append(env.procs, p)
	return p
}

// mpWaitLive polls each named process's /live until it responds 200 (or timeout).
func mpWaitLive(t *testing.T, env *mpEnv, timeout time.Duration, names ...string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 1 * time.Second}
	for time.Now().Before(deadline) {
		allUp := true
		for _, n := range names {
			port := 0
			switch n {
			case "adapter":
				port = env.ports.adapter
			case "canonicalizer":
				port = env.ports.canon
			case "publisher":
				port = env.ports.publisher
			case "storage":
				port = env.ports.storage
			}
			resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/live", port))
			if err != nil {
				allUp = false
				break
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				allUp = false
			}
		}
		if allUp {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Emit diagnostics before failing.
	var diag strings.Builder
	for _, n := range names {
		h := mpProcHealth(env, n)
		fmt.Fprintf(&diag, "[%s running=%v healthy=%v restarts=%d exit=%d err=%s] ", n, h.Running, h.Healthy, h.Restarts, h.LastExitCode, h.Error)
	}
	t.Fatalf("processes not live within %s: %s", timeout, diag.String())
}

func mpProcHealth(env *mpEnv, name string) process.ProcessHealth {
	for _, p := range env.procs {
		if p.Name == name {
			return p.Health()
		}
	}
	return process.ProcessHealth{}
}

// shutdown stops all started processes in reverse order (graceful SIGTERM →
// SIGKILL) and cancels the env context. Registered via t.Cleanup.
func (env *mpEnv) shutdown() {
	env.cancel()
	for i := len(env.procs) - 1; i >= 0; i-- {
		_ = env.procs[i].Stop()
	}
	env.procs = nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Test-side IPC clients (the test plays the adapter role, injecting raw frames).
// ─────────────────────────────────────────────────────────────────────────────

func mpDial(t *testing.T, socketPath string) *ipc.Client {
	t.Helper()
	// Unique spool dir so a stale spool from a prior test can't inject old frames.
	sp := filepath.Join(t.TempDir(), "test-client.spool")
	cl, err := ipc.NewClient(socketPath, ipc.ClientConfig{
		Backoff:       []int{0, 0, 1},
		DialTimeout:   2 * time.Second,
		WriteTimeout:  2 * time.Second,
		SpoolPath:     sp,
		MaxSpoolBytes: 64 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("new client %s: %v", socketPath, err)
	}
	cl.Start()
	return cl
}

func mpSendRaw(t *testing.T, cl *ipc.Client, source string, payload []byte) {
	t.Helper()
	rm := adapter.RawMessage{
		Source:      source,
		Payload:     payload,
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: uint64(time.Now().UnixNano()),
	}
	msg, err := pipeline.EncodeRaw(rm)
	if err != nil {
		t.Fatalf("EncodeRaw: %v", err)
	}
	if err := cl.Send(msg); err != nil {
		t.Fatalf("client Send: %v", err)
	}
}

func mpSendCanonical(t *testing.T, cl *ipc.Client, ev *canonicalizer.CanonicalEvent) {
	t.Helper()
	msg, err := pipeline.EncodeCanonical(ev)
	if err != nil {
		t.Fatalf("EncodeCanonical: %v", err)
	}
	if err := cl.Send(msg); err != nil {
		t.Fatalf("client Send: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WAL oracle: read the canonical events the storage process durably wrote.
// ─────────────────────────────────────────────────────────────────────────────

// mpReadWAL reads every wal_*.jsonl file in the WAL dir and returns the decoded
// canonical events. Returns an empty (non-nil) slice if no WAL file exists yet.
func mpReadWAL(t *testing.T, env *mpEnv) []canonicalizer.CanonicalEvent {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(env.walDir, "wal_*.jsonl"))
	if err != nil {
		t.Fatalf("glob wal: %v", err)
	}
	var out []canonicalizer.CanonicalEvent
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			continue
		}
		s := bufio.NewScanner(fh)
		s.Buffer(make([]byte, 0, 1<<20), 1<<24) // big lines (raw_payload is base64)
		for s.Scan() {
			line := s.Bytes()
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var ev canonicalizer.CanonicalEvent
			if err := json.Unmarshal(line, &ev); err != nil {
				continue // a partial/rotten line never breaks the test
			}
			out = append(out, ev)
		}
		_ = fh.Close()
	}
	if out == nil {
		out = []canonicalizer.CanonicalEvent{}
	}
	return out
}

// mpWaitForRawPayload polls the WAL until an event whose RawPayload equals want
// appears (byte-for-byte), or timeout. Returns the matching event or nil.
func mpWaitForRawPayload(t *testing.T, env *mpEnv, want []byte, timeout time.Duration) *canonicalizer.CanonicalEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events := mpReadWAL(t, env)
		for i := range events {
			if bytes.Equal(events[i].RawPayload, want) {
				ev := events[i]
				return &ev
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// mpWaitForEventCount polls until ≥min events whose RawPayload starts with prefix
// are in the WAL, or timeout.
func mpWaitForEventCount(t *testing.T, env *mpEnv, prefix []byte, min int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events := mpReadWAL(t, env)
		c := 0
		for i := range events {
			if bytes.HasPrefix(events[i].RawPayload, prefix) {
				c++
			}
		}
		if c >= min {
			return c
		}
		time.Sleep(100 * time.Millisecond)
	}
	return -1
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

// TestMultiprocess_ProcessesBoot verifies all 4 binaries boot, bind their UDS
// sockets, and expose a live /health endpoint (adapters disabled, so no external
// connection is attempted). This is the minimum bar: process isolation means each
// binary must stand on its own.
func TestMultiprocess_ProcessesBoot(t *testing.T) {
	env := mpNewEnv(t, false, "")

	mpStart(t, env, "adapter")
	mpStart(t, env, "canonicalizer")
	mpStart(t, env, "publisher")
	mpStart(t, env, "storage")

	mpWaitLive(t, env, 30*time.Second, "adapter", "canonicalizer", "publisher", "storage")

	// Each process reports "ok" or "degraded" (both mean alive). The key property:
	// none panicked, none exited.
	for _, n := range []string{"adapter", "canonicalizer", "publisher", "storage"} {
		h := mpProcHealth(env, n)
		if !h.Running {
			t.Errorf("%s not running after boot: %+v", n, h)
		}
	}
	t.Log("all 4 processes booted and live")
}

// TestMultiprocess_FullPipeline_TestInjected is the core lossless-across-IPC test:
// the test acts as the adapter, injecting one raw Binance trade at the
// adapter→canonicalizer socket, and asserts the event traverses canonicalizer →
// publisher → storage and lands in the WAL with raw_payload byte-for-byte intact
// and price correctly parsed.
func TestMultiprocess_FullPipeline_TestInjected(t *testing.T) {
	env := mpNewEnv(t, false, "")

	mpStart(t, env, "canonicalizer")
	mpStart(t, env, "publisher")
	mpStart(t, env, "storage")
	mpWaitLive(t, env, 30*time.Second, "canonicalizer", "publisher", "storage")

	trade := []byte(`{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":1,"p":"50000.00","q":"0.5","T":1700000000000,"m":false}`)

	cl := mpDial(t, env.sockets.adapterCanon)
	defer cl.Stop()
	mpSendRaw(t, cl, "BINANCE", trade)

	ev := mpWaitForRawPayload(t, env, trade, 15*time.Second)
	if ev == nil {
		t.Fatalf("raw_payload not found in WAL within 15s")
	}
	if !bytes.Equal(ev.RawPayload, trade) {
		t.Errorf("raw_payload byte-for-byte mismatch:\n got %q\nwant %q", ev.RawPayload, trade)
	}
	if ev.Price != 50000.0 {
		t.Errorf("price not canonicalized: got %v want 50000", ev.Price)
	}
	if ev.Size != 0.5 {
		t.Errorf("size not canonicalized: got %v want 0.5", ev.Size)
	}
	if ev.Source != "BINANCE" {
		t.Errorf("source lost across IPC: %q", ev.Source)
	}
	// Symbol mapping: canon.Process calls ToCanonical("BINANCE", "BTCUSDT")
	// (uppercase) and the mapper now collapses source casing via normalizeSource,
	// so the symbol must resolve to the canonical "BTC/USD" (mappings/binance.json).
	// Before the case-collapse fix this returned "UNKNOWN"; this assertion is the
	// end-to-end regression for that bug.
	if ev.CanonicalSymbol != "BTC/USD" {
		t.Errorf("canonical symbol not mapped: got %q want %q", ev.CanonicalSymbol, "BTC/USD")
	}
	t.Logf("FullPipeline: event reached WAL, raw_payload preserved byte-for-byte, price=%v size=%v symbol=%q",
		ev.Price, ev.Size, ev.CanonicalSymbol)
}

// TestMultiprocess_PublisherToStorage_Edge injects a canonical frame directly at
// the storage socket (publisher→storage edge) and verifies it lands in the WAL.
// This isolates the storage process's inbound + WAL write path.
func TestMultiprocess_PublisherToStorage_Edge(t *testing.T) {
	env := mpNewEnv(t, false, "")

	mpStart(t, env, "storage")
	mpWaitLive(t, env, 30*time.Second, "storage")

	marker := []byte("mp-pub2storage-marker")
	ev := &canonicalizer.CanonicalEvent{
		EventID:          "evt_edge_pub2storage",
		Source:           "BINANCE",
		CanonicalSymbol:  "BTC/USD",
		ExchangeTimestamp: 1700000000 * 1e9,
		LocalHWTimestamp:  time.Now().UnixNano(),
		EventType:        "TRADE",
		Price:            12345.67,
		Size:             0.1,
		Side:             "BUY",
		RawPayload:        marker,
		RawFormat:        "JSON",
	}

	cl := mpDial(t, env.sockets.publisherStorage)
	defer cl.Stop()
	mpSendCanonical(t, cl, ev)

	got := mpWaitForRawPayload(t, env, marker, 10*time.Second)
	if got == nil {
		t.Fatalf("canonical frame not found in WAL within 10s")
	}
	if got.Price != 12345.67 {
		t.Errorf("price lost across storage inbound: got %v", got.Price)
	}
	if got.CanonicalSymbol != "BTC/USD" {
		t.Errorf("symbol lost: %q", got.CanonicalSymbol)
	}
	t.Logf("PublisherToStorage: canonical frame reached WAL, price=%v", got.Price)
}

// TestMultiprocess_LosslessMultipleEvents injects 20 distinct raw frames and
// asserts all 20 land in the WAL — the no-loss-under-normal-load property across
// 3 processes and 2 IPC hops.
func TestMultiprocess_LosslessMultipleEvents(t *testing.T) {
	env := mpNewEnv(t, false, "")

	mpStart(t, env, "canonicalizer")
	mpStart(t, env, "publisher")
	mpStart(t, env, "storage")
	mpWaitLive(t, env, 30*time.Second, "canonicalizer", "publisher", "storage")

	const n = 20
	prefix := []byte(`{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":`)
	cl := mpDial(t, env.sockets.adapterCanon)
	defer cl.Stop()
	for i := 0; i < n; i++ {
		trade := []byte(fmt.Sprintf(`{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":%d,"p":"50000.00","q":"0.5","T":1700000000000,"m":false}`, i))
		mpSendRaw(t, cl, "BINANCE", trade)
	}

	got := mpWaitForEventCount(t, env, prefix, n, 20*time.Second)
	if got < n {
		t.Errorf("lossless count: got %d want %d (events lost across IPC)", got, n)
	}
	t.Logf("LosslessMultipleEvents: %d/%d events reached WAL", got, n)
}

// TestMultiprocess_RealAdapter_FourProcesses is the most "real" test: the ACTUAL
// adapter process connects to a local Binance-like WS mock and forwards trades
// through all 4 processes to the WAL. No test-side frame injection — the data
// originates from the real adapter binary.
func TestMultiprocess_RealAdapter_FourProcesses(t *testing.T) {
	trade := []byte(`{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":1,"p":"49999.50","q":"0.25","T":1700000000000,"m":false}`)
	// Start the WS mock FIRST so the adapter connects to a ready endpoint.
	ws := newRealAdapterWSMock(t, [][]byte{trade})

	env := mpNewEnv(t, true, ws.url()) // binance enabled → adapter dials the mock

	mpStart(t, env, "adapter")
	mpStart(t, env, "canonicalizer")
	mpStart(t, env, "publisher")
	mpStart(t, env, "storage")
	mpWaitLive(t, env, 30*time.Second, "adapter", "canonicalizer", "publisher", "storage")

	// The real adapter forwards trades from the mock. Poll the WAL for one whose
	// raw_payload carries the injected price.
	priceMark := []byte(`"49999.50"`)
	deadline := time.Now().Add(25 * time.Second)
	var found *canonicalizer.CanonicalEvent
	for time.Now().Before(deadline) && found == nil {
		events := mpReadWAL(t, env)
		for i := range events {
			if bytes.Contains(events[i].RawPayload, priceMark) {
				ev := events[i]
				found = &ev
				break
			}
		}
		if found == nil {
			time.Sleep(150 * time.Millisecond)
		}
	}
	if found == nil {
		ah := mpProcHealth(env, "adapter")
		t.Fatalf("real-adapter trade did not reach WAL within 25s (adapter running=%v recv via /health component)", ah.Running)
	}
	if !bytes.Contains(found.RawPayload, priceMark) {
		t.Errorf("raw_payload price not preserved: %q", found.RawPayload)
	}
	t.Logf("RealAdapter 4-process: trade reached WAL, raw_payload=%q", found.RawPayload)
}
