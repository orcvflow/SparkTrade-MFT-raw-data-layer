//go:build chaos

// Package chaos — Addım C multi-process chaos tests.
//
// These tests kill real pipeline processes mid-stream and verify the Homalos
// invariant: one crash does NOT take down the others, the supervisor
// (pkg/process, mirroring systemd Restart=always) auto-restarts the crashed
// process, the lossless on-disk IPC spool holds everything during the outage,
// and on restart the spool drains (FIFO) so no data is lost.
//
// Run: go test -tags=chaos ./test/chaos/ -run TestMultiprocessChaos -v -timeout 300s
// (Add -race to race-check the test harness + test-side IPC clients. The child
// binaries are production builds — not race-instrumented; see ADDIM_C_PHASES.md.)
package chaos

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
	"syscall"
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
// Isolated topology (mirrors test/integration, mpC-prefixed to avoid clashing
// with the existing chaos_test.go/connection_drop_test.go helpers).
// ─────────────────────────────────────────────────────────────────────────────

type mpCSockets struct {
	adapterCanon, canonPublisher, publisherStorage string
}
type mpCPorts struct {
	adapter, canon, publisher, storage int
}

type mpCEnv struct {
	t        *testing.T
	tmpDir   string
	repoRoot string
	binDir   string
	cfgPath  string
	sockets  mpCSockets
	ports    mpCPorts
	walDir   string
	ctx      context.Context
	cancel   context.CancelFunc
	procs    []*process.Process
}

var mpCBuildOnce sync.Once
var mpCBinDir string
var mpCBuildErr error

func mpCBuildBinaries(t *testing.T) string {
	mpCBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "rdl-mpc-bin-*")
		if err != nil {
			mpCBuildErr = err
			return
		}
		repo := mpCRepoRoot()
		for _, name := range []string{"adapter", "canonicalizer", "publisher", "storage"} {
			out := filepath.Join(dir, name)
			cmd := exec.Command("go", "build", "-o", out, "./cmd/"+name)
			cmd.Dir = repo
			cmd.Env = os.Environ()
			if b, err := cmd.CombinedOutput(); err != nil {
				mpCBuildErr = fmt.Errorf("go build %s: %w\n%s", name, err, b)
				return
			}
		}
		mpCBinDir = dir
	})
	if mpCBuildErr != nil {
		t.Fatalf("build binaries: %v", mpCBuildErr)
	}
	return mpCBinDir
}

func mpCRepoRoot() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func mpCFreePorts(t *testing.T, n int) []int {
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

func mpCNewEnv(t *testing.T) *mpCEnv {
	t.Helper()
	tmp := t.TempDir()
	ports := mpCFreePorts(t, 4)
	env := &mpCEnv{
		t:        t,
		tmpDir:   tmp,
		repoRoot: mpCRepoRoot(),
		sockets: mpCSockets{
			adapterCanon:     filepath.Join(tmp, "ac.sock"),
			canonPublisher:   filepath.Join(tmp, "cp.sock"),
			publisherStorage: filepath.Join(tmp, "ps.sock"),
		},
		ports:  mpCPorts{adapter: ports[0], canon: ports[1], publisher: ports[2], storage: ports[3]},
		walDir: filepath.Join(tmp, "wal"),
	}
	env.ctx, env.cancel = context.WithCancel(context.Background())
	env.binDir = mpCBuildBinaries(t)
	env.cfgPath = mpCWriteConfig(t, env)
	t.Cleanup(func() { env.shutdown() })
	return env
}

func mpCWriteConfig(t *testing.T, env *mpCEnv) string {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.MappingsDir = filepath.Join(env.repoRoot, "mappings")
	cfg.Adapters.Binance.Enabled = false
	cfg.Adapters.Binance.Symbols = []string{"btcusdt"}
	cfg.Adapters.Binance.Reconnect.MaxAttempts = 3
	cfg.Adapters.Binance.Reconnect.BackoffSeconds = []int{0, 0, 1} // very fast IPC reconnect
	cfg.Adapters.IB.Enabled = false
	cfg.Adapters.IB.Reconnect.BackoffSeconds = []int{0, 0, 1}
	cfg.Adapters.IB.RequestTimeout = "2s"
	cfg.WorkerPool.QueueSize = 1000
	cfg.WorkerPool.Autoscale.Enabled = true
	cfg.WorkerPool.Autoscale.MinWorkers = 4
	cfg.WorkerPool.Autoscale.MaxWorkers = 8
	cfg.WorkerPool.Autoscale.HighWaterMark = 0.8
	cfg.WorkerPool.Autoscale.LowWaterMark = 0.2
	cfg.Publisher.Zeromq.Enabled = false
	cfg.Storage.WAL.Directory = env.walDir
	cfg.Storage.DolphinDB.Enabled = false
	cfg.Storage.DolphinDB.Password = ""
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

// mpCStart launches one process with a SHORT auto-restart backoff so chaos
// recovery lands within ~1s (mirrors systemd Restart=always at test speed).
func mpCStart(t *testing.T, env *mpCEnv, name string) *process.Process {
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
	p.GracefulTimeout = 3 * time.Second
	// MaxRetries=0 → unlimited (Restart=always). Short backoff for fast recovery.
	p.Policy = process.RestartPolicy{
		MaxRetries: 0,
		Backoff:    []time.Duration{300 * time.Millisecond, 500 * time.Millisecond, 1 * time.Second, 2 * time.Second},
	}
	if err := p.Start(env.ctx); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	env.procs = append(env.procs, p)
	return p
}

func mpCPort(env *mpCEnv, name string) int {
	switch name {
	case "adapter":
		return env.ports.adapter
	case "canonicalizer":
		return env.ports.canon
	case "publisher":
		return env.ports.publisher
	case "storage":
		return env.ports.storage
	}
	return 0
}

func mpCWaitLive(t *testing.T, env *mpCEnv, timeout time.Duration, names ...string) {
	t.Helper()
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allUp := true
		for _, n := range names {
			resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/live", mpCPort(env, n)))
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
	var diag strings.Builder
	for _, n := range names {
		h := mpCProcHealth(env, n)
		fmt.Fprintf(&diag, "[%s running=%v restarts=%d exit=%d err=%s] ", n, h.Running, h.Restarts, h.LastExitCode, h.Error)
	}
	t.Fatalf("processes not live within %s: %s", timeout, diag.String())
}

// mpCStillLive asserts the named processes stay live (respond /live 200) for the
// whole duration — i.e. a peer's crash did not take them down.
func mpCStillLive(t *testing.T, env *mpCEnv, dur time.Duration, names ...string) {
	t.Helper()
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		for _, n := range names {
			resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/live", mpCPort(env, n)))
			if err != nil {
				t.Errorf("%s went down during peer's outage (Homalos invariant broken): %v", n, err)
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("%s not live during peer's outage: status %d", n, resp.StatusCode)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func mpCProcHealth(env *mpCEnv, name string) process.ProcessHealth {
	for _, p := range env.procs {
		if p.Name == name {
			return p.Health()
		}
	}
	return process.ProcessHealth{}
}

func mpCProc(env *mpCEnv, name string) *process.Process {
	for _, p := range env.procs {
		if p.Name == name {
			return p
		}
	}
	return nil
}

func (env *mpCEnv) shutdown() {
	env.cancel()
	for i := len(env.procs) - 1; i >= 0; i-- {
		_ = env.procs[i].Stop()
	}
	env.procs = nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Crash + restart helpers.
// ─────────────────────────────────────────────────────────────────────────────

// mpCCrashKill sends SIGKILL to the child PID WITHOUT going through Stop, so the
// supervisor treats it as a crash and auto-restarts it (mirroring systemd
// Restart=always on an unexpected exit).
func mpCCrashKill(t *testing.T, p *process.Process) {
	t.Helper()
	pid := p.PID()
	if pid == 0 {
		t.Fatalf("crash: %s has no PID (not running?)", p.Name)
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("crash kill %s (pid %d): %v", p.Name, pid, err)
	}
}

// mpCWaitRestarted confirms the crash registered (Running went false) then waits
// for the monitor to restart the child (Running true again, Restarts increased).
func mpCWaitRestarted(t *testing.T, p *process.Process, baseRestarts int32, timeout time.Duration) {
	t.Helper()
	downDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(downDeadline) {
		if !p.Running() {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	upDeadline := time.Now().Add(timeout)
	for time.Now().Before(upDeadline) {
		if p.Running() && p.Restarts() > baseRestarts {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s did not auto-restart within %s (running=%v restarts=%d base=%d)",
		p.Name, timeout, p.Running(), p.Restarts(), baseRestarts)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test-side IPC client + WAL oracle.
// ─────────────────────────────────────────────────────────────────────────────

func mpCDial(t *testing.T, socketPath string) *ipc.Client {
	t.Helper()
	sp := filepath.Join(t.TempDir(), "test-client.spool")
	cl, err := ipc.NewClient(socketPath, ipc.ClientConfig{
		Backoff:       []int{0, 0, 0, 1},
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

func mpCSendRaw(t *testing.T, cl *ipc.Client, source string, payload []byte) {
	t.Helper()
	rm := adapter.RawMessage{Source: source, Payload: payload, ReceivedAt: time.Now().UnixNano(), SequenceNum: uint64(time.Now().UnixNano())}
	msg, err := pipeline.EncodeRaw(rm)
	if err != nil {
		t.Fatalf("EncodeRaw: %v", err)
	}
	if err := cl.Send(msg); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func mpCReadWAL(t *testing.T, env *mpCEnv) []canonicalizer.CanonicalEvent {
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
		s.Buffer(make([]byte, 0, 1<<20), 1<<24)
		for s.Scan() {
			line := s.Bytes()
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var ev canonicalizer.CanonicalEvent
			if err := json.Unmarshal(line, &ev); err != nil {
				continue
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

// mpCWaitForMarkerCount polls until ≥min events whose RawPayload contains marker
// are in the WAL, or timeout. Returns the count (or -1 on timeout).
func mpCWaitForMarkerCount(t *testing.T, env *mpCEnv, marker []byte, min int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events := mpCReadWAL(t, env)
		c := 0
		for i := range events {
			if bytes.Contains(events[i].RawPayload, marker) {
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

func mpCBinanceTrade(q string, a int) []byte {
	return []byte(fmt.Sprintf(`{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":%d,"p":"50000.00","q":"%s","T":1700000000000,"m":false}`, a, q))
}

// ─────────────────────────────────────────────────────────────────────────────
// CHAOS TESTS
// ─────────────────────────────────────────────────────────────────────────────

// TestMultiprocessChaos_StorageKill_AutoRestartAndLossless kills the storage
// process mid-stream. The publisher's outClient detects the EOF and spools the
// inbound events to disk (lossless). The supervisor auto-restarts storage; the
// publisher reconnects and drains its spool (FIFO) so every event injected DURING
// the outage lands in the WAL. This is the real, testable "kill storage, no data
// lost" property — only possible because the spool lives in a separate process.
func TestMultiprocessChaos_StorageKill_AutoRestartAndLossless(t *testing.T) {
	env := mpCNewEnv(t)
	mpCStart(t, env, "canonicalizer")
	mpCStart(t, env, "publisher")
	mpCStart(t, env, "storage")
	mpCWaitLive(t, env, 30*time.Second, "canonicalizer", "publisher", "storage")

	const baselineN = 3
	const outageN = 5
	baselineMark := []byte(`"q":"0.5"`)
	outageMark := []byte(`"q":"0.777"`)

	cl := mpCDial(t, env.sockets.adapterCanon)
	defer cl.Stop()
	for i := 0; i < baselineN; i++ {
		mpCSendRaw(t, cl, "BINANCE", mpCBinanceTrade("0.5", i))
	}
	if c := mpCWaitForMarkerCount(t, env, baselineMark, baselineN, 15*time.Second); c < baselineN {
		t.Fatalf("baseline: only %d/%d events reached WAL before kill", c, baselineN)
	}

	storage := mpCProc(env, "storage")
	baseRestarts := storage.Restarts()
	t.Logf("killing storage (pid=%d, baseline restarts=%d) — publisher must spool", storage.PID(), baseRestarts)
	mpCCrashKill(t, storage)

	// Peers must stay alive (Homalos invariant).
	mpCStillLive(t, env, 2*time.Second, "canonicalizer", "publisher")

	// Inject DURING the outage — these must be spooled on the publisher.
	for i := 0; i < outageN; i++ {
		mpCSendRaw(t, cl, "BINANCE", mpCBinanceTrade("0.777", 100+i))
	}

	// Storage auto-restarts; publisher reconnects + drains the spool.
	mpCWaitRestarted(t, storage, baseRestarts, 15*time.Second)
	got := mpCWaitForMarkerCount(t, env, outageMark, outageN, 25*time.Second)
	if got < outageN {
		t.Errorf("LOSSLESS FAILURE: only %d/%d outage events reached WAL after storage restart (spool did not drain)", got, outageN)
	} else {
		t.Logf("lossless: %d/%d outage events drained from publisher spool after storage restart (restarts=%d)", got, outageN, storage.Restarts())
	}
}

// TestMultiprocessChaos_PublisherKill_AutoRestart kills the publisher. The
// canonicalizer's outClient spools inbound events; on publisher restart the spool
// drains and events flow to storage. Storage stays live throughout.
func TestMultiprocessChaos_PublisherKill_AutoRestart(t *testing.T) {
	env := mpCNewEnv(t)
	mpCStart(t, env, "canonicalizer")
	mpCStart(t, env, "publisher")
	mpCStart(t, env, "storage")
	mpCWaitLive(t, env, 30*time.Second, "canonicalizer", "publisher", "storage")

	outageMark := []byte(`"q":"0.999"`)
	cl := mpCDial(t, env.sockets.adapterCanon)
	defer cl.Stop()
	// baseline to confirm the chain works
	mpCSendRaw(t, cl, "BINANCE", mpCBinanceTrade("0.5", 0))
	mpCWaitForMarkerCount(t, env, []byte(`"q":"0.5"`), 1, 15*time.Second)

	pub := mpCProc(env, "publisher")
	baseRestarts := pub.Restarts()
	t.Logf("killing publisher (pid=%d)", pub.PID())
	mpCCrashKill(t, pub)

	mpCStillLive(t, env, 2*time.Second, "canonicalizer", "storage")

	// Inject during outage — canonicalizer's outClient spools (canon→publisher).
	for i := 0; i < 4; i++ {
		mpCSendRaw(t, cl, "BINANCE", mpCBinanceTrade("0.999", 200+i))
	}

	mpCWaitRestarted(t, pub, baseRestarts, 15*time.Second)
	got := mpCWaitForMarkerCount(t, env, outageMark, 4, 25*time.Second)
	if got < 4 {
		t.Errorf("LOSSLESS FAILURE: only %d/4 outage events reached WAL after publisher restart", got)
	} else {
		t.Logf("lossless: %d/4 outage events drained from canonicalizer spool after publisher restart (restarts=%d)", got, pub.Restarts())
	}
}

// TestMultiprocessChaos_CanonicalizerKill_AutoRestart kills the canonicalizer
// (the source of the pipeline). There is nothing to spool (the source is down),
// so this tests the recovery axis: publisher+storage stay healthy, the
// canonicalizer auto-restarts, and the test re-injects → flow resumes.
func TestMultiprocessChaos_CanonicalizerKill_AutoRestart(t *testing.T) {
	env := mpCNewEnv(t)
	mpCStart(t, env, "canonicalizer")
	mpCStart(t, env, "publisher")
	mpCStart(t, env, "storage")
	mpCWaitLive(t, env, 30*time.Second, "canonicalizer", "publisher", "storage")

	canon := mpCProc(env, "canonicalizer")
	baseRestarts := canon.Restarts()
	t.Logf("killing canonicalizer (pid=%d) — peers must stay healthy", canon.PID())
	mpCCrashKill(t, canon)

	// Peers must stay alive the whole time the source is down.
	mpCStillLive(t, env, 3*time.Second, "publisher", "storage")

	mpCWaitRestarted(t, canon, baseRestarts, 15*time.Second)

	// Re-inject after recovery → flow must resume → WAL.
	recoverMark := []byte(`"q":"0.555"`)
	cl := mpCDial(t, env.sockets.adapterCanon) // fresh client (old conn broke when canon died)
	defer cl.Stop()
	mpCSendRaw(t, cl, "BINANCE", mpCBinanceTrade("0.555", 300))
	got := mpCWaitForMarkerCount(t, env, recoverMark, 1, 20*time.Second)
	if got < 1 {
		t.Errorf("flow did not resume after canonicalizer restart (restarts=%d)", canon.Restarts())
	} else {
		t.Logf("recovered: flow resumed after canonicalizer restart (restarts=%d)", canon.Restarts())
	}
}

// TestMultiprocessChaos_AllKill_Recovery kills all 3 downstream processes at once.
// Each supervisor restarts its own child; once all are back, the full chain
// resumes and events reach the WAL.
func TestMultiprocessChaos_AllKill_Recovery(t *testing.T) {
	env := mpCNewEnv(t)
	mpCStart(t, env, "canonicalizer")
	mpCStart(t, env, "publisher")
	mpCStart(t, env, "storage")
	mpCWaitLive(t, env, 30*time.Second, "canonicalizer", "publisher", "storage")

	canon := mpCProc(env, "canonicalizer")
	pub := mpCProc(env, "publisher")
	stor := mpCProc(env, "storage")
	cb, pb, sb := canon.Restarts(), pub.Restarts(), stor.Restarts()

	t.Log("killing all 3 downstream processes at once")
	mpCCrashKill(t, canon)
	mpCCrashKill(t, pub)
	mpCCrashKill(t, stor)

	mpCWaitRestarted(t, canon, cb, 20*time.Second)
	mpCWaitRestarted(t, pub, pb, 20*time.Second)
	mpCWaitRestarted(t, stor, sb, 20*time.Second)

	// Full chain resumed → inject → WAL.
	recoverMark := []byte(`"q":"0.666"`)
	cl := mpCDial(t, env.sockets.adapterCanon)
	defer cl.Stop()
	mpCSendRaw(t, cl, "BINANCE", mpCBinanceTrade("0.666", 400))
	got := mpCWaitForMarkerCount(t, env, recoverMark, 1, 25*time.Second)
	if got < 1 {
		t.Errorf("full chain did not resume after all-kill (restarts canon=%d pub=%d stor=%d)", canon.Restarts(), pub.Restarts(), stor.Restarts())
	} else {
		t.Logf("recovered: full chain resumed after all-kill (restarts canon=%d pub=%d stor=%d)", canon.Restarts(), pub.Restarts(), stor.Restarts())
	}
}

// TestMultiprocessChaos_RaceConcurrentInject fires N goroutines, each its own IPC
// client, each sending K distinct frames concurrently. Asserts no loss under
// concurrent producers. Run under -race to race-check the test harness + the
// test-side IPC clients (the child binaries are production builds — not
// race-instrumented; see ADDIM_C_PHASES.md).
//
// The clients are kept alive UNTIL AFTER the WAL is polled: cl.Send is async
// (spool + drainLoop), so stopping a client mid-drain would discard undelivered
// spooled frames — a test-harness artifact, not a pipeline loss.
func TestMultiprocessChaos_RaceConcurrentInject(t *testing.T) {
	env := mpCNewEnv(t)
	mpCStart(t, env, "canonicalizer")
	mpCStart(t, env, "publisher")
	mpCStart(t, env, "storage")
	mpCWaitLive(t, env, 30*time.Second, "canonicalizer", "publisher", "storage")

	const goroutines = 8
	const perG = 5
	want := goroutines * perG
	marker := []byte(`"q":"0.888"`)

	clients := make([]*ipc.Client, goroutines)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("goroutine %d panicked: %v", gid, r)
				}
			}()
			cl, err := ipc.NewClient(env.sockets.adapterCanon, ipc.ClientConfig{
				Backoff:       []int{0, 0, 0, 1},
				DialTimeout:   2 * time.Second,
				WriteTimeout:  2 * time.Second,
				SpoolPath:     filepath.Join(env.tmpDir, fmt.Sprintf("race-%d.spool", gid)),
				MaxSpoolBytes: 64 * 1024 * 1024,
			})
			if err != nil {
				t.Errorf("dial %d: %v", gid, err)
				return
			}
			clients[gid] = cl
			cl.Start()
			for i := 0; i < perG; i++ {
				rm := adapter.RawMessage{Source: "BINANCE", Payload: mpCBinanceTrade("0.888", gid*1000+i), ReceivedAt: time.Now().UnixNano()}
				msg, err := pipeline.EncodeRaw(rm)
				if err != nil {
					t.Errorf("encode %d: %v", gid, err)
					return
				}
				if err := cl.Send(msg); err != nil {
					t.Errorf("send %d: %v", gid, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// Poll the WAL while clients are still alive (drainLoops finish delivering).
	got := mpCWaitForMarkerCount(t, env, marker, want, 30*time.Second)
	for _, cl := range clients {
		if cl != nil {
			cl.Stop()
		}
	}
	if got < want {
		t.Errorf("concurrent inject loss: got %d want %d", got, want)
	} else {
		t.Logf("concurrent inject: %d/%d events reached WAL losslessly", got, want)
	}
}
