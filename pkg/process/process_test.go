package process

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helper-binary builder (built once per test binary via sync.Once).
// ─────────────────────────────────────────────────────────────────────────────

var (
	healthProcOnce sync.Once
	healthProcBin  string
	healthProcErr  error
)

// healthProcBinary builds testdata/healthproc into a temp binary (once) and
// returns its path.
func healthProcBinary(t *testing.T) string {
	t.Helper()
	healthProcOnce.Do(func() {
		dir, err := os.MkdirTemp("", "proc-test-")
		if err != nil {
			healthProcErr = err
			return
		}
		bin := filepath.Join(dir, "healthproc")
		// cwd during `go test` is the package dir (pkg/process).
		cmd := exec.Command("go", "build", "-o", bin, "./testdata/healthproc")
		out, err := cmd.CombinedOutput()
		if err != nil {
			healthProcErr = fmt.Errorf("go build healthproc: %v\n%s", err, out)
			return
		}
		healthProcBin = bin
	})
	if healthProcErr != nil {
		t.Fatalf("healthproc build failed: %v", healthProcErr)
	}
	return healthProcBin
}

func waitFor(t *testing.T, deadline time.Duration, fn func() bool) bool {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if fn() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func fastBackoff() []time.Duration {
	return []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond}
}

// newHealthProc builds a Process that runs the healthproc helper on the given
// port, with a health probe.
func newHealthProc(t *testing.T, name string, port int) *Process {
	t.Helper()
	bin := healthProcBinary(t)
	p := New(name, bin)
	p.Env = []string{fmt.Sprintf("HEALTH_PORT=%d", port)}
	p.HealthURL = fmt.Sprintf("http://127.0.0.1:%d/health", port)
	p.Policy.Backoff = fastBackoff()
	p.GracefulTimeout = 2 * time.Second
	return p
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestProcessManager_StartAll(t *testing.T) {
	ports := []int{18101, 18102, 18103}
	procs := make([]*Process, 0, len(ports))
	for i, port := range ports {
		procs = append(procs, newHealthProc(t, fmt.Sprintf("p%d", i), port))
	}
	mgr := NewManager(procs...)
	defer mgr.StopAll()

	if errs := mgr.StartAll(context.Background()); len(errs) != 0 {
		t.Fatalf("StartAll errors: %v", errs)
	}
	if !waitFor(t, 5*time.Second, mgr.AllHealthy) {
		for _, h := range mgr.Health() {
			t.Logf("  %s: running=%v healthy=%v pid=%d err=%s", h.Name, h.Running, h.Healthy, h.PID, h.Error)
		}
		t.Fatalf("processes did not become healthy")
	}
}

func TestProcessManager_AutoRestart(t *testing.T) {
	p := newHealthProc(t, "restarter", 18111)
	mgr := NewManager(p)
	defer mgr.StopAll()

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool { return p.Health().Healthy }) {
		t.Fatalf("process never healthy: %+v", p.Health())
	}
	pid := p.PID()
	if pid == 0 {
		t.Fatal("no PID")
	}

	// Kill the underlying OS process. The monitor must restart it.
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill: %v", err)
	}

	if !waitFor(t, 5*time.Second, func() bool { return p.Restarts() >= 1 }) {
		t.Fatalf("process did not register a restart: %+v", p.Health())
	}
	// After restart it must become healthy again (on the same port).
	if !waitFor(t, 5*time.Second, func() bool { return p.Health().Healthy }) {
		t.Fatalf("process did not recover after restart: %+v", p.Health())
	}
	if p.PID() == 0 {
		t.Error("no PID after restart")
	}
}

func TestProcessManager_Health(t *testing.T) {
	p := newHealthProc(t, "healthcheck", 18121)
	mgr := NewManager(p)
	defer mgr.StopAll()

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool { return p.Health().Healthy }) {
		t.Fatalf("never healthy: %+v", p.Health())
	}

	hlth := mgr.Health()
	if len(hlth) != 1 {
		t.Fatalf("Health len=%d", len(hlth))
	}
	if !hlth[0].Healthy || !hlth[0].Running || hlth[0].PID == 0 {
		t.Errorf("unexpected health: %+v", hlth[0])
	}

	// Stop → unhealthy.
	_ = mgr.StopAll()
	if !waitFor(t, 3*time.Second, func() bool { return !p.Running() }) {
		t.Errorf("still running after Stop: %+v", p.Health())
	}
	hlth = mgr.Health()
	if hlth[0].Running || hlth[0].Healthy {
		t.Errorf("expected stopped: %+v", hlth[0])
	}
}

func TestProcess_StopGraceful(t *testing.T) {
	p := newHealthProc(t, "graceful", 18131)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool { return p.Health().Healthy }) {
		t.Fatalf("never healthy: %+v", p.Health())
	}
	start := time.Now()
	if err := p.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// SIGTERM should have stopped it well within the graceful timeout.
	if time.Since(start) > 3*time.Second {
		t.Errorf("Stop took too long (escalation?): %v", time.Since(start))
	}
	if p.Running() {
		t.Errorf("still running after Stop")
	}
}

// TestProcess_MaxRetriesExhausted: a process that always crashes (CRASH_AFTER_MS)
// with MaxRetries=2 stops restarting after the cap.
func TestProcess_MaxRetriesExhausted(t *testing.T) {
	bin := healthProcBinary(t)
	p := New("crasher", bin)
	p.Env = []string{"HEALTH_PORT=18141", "CRASH_AFTER_MS=30"}
	p.HealthURL = "http://127.0.0.1:18141/health"
	p.Policy.MaxRetries = 2
	p.Policy.Backoff = []time.Duration{10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond}
	p.GracefulTimeout = 2 * time.Second

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Stop()

	// Monitor should give up after MaxRetries and exit (running==false, no PID).
	if !waitFor(t, 5*time.Second, func() bool {
		return p.Restarts() >= 3 && !p.Running() && p.PID() == 0
	}) {
		t.Fatalf("did not exhaust retries: %+v", p.Health())
	}
	if ce := p.Health().Error; ce == "" {
		t.Errorf("expected max-retries error recorded")
	} else {
		t.Logf("retries exhausted correctly: %s (restarts=%d)", ce, p.Restarts())
	}
}

// TestProcess_StartReEntrantBlocked: calling Start twice returns
// ErrAlreadyRunning.
func TestProcess_StartReEntrantBlocked(t *testing.T) {
	p := newHealthProc(t, "once", 18151)
	defer p.Stop()
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := p.Start(context.Background()); err != ErrAlreadyRunning {
		t.Errorf("second Start: got %v, want %v", err, ErrAlreadyRunning)
	}
}

// TestProcess_HealthProbeNoServer: a process with no reachable health URL
// reports Healthy=false (probe failure).
func TestProcess_HealthProbeFailure(t *testing.T) {
	bin := healthProcBinary(t)
	p := New("noprobe", bin)
	// No HEALTH_PORT → binds :0 (random port) but HealthURL targets a dead port.
	p.Env = []string{"HEALTH_PORT=0"}
	p.HealthURL = "http://127.0.0.1:19999/health" // nothing listening
	p.Policy.Backoff = fastBackoff()
	p.GracefulTimeout = 1 * time.Second
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Stop()

	if !waitFor(t, 3*time.Second, func() bool { return p.Running() }) {
		t.Fatalf("process did not start: %+v", p.Health())
	}
	h := p.Health()
	if h.Healthy {
		t.Errorf("expected probe failure (Healthy=false), got %+v", h)
	}
}
