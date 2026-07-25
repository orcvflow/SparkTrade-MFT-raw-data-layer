// Package process implements a small process supervisor for Addım C: it spawns
// the 4 pipeline processes (adapter/canonicalizer/publisher/storage) as child
// binaries, monitors them, and auto-restarts a crashed process with backoff so
// one crash doesn't take down the others (Homalos pattern). In production the
// same restart contract is enforced by systemd (Restart=always, RestartSec=5).
package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Errors surfaced by the supervisor.
var (
	ErrAlreadyRunning = errors.New("process: already running")
	ErrMaxRetries     = errors.New("process: max retries reached")
)

// DefaultBackoff mirrors CLAUDE.md's reconnection schedule (systemd RestartSec
// uses a similar exponential ramp).
var DefaultBackoff = []time.Duration{
	1 * time.Second, 2 * time.Second, 4 * time.Second,
	8 * time.Second, 16 * time.Second, 30 * time.Second,
}

// RestartPolicy governs auto-restart behavior.
type RestartPolicy struct {
	// MaxRetries caps restart attempts; 0 means unlimited (Restart=always).
	MaxRetries int
	// Backoff is the ramp between restart attempts. Defaults to DefaultBackoff.
	Backoff []time.Duration
}

// Process wraps a child binary the supervisor spawns and watches.
type Process struct {
	Name string
	Cmd  string
	Args []string
	Env  []string // extra env (appended to os.Environ())
	// HealthURL is probed (GET) for liveness; "" means "running == healthy".
	HealthURL string
	// HealthTimeout caps each health probe (default 1s).
	HealthTimeout time.Duration
	// GracefulTimeout caps SIGTERM→SIGKILL escalation (default 5s).
	GracefulTimeout time.Duration

	Policy RestartPolicy

	mu     sync.Mutex
	cmd    *exec.Cmd
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	running  atomic.Bool
	started  atomic.Bool // Start was called (Start is not re-entrant)
	stopped  atomic.Bool
	restarts atomic.Int32
	exitCode atomic.Int32 // -1 = not yet exited
	lastStart atomic.Int64

	errMu  sync.Mutex
	lastErr error
}

// New builds a Process with sensible defaults. Never panics.
func New(name, cmd string, args ...string) *Process {
	return &Process{
		Name:           name,
		Cmd:            cmd,
		Args:           args,
		HealthTimeout:  1 * time.Second,
		GracefulTimeout: 5 * time.Second,
		Policy:         RestartPolicy{Backoff: append([]time.Duration(nil), DefaultBackoff...)},
		exitCode:       atomic.Int32{}, // 0 initially
	}
}

// Start launches the process and the monitor goroutine. The ctx becomes the
// parent of the process's own lifecycle context. Returns ErrAlreadyRunning if
// Start was already called (Start is not re-entrant; build a new Process to
// restart after Stop). Never panics.
func (p *Process) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.exitCode.Store(-1) // not yet exited

	p.wg.Add(1)
	go p.monitor()
	return nil
}

// monitor runs the process and restarts it on crash until stopped.
func (p *Process) monitor() {
	defer p.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			p.setLastErr(fmt.Errorf("process: panic in monitor: %v", r))
		}
		p.running.Store(false)
	}()

	attempt := 0
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		cmd := exec.Command(p.Cmd, p.Args...)
		cmd.Env = append(os.Environ(), p.Env...)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard

		if err := cmd.Start(); err != nil {
			p.setLastErr(fmt.Errorf("process %s: start failed: %w", p.Name, err))
		} else {
			p.mu.Lock()
			p.cmd = cmd
			p.mu.Unlock()
			p.lastStart.Store(time.Now().UnixNano())
			p.running.Store(true)

			err := cmd.Wait()
			p.running.Store(false)
			p.exitCode.Store(int32(exitCode(err)))
			p.mu.Lock()
			p.cmd = nil
			p.mu.Unlock()

			if p.stopped.Load() {
				return // graceful shutdown
			}
			if err != nil {
				p.setLastErr(fmt.Errorf("process %s: exited: %w", p.Name, err))
			}
		}

		// Crashed (or start failed) → schedule a restart.
		p.restarts.Add(1)
		if p.Policy.MaxRetries > 0 && int(p.restarts.Load()) > p.Policy.MaxRetries {
			p.setLastErr(fmt.Errorf("process %s: %w", p.Name, ErrMaxRetries))
			return
		}
		p.backoffWait(attempt)
		attempt++
	}
}

// Stop signals the process (SIGTERM, escalating to SIGKILL) and waits for the
// monitor to exit. Idempotent. Never panics.
func (p *Process) Stop() error {
	if !p.stopped.CompareAndSwap(false, true) {
		return nil
	}
	p.cancel() // unblock any backoff wait

	p.mu.Lock()
	cmd := p.cmd
	p.mu.Unlock()

	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			select {
			case <-done:
			case <-time.After(p.GracefulTimeout):
				_ = cmd.Process.Kill() // escalation
			}
		}()
		p.wg.Wait()
		close(done)
	} else {
		p.wg.Wait()
	}
	return nil
}

// backoffWait sleeps per the policy, but bails out on context cancel.
func (p *Process) backoffWait(attempt int) {
	bp := p.Policy.Backoff
	if len(bp) == 0 {
		bp = DefaultBackoff
	}
	idx := attempt
	if idx >= len(bp) {
		idx = len(bp) - 1
	}
	d := bp[idx]
	select {
	case <-p.ctx.Done():
	case <-time.After(d):
	}
}

// PID returns the current child PID (0 if not running). For tests/monitoring.
func (p *Process) PID() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil && p.cmd.Process != nil {
		return p.cmd.Process.Pid
	}
	return 0
}

// ProcessHealth is a point-in-time snapshot.
type ProcessHealth struct {
	Name        string
	Running     bool
	Healthy     bool // health probe result (or running when no probe configured)
	Restarts    int32
	LastExitCode int32
	PID         int
	Error       string
}

// Health returns a snapshot, probing HealthURL if configured. Never panics.
func (p *Process) Health() ProcessHealth {
	pid := p.PID()
	running := p.running.Load()

	healthy := false
	if p.HealthURL != "" {
		healthy = p.probe()
	} else {
		healthy = running
	}

	p.errMu.Lock()
	errStr := ""
	if p.lastErr != nil {
		errStr = p.lastErr.Error()
	}
	p.errMu.Unlock()

	return ProcessHealth{
		Name:         p.Name,
		Running:      running,
		Healthy:      healthy,
		Restarts:     p.restarts.Load(),
		LastExitCode: p.exitCode.Load(),
		PID:          pid,
		Error:        errStr,
	}
}

// probe does a GET with a short timeout. Never panics.
func (p *Process) probe() bool {
	defer func() {
		if r := recover(); r != nil {
			// a faulty probe must never crash the caller
		}
	}()
	timeout := p.HealthTimeout
	if timeout <= 0 {
		timeout = 1 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(p.HealthURL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

// Restarts returns the number of restart attempts so far.
func (p *Process) Restarts() int32 { return p.restarts.Load() }

// Running reports whether the process is currently alive.
func (p *Process) Running() bool { return p.running.Load() }

func (p *Process) setLastErr(err error) {
	p.errMu.Lock()
	p.lastErr = err
	p.errMu.Unlock()
}

// exitCode extracts the exit code from an exec error. Returns -1 for a signal
// kill or unknown error, 0 for a clean exit.
func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}
