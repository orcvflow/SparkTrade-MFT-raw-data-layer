package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// Snapshot is what a process's health probe returns. Status is "ok" or
// "degraded"; Component carries process-specific stats (pool/wal/db/adapter…),
// marshaled as-is. Implementations must never panic.
type Snapshot struct {
	Status    string `json:"status"`              // "ok" | "degraded"
	Component any   `json:"component,omitempty"`  // process-specific payload
}

// SnapshotFunc is the per-process health callback. The Server calls it on every
// GET /health. It must be cheap and panic-safe (the Server also wraps it).
type SnapshotFunc func() Snapshot

// Server is a reusable HTTP health/metrics server (Addım C). Each of the 4
// process binaries constructs one with its own SnapshotFunc and a per-process
// port (8081-8084). It exposes /health, /ready, /live, and /metrics.
//
// Design (CLAUDE.md):
//   - Never panic: every handler and Start/Stop is wrapped in defer/recover.
//   - Always observable: /health + /metrics on every process.
//   - Graceful: Stop shuts the listener and waits for in-flight requests.
type Server struct {
	name     string
	addr     string
	snapshot SnapshotFunc
	ready    func() bool

	mu       sync.Mutex
	listener net.Listener
	httpSrv  *http.Server
	started  bool
	stopped  bool

	wg sync.WaitGroup
}

// NewServer builds a health server bound to :port. snapshot may be nil (in which
// case /health reports a bare "ok"). Never panics.
func NewServer(name string, port int, snapshot SnapshotFunc) *Server {
	return &Server{
		name:     name,
		addr:     fmt.Sprintf(":%d", port),
		snapshot: snapshot,
		ready:    func() bool { return true },
	}
}

// SetReady installs a readiness predicate for GET /ready. Never panics.
func (s *Server) SetReady(fn func() bool) {
	if fn == nil {
		return
	}
	s.ready = fn
}

// Start binds the listener and serves in the background. Returns an error if the
// port is already in use or Start was already called. Never panics.
func (s *Server) Start() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("health: panic in Start: %v", r)
		}
	}()
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("health: server %s already started", s.name)
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("health: listen %s: %w", s.addr, err)
	}
	s.listener = ln
	s.started = true
	s.mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/ready", s.readyHandler)
	mux.HandleFunc("/live", s.liveHandler)
	// Register() is idempotent (metrics.go); first call wins.
	Register()
	mux.Handle("/metrics", MetricsHandler())

	s.httpSrv = &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				// a serve panic must never escape; the listener is closed on Stop.
				_ = r
			}
		}()
		if err := s.httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			// Listener closed (Stop) → ErrServerClosed is expected.
		}
	}()
	return nil
}

// Port returns the actual bound port (useful when bound to :0). 0 if not started.
func (s *Server) Port() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return 0
	}
	if tcp, ok := s.listener.Addr().(*net.TCPAddr); ok {
		return tcp.Port
	}
	return 0
}

// healthHandler returns the process snapshot + runtime memory/goroutine info.
func (s *Server) healthHandler(w http.ResponseWriter, _ *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			writeJSON(w, http.StatusInternalServerError, Snapshot{Status: "degraded"})
		}
	}()
	var snap Snapshot
	if s.snapshot != nil {
		snap = safeSnapshot(s.snapshot)
	} else {
		snap = Snapshot{Status: "ok"}
	}
	if snap.Status == "" {
		snap.Status = "ok"
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	payload := struct {
		Name      string  `json:"name"`
		Status    string  `json:"status"`
		Timestamp string  `json:"timestamp"`
		Component any     `json:"component,omitempty"`
		Memory    memJSON `json:"memory"`
		Goroutines int     `json:"goroutines"`
	}{
		Name:      s.name,
		Status:    snap.Status,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Component: snap.Component,
		Memory: memJSON{
			AllocMB: float64(mem.Alloc) / 1024 / 1024,
			SysMB:   float64(mem.Sys) / 1024 / 1024,
			HeapObj: mem.HeapObjects,
		},
		Goroutines: runtime.NumGoroutine(),
	}

	code := http.StatusOK
	if snap.Status != "ok" {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, payload)
}

type memJSON struct {
	AllocMB float64 `json:"alloc_mb"`
	SysMB   float64 `json:"sys_mb"`
	HeapObj uint64  `json:"heap_objects"`
}

func (s *Server) readyHandler(w http.ResponseWriter, _ *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]bool{"ready": false})
		}
	}()
	if s.ready != nil && s.ready() {
		writeJSON(w, http.StatusOK, map[string]bool{"ready": true})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]bool{"ready": false})
}

func (s *Server) liveHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"alive": true})
}

// Stop gracefully shuts the server down, waiting for in-flight requests. Never
// panics; idempotent.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.stopped || s.httpSrv == nil {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	srv := s.httpSrv
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	s.wg.Wait()
	return nil
}

// safeSnapshot runs the callback with panic recovery, returning degraded on failure.
func safeSnapshot(fn SnapshotFunc) (snap Snapshot) {
	defer func() {
		if r := recover(); r != nil {
			snap = Snapshot{Status: "degraded"}
		}
	}()
	return fn()
}

// writeJSON marshals v and writes it with the given status. Never panics.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
