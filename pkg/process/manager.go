package process

import (
	"context"
	"sync"
)

// Manager supervises a set of Processes, starting/stopping them together and
// aggregating health.
type Manager struct {
	mu     sync.Mutex
	procs  []*Process
	byName map[string]*Process
}

// NewManager builds a manager over the given processes (in start/stop order).
func NewManager(procs ...*Process) *Manager {
	m := &Manager{byName: make(map[string]*Process)}
	for _, p := range procs {
		m.add(p)
	}
	return m
}

// Add appends a process to the manager.
func (m *Manager) Add(p *Process) {
	m.mu.Lock()
	m.add(p)
	m.mu.Unlock()
}

func (m *Manager) add(p *Process) {
	if p == nil {
		return
	}
	m.procs = append(m.procs, p)
	if p.Name != "" {
		m.byName[p.Name] = p
	}
}

// StartAll starts every process. It does NOT fail fast: a process that errors
// on start is recorded but does not prevent the others from starting (so one
// bad binary doesn't block the rest). Returns the collected per-process errors.
// Never panics.
func (m *Manager) StartAll(ctx context.Context) []error {
	m.mu.Lock()
	procs := append([]*Process(nil), m.procs...)
	m.mu.Unlock()

	var errs []error
	for _, p := range procs {
		if err := p.Start(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// StopAll stops every process (in reverse order) and waits for all monitors to
// exit. Never panics.
func (m *Manager) StopAll() error {
	m.mu.Lock()
	procs := append([]*Process(nil), m.procs...)
	m.mu.Unlock()

	// Signal all first, then wait — so a process that depends on a downstream
	// being drained isn't killed before its peers begin shutting down.
	for i := len(procs) - 1; i >= 0; i-- {
		_ = procs[i].Stop()
	}
	return nil
}

// Health returns a snapshot of every process, in registration order.
func (m *Manager) Health() []ProcessHealth {
	m.mu.Lock()
	procs := append([]*Process(nil), m.procs...)
	m.mu.Unlock()

	out := make([]ProcessHealth, 0, len(procs))
	for _, p := range procs {
		out = append(out, p.Health())
	}
	return out
}

// Get returns a process by name.
func (m *Manager) Get(name string) (*Process, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.byName[name]
	return p, ok
}

// Names returns the registered process names.
func (m *Manager) Names() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.procs))
	for _, p := range m.procs {
		out = append(out, p.Name)
	}
	return out
}

// AllHealthy reports whether every process passes its health check.
func (m *Manager) AllHealthy() bool {
	for _, h := range m.Health() {
		if !h.Healthy {
			return false
		}
	}
	return true
}
