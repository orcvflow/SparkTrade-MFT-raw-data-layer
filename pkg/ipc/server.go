package ipc

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// ErrServerClosed is returned (or surfaced via Stats) when the server is stopped.
var ErrServerClosed = errors.New("ipc: server closed")

// Handler processes one inbound IPCMessage and may return a reply. The server
// runs Handler in a per-connection goroutine; Handler MUST be safe for
// concurrent use. Panics inside Handler are recovered (the reply is dropped and
// an error recorded) so a faulty handler never crashes the process.
type Handler func(m *IPCMessage) *IPCMessage

// Server is a UDS server that frames and dispatches IPCMessages.
type Server struct {
	path     string
	listener net.Listener
	handler  Handler

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	quit   chan struct{}

	wg sync.WaitGroup

	recv       atomic.Uint64
	sent       atomic.Uint64
	active     atomic.Int64
	errs       []error
}

// Listen creates a UDS server at path and begins accepting connections.
// A stale socket file at path is removed first. The parent directory is
// created if missing. Never panics.
func Listen(path string, handler Handler) (*Server, error) {
	// Best-effort removal of a stale socket file from a previous crash.
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		// Non-fatal: if removal fails (e.g. not a socket), bind will report a
		// clearer error below.
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("ipc: create socket dir: %w", err)
		}
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("ipc: listen %s: %w", path, err)
	}

	s := &Server{
		path:     path,
		listener: ln,
		handler:  handler,
		quit:     make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
		errs:     make([]error, 0),
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Path returns the socket path the server is bound to.
func (s *Server) Path() string { return s.path }

// acceptLoop accepts connections until the listener is closed.
func (s *Server) acceptLoop() {
	defer s.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			s.addError(fmt.Errorf("ipc: panic in acceptLoop: %v", r))
		}
	}()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return // listener closed via Stop
		}
		s.trackConn(conn, true)
		s.wg.Add(1)
		go s.serveConn(conn)
	}
}

// serveConn reads frames, dispatches to the handler, and writes any reply.
func (s *Server) serveConn(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			s.addError(fmt.Errorf("ipc: panic in serveConn: %v", r))
		}
	}()
	defer func() {
		s.trackConn(conn, false)
		_ = conn.Close()
	}()

	var readBuf []byte
	for {
		// Prompt-shutdown check; the real break is conn.Close() in Stop().
		select {
		case <-s.quit:
			return
		default:
		}

		n, err := ReadFrame(conn, &readBuf)
		if err != nil {
			return // EOF, closed, or read error → drop this connection
		}
		if n == 0 {
			continue
		}
		m, err := Unmarshal(readBuf[:n])
		if err != nil {
			s.addError(err)
			continue
		}
		s.recv.Add(1)

		if s.handler == nil {
			continue
		}
		reply := s.safeHandle(m)
		if reply == nil {
			continue
		}

		out, err := Marshal(reply)
		if err != nil {
			s.addError(err)
			continue
		}
		_, werr := WriteFrame(conn, out)
		PutBuf(out) // recycle regardless of write outcome
		if werr != nil {
			s.addError(werr)
			return // write failed → connection is broken
		}
		s.sent.Add(1)
	}
}

// safeHandle invokes the handler with panic recovery.
func (s *Server) safeHandle(m *IPCMessage) (reply *IPCMessage) {
	defer func() {
		if r := recover(); r != nil {
			s.addError(fmt.Errorf("ipc: panic in handler: %v", r))
			reply = nil
		}
	}()
	return s.handler(m)
}

func (s *Server) trackConn(conn net.Conn, add bool) {
	s.mu.Lock()
	if add {
		s.conns[conn] = struct{}{}
		s.active.Add(1)
	} else {
		delete(s.conns, conn)
		s.active.Add(-1)
	}
	s.mu.Unlock()
}

// Stop closes the listener and all active connections, waits for goroutines to
// exit, and removes the socket file. Idempotent. Never panics.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.quit)
	_ = s.listener.Close()
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	_ = os.Remove(s.path)
	return nil
}

// ServerStats is a point-in-time snapshot of server counters.
type ServerStats struct {
	Path       string
	ActiveConn int64
	Received   uint64
	Sent       uint64
	Closed     bool
	Errors     []error
}

// Stats returns a snapshot. The Errors slice is a copy.
func (s *Server) Stats() ServerStats {
	s.mu.Lock()
	errs := make([]error, len(s.errs))
	copy(errs, s.errs)
	closed := s.closed
	s.mu.Unlock()
	return ServerStats{
		Path:       s.path,
		ActiveConn: s.active.Load(),
		Received:   s.recv.Load(),
		Sent:       s.sent.Load(),
		Closed:     closed,
		Errors:     errs,
	}
}

func (s *Server) addError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, err)
	if len(s.errs) > 10 {
		s.errs = s.errs[1:]
	}
}
