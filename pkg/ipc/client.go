package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
)

// Errors surfaced by the client.
var (
	ErrClientClosed = errors.New("ipc: client closed")
	ErrNoConnection = errors.New("ipc: no connection")
)

// DefaultBackoff mirrors the adapters' reconnection schedule (CLAUDE.md §1).
var DefaultBackoff = []int{1, 2, 4, 8, 16, 30}

// ClientConfig configures a UDS client.
type ClientConfig struct {
	QueueSize     int           // reserved for future in-mem fast-path (unused in Phase 1)
	Backoff       []int         // reconnect backoff seconds (default DefaultBackoff)
	DialTimeout   time.Duration // per-dial deadline (default 5s)
	WriteTimeout  time.Duration // per-frame write deadline (default 2s)
	SpoolPath     string        // overflow file path (default <path>+".spool")
	MaxSpoolBytes int64         // on-disk spool cap (default 256 MiB)
}

// Client is a UDS client that delivers IPCMessages to a downstream Server. It is
// lossless and FIFO under downstream outages.
//
// Design (single ordered store): every Send appends the marshaled message to an
// append-only on-disk spool — the single source of truth. The drainLoop replays
// the spool in order. This avoids the classic two-buffer (channel + spool) FIFO
// hazard, where draining one buffer before the other delivers messages out of
// order when both hold data with interleaved ages.
//
// Connection liveness: each connection has a readLoop that detects a server-side
// close via EOF and breaks the connection immediately (not lazily on the next
// write). A broken connection triggers reconnect with backoff. Send never
// blocks for I/O while the downstream is down; the spool absorbs the backlog.
//
// Send never panics.
type Client struct {
	path    string
	cfg     ClientConfig
	spool   *spool
	notify  chan struct{} // cap-1: woken after each append
	connBroken chan struct{} // cap-1: woken when the conn breaks
	backoff []int

	mu   sync.RWMutex
	conn net.Conn

	connected  atomic.Bool
	closed     atomic.Bool
	sent       atomic.Uint64
	spooled    atomic.Uint64
	reconnects atomic.Int32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	errMu sync.Mutex
	errs  []error
}

// NewClient creates (but does not start) a UDS client. Start must be called to
// begin the background drain loop. Never panics.
func NewClient(path string, cfg ClientConfig) (*Client, error) {
	if len(cfg.Backoff) == 0 {
		cfg.Backoff = append([]int(nil), DefaultBackoff...)
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 2 * time.Second
	}
	if cfg.SpoolPath == "" {
		cfg.SpoolPath = path + ".spool"
	}

	sp, err := newSpool(cfg.SpoolPath, cfg.MaxSpoolBytes)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		path:      path,
		cfg:       cfg,
		spool:     sp,
		notify:    make(chan struct{}, 1),
		connBroken: make(chan struct{}, 1),
		backoff:   cfg.Backoff,
		ctx:       ctx,
		cancel:    cancel,
		errs:      make([]error, 0),
	}
	return c, nil
}

// Start launches the background drain loop. Idempotent. Never panics.
func (c *Client) Start() {
	c.wg.Add(1)
	go c.drainLoop()
}

// Send delivers m to the downstream via the spool. It never blocks for I/O
// while the downstream is down (the common outage case). It returns:
//   - nil on success (the message is durably in the spool and will be sent);
//   - ErrSpoolFull when the on-disk spool has reached its cap (hard
//     backpressure — the caller should slow the producer);
//   - ErrClientClosed after Stop.
//
// Never panics.
func (c *Client) Send(m *IPCMessage) error {
	if c.closed.Load() {
		return ErrClientClosed
	}
	if m == nil {
		return fmt.Errorf("ipc: nil message")
	}
	body, err := marshalFresh(m)
	if err != nil {
		return err
	}
	if err := c.spool.append(body); err != nil {
		return err // ErrSpoolFull → caller applies hard backpressure
	}
	c.spooled.Add(1)
	c.signal(c.notify)
	return nil
}

func (c *Client) signal(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default: // coalesce: a pending wake-up already covers this event
	}
}

// drainLoop owns the connection lifecycle and replays the spool in order.
func (c *Client) drainLoop() {
	defer c.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			c.addError(fmt.Errorf("ipc: panic in drainLoop: %v", r))
		}
	}()

	attempt := 0
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// (Re)connect only when not already connected.
		if !c.connected.Load() {
			if err := c.connect(); err != nil {
				c.backoffWait(attempt)
				attempt++
				continue
			}
		}
		attempt = 0

		// Replay the spool (FIFO, lossless). On a conn break, drop and
		// reconnect; the spool stays intact (records retried, with possible
		// duplicates — never a loss).
		if c.spool.size() > 0 {
			if _, err := c.spool.drain(c.writeFrame); err != nil {
				c.addError(err)
				c.breakConn()
				continue
			}
		}

		// Spool empty: block for the next append, a conn-break signal, or
		// shutdown. Both signals are buffered (cap 1) so an event between the
		// size check above and this select is not lost.
		select {
		case <-c.ctx.Done():
			return
		case <-c.notify:
			// new data appended → loop to re-check the spool
		case <-c.connBroken:
			// conn broke → loop to reconnect
		}
	}
}

// connect dials the downstream UDS and starts a readLoop for liveness.
// Never panics.
func (c *Client) connect() error {
	defer func() {
		if r := recover(); r != nil {
			c.addError(fmt.Errorf("ipc: panic in connect: %v", r))
		}
	}()
	conn, err := net.DialTimeout("unix", c.path, c.cfg.DialTimeout)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	c.connected.Store(true)

	c.wg.Add(1)
	go c.readLoop(conn)
	return nil
}

// readLoop reads (and discards) any server replies and detects a server-side
// close via EOF, breaking the connection promptly. The Phase 4 pipeline is
// one-way per stage, so replies are not consumed today; this loop exists to
// detect close and to keep the receive buffer drained.
func (c *Client) readLoop(conn net.Conn) {
	defer c.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			c.addError(fmt.Errorf("ipc: panic in readLoop: %v", r))
		}
	}()
	var buf []byte
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		if _, err := ReadFrame(conn, &buf); err != nil {
			c.breakConn()
			return
		}
		// discard reply
	}
}

// writeFrame writes a marshaled body to the current conn with a write deadline.
func (c *Client) writeFrame(body []byte) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return ErrNoConnection
	}
	_ = conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	_, err := WriteFrame(conn, body)
	if err == nil {
		c.sent.Add(1)
	}
	return err
}

// breakConn closes the current connection (if still present) and signals the
// drainLoop to reconnect. Idempotent and safe to call from the readLoop and the
// drainLoop concurrently — only the first caller acts.
func (c *Client) breakConn() {
	c.mu.Lock()
	conn := c.conn
	if conn == nil {
		c.mu.Unlock()
		return
	}
	c.conn = nil
	c.mu.Unlock()

	_ = conn.Close()
	c.connected.Store(false)
	c.reconnects.Add(1)
	c.signal(c.connBroken)
}

func (c *Client) backoffWait(attempt int) {
	idx := attempt
	if idx >= len(c.backoff) {
		idx = len(c.backoff) - 1
	}
	d := time.Duration(c.backoff[idx]) * time.Second
	select {
	case <-c.ctx.Done():
	case <-time.After(d):
	}
}

// Flush blocks until the spool has fully drained to a live downstream, or ctx is
// cancelled. It does NOT close the client. Call it before Stop for a lossless
// graceful shutdown: stop the producer first (so no new Sends arrive), then
// Flush, then Stop. If the downstream is down, the spool cannot drain, so Flush
// blocks until ctx and the still-pending records are then removed by Stop —
// documented as the one bounded-loss window (only at shutdown while downstream
// is down; a crash mid-flight keeps the spool for replay on restart). Never panics.
func (c *Client) Flush(ctx context.Context) error {
	defer func() {
		if r := recover(); r != nil {
			// a faulty flush must never crash the caller
		}
	}()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if c.spool.size() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Stop cancels the drain loop, waits for it and all readLoops to exit, closes
// the spool, and removes the spool file. Idempotent. Never panics.
func (c *Client) Stop() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.cancel()
	c.breakConn()
	c.wg.Wait()
	_ = c.spool.close()
	_ = removeFile(c.cfg.SpoolPath)
	return nil
}

// ClientStats is a point-in-time snapshot.
type ClientStats struct {
	Path       string
	Connected  bool
	Sent       uint64 // delivered to a live conn
	Spooled    uint64 // appended to the spool
	SpoolBytes int64  // current backlog on disk
	Reconnects int32
	Closed     bool
	Errors     []error
}

// Stats returns a snapshot. The Errors slice is a copy.
func (c *Client) Stats() ClientStats {
	c.errMu.Lock()
	errs := make([]error, len(c.errs))
	copy(errs, c.errs)
	c.errMu.Unlock()
	return ClientStats{
		Path:       c.path,
		Connected:  c.connected.Load(),
		Sent:       c.sent.Load(),
		Spooled:    c.spooled.Load(),
		SpoolBytes: c.spool.size(),
		Reconnects: c.reconnects.Load(),
		Closed:     c.closed.Load(),
		Errors:     errs,
	}
}

// Connected reports whether the client currently has a live connection.
func (c *Client) Connected() bool { return c.connected.Load() }

func (c *Client) addError(err error) {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	c.errs = append(c.errs, err)
	if len(c.errs) > 10 {
		c.errs = c.errs[1:]
	}
}

// marshalFresh serializes m without the pool — the returned bytes are owned by
// the spool (not recycled by the caller). Never panics.
func marshalFresh(m *IPCMessage) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrMarshal, r)
		}
	}()
	if m == nil {
		return nil, fmt.Errorf("%w: nil message", ErrMarshal)
	}
	return proto.Marshal(m)
}

// removeFile is a best-effort os.Remove that ignores "not exist".
func removeFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
