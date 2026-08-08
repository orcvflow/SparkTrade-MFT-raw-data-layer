package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// MT5ZMQAdapter implements Adapter for MetaTrader 5 via ZeroMQ bridge
// Architecture: MT5 Terminal (Wine) → MQL5 EA (ZMQ PUB) → Go (ZMQ SUB)
// Evidence: ZeroMQ ~200μs latency, 3.2M msg/s throughput
type MT5ZMQAdapter struct {
	endpoint string
	socket   *zmq.Socket
	ctx      context.Context
	cancel   context.CancelFunc

	// State
	connected      atomic.Bool
	running        atomic.Bool
	messagesRecv   atomic.Uint64
	reconnectCount atomic.Int32
	startTime      time.Time
	lastMessage    atomic.Value // time.Time

	// Configuration
	reconnectAttempts int
	backoffSeconds    []int
	receiveTimeout    time.Duration

	// Synchronization
	mu     sync.RWMutex
	stopCh chan struct{}
	errors []error
}

// NewMT5ZMQAdapter creates a new MT5 ZeroMQ adapter
// Default endpoint: tcp://localhost:5556 (MQL5 EA publishes on this)
func NewMT5ZMQAdapter(endpoint string, config AdapterConfig) Adapter {
	if endpoint == "" {
		endpoint = "tcp://localhost:5556"
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &MT5ZMQAdapter{
		endpoint:          endpoint,
		reconnectAttempts: config.ReconnectAttempts,
		backoffSeconds:    config.BackoffSeconds,
		receiveTimeout:    1 * time.Second, // Non-blocking receive
		ctx:               ctx,
		cancel:            cancel,
		stopCh:            make(chan struct{}),
		startTime:         time.Now(),
		errors:            make([]error, 0),
	}
}

// Name returns the adapter name
func (m *MT5ZMQAdapter) Name() string {
	return "MT5"
}

// Connect establishes ZeroMQ SUB connection with paranoid error handling
func (m *MT5ZMQAdapter) Connect(ctx context.Context) error {
	// Never panic - use defer recover (CLAUDE.md principle)
	defer func() {
		if r := recover(); r != nil {
			m.addError(fmt.Errorf("panic in Connect: %v", r))
		}
	}()

	// Check if already connected
	if m.connected.Load() {
		return nil
	}

	// Create ZeroMQ SUB socket
	socket, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		return NewAdapterError(ErrorConnection, "MT5", "failed to create ZMQ socket", err)
	}

	// Set socket options
	// HWM: high water mark for receive buffer (prevent memory exhaustion)
	socket.SetRcvhwm(10000)
	// Receive timeout (non-blocking mode)
	socket.SetRcvtimeo(m.receiveTimeout)
	// Linger: don't wait on close
	socket.SetLinger(0)

	// Connect to MT5 publisher (tcp://localhost:5556)
	if err := socket.Connect(m.endpoint); err != nil {
		socket.Close()
		return NewAdapterError(ErrorConnection, "MT5", fmt.Sprintf("failed to connect to %s", m.endpoint), err)
	}

	// Subscribe to all topics (empty string = receive all messages)
	if err := socket.SetSubscribe(""); err != nil {
		socket.Close()
		return NewAdapterError(ErrorProtocol, "MT5", "failed to set subscription", err)
	}

	m.mu.Lock()
	m.socket = socket
	m.startTime = time.Now()
	m.mu.Unlock()

	m.connected.Store(true)

	return nil
}

// Start begins receiving data (runs in goroutine)
func (m *MT5ZMQAdapter) Start(ctx context.Context, output chan<- RawMessage) error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			m.addError(fmt.Errorf("panic in Start: %v", r))
		}
	}()

	if m.running.Load() {
		return fmt.Errorf("adapter already running")
	}

	m.running.Store(true)

	// Main receive loop (blocking)
	return m.receiveLoop(ctx, output)
}

// receiveLoop is the main message receive loop
func (m *MT5ZMQAdapter) receiveLoop(ctx context.Context, output chan<- RawMessage) error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			m.addError(fmt.Errorf("panic in receiveLoop: %v", r))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.stopCh:
			return nil
		default:
			// Attempt to receive message
			data, err := m.safeRecv()

			if err != nil {
				// Check if it's timeout (expected for non-blocking mode)
				if err.Error() == "resource temporarily unavailable" {
					// No message available, continue
					time.Sleep(10 * time.Millisecond)
					continue
				}

				// Connection error - attempt reconnect
				m.connected.Store(false)
				if err := m.reconnect(ctx); err != nil {
					m.addError(err)
					// reconnect() already handles backoff internally, just wait briefly before retry
					time.Sleep(2 * time.Second)
					continue
				}
				continue
			}

			// Validate JSON (MT5 EA sends JSON)
			if !json.Valid(data) {
				m.addError(fmt.Errorf("invalid JSON received"))
				continue
			}

			// Update metrics BEFORE sending (prevent race with consumer reading metrics)
			m.messagesRecv.Add(1)
			m.lastMessage.Store(time.Now())

			// Send raw message to pipeline (non-blocking with timeout)
			msg := RawMessage{
				Source:      m.Name(),
				Payload:     data, // UNTOUCHED - byte-for-byte preservation
				ReceivedAt:  time.Now().UnixNano(),
				SequenceNum: m.messagesRecv.Load(),
			}

			select {
			case output <- msg:
				// Success
			case <-time.After(5 * time.Second):
				// Output channel blocked - backpressure
				m.addError(fmt.Errorf("output channel blocked (backpressure)"))
				// Undo metrics increment (message wasn't delivered)
				m.messagesRecv.Add(^uint64(0)) // Decrement
			}
		}
	}
}

// safeRecv receives a message with panic protection
func (m *MT5ZMQAdapter) safeRecv() (data []byte, err error) {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in safeRecv: %v", r)
		}
	}()

	m.mu.RLock()
	socket := m.socket
	m.mu.RUnlock()

	if socket == nil {
		return nil, fmt.Errorf("socket not initialized")
	}

	// RecvBytes with timeout (set in Connect)
	return socket.RecvBytes(0)
}

// reconnect attempts to reconnect with exponential backoff
func (m *MT5ZMQAdapter) reconnect(ctx context.Context) error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			m.addError(fmt.Errorf("panic in reconnect: %v", r))
		}
	}()

	m.reconnectCount.Add(1)
	reconnects := int(m.reconnectCount.Load())

	// Check max attempts
	if m.reconnectAttempts > 0 && reconnects > m.reconnectAttempts {
		return fmt.Errorf("max reconnect attempts reached (%d)", m.reconnectAttempts)
	}

	// Close existing socket
	m.mu.Lock()
	if m.socket != nil {
		m.socket.Close()
		m.socket = nil
	}
	m.mu.Unlock()

	m.connected.Store(false)

	// Exponential backoff
	backoff := m.getBackoff()
	time.Sleep(time.Duration(backoff) * time.Second)

	// Attempt reconnect
	if err := m.Connect(ctx); err != nil {
		return fmt.Errorf("reconnect failed: %w", err)
	}

	return nil
}

// getBackoff returns current backoff duration
func (m *MT5ZMQAdapter) getBackoff() int {
	reconnects := int(m.reconnectCount.Load())

	// Use provided backoff sequence
	if reconnects < len(m.backoffSeconds) {
		return m.backoffSeconds[reconnects]
	}

	// Max backoff: 30 seconds (CLAUDE.md: 1,2,4,8,16,30)
	return 30
}

// Stop gracefully stops the adapter
func (m *MT5ZMQAdapter) Stop() error {
	// Never panic
	defer func() {
		if r := recover(); r != nil {
			m.addError(fmt.Errorf("panic in Stop: %v", r))
		}
	}()

	m.running.Store(false)

	// Signal stop
	close(m.stopCh)

	// Cancel context
	m.cancel()

	// Close socket
	m.mu.Lock()
	if m.socket != nil {
		m.socket.Close()
		m.socket = nil
	}
	m.mu.Unlock()

	m.connected.Store(false)

	return nil
}

// Health returns current health status
func (m *MT5ZMQAdapter) Health() HealthStatus {
	m.mu.RLock()
	errors := make([]error, len(m.errors))
	copy(errors, m.errors)
	m.mu.RUnlock()

	lastMsg := time.Time{}
	if v := m.lastMessage.Load(); v != nil {
		lastMsg = v.(time.Time)
	}

	return HealthStatus{
		Connected:      m.connected.Load(),
		LastMessage:    lastMsg,
		MessagesRecv:   m.messagesRecv.Load(),
		MessagesSent:   0, // MT5 is receive-only
		Errors:         errors,
		ReconnectCount: int(m.reconnectCount.Load()),
		Uptime:         time.Since(m.startTime),
	}
}

// addError adds an error to the error list (max 10)
func (m *MT5ZMQAdapter) addError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.errors = append(m.errors, err)
	if len(m.errors) > 10 {
		m.errors = m.errors[1:] // Keep last 10 errors
	}
}
