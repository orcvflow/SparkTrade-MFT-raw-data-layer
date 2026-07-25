package adapter

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// IBAdapter implements Adapter for Interactive Brokers Gateway/TWS
// Supports TCP socket connection (port 7496 live, 7497 paper)
type IBAdapter struct {
	host     string
	port     int
	clientID int
	symbols  []string
	conn     net.Conn
	
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
	requestTimeout    time.Duration
	
	// Synchronization
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	errors []error
}

// NewIBAdapter creates a new IB Gateway adapter
func NewIBAdapter(host string, port int, clientID int, symbols []string, config AdapterConfig) *IBAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &IBAdapter{
		host:              host,
		port:              port,
		clientID:          clientID,
		symbols:           symbols,
		reconnectAttempts: config.ReconnectAttempts,
		backoffSeconds:    config.BackoffSeconds,
		requestTimeout:    config.Timeout,
		ctx:               ctx,
		cancel:            cancel,
		startTime:         time.Now(),
		errors:            make([]error, 0),
	}
}

// Name returns the adapter name
func (ib *IBAdapter) Name() string {
	return "IB"
}

// Connect establishes TCP connection to IB Gateway/TWS
func (ib *IBAdapter) Connect(ctx context.Context) error {
	// Never panic - use defer recover
	defer func() {
		if r := recover(); r != nil {
			ib.addError(fmt.Errorf("panic in Connect: %v", r))
		}
	}()
	
	// Check if already connected
	if ib.connected.Load() {
		return nil
	}
	
	// Connect with timeout
	address := fmt.Sprintf("%s:%d", ib.host, ib.port)
	dialer := net.Dialer{
		Timeout: 10 * time.Second,
	}
	
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return NewAdapterError(ErrorConnection, "IB", "failed to connect", err)
	}
	
	ib.mu.Lock()
	ib.conn = conn
	ib.startTime = time.Now()
	ib.mu.Unlock()

	// Send handshake
	if err := ib.sendHandshake(); err != nil {
		conn.Close()
		ib.conn = nil
		return err
	}
	
	ib.connected.Store(true)
	
	// Subscribe to symbols
	if err := ib.subscribeSymbols(); err != nil {
		conn.Close()
		ib.conn = nil
		ib.connected.Store(false)
		return err
	}
	
	return nil
}

// sendHandshake sends IB API handshake
// Protocol: API version negotiation
func (ib *IBAdapter) sendHandshake() error {
	ib.mu.RLock()
	conn := ib.conn
	ib.mu.RUnlock()
	
	if conn == nil {
		return NewAdapterError(ErrorConnection, "IB", "no connection", nil)
	}
	
	// IB API handshake format: "API\0" prefix + client version
	// This is a simplified version - real implementation needs full IB API protocol
	handshake := []byte(fmt.Sprintf("v%d..%d\x00", 100, ib.clientID))
	
	conn.SetWriteDeadline(time.Now().Add(ib.requestTimeout))
	_, err := conn.Write(handshake)
	if err != nil {
		return NewAdapterError(ErrorConnection, "IB", "handshake failed", err)
	}
	
	return nil
}

// subscribeSymbols subscribes to market data for all symbols
func (ib *IBAdapter) subscribeSymbols() error {
	// Note: This is a simplified version
	// Real IB API requires:
	// 1. Contract details request
	// 2. Market data type selection (live/delayed/frozen)
	// 3. Market data subscription for each contract
	
	if len(ib.symbols) == 0 {
		return NewAdapterError(ErrorProtocol, "IB", "no symbols to subscribe", nil)
	}
	
	// For each symbol, send subscription request
	// In real implementation, this would use IB API message format
	for _, symbol := range ib.symbols {
		if err := ib.subscribeSymbol(symbol); err != nil {
			return err
		}
	}
	
	return nil
}

// subscribeSymbol subscribes to a single symbol
func (ib *IBAdapter) subscribeSymbol(symbol string) error {
	ib.mu.RLock()
	conn := ib.conn
	ib.mu.RUnlock()
	
	if conn == nil {
		return NewAdapterError(ErrorConnection, "IB", "no connection", nil)
	}
	
	// Simplified subscription message
	// Real IB API uses binary protocol with message IDs
	subscribeMsg := []byte(fmt.Sprintf("REQ_MKT_DATA:%s\x00", symbol))
	
	conn.SetWriteDeadline(time.Now().Add(ib.requestTimeout))
	_, err := conn.Write(subscribeMsg)
	if err != nil {
		return NewAdapterError(ErrorConnection, "IB", "subscribe failed", err)
	}
	
	return nil
}

// Start begins receiving data (runs in goroutine)
func (ib *IBAdapter) Start(ctx context.Context, output chan<- RawMessage) error {
	if ib.running.Load() {
		return fmt.Errorf("adapter already running")
	}
	
	ib.running.Store(true)
	
	// Start main receive loop
	go ib.receiveLoop(ctx, output)
	
	return nil
}

// receiveLoop is the main message receiving loop with paranoid error handling
func (ib *IBAdapter) receiveLoop(ctx context.Context, output chan<- RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			ib.addError(fmt.Errorf("panic in receiveLoop: %v", r))
		}
		ib.running.Store(false)
	}()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ib.ctx.Done():
			return
		default:
			msg, err := ib.safeRead()
			if err != nil {
				ib.addError(err)
				
				// Auto-reconnect with exponential backoff
				if !ib.connected.Load() {
					ib.reconnect(ctx)
				}
				continue
			}
			
			if msg != nil {
				// Increment BEFORE the send: the output channel is buffered, so a
				// consumer that receives from it and then reads messagesRecv must
				// already see the increment (logical race otherwise).
				ib.messagesRecv.Add(1)
				ib.lastMessage.Store(time.Now())

				// Send to output channel with timeout
				select {
				case output <- *msg:
				case <-time.After(1 * time.Second):
					ib.messagesRecv.Add(^uint64(0)) // -1: undo the pre-increment (undelivered)
					ib.addError(fmt.Errorf("output channel blocked"))
				}
			}
		}
	}
}

// safeRead reads a message with panic recovery
// IB API uses binary protocol with length-prefixed messages
func (ib *IBAdapter) safeRead() (msg *RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in read: %v", r)
		}
	}()
	
	ib.mu.RLock()
	conn := ib.conn
	ib.mu.RUnlock()
	
	if conn == nil {
		ib.connected.Store(false)
		return nil, NewAdapterError(ErrorConnection, "IB", "no connection", nil)
	}
	
	// Read with timeout
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	
	// Read message length (4 bytes, big-endian)
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBuf); err != nil {
		ib.connected.Store(false)
		return nil, NewAdapterError(ErrorConnection, "IB", "read length failed", err)
	}
	
	messageLength := binary.BigEndian.Uint32(lengthBuf)
	
	// Sanity check: max message size 1MB
	if messageLength > 1024*1024 {
		ib.connected.Store(false)
		return nil, NewAdapterError(ErrorProtocol, "IB", "message too large", nil)
	}
	
	// Read message payload
	payload := make([]byte, messageLength)
	if _, err := io.ReadFull(conn, payload); err != nil {
		ib.connected.Store(false)
		return nil, NewAdapterError(ErrorConnection, "IB", "read payload failed", err)
	}
	
	// Create RawMessage (payload is UNTOUCHED)
	return &RawMessage{
		Source:     "IB",
		Payload:    payload,
		ReceivedAt: time.Now().UnixNano(),
	}, nil
}

// reconnect implements exponential backoff reconnection
func (ib *IBAdapter) reconnect(ctx context.Context) {
	for attempt := 0; attempt < ib.reconnectAttempts; attempt++ {
		// Calculate backoff
		backoffIdx := attempt
		if backoffIdx >= len(ib.backoffSeconds) {
			backoffIdx = len(ib.backoffSeconds) - 1
		}
		backoff := time.Duration(ib.backoffSeconds[backoffIdx]) * time.Second
		
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		
		// Attempt reconnect
		if err := ib.Connect(ctx); err != nil {
			ib.addError(fmt.Errorf("reconnect attempt %d failed: %w", attempt+1, err))
			continue
		}
		
		ib.reconnectCount.Add(1)
		return
	}
	
	ib.addError(fmt.Errorf("max reconnect attempts reached"))
}

// Stop gracefully stops the adapter
func (ib *IBAdapter) Stop() error {
	ib.running.Store(false)
	ib.connected.Store(false)
	
	ib.mu.Lock()
	if ib.conn != nil {
		ib.conn.Close()
		ib.conn = nil
	}
	ib.mu.Unlock()
	
	ib.cancel()
	return nil
}

// Health returns current health status
func (ib *IBAdapter) Health() HealthStatus {
	ib.mu.RLock()
	errors := make([]error, len(ib.errors))
	copy(errors, ib.errors)
	startTime := ib.startTime
	ib.mu.RUnlock()

	lastMsg := time.Time{}
	if v := ib.lastMessage.Load(); v != nil {
		lastMsg = v.(time.Time)
	}

	return HealthStatus{
		Connected:      ib.connected.Load(),
		LastMessage:    lastMsg,
		MessagesRecv:   ib.messagesRecv.Load(),
		Errors:         errors,
		ReconnectCount: int(ib.reconnectCount.Load()),
		Uptime:         time.Since(startTime),
	}
}

// addError adds an error to the error list (max 10)
func (ib *IBAdapter) addError(err error) {
	ib.mu.Lock()
	defer ib.mu.Unlock()
	
	ib.errors = append(ib.errors, err)
	if len(ib.errors) > 10 {
		ib.errors = ib.errors[1:]
	}
}
