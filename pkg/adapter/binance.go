package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"raw-data-layer/pkg/health"
)

// BinanceAdapter implements Adapter for Binance WebSocket
type BinanceAdapter struct {
	endpoint string
	symbols  []string
	conn     *websocket.Conn
	
	// State
	connected     atomic.Bool
	running       atomic.Bool
	messagesRecv  atomic.Uint64
	reconnectCount atomic.Int32
	startTime     time.Time
	lastMessage   atomic.Value // time.Time
	
	// Configuration
	reconnectAttempts int
	backoffSeconds    []int
	heartbeatInterval time.Duration
	sessionRotation   time.Duration
	
	// Synchronization
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	errors []error
}

// NewBinanceAdapter creates a new Binance WebSocket adapter
func NewBinanceAdapter(endpoint string, symbols []string, config AdapterConfig) *BinanceAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &BinanceAdapter{
		endpoint:          endpoint,
		symbols:           symbols,
		reconnectAttempts: config.ReconnectAttempts,
		backoffSeconds:    config.BackoffSeconds,
		heartbeatInterval: 30 * time.Second,
		sessionRotation:   24 * time.Hour, // Binance doesn't guarantee >24h
		ctx:               ctx,
		cancel:            cancel,
		startTime:         time.Now(),
		errors:            make([]error, 0),
	}
}

// Name returns the adapter name
func (b *BinanceAdapter) Name() string {
	return "BINANCE"
}

// Connect establishes WebSocket connection with paranoid error handling
func (b *BinanceAdapter) Connect(ctx context.Context) error {
	// Never panic - use defer recover
	defer func() {
		if r := recover(); r != nil {
			b.addError(fmt.Errorf("panic in Connect: %v", r))
		}
	}()
	
	// Check if already connected
	if b.connected.Load() {
		return nil
	}
	
	// Connect with timeout. Use a local dialer — websocket.DefaultDialer is a
	// shared package variable and mutating it races with concurrent Connect
	// calls (e.g. receiveLoop's reconnect).
	dialer := &websocket.Dialer{HandshakeTimeout: 10 * time.Second}

	conn, _, err := dialer.DialContext(ctx, b.endpoint, nil)
	if err != nil {
		return NewAdapterError(ErrorConnection, "BINANCE", "failed to connect", err)
	}
	
	b.mu.Lock()
	b.conn = conn
	b.startTime = time.Now()
	b.mu.Unlock()

	b.connected.Store(true)
	
	// Subscribe to symbols
	if err := b.subscribe(); err != nil {
		b.conn.Close()
		b.connected.Store(false)
		return err
	}
	
	return nil
}

// subscribe sends subscription message
// Fixes: empty subscription string bug (project-chrono)
func (b *BinanceAdapter) subscribe() error {
	if len(b.symbols) == 0 {
		return NewAdapterError(ErrorProtocol, "BINANCE", "no symbols to subscribe", nil)
	}
	
	// Build subscription params (avoid empty string)
	params := make([]string, len(b.symbols))
	for i, symbol := range b.symbols {
		params[i] = symbol + "@aggTrade"
	}
	
	subscribeMsg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": params,
		"id":     1,
	}
	
	b.mu.RLock()
	conn := b.conn
	b.mu.RUnlock()
	
	if conn == nil {
		return NewAdapterError(ErrorConnection, "BINANCE", "no connection", nil)
	}
	
	return conn.WriteJSON(subscribeMsg)
}

// Start begins receiving data (runs in goroutine)
func (b *BinanceAdapter) Start(ctx context.Context, output chan<- RawMessage) error {
	if b.running.Load() {
		return fmt.Errorf("adapter already running")
	}
	
	b.running.Store(true)
	
	// Start heartbeat goroutine
	go b.heartbeatLoop()
	
	// Start session rotation goroutine (24h proactive reconnect)
	go b.sessionRotationLoop()
	
	// Start main receive loop
	go b.receiveLoop(ctx, output)
	
	return nil
}

// receiveLoop is the main message receiving loop with paranoid error handling
func (b *BinanceAdapter) receiveLoop(ctx context.Context, output chan<- RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			b.addError(fmt.Errorf("panic in receiveLoop: %v", r))
		}
		b.running.Store(false)
	}()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.ctx.Done():
			return
		default:
			msg, err := b.safeRead()
			if err != nil {
				b.addError(err)
				
				// Auto-reconnect with exponential backoff
				if !b.connected.Load() {
					b.reconnect(ctx)
				}
				continue
			}
			
			if msg != nil {
				// Increment BEFORE the send: the output channel is buffered, so a
				// consumer that receives from it and then reads Stats()/messagesRecv
				// must already see the increment (otherwise a logical race: send →
				// consumer reads → worker finally increments).
				b.messagesRecv.Add(1)
				health.MessagesReceived.WithLabelValues("BINANCE").Inc()
				b.lastMessage.Store(time.Now())

				// Send to output channel with timeout
				select {
				case output <- *msg:
				case <-time.After(1 * time.Second):
					b.messagesRecv.Add(^uint64(0)) // -1: undo the pre-increment (undelivered)
					b.addError(fmt.Errorf("output channel blocked"))
				}
			}
		}
	}
}

// safeRead reads a message with panic recovery
func (b *BinanceAdapter) safeRead() (msg *RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in read: %v", r)
		}
	}()
	
	b.mu.RLock()
	conn := b.conn
	b.mu.RUnlock()
	
	if conn == nil {
		b.connected.Store(false)
		return nil, NewAdapterError(ErrorConnection, "BINANCE", "no connection", nil)
	}
	
	// Read with timeout
	conn.SetReadDeadline(time.Now().Add(35 * time.Second)) // Slightly longer than heartbeat
	
	messageType, payload, err := conn.ReadMessage()
	if err != nil {
		b.connected.Store(false)
		return nil, NewAdapterError(ErrorConnection, "BINANCE", "read failed", err)
	}
	
	if messageType != websocket.TextMessage {
		return nil, nil // Skip non-text messages
	}
	
	// Check if it's a subscription acknowledgement (silently ignore)
	var ack map[string]interface{}
	if err := json.Unmarshal(payload, &ack); err == nil {
		if result, ok := ack["result"]; ok && result == nil {
			return nil, nil // Skip ACK
		}
	}
	
	// Create RawMessage (payload is UNTOUCHED)
	return &RawMessage{
		Source:     "BINANCE",
		Payload:    payload,
		ReceivedAt: time.Now().UnixNano(),
	}, nil
}

// reconnect implements exponential backoff reconnection
func (b *BinanceAdapter) reconnect(ctx context.Context) {
	for attempt := 0; attempt < b.reconnectAttempts; attempt++ {
		// Calculate backoff
		backoffIdx := attempt
		if backoffIdx >= len(b.backoffSeconds) {
			backoffIdx = len(b.backoffSeconds) - 1
		}
		backoff := time.Duration(b.backoffSeconds[backoffIdx]) * time.Second
		
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		
		// Attempt reconnect
		if err := b.Connect(ctx); err != nil {
			b.addError(fmt.Errorf("reconnect attempt %d failed: %w", attempt+1, err))
			continue
		}
		
		b.reconnectCount.Add(1)
		return
	}
	
	b.addError(fmt.Errorf("max reconnect attempts reached"))
}

// heartbeatLoop sends ping/pong every 30 seconds
func (b *BinanceAdapter) heartbeatLoop() {
	ticker := time.NewTicker(b.heartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			if !b.connected.Load() {
				continue
			}
			
			b.mu.RLock()
			conn := b.conn
			b.mu.RUnlock()
			
			if conn == nil {
				continue
			}
			
			// Send ping
			if err := conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				b.addError(fmt.Errorf("heartbeat failed: %w", err))
				b.connected.Store(false)
			}
		}
	}
}

// sessionRotationLoop proactively reconnects before 24h limit
func (b *BinanceAdapter) sessionRotationLoop() {
	ticker := time.NewTicker(b.sessionRotation - 1*time.Hour) // Reconnect 1h before limit
	defer ticker.Stop()
	
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.addError(fmt.Errorf("proactive session rotation (24h limit approaching)"))
			b.Stop()
			b.Connect(b.ctx)
		}
	}
}

// Stop gracefully stops the adapter
func (b *BinanceAdapter) Stop() error {
	b.running.Store(false)
	b.connected.Store(false)
	
	b.mu.Lock()
	if b.conn != nil {
		b.conn.Close()
		b.conn = nil
	}
	b.mu.Unlock()
	
	b.cancel()
	return nil
}

// Health returns current health status
func (b *BinanceAdapter) Health() HealthStatus {
	b.mu.RLock()
	errors := make([]error, len(b.errors))
	copy(errors, b.errors)
	startTime := b.startTime
	b.mu.RUnlock()

	lastMsg := time.Time{}
	if v := b.lastMessage.Load(); v != nil {
		lastMsg = v.(time.Time)
	}

	return HealthStatus{
		Connected:      b.connected.Load(),
		LastMessage:    lastMsg,
		MessagesRecv:   b.messagesRecv.Load(),
		Errors:         errors,
		ReconnectCount: int(b.reconnectCount.Load()),
		Uptime:         time.Since(startTime),
	}
}

// addError adds an error to the error list (max 10)
func (b *BinanceAdapter) addError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	b.errors = append(b.errors, err)
	if len(b.errors) > 10 {
		b.errors = b.errors[1:]
	}
}
