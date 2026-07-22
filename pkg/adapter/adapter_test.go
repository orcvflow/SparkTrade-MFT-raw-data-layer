package adapter

import (
	"context"
	"testing"
	"time"
)

// TestAdapterConfig_Defaults tests default adapter configuration
func TestAdapterConfig_Defaults(t *testing.T) {
	config := AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: 10,
		BackoffSeconds:    []int{1, 2, 4, 8, 16, 30},
		Timeout:           10 * time.Second,
	}

	if !config.Enabled {
		t.Error("Expected Enabled to be true")
	}
	if config.ReconnectAttempts != 10 {
		t.Errorf("Expected ReconnectAttempts=10, got %d", config.ReconnectAttempts)
	}
	if len(config.BackoffSeconds) != 6 {
		t.Errorf("Expected 6 backoff values, got %d", len(config.BackoffSeconds))
	}
}

// TestAdapterError tests error handling
func TestAdapterError(t *testing.T) {
	tests := []struct {
		name     string
		errType  ErrorType
		source   string
		message  string
		baseErr  error
		expected string
	}{
		{
			name:     "Connection error with base error",
			errType:  ErrorConnection,
			source:   "BINANCE",
			message:  "failed to connect",
			baseErr:  context.DeadlineExceeded,
			expected: "BINANCE: failed to connect: context deadline exceeded",
		},
		{
			name:     "Protocol error without base error",
			errType:  ErrorProtocol,
			source:   "IB",
			message:  "invalid message format",
			baseErr:  nil,
			expected: "IB: invalid message format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewAdapterError(tt.errType, tt.source, tt.message, tt.baseErr)
			if err.Error() != tt.expected {
				t.Errorf("Expected error message: %s, got: %s", tt.expected, err.Error())
			}
			if err.Type != tt.errType {
				t.Errorf("Expected error type: %v, got: %v", tt.errType, err.Type)
			}
		})
	}
}

// TestRawMessage_Immutability tests that RawMessage payload is not modified
func TestRawMessage_Immutability(t *testing.T) {
	originalPayload := []byte(`{"test": "data"}`)
	msg := RawMessage{
		Source:     "TEST",
		Payload:    originalPayload,
		ReceivedAt: time.Now().UnixNano(),
	}

	// Verify payload is byte-for-byte identical
	for i, b := range originalPayload {
		if msg.Payload[i] != b {
			t.Errorf("Payload modified at index %d: expected %d, got %d", i, b, msg.Payload[i])
		}
	}
}

// MockAdapter implements Adapter for testing
type MockAdapter struct {
	name           string
	connected      bool
	messagesRecv   uint64
	connectErr     error
	startErr       error
	shouldPanic    bool
}

func NewMockAdapter(name string) *MockAdapter {
	return &MockAdapter{
		name:      name,
		connected: false,
	}
}

func (m *MockAdapter) Connect(ctx context.Context) error {
	if m.connectErr != nil {
		return m.connectErr
	}
	m.connected = true
	return nil
}

func (m *MockAdapter) Start(ctx context.Context, output chan<- RawMessage) error {
	if m.startErr != nil {
		return m.startErr
	}
	
	go func() {
		if m.shouldPanic {
			panic("mock panic")
		}
		
		// Send test message
		msg := RawMessage{
			Source:     m.name,
			Payload:    []byte("test"),
			ReceivedAt: time.Now().UnixNano(),
		}
		select {
		case output <- msg:
			m.messagesRecv++
		case <-ctx.Done():
			return
		}
	}()
	
	return nil
}

func (m *MockAdapter) Stop() error {
	m.connected = false
	return nil
}

func (m *MockAdapter) Name() string {
	return m.name
}

func (m *MockAdapter) Health() HealthStatus {
	return HealthStatus{
		Connected:    m.connected,
		MessagesRecv: m.messagesRecv,
	}
}

// TestMockAdapter tests the mock adapter
func TestMockAdapter(t *testing.T) {
	adapter := NewMockAdapter("MOCK")
	
	if adapter.Name() != "MOCK" {
		t.Errorf("Expected name MOCK, got %s", adapter.Name())
	}
	
	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Errorf("Connect failed: %v", err)
	}
	
	if !adapter.connected {
		t.Error("Expected adapter to be connected")
	}
	
	output := make(chan RawMessage, 1)
	if err := adapter.Start(ctx, output); err != nil {
		t.Errorf("Start failed: %v", err)
	}
	
	// Wait for message
	select {
	case msg := <-output:
		if msg.Source != "MOCK" {
			t.Errorf("Expected source MOCK, got %s", msg.Source)
		}
		if string(msg.Payload) != "test" {
			t.Errorf("Expected payload 'test', got %s", string(msg.Payload))
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for message")
	}
	
	if err := adapter.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
	
	if adapter.connected {
		t.Error("Expected adapter to be disconnected")
	}
}

// TestMockAdapter_ConnectionError tests connection error handling
func TestMockAdapter_ConnectionError(t *testing.T) {
	adapter := NewMockAdapter("MOCK")
	adapter.connectErr = context.DeadlineExceeded
	
	ctx := context.Background()
	if err := adapter.Connect(ctx); err == nil {
		t.Error("Expected Connect to return error")
	}
	
	if adapter.connected {
		t.Error("Expected adapter to not be connected")
	}
}

// TestMockAdapter_StartError tests start error handling
func TestMockAdapter_StartError(t *testing.T) {
	adapter := NewMockAdapter("MOCK")
	adapter.startErr = context.Canceled
	
	ctx := context.Background()
	output := make(chan RawMessage, 1)
	
	if err := adapter.Start(ctx, output); err == nil {
		t.Error("Expected Start to return error")
	}
}

// TestHealthStatus tests health status reporting
func TestHealthStatus(t *testing.T) {
	status := HealthStatus{
		Connected:      true,
		LastMessage:    time.Now(),
		MessagesRecv:   1000,
		MessagesSent:   500,
		Errors:         []error{},
		ReconnectCount: 2,
		Uptime:         1 * time.Hour,
	}
	
	if !status.Connected {
		t.Error("Expected Connected to be true")
	}
	if status.MessagesRecv != 1000 {
		t.Errorf("Expected MessagesRecv=1000, got %d", status.MessagesRecv)
	}
	if status.ReconnectCount != 2 {
		t.Errorf("Expected ReconnectCount=2, got %d", status.ReconnectCount)
	}
}

// BenchmarkRawMessage_Creation benchmarks RawMessage creation
func BenchmarkRawMessage_Creation(b *testing.B) {
	payload := []byte(`{"symbol":"BTCUSDT","price":50000.0}`)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = RawMessage{
			Source:     "BINANCE",
			Payload:    payload,
			ReceivedAt: time.Now().UnixNano(),
		}
	}
}

// BenchmarkMockAdapter_Throughput benchmarks message throughput
func BenchmarkMockAdapter_Throughput(b *testing.B) {
	adapter := NewMockAdapter("MOCK")
	ctx := context.Background()
	adapter.Connect(ctx)
	
	output := make(chan RawMessage, 10000)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adapter.Start(ctx, output)
		<-output
	}
}
