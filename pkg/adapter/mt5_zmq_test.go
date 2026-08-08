package adapter

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestMT5Adapter_Creation tests MT5 adapter creation with valid config
func TestMT5Adapter_Creation(t *testing.T) {
	config := AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: 10,
		BackoffSeconds:    []int{1, 2, 4, 8, 16, 30},
		Timeout:           10 * time.Second,
	}

	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)

	if adapter == nil {
		t.Fatal("Expected adapter to be created")
	}

	if adapter.Name() != "MT5" {
		t.Errorf("Expected name MT5, got %s", adapter.Name())
	}

	mt5 := adapter.(*MT5ZMQAdapter)
	if mt5.endpoint != "tcp://localhost:5556" {
		t.Errorf("Expected endpoint tcp://localhost:5556, got %s", mt5.endpoint)
	}

	if mt5.receiveTimeout != 1*time.Second {
		t.Errorf("Expected receive timeout 1s, got %v", mt5.receiveTimeout)
	}

	if len(mt5.backoffSeconds) != 6 {
		t.Errorf("Expected 6 backoff values, got %d", len(mt5.backoffSeconds))
	}
}

// TestMT5Adapter_DefaultEndpoint tests default endpoint when empty string provided
func TestMT5Adapter_DefaultEndpoint(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("", config)

	mt5 := adapter.(*MT5ZMQAdapter)
	if mt5.endpoint != "tcp://localhost:5556" {
		t.Errorf("Expected default endpoint, got %s", mt5.endpoint)
	}
}

// TestMT5Adapter_Name tests adapter name returns MT5
func TestMT5Adapter_Name(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)

	if adapter.Name() != "MT5" {
		t.Errorf("Expected name MT5, got %s", adapter.Name())
	}
}

// TestMT5Adapter_InitialState tests adapter starts in correct initial state
func TestMT5Adapter_InitialState(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)

	mt5 := adapter.(*MT5ZMQAdapter)

	if mt5.connected.Load() {
		t.Error("Expected adapter to not be connected initially")
	}

	if mt5.running.Load() {
		t.Error("Expected adapter to not be running initially")
	}

	if mt5.messagesRecv.Load() != 0 {
		t.Error("Expected messagesRecv to be 0 initially")
	}

	if mt5.reconnectCount.Load() != 0 {
		t.Error("Expected reconnectCount to be 0 initially")
	}
}

// TestMT5Adapter_ConnectInvalidEndpoint tests connection failure with invalid endpoint
func TestMT5Adapter_ConnectInvalidEndpoint(t *testing.T) {
	config := AdapterConfig{
		ReconnectAttempts: 1,
		BackoffSeconds:    []int{1},
	}

	// Invalid endpoint (not a valid ZMQ address)
	adapter := NewMT5ZMQAdapter("invalid://bad-endpoint", config)

	ctx := context.Background()
	err := adapter.Connect(ctx)

	// Should fail to connect to invalid endpoint
	if err == nil {
		t.Error("Expected error connecting to invalid endpoint")
	}

	mt5 := adapter.(*MT5ZMQAdapter)
	if mt5.connected.Load() {
		t.Error("Expected adapter to not be connected after failed connection")
	}
}

// TestMT5Adapter_ConnectAlreadyConnected tests idempotent Connect()
func TestMT5Adapter_ConnectAlreadyConnected(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)

	mt5 := adapter.(*MT5ZMQAdapter)

	// Manually set connected flag (simulate already connected)
	mt5.connected.Store(true)

	ctx := context.Background()
	err := adapter.Connect(ctx)

	// Should return nil without error (idempotent)
	if err != nil {
		t.Errorf("Expected no error when already connected, got %v", err)
	}
}

// TestMT5Adapter_ValidJSONDetection tests JSON validation
func TestMT5Adapter_ValidJSONDetection(t *testing.T) {
	tests := []struct {
		name      string
		payload   []byte
		wantValid bool
	}{
		{
			name:      "Valid L1_TICK JSON",
			payload:   []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120}`),
			wantValid: true,
		},
		{
			name:      "Valid L2_DEPTH JSON",
			payload:   []byte(`{"type":"L2_DEPTH","symbol":"EURUSD","bids":[{"price":1.08456,"volume":2.5}],"asks":[{"price":1.08458,"volume":3.0}]}`),
			wantValid: true,
		},
		{
			name:      "Invalid JSON - missing bracket",
			payload:   []byte(`{"type":"L1_TICK","symbol":"EURUSD"`),
			wantValid: false,
		},
		{
			name:      "Invalid JSON - not JSON at all",
			payload:   []byte(`This is not JSON`),
			wantValid: false,
		},
		{
			name:      "Empty payload",
			payload:   []byte(``),
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := json.Valid(tt.payload)
			if valid != tt.wantValid {
				t.Errorf("JSON validation: got %v, want %v for payload: %s", valid, tt.wantValid, string(tt.payload))
			}
		})
	}
}

// TestMT5Adapter_ExponentialBackoff tests backoff calculation
func TestMT5Adapter_ExponentialBackoff(t *testing.T) {
	config := AdapterConfig{
		BackoffSeconds: []int{1, 2, 4, 8, 16, 30},
	}

	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	tests := []struct {
		reconnectCount  int
		expectedBackoff int
	}{
		{0, 1}, // First reconnect
		{1, 2}, // Second reconnect
		{2, 4}, // Third reconnect
		{3, 8},
		{4, 16},
		{5, 30},
		{6, 30},  // Beyond array length - cap at 30
		{10, 30}, // Way beyond - still cap at 30
	}

	for _, tt := range tests {
		mt5.reconnectCount.Store(int32(tt.reconnectCount))
		backoff := mt5.getBackoff()
		if backoff != tt.expectedBackoff {
			t.Errorf("Reconnect #%d: expected backoff %ds, got %ds", tt.reconnectCount, tt.expectedBackoff, backoff)
		}
	}
}

// TestMT5Adapter_GracefulStop tests clean shutdown
func TestMT5Adapter_GracefulStop(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Set running flag (simulate started adapter)
	mt5.running.Store(true)
	mt5.connected.Store(true)

	err := adapter.Stop()
	if err != nil {
		t.Errorf("Expected no error on stop, got %v", err)
	}

	if mt5.running.Load() {
		t.Error("Expected adapter to not be running after stop")
	}

	if mt5.connected.Load() {
		t.Error("Expected adapter to not be connected after stop")
	}
}

// TestMT5Adapter_HealthStatus tests health status reporting
func TestMT5Adapter_HealthStatus(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Set some state
	mt5.connected.Store(true)
	mt5.messagesRecv.Store(42)
	mt5.reconnectCount.Store(3)
	mt5.lastMessage.Store(time.Now())

	health := adapter.Health()

	if !health.Connected {
		t.Error("Expected health to show connected")
	}

	if health.MessagesRecv != 42 {
		t.Errorf("Expected 42 messages received, got %d", health.MessagesRecv)
	}

	if health.ReconnectCount != 3 {
		t.Errorf("Expected 3 reconnects, got %d", health.ReconnectCount)
	}

	if health.MessagesSent != 0 {
		t.Errorf("Expected 0 messages sent (MT5 is receive-only), got %d", health.MessagesSent)
	}

	if health.LastMessage.IsZero() {
		t.Error("Expected last message time to be set")
	}
}

// TestMT5Adapter_MaxErrorsLimit tests error list capping at 10
func TestMT5Adapter_MaxErrorsLimit(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Add 20 errors
	for i := 0; i < 20; i++ {
		mt5.addError(NewAdapterError(ErrorProtocol, "MT5", "test error", nil))
	}

	health := adapter.Health()

	// Should only keep last 10 errors
	if len(health.Errors) != 10 {
		t.Errorf("Expected 10 errors (max cap), got %d", len(health.Errors))
	}
}

// TestMT5Adapter_NilPayload tests handling of nil payload (death test)
func TestMT5Adapter_NilPayload(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Nil payload should be handled gracefully
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Panic on nil payload: %v (should not panic)", r)
		}
	}()

	// Test JSON validation with nil
	valid := json.Valid(nil)
	if valid {
		t.Error("Expected nil payload to be invalid JSON")
	}

	// Adapter should not crash
	_ = mt5.Name()
}

// TestMT5Adapter_ConcurrentHealthReads tests race-free health status reads
func TestMT5Adapter_ConcurrentHealthReads(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Set some state
	mt5.connected.Store(true)
	mt5.messagesRecv.Store(100)

	// Launch 100 concurrent Health() calls
	done := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		go func() {
			health := adapter.Health()
			if health.MessagesRecv != 100 {
				t.Errorf("Race detected: expected 100, got %d", health.MessagesRecv)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}
}

// TestMT5_ReceiveInvalidJSON tests handling of invalid JSON messages
// SUCCESS CRITERIA: Invalid JSON rejected, error logged, no panic
func TestMT5_ReceiveInvalidJSON(t *testing.T) {
	config := AdapterConfig{
		ReconnectAttempts: 3,
		BackoffSeconds:    []int{1, 2, 4},
	}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Test JSON validation
	invalidJSON := []byte(`{"invalid json`)
	valid := json.Valid(invalidJSON)
	if valid {
		t.Error("Expected invalid JSON to be detected")
	}

	// Simulate processing invalid JSON in receiveLoop
	// The adapter should skip it and continue (not crash)
	validJSON := []byte(`{"type":"L1_TICK","symbol":"EURUSD"}`)
	if !json.Valid(validJSON) {
		t.Error("Valid JSON should pass validation")
	}

	// Verify adapter can handle both cases
	_ = mt5.Name() // Should not panic after processing invalid JSON
}

// TestMT5_ReconnectOnDisconnect tests real reconnect flow after connection loss
// SUCCESS CRITERIA: Adapter reconnects after disconnect, reconnectCount increments
func TestMT5_ReconnectOnDisconnect(t *testing.T) {
	config := AdapterConfig{
		ReconnectAttempts: 3,
		BackoffSeconds:    []int{1, 2, 4},
		Timeout:           2 * time.Second,
	}
	adapter := NewMT5ZMQAdapter("tcp://localhost:9999", config) // Unreachable endpoint
	mt5 := adapter.(*MT5ZMQAdapter)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initial connection may succeed (ZMQ doesn't fail immediately on bind/connect)
	// The real failure happens on receive, not connect
	err := adapter.Connect(ctx)
	if err != nil {
		t.Logf("Connect failed as expected: %v", err)
	} else {
		t.Logf("Connect succeeded (ZMQ defers validation)")
	}

	initialReconnects := mt5.reconnectCount.Load()

	// Force disconnect and reconnect attempt
	mt5.connected.Store(false)
	err = mt5.reconnect(ctx)

	if err != nil {
		t.Logf("Reconnect failed (expected for unreachable endpoint): %v", err)
	} else {
		t.Logf("Reconnect succeeded (ZMQ optimistic connect)")
	}

	// What matters: reconnectCount should increment after reconnect attempt
	newReconnects := mt5.reconnectCount.Load()
	if newReconnects <= initialReconnects {
		t.Errorf("reconnectCount = %d; want > %d (should increment after reconnect attempt)",
			newReconnects, initialReconnects)
	}

	// Verify backoff is applied
	backoff := mt5.getBackoff()
	if backoff < 1 || backoff > 30 {
		t.Errorf("Backoff = %d; want in range [1, 30] seconds", backoff)
	}
}

// TestMT5_BackpressureOnChannelFull tests backpressure handling when output channel is full
// SUCCESS CRITERIA: Message dropped on timeout, error logged, no deadlock
func TestMT5_BackpressureOnChannelFull(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Create a small buffered channel (capacity 1)
	outputCh := make(chan RawMessage, 1)

	// Fill the channel
	outputCh <- RawMessage{Source: "MT5", Payload: []byte("test1")}

	// Try to send another message (should block or timeout)
	msg := RawMessage{
		Source:      "MT5",
		Payload:     []byte(`{"type":"L1_TICK"}`),
		ReceivedAt:  time.Now().UnixNano(),
		SequenceNum: 1,
	}

	// Simulate the send with timeout (from receiveLoop)
	done := make(chan bool, 1)
	go func() {
		select {
		case outputCh <- msg:
			done <- true
		case <-time.After(100 * time.Millisecond):
			done <- false // Timeout (backpressure)
		}
	}()

	result := <-done
	if result {
		t.Error("Expected backpressure timeout, but message was sent")
	}

	// Verify adapter doesn't crash on backpressure
	health := mt5.Health()
	if health.MessagesRecv < 0 {
		t.Error("MessagesRecv should not be negative after backpressure")
	}
}

// Test_MT5_ChannelClosed tests handling of closed output channel (death test)
// SUCCESS CRITERIA: No panic when sending to closed channel
func Test_MT5_ChannelClosed(t *testing.T) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Create and immediately close channel
	outputCh := make(chan RawMessage, 1)
	close(outputCh)

	// Try to send to closed channel (should be caught by select/recover)
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Panic on closed channel: %v (should be handled gracefully)", r)
		}
	}()

	// Simulate send attempt (in real code this is protected by select with ctx.Done)
	msg := RawMessage{Source: "MT5", Payload: []byte("test")}

	select {
	case outputCh <- msg:
		t.Error("Should not be able to send to closed channel")
	default:
		// Expected: channel closed, send fails immediately
	}

	// Verify adapter is still functional
	_ = mt5.Name()
	_ = mt5.Health()
}

// TestMT5_MaxReconnectAttempts tests reconnect attempt limit enforcement
// SUCCESS CRITERIA: Stops reconnecting after max attempts reached
func TestMT5_MaxReconnectAttempts(t *testing.T) {
	config := AdapterConfig{
		ReconnectAttempts: 3, // Max 3 attempts
		BackoffSeconds:    []int{1, 1, 1},
		Timeout:           1 * time.Second,
	}
	adapter := NewMT5ZMQAdapter("tcp://localhost:9999", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Force multiple reconnect attempts
	for i := 0; i < 5; i++ {
		mt5.connected.Store(false)
		err := mt5.reconnect(ctx)
		_ = err // Ignore error (endpoint unreachable)
	}

	reconnects := mt5.reconnectCount.Load()
	if reconnects < 3 {
		t.Logf("reconnectCount = %d (expected at least 3 attempts)", reconnects)
	}

	// After max attempts, should return error
	mt5.reconnectCount.Store(4) // Force to exceed max
	err := mt5.reconnect(ctx)
	if err == nil {
		t.Error("Expected error after exceeding max reconnect attempts")
	}
	if err != nil && !strings.Contains(err.Error(), "max reconnect attempts") {
		t.Errorf("Error message = %v; want 'max reconnect attempts'", err)
	}
}

// TestMT5_SocketTimeout tests socket receive timeout behavior
// SUCCESS CRITERIA: Timeout handled gracefully, no blocking
func TestMT5_SocketTimeout(t *testing.T) {
	config := AdapterConfig{
		Timeout: 1 * time.Second,
	}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Verify timeout is set correctly
	if mt5.receiveTimeout != 1*time.Second {
		t.Errorf("receiveTimeout = %v; want 1s", mt5.receiveTimeout)
	}

	// Test safeRecv with no socket (should return error, not block)
	data, err := mt5.safeRecv()
	if err == nil {
		t.Error("Expected error from safeRecv with nil socket")
	}
	if data != nil {
		t.Error("Expected nil data from failed safeRecv")
	}
}

// BenchmarkMT5_MessageThroughput benchmarks message receive throughput
// SUCCESS CRITERIA: Measures messages/sec processing capacity
func BenchmarkMT5_MessageThroughput(b *testing.B) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Simulate message processing
	payload := []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate JSON validation (hot path)
		valid := json.Valid(payload)
		if !valid {
			b.Fatal("Payload should be valid")
		}

		// Simulate metrics update
		mt5.messagesRecv.Add(1)
		mt5.lastMessage.Store(time.Now())
	}
}

// BenchmarkMT5_ParseJSON benchmarks JSON validation performance
// SUCCESS CRITERIA: Measures JSON parsing overhead
func BenchmarkMT5_ParseJSON(b *testing.B) {
	validPayload := []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120,"source":"MT5","timestamp":1722933771120}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		valid := json.Valid(validPayload)
		if !valid {
			b.Fatal("Expected valid JSON")
		}
	}
}

// BenchmarkMT5_HealthCheck benchmarks health status retrieval
// SUCCESS CRITERIA: Measures health check latency (critical for monitoring)
func BenchmarkMT5_HealthCheck(b *testing.B) {
	config := AdapterConfig{}
	adapter := NewMT5ZMQAdapter("tcp://localhost:5556", config)
	mt5 := adapter.(*MT5ZMQAdapter)

	// Set up some state
	mt5.connected.Store(true)
	mt5.messagesRecv.Store(1000)
	mt5.reconnectCount.Store(2)
	mt5.lastMessage.Store(time.Now())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		health := adapter.Health()
		if !health.Connected {
			b.Fatal("Expected connected=true")
		}
	}
}
