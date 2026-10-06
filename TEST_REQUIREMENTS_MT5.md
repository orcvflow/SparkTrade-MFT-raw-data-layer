# MT5 Test Requirements (20-Year Veteran Standards)

## 📋 Required Tests (ABSOLUTE MINIMUM for Production)

### 1. pkg/adapter/mt5_zmq_test.go (15 tests minimum)

#### Unit Tests (Mock ZMQ Socket):
```go
// TestMT5_Connect — Happy path
func TestMT5_Connect(t *testing.T) {
    adapter := adapter.NewMT5ZMQAdapter("tcp://localhost:5556", config)
    err := adapter.Connect(context.Background())
    assert.NoError(t, err)
    assert.True(t, adapter.Health().Connected)
}

// TestMT5_ConnectAlreadyConnected — Idempotence
func TestMT5_ConnectAlreadyConnected(t *testing.T) {
    // Connect twice → no error, same socket
}

// TestMT5_ConnectInvalidEndpoint — Error handling
func TestMT5_ConnectInvalidEndpoint(t *testing.T) {
    adapter := adapter.NewMT5ZMQAdapter("invalid://endpoint", config)
    err := adapter.Connect(context.Background())
    assert.Error(t, err)
}

// TestMT5_ReceiveValidJSON — L1_TICK parse
func TestMT5_ReceiveValidJSON(t *testing.T) {
    // Mock ZMQ → send L1_TICK JSON
    // Verify: RawMessage created, metrics updated
}

// TestMT5_ReceiveInvalidJSON — Graceful error
func TestMT5_ReceiveInvalidJSON(t *testing.T) {
    // Mock ZMQ → send corrupted JSON
    // Verify: no panic, error logged, continue running
}

// TestMT5_ReconnectOnDisconnect — Exponential backoff
func TestMT5_ReconnectOnDisconnect(t *testing.T) {
    // Mock ZMQ → close socket after 5 messages
    // Verify: adapter detects, reconnects, resumes
    // Verify: reconnect_count incremented
}

// TestMT5_BackpressureOnChannelFull — Non-blocking send
func TestMT5_BackpressureOnChannelFull(t *testing.T) {
    output := make(chan adapter.RawMessage) // No buffer
    // Don't drain channel
    // Verify: timeout after 5s, metrics decremented
}

// TestMT5_GracefulStop — No panic
func TestMT5_GracefulStop(t *testing.T) {
    adapter.Start(ctx, output)
    time.Sleep(100 * time.Millisecond)
    err := adapter.Stop()
    assert.NoError(t, err)
    assert.False(t, adapter.Health().Connected)
}

// TestMT5_HealthStatus — All fields populated
func TestMT5_HealthStatus(t *testing.T) {
    health := adapter.Health()
    assert.NotZero(t, health.MessagesRecv)
    assert.NotZero(t, health.LastMessage)
}

// TestMT5_MaxErrors — Error list cap
func TestMT5_MaxErrors(t *testing.T) {
    // Add 20 errors
    // Verify: only last 10 kept
}
```

#### Race Tests:
```go
// TestMT5_ConcurrentHealthReads — No data race
func TestMT5_ConcurrentHealthReads(t *testing.T) {
    for i := 0; i < 100; i++ {
        go adapter.Health()
    }
    // Run with: go test -race
}
```

#### Benchmark:
```go
// BenchmarkMT5_MessageThroughput
func BenchmarkMT5_MessageThroughput(b *testing.B) {
    // Target: >10K msg/s
}
```

### 2. pkg/canonicalizer/mt5_test.go (10 tests minimum)

```go
// TestMT5_ParseL1Tick — Happy path
func TestMT5_ParseL1Tick(t *testing.T) {
    raw := adapter.RawMessage{
        Payload: []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120}`),
    }
    ev, err := canon.parseMT5(raw)
    assert.NoError(t, err)
    assert.Equal(t, "QUOTE", ev.EventType)
    assert.Equal(t, "EUR/USD", ev.CanonicalSymbol)
    assert.Equal(t, 1.08457, ev.Price)
    assert.NotNil(t, ev.ForexMetadata)
}

// TestMT5_ParseL2Depth — Order book
func TestMT5_ParseL2Depth(t *testing.T) {
    raw := adapter.RawMessage{
        Payload: []byte(`{"type":"L2_DEPTH","symbol":"EURUSD","bids":[{"price":1.08456,"volume":2.5}],"asks":[{"price":1.08458,"volume":3.0}]}`),
    }
    ev, err := canon.parseMT5(raw)
    assert.NoError(t, err)
    assert.Equal(t, "BOOK_SNAPSHOT", ev.EventType)
    assert.Len(t, ev.Levels, 2)
}

// TestMT5_InvalidJSON — No panic
func TestMT5_InvalidJSON(t *testing.T) {
    raw := adapter.RawMessage{Payload: []byte(`{invalid`)}
    ev, err := canon.parseMT5(raw)
    assert.Error(t, err)
    assert.Equal(t, "UNKNOWN", ev.EventType) // Fallback
}

// TestMT5_SanitizeNegativePrice — Paranoid math
func TestMT5_SanitizeNegativePrice(t *testing.T) {
    raw := adapter.RawMessage{
        Payload: []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":-1.0,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120}`),
    }
    ev, err := canon.parseMT5(raw)
    assert.NoError(t, err)
    assert.Equal(t, 0.0, ev.ForexMetadata.Bid) // Sanitized
}

// TestMT5_UnknownSymbol — Symbol mapper fallback
func TestMT5_UnknownSymbol(t *testing.T) {
    raw := adapter.RawMessage{
        Payload: []byte(`{"type":"L1_TICK","symbol":"UNKNOWN123","bid":1.0,"ask":1.1,"last":1.05,"volume":1.0,"time":1722933771120}`),
    }
    ev, err := canon.parseMT5(raw)
    assert.NoError(t, err)
    // Should use original symbol if mapping fails
}

// TestMT5_RawPayloadPreserved — Byte-for-byte
func TestMT5_RawPayloadPreserved(t *testing.T) {
    payload := []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120}`)
    raw := adapter.RawMessage{Payload: payload}
    ev, err := canon.parseMT5(raw)
    assert.NoError(t, err)
    assert.Equal(t, payload, ev.RawPayload) // EXACT match
}
```

### 3. test/integration/mt5_test.go (3 tests minimum)

```go
// TestIntegration_MT5_EndToEnd — Full pipeline
func TestIntegration_MT5_EndToEnd(t *testing.T) {
    // 1. Start mock MT5 ZMQ PUB (sends L1_TICK JSON)
    // 2. Start adapter
    // 3. Start canonicalizer
    // 4. Verify canonical event created
    // 5. Verify symbol mapping (EURUSD → EUR/USD)
}

// TestIntegration_MT5_Binance_MultiSource — Both adapters
func TestIntegration_MT5_Binance_MultiSource(t *testing.T) {
    // 1. Start MT5 + Binance adapters
    // 2. Both send data simultaneously
    // 3. Verify no message loss
    // 4. Verify correct symbol mapping for both
}

// TestIntegration_MT5_WALReplay — Lossless persistence
func TestIntegration_MT5_WALReplay(t *testing.T) {
    // 1. Send MT5 events to pipeline
    // 2. Kill DolphinDB
    // 3. Verify events written to WAL
    // 4. Restart DolphinDB
    // 5. Verify WAL replayed
}
```

---

## 🎯 Test Coverage Targets (Industry Standard)

| Component | Line Coverage | Branch Coverage | Must Have |
|-----------|---------------|-----------------|-----------|
| MT5 Adapter | **≥ 85%** | **≥ 80%** | ✅ Happy path + error cases |
| MT5 Canonicalizer | **≥ 90%** | **≥ 85%** | ✅ All JSON types + sanitization |
| Integration | **≥ 70%** | **≥ 60%** | ✅ End-to-end scenarios |

---

## 🚨 Critical Tests (Death Tests)

Bu testlər olmadan production-a QOYMA!

```go
// Test_MT5_NilPayload — No panic
func Test_MT5_NilPayload(t *testing.T) {
    raw := adapter.RawMessage{Payload: nil}
    ev, err := canon.parseMT5(raw)
    assert.Error(t, err)
    assert.Equal(t, "UNKNOWN", ev.EventType)
}

// Test_MT5_OverflowPrice — Sanitize 1e308
func Test_MT5_OverflowPrice(t *testing.T) {
    raw := adapter.RawMessage{
        Payload: []byte(`{"type":"L1_TICK","symbol":"EURUSD","bid":1e308,"ask":1e308,"last":1e308,"volume":0.5,"time":1722933771120}`),
    }
    ev, err := canon.parseMT5(raw)
    assert.NoError(t, err)
    assert.Equal(t, 0.0, ev.Price) // math.IsInf → 0.0
}

// Test_MT5_ChannelClosed — No panic on send
func Test_MT5_ChannelClosed(t *testing.T) {
    output := make(chan adapter.RawMessage)
    close(output) // Close before adapter sends
    // Verify: adapter detects, logs error, doesn't crash
}
```

---

## ⏱️ Time Estimate

| Task | Lines of Code | Time |
|------|---------------|------|
| `mt5_zmq_test.go` | ~400 lines | **2 hours** |
| `mt5_test.go` | ~250 lines | **1.5 hours** |
| `mt5_integration_test.go` | ~150 lines | **1 hour** |
| **Total** | ~800 lines | **4.5 hours** |

---

## 📝 Next Steps

1. **Write tests BEFORE claiming "Task Complete"**
2. **Run `go test ./pkg/... -race -cover`**
3. **Target: ALL tests green, coverage ≥ 85%**
4. **Only THEN move to Task #4 (Main Wiring)**

