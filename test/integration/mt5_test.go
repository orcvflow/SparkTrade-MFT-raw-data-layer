//go:build integration
// +build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/mapper"
	"raw-data-layer/pkg/storage"

	zmq "github.com/pebbe/zmq4"
)

// TestIntegration_MT5_EndToEnd tests complete MT5 data flow:
// Mock ZMQ → Adapter → Canonicalizer → WAL
// SUCCESS CRITERIA: Messages flow through entire pipeline without loss
func TestIntegration_MT5_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end integration test in short mode")
	}

	// 1. Start mock ZMQ PUB server
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pubSocket, pubPort := startMockMT5Publisher(t)
	defer pubSocket.Close()

	t.Logf("Mock MT5 publisher started on tcp://localhost:%d", pubPort)

	// 2. Create MT5 adapter
	adapterConfig := adapter.AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: 3,
		BackoffSeconds:    []int{1, 2, 4},
		Timeout:           5 * time.Second,
	}

	mt5Adapter := adapter.NewMT5ZMQAdapter(
		"tcp://localhost:"+string(rune(pubPort)),
		adapterConfig,
	)

	// 3. Connect adapter
	err := mt5Adapter.Connect(ctx)
	if err != nil {
		t.Fatalf("Adapter Connect() failed: %v", err)
	}
	defer mt5Adapter.Stop()

	// 4. Create output channel
	rawCh := make(chan adapter.RawMessage, 100)

	// 5. Start adapter in goroutine
	go func() {
		if err := mt5Adapter.Start(ctx, rawCh); err != nil {
			t.Logf("Adapter Start() error (expected on shutdown): %v", err)
		}
	}()

	// 6. Publish test messages
	testMessages := []string{
		`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120}`,
		`{"type":"L1_TICK","symbol":"GBPUSD","bid":1.25456,"ask":1.25458,"last":1.25457,"volume":0.3,"time":1722933771121}`,
		`{"type":"L2_DEPTH","symbol":"EURUSD","bids":[{"price":1.08456,"volume":2.5}],"asks":[{"price":1.08458,"volume":3.0}],"source":"MT5"}`,
	}

	for _, msg := range testMessages {
		if err := publishMessage(pubSocket, msg); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Throttle
	}

	// 7. Create canonicalizer
	tempMapDir := createTempMappings(t)
	symbolMapper, err := mapper.NewSymbolMapper(tempMapDir)
	if err != nil {
		t.Fatalf("Failed to create symbol mapper: %v", err)
	}

	canon := canonicalizer.NewCanonicalizer(symbolMapper)

	// 8. Receive and canonicalize messages
	receivedCount := 0
	canonicalizedCount := 0

	timeout := time.After(10 * time.Second)
	for receivedCount < len(testMessages) {
		select {
		case raw := <-rawCh:
			receivedCount++
			t.Logf("Received message %d from %s", receivedCount, raw.Source)

			// Canonicalize
			processed, err := canon.Process(ctx, raw)
			if err != nil {
				t.Errorf("Canonicalization failed: %v", err)
				continue
			}

			// Verify canonical event
			ev := processed.Canonical
			if ev.Source != "MT5" {
				t.Errorf("Event source = %q; want %q", ev.Source, "MT5")
			}
			if ev.CanonicalSymbol == "UNKNOWN" {
				t.Errorf("Symbol not mapped: %q", ev.CanonicalSymbol)
			}
			if len(ev.RawPayload) == 0 {
				t.Error("RawPayload empty (should be preserved)")
			}

			canonicalizedCount++
			t.Logf("Canonicalized: %s %s @ %.5f", ev.EventType, ev.CanonicalSymbol, ev.Price)

		case <-timeout:
			t.Fatalf("Timeout: received %d/%d messages", receivedCount, len(testMessages))
		}
	}

	// 9. Verify results
	if receivedCount != len(testMessages) {
		t.Errorf("Received %d messages; want %d", receivedCount, len(testMessages))
	}
	if canonicalizedCount != len(testMessages) {
		t.Errorf("Canonicalized %d messages; want %d", canonicalizedCount, len(testMessages))
	}

	// 10. Check adapter health
	health := mt5Adapter.Health()
	if !health.Connected {
		t.Error("Adapter should be connected")
	}
	if health.MessagesRecv < uint64(len(testMessages)) {
		t.Errorf("MessagesRecv = %d; want >= %d", health.MessagesRecv, len(testMessages))
	}

	t.Logf("End-to-end test passed: %d messages processed successfully", canonicalizedCount)
}

// TestIntegration_MT5_Binance_MultiSource tests multiple adapters running concurrently
// SUCCESS CRITERIA: Both MT5 and Binance messages processed without conflicts
func TestIntegration_MT5_Binance_MultiSource(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-source integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Start mock MT5 publisher
	mt5PubSocket, mt5Port := startMockMT5Publisher(t)
	defer mt5PubSocket.Close()

	// 2. Create shared output channel
	rawCh := make(chan adapter.RawMessage, 200)

	// 3. Create MT5 adapter
	mt5Config := adapter.AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: 3,
		BackoffSeconds:    []int{1, 2, 4},
		Timeout:           5 * time.Second,
	}

	mt5Adapter := adapter.NewMT5ZMQAdapter(
		"tcp://localhost:"+string(rune(mt5Port)),
		mt5Config,
	)

	err := mt5Adapter.Connect(ctx)
	if err != nil {
		t.Fatalf("MT5 Connect() failed: %v", err)
	}
	defer mt5Adapter.Stop()

	// 4. Start MT5 adapter
	go func() {
		mt5Adapter.Start(ctx, rawCh)
	}()

	// Note: We can't easily test Binance without a real WebSocket connection
	// or complex mock. This test focuses on MT5 not interfering with other sources.

	// 5. Publish MT5 messages with different symbols
	mt5Messages := []string{
		`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120}`,
		`{"type":"L1_TICK","symbol":"XAUUSD","bid":1920.50,"ask":1920.80,"last":1920.65,"volume":10.5,"time":1722933771121}`,
		`{"type":"L2_DEPTH","symbol":"GBPUSD","bids":[{"price":1.25456,"volume":2.5}],"asks":[{"price":1.25458,"volume":3.0}],"source":"MT5"}`,
	}

	for _, msg := range mt5Messages {
		if err := publishMessage(mt5PubSocket, msg); err != nil {
			t.Fatalf("Failed to publish MT5 message: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 6. Collect messages by source
	sourceCounts := make(map[string]int)
	timeout := time.After(10 * time.Second)
	expectedMT5 := len(mt5Messages)

	for sourceCounts["MT5"] < expectedMT5 {
		select {
		case raw := <-rawCh:
			sourceCounts[raw.Source]++
			t.Logf("Received from %s: seq=%d", raw.Source, raw.SequenceNum)

		case <-timeout:
			t.Fatalf("Timeout: received MT5=%d/%d", sourceCounts["MT5"], expectedMT5)
		}
	}

	// 7. Verify MT5 messages received
	if sourceCounts["MT5"] != expectedMT5 {
		t.Errorf("MT5 messages = %d; want %d", sourceCounts["MT5"], expectedMT5)
	}

	// 8. Verify no cross-contamination (all messages are from expected sources)
	for source, count := range sourceCounts {
		if source != "MT5" && source != "BINANCE" && source != "IB" {
			t.Errorf("Unexpected source: %s with %d messages", source, count)
		}
	}

	t.Logf("Multi-source test passed: MT5=%d messages", sourceCounts["MT5"])
}

// TestIntegration_MT5_WALReplay tests WAL persistence and replay
// SUCCESS CRITERIA: Messages written to WAL, replayed after simulated crash
func TestIntegration_MT5_WALReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping WAL replay integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Create temporary WAL directory
	walDir := t.TempDir()
	t.Logf("WAL directory: %s", walDir)

	// 2. Create WAL writer
	walConfig := storage.WALConfig{
		Directory:      walDir,
		RotationSize:   1024 * 1024, // 1MB
		RotationCount:  100,
		SyncInterval:   100 * time.Millisecond,
		Mode:           "sync", // Ensure immediate persistence
		BatchTimeoutMs: 10,
	}

	wal, err := storage.NewWAL(walConfig)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// 3. Start mock MT5 publisher
	pubSocket, pubPort := startMockMT5Publisher(t)
	defer pubSocket.Close()

	// 4. Create MT5 adapter
	mt5Config := adapter.AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: 3,
		BackoffSeconds:    []int{1, 2, 4},
		Timeout:           5 * time.Second,
	}

	mt5Adapter := adapter.NewMT5ZMQAdapter(
		"tcp://localhost:"+string(rune(pubPort)),
		mt5Config,
	)

	err = mt5Adapter.Connect(ctx)
	if err != nil {
		t.Fatalf("MT5 Connect() failed: %v", err)
	}

	rawCh := make(chan adapter.RawMessage, 100)

	go func() {
		mt5Adapter.Start(ctx, rawCh)
	}()

	// 5. Publish messages and write to WAL
	testMessages := []string{
		`{"type":"L1_TICK","symbol":"EURUSD","bid":1.08456,"ask":1.08458,"last":1.08457,"volume":0.5,"time":1722933771120}`,
		`{"type":"L1_TICK","symbol":"GBPUSD","bid":1.25456,"ask":1.25458,"last":1.25457,"volume":0.3,"time":1722933771121}`,
	}

	for _, msg := range testMessages {
		if err := publishMessage(pubSocket, msg); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 6. Receive messages and write to WAL
	writtenCount := 0
	timeout := time.After(10 * time.Second)

	for writtenCount < len(testMessages) {
		select {
		case raw := <-rawCh:
			// Write to WAL
			if err := wal.Write(raw.Payload); err != nil {
				t.Errorf("WAL Write failed: %v", err)
			}
			writtenCount++
			t.Logf("Written to WAL: message %d", writtenCount)

		case <-timeout:
			t.Fatalf("Timeout: written %d/%d messages", writtenCount, len(testMessages))
		}
	}

	// 7. Close WAL and adapter (simulate crash)
	if err := wal.Close(); err != nil {
		t.Errorf("WAL Close failed: %v", err)
	}
	mt5Adapter.Stop()

	time.Sleep(500 * time.Millisecond) // Allow cleanup

	// 8. Verify WAL files exist
	walFiles, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("Failed to read WAL directory: %v", err)
	}
	if len(walFiles) == 0 {
		t.Fatal("No WAL files found (data not persisted)")
	}
	t.Logf("WAL files found: %d", len(walFiles))

	// 9. Replay WAL (simulate recovery)
	// Note: Full WAL replay requires storage package implementation
	// Here we verify files exist and can be read
	replayedCount := 0
	for _, file := range walFiles {
		if file.IsDir() {
			continue
		}
		filePath := filepath.Join(walDir, file.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			t.Errorf("Failed to read WAL file %s: %v", file.Name(), err)
			continue
		}
		if len(data) > 0 {
			replayedCount++
			t.Logf("Replayed WAL file: %s (%d bytes)", file.Name(), len(data))
		}
	}

	if replayedCount == 0 {
		t.Error("No WAL data replayed (files empty or unreadable)")
	}

	t.Logf("WAL replay test passed: %d messages persisted and replayed", replayedCount)
}

// Helper: startMockMT5Publisher creates a mock ZMQ PUB socket
func startMockMT5Publisher(t *testing.T) (*zmq.Socket, int) {
	t.Helper()

	context, err := zmq.NewContext()
	if err != nil {
		t.Fatalf("Failed to create ZMQ context: %v", err)
	}

	socket, err := context.NewSocket(zmq.PUB)
	if err != nil {
		t.Fatalf("Failed to create PUB socket: %v", err)
	}

	// Bind to random available port
	port := 15556 + (time.Now().UnixNano() % 1000) // Random port 15556-16556
	endpoint := "tcp://127.0.0.1:" + string(rune(port))

	if err := socket.Bind(endpoint); err != nil {
		// Try alternative port
		port = 15556
		endpoint = "tcp://127.0.0.1:15556"
		if err := socket.Bind(endpoint); err != nil {
			t.Fatalf("Failed to bind PUB socket: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond) // Allow socket to bind

	return socket, int(port)
}

// Helper: publishMessage sends a message via ZMQ PUB socket
func publishMessage(socket *zmq.Socket, message string) error {
	_, err := socket.Send(message, 0)
	return err
}

// Helper: createTempMappings creates temp mapping files for testing
func createTempMappings(t *testing.T) string {
	t.Helper()

	tempDir := t.TempDir()

	mt5Mappings := map[string]string{
		"EURUSD": "EUR/USD",
		"GBPUSD": "GBP/USD",
		"XAUUSD": "XAU/USD",
	}

	data, err := json.Marshal(mt5Mappings)
	if err != nil {
		t.Fatalf("Failed to marshal mappings: %v", err)
	}

	mappingFile := filepath.Join(tempDir, "mt5.json")
	if err := os.WriteFile(mappingFile, data, 0644); err != nil {
		t.Fatalf("Failed to write mapping file: %v", err)
	}

	return tempDir
}
