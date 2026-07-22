package publisher

import (
	"testing"
	"time"

	"raw-data-layer/pkg/canonicalizer"
	
	zmq "github.com/pebbe/zmq4"
)

// TestDefaultPublisherConfig tests default configuration
func TestDefaultPublisherConfig(t *testing.T) {
	config := DefaultPublisherConfig()
	
	if config.Endpoint != "tcp://*:5555" {
		t.Errorf("Expected endpoint tcp://*:5555, got %s", config.Endpoint)
	}
	
	if config.HeartbeatInterval != 5*time.Second {
		t.Errorf("Expected heartbeat 5s, got %v", config.HeartbeatInterval)
	}
	
	if config.SendTimeout != 1*time.Second {
		t.Errorf("Expected send timeout 1s, got %v", config.SendTimeout)
	}
}

// TestNewZMQPublisher tests publisher creation
func TestNewZMQPublisher(t *testing.T) {
	config := PublisherConfig{
		Endpoint:          "tcp://*:15555", // Different port for testing
		HeartbeatInterval: 5 * time.Second,
		SendTimeout:       1 * time.Second,
	}
	
	pub, err := NewZMQPublisher(config)
	if err != nil {
		t.Fatalf("NewZMQPublisher failed: %v", err)
	}
	
	if pub == nil {
		t.Fatal("Expected publisher to be created")
	}
	
	if pub.endpoint != "tcp://*:15555" {
		t.Errorf("Expected endpoint tcp://*:15555, got %s", pub.endpoint)
	}
	
	if pub.running.Load() {
		t.Error("Expected publisher to not be running initially")
	}
}

// TestZMQPublisher_Start tests publisher start
func TestZMQPublisher_Start(t *testing.T) {
	config := PublisherConfig{
		Endpoint:          "tcp://*:15556",
		HeartbeatInterval: 5 * time.Second,
		SendTimeout:       1 * time.Second,
	}
	
	pub, _ := NewZMQPublisher(config)
	
	if err := pub.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pub.Stop()
	
	if !pub.running.Load() {
		t.Error("Expected publisher to be running")
	}
	
	if pub.socket == nil {
		t.Error("Expected socket to be initialized")
	}
}

// TestZMQPublisher_Publish tests message publication
func TestZMQPublisher_Publish(t *testing.T) {
	config := PublisherConfig{
		Endpoint:          "tcp://*:15557",
		HeartbeatInterval: 10 * time.Second, // Long interval to avoid heartbeat interference
		SendTimeout:       1 * time.Second,
	}
	
	pub, _ := NewZMQPublisher(config)
	if err := pub.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer pub.Stop()
	
	// Create subscriber
	sub, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}
	defer sub.Close()
	
	if err := sub.Connect("tcp://localhost:15557"); err != nil {
		t.Fatalf("Failed to connect subscriber: %v", err)
	}
	
	// Subscribe to all topics
	sub.SetSubscribe("")
	
	// Give ZeroMQ time to establish connection
	time.Sleep(100 * time.Millisecond)
	
	// Publish event
	event := &canonicalizer.CanonicalEvent{
		EventID:           "evt_test_123",
		Source:            "TEST",
		CanonicalSymbol:   "BTC/USD",
		ExchangeTimestamp: time.Now().UnixNano(),
		LocalHWTimestamp:  time.Now().UnixNano(),
		EventType:         "TRADE",
		Price:             50000.0,
		Size:              1.0,
		Side:              "BUY",
		RawPayload:        []byte("test"),
	}
	
	if err := pub.Publish(event); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	
	// Receive message
	sub.SetRcvtimeo(2 * time.Second)
	msg, err := sub.RecvMessage(0)
	if err != nil {
		t.Fatalf("Failed to receive message: %v", err)
	}
	
	// Verify topic
	if len(msg) < 2 {
		t.Fatalf("Expected multipart message [topic, data], got %d parts", len(msg))
	}
	
	topic := msg[0]
	if topic != "BTC/USD" {
		t.Errorf("Expected topic BTC/USD, got %s", topic)
	}
	
	// Verify stats
	stats := pub.Stats()
	if stats.Published == 0 {
		t.Error("Expected published count > 0")
	}
}

// TestZMQPublisher_TopicFiltering tests topic-based filtering
func TestZMQPublisher_TopicFiltering(t *testing.T) {
	config := PublisherConfig{
		Endpoint:          "tcp://*:15558",
		HeartbeatInterval: 10 * time.Second,
		SendTimeout:       1 * time.Second,
	}
	
	pub, _ := NewZMQPublisher(config)
	pub.Start()
	defer pub.Stop()
	
	// Create subscriber that only subscribes to AAPL
	sub, _ := zmq.NewSocket(zmq.SUB)
	defer sub.Close()
	sub.Connect("tcp://localhost:15558")
	sub.SetSubscribe("AAPL") // Only subscribe to AAPL topic
	sub.SetRcvtimeo(1 * time.Second)
	
	time.Sleep(100 * time.Millisecond)
	
	// Publish BTC/USD (should not be received)
	btcEvent := &canonicalizer.CanonicalEvent{
		CanonicalSymbol: "BTC/USD",
		EventID:         "evt_btc",
		EventType:       "TRADE",
		RawPayload:      []byte("btc"),
	}
	pub.Publish(btcEvent)
	
	// Publish AAPL (should be received)
	aaplEvent := &canonicalizer.CanonicalEvent{
		CanonicalSymbol: "AAPL",
		EventID:         "evt_aapl",
		EventType:       "TRADE",
		RawPayload:      []byte("aapl"),
	}
	pub.Publish(aaplEvent)
	
	// Try to receive (should only get AAPL)
	msg, err := sub.RecvMessage(0)
	if err != nil {
		t.Fatalf("Failed to receive AAPL message: %v", err)
	}
	
	if msg[0] != "AAPL" {
		t.Errorf("Expected AAPL topic, got %s", msg[0])
	}
	
	// Try to receive again (should timeout - no BTC/USD)
	_, err = sub.RecvMessage(0)
	if err == nil {
		t.Error("Expected timeout (should not receive BTC/USD)")
	}
}

// TestZMQPublisher_Heartbeat tests heartbeat functionality
func TestZMQPublisher_Heartbeat(t *testing.T) {
	config := PublisherConfig{
		Endpoint:          "tcp://*:15559",
		HeartbeatInterval: 1 * time.Second, // Fast heartbeat for testing
		SendTimeout:       1 * time.Second,
	}
	
	pub, _ := NewZMQPublisher(config)
	pub.Start()
	defer pub.Stop()
	
	// Create subscriber
	sub, _ := zmq.NewSocket(zmq.SUB)
	defer sub.Close()
	sub.Connect("tcp://localhost:15559")
	sub.SetSubscribe("HEARTBEAT") // Subscribe to heartbeat topic
	sub.SetRcvtimeo(3 * time.Second)
	
	time.Sleep(100 * time.Millisecond)
	
	// Wait for heartbeat
	msg, err := sub.RecvMessage(0)
	if err != nil {
		t.Fatalf("Failed to receive heartbeat: %v", err)
	}
	
	if msg[0] != "HEARTBEAT" {
		t.Errorf("Expected HEARTBEAT topic, got %s", msg[0])
	}
}

// TestZMQPublisher_Stop tests graceful shutdown
func TestZMQPublisher_Stop(t *testing.T) {
	config := PublisherConfig{
		Endpoint:          "tcp://*:15560",
		HeartbeatInterval: 5 * time.Second,
		SendTimeout:       1 * time.Second,
	}
	
	pub, _ := NewZMQPublisher(config)
	pub.Start()
	
	// Publish some events
	for i := 0; i < 10; i++ {
		event := &canonicalizer.CanonicalEvent{
			EventID:         "evt_123",
			CanonicalSymbol: "BTC/USD",
			EventType:       "TRADE",
			RawPayload:      []byte("test"),
		}
		pub.Publish(event)
	}
	
	// Stop should complete without hanging
	if err := pub.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
	
	if pub.running.Load() {
		t.Error("Expected publisher to not be running after Stop")
	}
	
	if pub.socket != nil {
		t.Error("Expected socket to be closed after Stop")
	}
}

// TestZMQPublisher_Stats tests statistics tracking
func TestZMQPublisher_Stats(t *testing.T) {
	config := PublisherConfig{
		Endpoint:          "tcp://*:15561",
		HeartbeatInterval: 10 * time.Second,
		SendTimeout:       1 * time.Second,
	}
	
	pub, _ := NewZMQPublisher(config)
	pub.Start()
	defer pub.Stop()
	
	// Publish events
	for i := 0; i < 5; i++ {
		event := &canonicalizer.CanonicalEvent{
			EventID:         "evt_123",
			CanonicalSymbol: "BTC/USD",
			EventType:       "TRADE",
			RawPayload:      []byte("test"),
		}
		pub.Publish(event)
	}
	
	stats := pub.Stats()
	
	if !stats.Running {
		t.Error("Expected Running=true")
	}
	
	if stats.Published < 5 {
		t.Errorf("Expected published >= 5, got %d", stats.Published)
	}
	
	if stats.LastPublished.IsZero() {
		t.Error("Expected LastPublished to be set")
	}
}

// TestPublisherStats_IsHealthy tests health check
func TestPublisherStats_IsHealthy(t *testing.T) {
	tests := []struct {
		name     string
		stats    PublisherStats
		expected bool
	}{
		{
			name: "Healthy publisher",
			stats: PublisherStats{
				Running:       true,
				Published:     1000,
				Dropped:       5, // 0.5% drop rate
				LastPublished: time.Now(),
			},
			expected: true,
		},
		{
			name: "Not running",
			stats: PublisherStats{
				Running:   false,
				Published: 1000,
			},
			expected: false,
		},
		{
			name: "No publications",
			stats: PublisherStats{
				Running:   true,
				Published: 0,
			},
			expected: false,
		},
		{
			name: "High drop rate (>1%)",
			stats: PublisherStats{
				Running:       true,
				Published:     900,
				Dropped:       100, // 10% drop rate
				LastPublished: time.Now(),
			},
			expected: false,
		},
		{
			name: "Stale publish (>10s)",
			stats: PublisherStats{
				Running:       true,
				Published:     1000,
				Dropped:       5,
				LastPublished: time.Now().Add(-20 * time.Second),
			},
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			healthy := tt.stats.IsHealthy()
			if healthy != tt.expected {
				t.Errorf("Expected IsHealthy=%v, got %v", tt.expected, healthy)
			}
		})
	}
}

// TestZMQPublisher_ThreadSafety tests concurrent publication
// This verifies the Homalos fix for race condition
func TestZMQPublisher_ThreadSafety(t *testing.T) {
	config := PublisherConfig{
		Endpoint:          "tcp://*:15562",
		HeartbeatInterval: 10 * time.Second,
		SendTimeout:       1 * time.Second,
	}
	
	pub, _ := NewZMQPublisher(config)
	pub.Start()
	defer pub.Stop()
	
	// Concurrent publications
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				event := &canonicalizer.CanonicalEvent{
					EventID:         "evt_123",
					CanonicalSymbol: "BTC/USD",
					EventType:       "TRADE",
					RawPayload:      []byte("test"),
				}
				pub.Publish(event)
			}
			done <- true
		}(i)
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
	
	stats := pub.Stats()
	if stats.Published < 1000 {
		t.Errorf("Expected published >= 1000, got %d", stats.Published)
	}
}

// BenchmarkZMQPublisher_Publish benchmarks publication performance
func BenchmarkZMQPublisher_Publish(b *testing.B) {
	config := PublisherConfig{
		Endpoint:          "tcp://*:15563",
		HeartbeatInterval: 1 * time.Hour, // No heartbeat during benchmark
		SendTimeout:       1 * time.Second,
	}
	
	pub, _ := NewZMQPublisher(config)
	pub.Start()
	defer pub.Stop()
	
	event := &canonicalizer.CanonicalEvent{
		EventID:         "evt_bench",
		CanonicalSymbol: "BTC/USD",
		EventType:       "TRADE",
		Price:           50000.0,
		Size:            1.0,
		RawPayload:      []byte("benchmark"),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pub.Publish(event)
	}
}
