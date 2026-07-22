package validation

import (
	"testing"
	"time"

	"raw-data-layer/pkg/canonicalizer"
)

// createValidEvent creates a valid canonical event for testing
func createValidEvent() *canonicalizer.CanonicalEvent {
	return &canonicalizer.CanonicalEvent{
		EventID:           "evt_123",
		Source:            "BINANCE",
		CanonicalSymbol:   "BTC/USD",
		ExchangeTimestamp: time.Now().UnixNano(),
		LocalHWTimestamp:  time.Now().UnixNano(),
		EventType:         "TRADE",
		Price:             50000.0,
		Size:              1.0,
		Side:              "BUY",
		RawPayload:        []byte("test"),
		RawFormat:         "JSON",
	}
}

// TestDefaultValidationRules tests default rules
func TestDefaultValidationRules(t *testing.T) {
	rules := DefaultValidationRules()
	
	if rules.MaxTimestampAge != 1*time.Hour {
		t.Errorf("Expected MaxTimestampAge=1h, got %v", rules.MaxTimestampAge)
	}
	
	if len(rules.RequiredFields) != 4 {
		t.Errorf("Expected 4 required fields, got %d", len(rules.RequiredFields))
	}
	
	if rules.MinPrice != 0.0 {
		t.Errorf("Expected MinPrice=0.0, got %f", rules.MinPrice)
	}
	
	if rules.MaxPrice != 1e9 {
		t.Errorf("Expected MaxPrice=1e9, got %f", rules.MaxPrice)
	}
	
	if rules.MaxErrorRate != 0.1 {
		t.Errorf("Expected MaxErrorRate=0.1, got %f", rules.MaxErrorRate)
	}
	
	if rules.MaxLatencyMs != 500 {
		t.Errorf("Expected MaxLatencyMs=500, got %d", rules.MaxLatencyMs)
	}
}

// TestNewValidator tests validator creation
func TestNewValidator(t *testing.T) {
	rules := DefaultValidationRules()
	validator := NewValidator(rules)
	
	if validator == nil {
		t.Fatal("Expected validator to be created")
	}
	
	if validator.totalValidated != 0 {
		t.Error("Expected totalValidated=0")
	}
	
	if validator.totalPassed != 0 {
		t.Error("Expected totalPassed=0")
	}
}

// TestValidator_ValidateValidEvent tests validation of a valid event
func TestValidator_ValidateValidEvent(t *testing.T) {
	rules := DefaultValidationRules()
	rules.AllowUnknownSymbol = true // Allow UNKNOWN for testing
	validator := NewValidator(rules)
	
	event := createValidEvent()
	processingStart := time.Now().UnixNano()
	
	result := validator.Validate(event, processingStart)
	
	if !result.Passed {
		t.Errorf("Expected validation to pass, got: %+v", result)
	}
	
	if result.EventID != "evt_123" {
		t.Errorf("Expected EventID evt_123, got %s", result.EventID)
	}
	
	if len(result.Layers) != 5 {
		t.Errorf("Expected 5 layers, got %d", len(result.Layers))
	}
	
	// Check all layers passed
	for name, layer := range result.Layers {
		if !layer.Passed {
			t.Errorf("Layer %s failed: %s", name, layer.Message)
		}
	}
}

// TestValidator_Layer1_Connectivity tests connectivity validation
func TestValidator_Layer1_Connectivity(t *testing.T) {
	rules := DefaultValidationRules()
	validator := NewValidator(rules)
	
	tests := []struct {
		name     string
		event    *canonicalizer.CanonicalEvent
		expected bool
	}{
		{
			name:     "Valid connectivity",
			event:    createValidEvent(),
			expected: true,
		},
		{
			name: "Old timestamp",
			event: &canonicalizer.CanonicalEvent{
				EventID:           "evt_123",
				Source:            "BINANCE",
				ExchangeTimestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
				LocalHWTimestamp:  time.Now().UnixNano(),
				RawPayload:        []byte("test"),
			},
			expected: false,
		},
		{
			name: "Missing local timestamp",
			event: &canonicalizer.CanonicalEvent{
				EventID:           "evt_123",
				Source:            "BINANCE",
				ExchangeTimestamp: time.Now().UnixNano(),
				LocalHWTimestamp:  0,
				RawPayload:        []byte("test"),
			},
			expected: false,
		},
		{
			name: "Unknown source",
			event: &canonicalizer.CanonicalEvent{
				EventID:           "evt_123",
				Source:            "UNKNOWN",
				ExchangeTimestamp: time.Now().UnixNano(),
				LocalHWTimestamp:  time.Now().UnixNano(),
				RawPayload:        []byte("test"),
			},
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layer := validator.validateConnectivity(tt.event)
			if layer.Passed != tt.expected {
				t.Errorf("Expected passed=%v, got %v. Message: %s", 
					tt.expected, layer.Passed, layer.Message)
			}
		})
	}
}

// TestValidator_Layer2_Protocol tests protocol compliance validation
func TestValidator_Layer2_Protocol(t *testing.T) {
	rules := DefaultValidationRules()
	validator := NewValidator(rules)
	
	tests := []struct {
		name     string
		event    *canonicalizer.CanonicalEvent
		expected bool
	}{
		{
			name:     "Valid protocol",
			event:    createValidEvent(),
			expected: true,
		},
		{
			name: "Missing EventID",
			event: &canonicalizer.CanonicalEvent{
				EventID:   "",
				Source:    "BINANCE",
				EventType: "TRADE",
				RawPayload: []byte("test"),
			},
			expected: false,
		},
		{
			name: "Unknown event type",
			event: &canonicalizer.CanonicalEvent{
				EventID:    "evt_123",
				Source:     "BINANCE",
				EventType:  "UNKNOWN",
				RawPayload: []byte("test"),
			},
			expected: false,
		},
		{
			name: "No raw payload",
			event: &canonicalizer.CanonicalEvent{
				EventID:    "evt_123",
				Source:     "BINANCE",
				EventType:  "TRADE",
				RawPayload: []byte{},
			},
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layer := validator.validateProtocol(tt.event)
			if layer.Passed != tt.expected {
				t.Errorf("Expected passed=%v, got %v. Checks: %+v", 
					tt.expected, layer.Passed, layer.Checks)
			}
		})
	}
}

// TestValidator_Layer3_DataIntegrity tests data integrity validation
func TestValidator_Layer3_DataIntegrity(t *testing.T) {
	rules := DefaultValidationRules()
	rules.AllowUnknownSymbol = false
	validator := NewValidator(rules)
	
	tests := []struct {
		name     string
		event    *canonicalizer.CanonicalEvent
		expected bool
	}{
		{
			name:     "Valid data",
			event:    createValidEvent(),
			expected: true,
		},
		{
			name: "Unknown symbol (not allowed)",
			event: &canonicalizer.CanonicalEvent{
				EventID:         "evt_123",
				CanonicalSymbol: "UNKNOWN",
				Price:           100.0,
				Size:            1.0,
				Side:            "BUY",
			},
			expected: false,
		},
		{
			name: "Price too high",
			event: &canonicalizer.CanonicalEvent{
				EventID:         "evt_123",
				CanonicalSymbol: "BTC/USD",
				Price:           2e9, // > 1e9 max
				Size:            1.0,
				Side:            "BUY",
			},
			expected: false,
		},
		{
			name: "Size too high",
			event: &canonicalizer.CanonicalEvent{
				EventID:         "evt_123",
				CanonicalSymbol: "BTC/USD",
				Price:           100.0,
				Size:            2e12, // > 1e12 max
				Side:            "BUY",
			},
			expected: false,
		},
		{
			name: "Invalid side",
			event: &canonicalizer.CanonicalEvent{
				EventID:         "evt_123",
				CanonicalSymbol: "BTC/USD",
				Price:           100.0,
				Size:            1.0,
				Side:            "INVALID",
			},
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layer := validator.validateDataIntegrity(tt.event)
			if layer.Passed != tt.expected {
				t.Errorf("Expected passed=%v, got %v. Message: %s", 
					tt.expected, layer.Passed, layer.Message)
			}
		})
	}
}

// TestValidator_Layer4_FaultTolerance tests fault tolerance validation
func TestValidator_Layer4_FaultTolerance(t *testing.T) {
	rules := DefaultValidationRules()
	validator := NewValidator(rules)
	
	tests := []struct {
		name     string
		event    *canonicalizer.CanonicalEvent
		expected bool
	}{
		{
			name:     "Valid fault tolerance",
			event:    createValidEvent(),
			expected: true,
		},
		{
			name: "No raw payload preserved",
			event: &canonicalizer.CanonicalEvent{
				EventID:    "evt_123",
				RawPayload: []byte{},
			},
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layer := validator.validateFaultTolerance(tt.event)
			if layer.Passed != tt.expected {
				t.Errorf("Expected passed=%v, got %v. Message: %s", 
					tt.expected, layer.Passed, layer.Message)
			}
		})
	}
}

// TestValidator_Layer5_Performance tests performance validation
func TestValidator_Layer5_Performance(t *testing.T) {
	rules := DefaultValidationRules()
	validator := NewValidator(rules)
	
	event := createValidEvent()
	
	tests := []struct {
		name            string
		processingStart int64
		expected        bool
	}{
		{
			name:            "Fast processing (<500ms)",
			processingStart: time.Now().UnixNano(),
			expected:        true,
		},
		{
			name:            "Slow processing (>500ms)",
			processingStart: time.Now().Add(-1 * time.Second).UnixNano(),
			expected:        false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layer := validator.validatePerformance(event, tt.processingStart)
			if layer.Passed != tt.expected {
				t.Errorf("Expected passed=%v, got %v. Latency: %dms", 
					tt.expected, layer.Passed, layer.LatencyMs)
			}
		})
	}
}

// TestValidator_Stats tests statistics tracking
func TestValidator_Stats(t *testing.T) {
	rules := DefaultValidationRules()
	rules.AllowUnknownSymbol = true
	validator := NewValidator(rules)
	
	// Validate some events
	event := createValidEvent()
	processingStart := time.Now().UnixNano()
	
	for i := 0; i < 10; i++ {
		validator.Validate(event, processingStart)
	}
	
	stats := validator.Stats()
	
	if stats.TotalValidated != 10 {
		t.Errorf("Expected 10 validated, got %d", stats.TotalValidated)
	}
	
	if stats.TotalPassed != 10 {
		t.Errorf("Expected 10 passed, got %d", stats.TotalPassed)
	}
	
	if stats.PassRate != 1.0 {
		t.Errorf("Expected pass rate 1.0, got %f", stats.PassRate)
	}
	
	if stats.TotalFailed != 0 {
		t.Errorf("Expected 0 failed, got %d", stats.TotalFailed)
	}
}

// TestValidator_Stats_WithFailures tests statistics with failures
func TestValidator_Stats_WithFailures(t *testing.T) {
	rules := DefaultValidationRules()
	rules.AllowUnknownSymbol = false
	validator := NewValidator(rules)
	
	processingStart := time.Now().UnixNano()
	
	// 7 valid events
	validEvent := createValidEvent()
	for i := 0; i < 7; i++ {
		validator.Validate(validEvent, processingStart)
	}
	
	// 3 invalid events (unknown symbol)
	invalidEvent := createValidEvent()
	invalidEvent.CanonicalSymbol = "UNKNOWN"
	for i := 0; i < 3; i++ {
		validator.Validate(invalidEvent, processingStart)
	}
	
	stats := validator.Stats()
	
	if stats.TotalValidated != 10 {
		t.Errorf("Expected 10 validated, got %d", stats.TotalValidated)
	}
	
	if stats.TotalPassed != 7 {
		t.Errorf("Expected 7 passed, got %d", stats.TotalPassed)
	}
	
	if stats.TotalFailed != 3 {
		t.Errorf("Expected 3 failed, got %d", stats.TotalFailed)
	}
	
	expectedPassRate := 0.7
	if stats.PassRate != expectedPassRate {
		t.Errorf("Expected pass rate %f, got %f", expectedPassRate, stats.PassRate)
	}
}

// TestValidationStats_IsHealthy tests health check
func TestValidationStats_IsHealthy(t *testing.T) {
	tests := []struct {
		name     string
		stats    ValidationStats
		expected bool
	}{
		{
			name: "Healthy (>90% pass rate)",
			stats: ValidationStats{
				TotalValidated: 100,
				TotalPassed:    95,
				PassRate:       0.95,
				LastValidated:  time.Now(),
			},
			expected: true,
		},
		{
			name: "Unhealthy (no validations)",
			stats: ValidationStats{
				TotalValidated: 0,
			},
			expected: false,
		},
		{
			name: "Unhealthy (<90% pass rate)",
			stats: ValidationStats{
				TotalValidated: 100,
				TotalPassed:    80,
				PassRate:       0.80,
				LastValidated:  time.Now(),
			},
			expected: false,
		},
		{
			name: "Unhealthy (stale validation)",
			stats: ValidationStats{
				TotalValidated: 100,
				TotalPassed:    95,
				PassRate:       0.95,
				LastValidated:  time.Now().Add(-2 * time.Minute),
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

// TestValidator_AllLayersIntegration tests all 5 layers together
func TestValidator_AllLayersIntegration(t *testing.T) {
	rules := DefaultValidationRules()
	rules.AllowUnknownSymbol = true
	validator := NewValidator(rules)
	
	event := createValidEvent()
	processingStart := time.Now().UnixNano()
	
	result := validator.Validate(event, processingStart)
	
	// Verify all 5 layers are present
	expectedLayers := []string{
		"connectivity",
		"protocol",
		"data_integrity",
		"fault_tolerance",
		"performance",
	}
	
	for _, layerName := range expectedLayers {
		if _, exists := result.Layers[layerName]; !exists {
			t.Errorf("Layer %s not found in result", layerName)
		}
	}
	
	// All layers should pass for valid event
	for name, layer := range result.Layers {
		if !layer.Passed {
			t.Errorf("Layer %s failed: %s", name, layer.Message)
		}
	}
	
	if !result.Passed {
		t.Error("Overall validation should pass for valid event")
	}
}

// BenchmarkValidator_Validate benchmarks validation performance
func BenchmarkValidator_Validate(b *testing.B) {
	rules := DefaultValidationRules()
	rules.AllowUnknownSymbol = true
	validator := NewValidator(rules)
	
	event := createValidEvent()
	processingStart := time.Now().UnixNano()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validator.Validate(event, processingStart)
	}
}

// BenchmarkValidator_Layer3 benchmarks data integrity layer
func BenchmarkValidator_Layer3(b *testing.B) {
	rules := DefaultValidationRules()
	validator := NewValidator(rules)
	
	event := createValidEvent()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validator.validateDataIntegrity(event)
	}
}
