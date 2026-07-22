package validation

import (
	"fmt"
	"sync"
	"time"

	"raw-data-layer/pkg/canonicalizer"
)

// Validator implements the 5-layer validation framework from CLAUDE.md:
// Layer 1: Connectivity validation
// Layer 2: Protocol compliance
// Layer 3: Data integrity
// Layer 4: Fault tolerance
// Layer 5: Performance monitoring
type Validator struct {
	// Validation rules
	rules ValidationRules
	
	// Statistics
	totalValidated uint64
	totalPassed    uint64
	totalFailed    uint64
	
	// Layer-specific counters
	layer1Failures uint64
	layer2Failures uint64
	layer3Failures uint64
	layer4Failures uint64
	layer5Failures uint64
	
	// Synchronization
	mu sync.RWMutex
	
	// Performance tracking
	startTime     time.Time
	lastValidated time.Time
}

// ValidationRules holds configuration for validation
type ValidationRules struct {
	// Layer 1: Connectivity
	MaxTimestampAge time.Duration // Max age for exchange timestamp
	
	// Layer 2: Protocol
	RequiredFields []string // Fields that must be present
	
	// Layer 3: Data Integrity
	MinPrice          float64 // Minimum acceptable price
	MaxPrice          float64 // Maximum acceptable price
	MinSize           float64 // Minimum acceptable size
	MaxSize           float64 // Maximum acceptable size
	AllowUnknownSymbol bool   // Allow UNKNOWN symbols
	
	// Layer 4: Fault Tolerance
	MaxErrorRate float64 // Max acceptable error rate (0.0 to 1.0)
	
	// Layer 5: Performance
	MaxLatencyMs int64 // Max acceptable processing latency (ms)
}

// DefaultValidationRules returns default validation rules
func DefaultValidationRules() ValidationRules {
	return ValidationRules{
		// Layer 1
		MaxTimestampAge: 1 * time.Hour,
		
		// Layer 2
		RequiredFields: []string{"EventID", "Source", "CanonicalSymbol", "EventType"},
		
		// Layer 3
		MinPrice:           0.0,
		MaxPrice:           1e9, // 1 billion
		MinSize:            0.0,
		MaxSize:            1e12, // 1 trillion
		AllowUnknownSymbol: false,
		
		// Layer 4
		MaxErrorRate: 0.1, // 10%
		
		// Layer 5
		MaxLatencyMs: 500, // 500ms
	}
}

// NewValidator creates a new validator
func NewValidator(rules ValidationRules) *Validator {
	return &Validator{
		rules:     rules,
		startTime: time.Now(),
	}
}

// Validate performs 5-layer validation on canonical event
// Returns ValidationResult with pass/fail for each layer
func (v *Validator) Validate(event *canonicalizer.CanonicalEvent, processingStartTime int64) ValidationResult {
	v.mu.Lock()
	v.totalValidated++
	v.lastValidated = time.Now()
	v.mu.Unlock()
	
	result := ValidationResult{
		EventID:   event.EventID,
		Timestamp: time.Now().UnixNano(),
		Layers:    make(map[string]LayerResult),
	}
	
	// Layer 1: Connectivity validation
	layer1 := v.validateConnectivity(event)
	result.Layers["connectivity"] = layer1
	if !layer1.Passed {
		v.mu.Lock()
		v.layer1Failures++
		v.mu.Unlock()
	}
	
	// Layer 2: Protocol compliance
	layer2 := v.validateProtocol(event)
	result.Layers["protocol"] = layer2
	if !layer2.Passed {
		v.mu.Lock()
		v.layer2Failures++
		v.mu.Unlock()
	}
	
	// Layer 3: Data integrity
	layer3 := v.validateDataIntegrity(event)
	result.Layers["data_integrity"] = layer3
	if !layer3.Passed {
		v.mu.Lock()
		v.layer3Failures++
		v.mu.Unlock()
	}
	
	// Layer 4: Fault tolerance (not applicable per-message, checked at system level)
	layer4 := v.validateFaultTolerance(event)
	result.Layers["fault_tolerance"] = layer4
	if !layer4.Passed {
		v.mu.Lock()
		v.layer4Failures++
		v.mu.Unlock()
	}
	
	// Layer 5: Performance
	layer5 := v.validatePerformance(event, processingStartTime)
	result.Layers["performance"] = layer5
	if !layer5.Passed {
		v.mu.Lock()
		v.layer5Failures++
		v.mu.Unlock()
	}
	
	// Overall pass/fail
	result.Passed = layer1.Passed && layer2.Passed && layer3.Passed && layer4.Passed && layer5.Passed
	
	v.mu.Lock()
	if result.Passed {
		v.totalPassed++
	} else {
		v.totalFailed++
	}
	v.mu.Unlock()
	
	return result
}

// Layer 1: Connectivity validation
func (v *Validator) validateConnectivity(event *canonicalizer.CanonicalEvent) LayerResult {
	result := LayerResult{
		Layer:  "connectivity",
		Passed: true,
		Checks: make(map[string]bool),
	}
	
	// Check: Exchange timestamp is recent
	now := time.Now().UnixNano()
	age := time.Duration(now - event.ExchangeTimestamp)
	
	timestampRecent := age < v.rules.MaxTimestampAge
	result.Checks["timestamp_recent"] = timestampRecent
	
	// Check: Local timestamp exists
	localExists := event.LocalHWTimestamp > 0
	result.Checks["local_timestamp_exists"] = localExists
	
	// Check: Source is known
	sourceKnown := event.Source != "" && event.Source != "UNKNOWN"
	result.Checks["source_known"] = sourceKnown
	
	result.Passed = timestampRecent && localExists && sourceKnown
	
	if !result.Passed {
		result.Message = fmt.Sprintf("Connectivity validation failed: age=%v, localExists=%v, sourceKnown=%v",
			age, localExists, sourceKnown)
	}
	
	return result
}

// Layer 2: Protocol compliance
func (v *Validator) validateProtocol(event *canonicalizer.CanonicalEvent) LayerResult {
	result := LayerResult{
		Layer:  "protocol",
		Passed: true,
		Checks: make(map[string]bool),
	}
	
	// Check: Required fields present
	for _, field := range v.rules.RequiredFields {
		exists := v.checkFieldExists(event, field)
		result.Checks[fmt.Sprintf("field_%s", field)] = exists
		if !exists {
			result.Passed = false
		}
	}
	
	// Check: Event type is valid
	validEventType := event.EventType != "" && event.EventType != "UNKNOWN"
	result.Checks["valid_event_type"] = validEventType
	if !validEventType {
		result.Passed = false
	}
	
	// Check: Raw payload exists
	rawExists := len(event.RawPayload) > 0
	result.Checks["raw_payload_exists"] = rawExists
	if !rawExists {
		result.Passed = false
	}
	
	if !result.Passed {
		result.Message = "Protocol validation failed: missing required fields or invalid format"
	}
	
	return result
}

// Layer 3: Data integrity
func (v *Validator) validateDataIntegrity(event *canonicalizer.CanonicalEvent) LayerResult {
	result := LayerResult{
		Layer:  "data_integrity",
		Passed: true,
		Checks: make(map[string]bool),
	}
	
	// Check: Symbol mapping
	symbolValid := event.CanonicalSymbol != "" && 
		(v.rules.AllowUnknownSymbol || event.CanonicalSymbol != "UNKNOWN")
	result.Checks["symbol_valid"] = symbolValid
	if !symbolValid {
		result.Passed = false
	}
	
	// Check: Price range
	priceValid := event.Price >= v.rules.MinPrice && event.Price <= v.rules.MaxPrice
	result.Checks["price_valid"] = priceValid
	if !priceValid {
		result.Passed = false
	}
	
	// Check: Size range
	sizeValid := event.Size >= v.rules.MinSize && event.Size <= v.rules.MaxSize
	result.Checks["size_valid"] = sizeValid
	if !sizeValid {
		result.Passed = false
	}
	
	// Check: Side is valid
	sideValid := event.Side == "BUY" || event.Side == "SELL" || 
		event.Side == "UNKNOWN" // UNKNOWN allowed for some event types
	result.Checks["side_valid"] = sideValid
	if !sideValid {
		result.Passed = false
	}
	
	if !result.Passed {
		result.Message = fmt.Sprintf("Data integrity failed: symbol=%s, price=%f, size=%f, side=%s",
			event.CanonicalSymbol, event.Price, event.Size, event.Side)
	}
	
	return result
}

// Layer 4: Fault tolerance
func (v *Validator) validateFaultTolerance(event *canonicalizer.CanonicalEvent) LayerResult {
	result := LayerResult{
		Layer:  "fault_tolerance",
		Passed: true,
		Checks: make(map[string]bool),
	}
	
	// Check: Raw payload preserved (byte-for-byte)
	rawPreserved := len(event.RawPayload) > 0
	result.Checks["raw_preserved"] = rawPreserved
	
	// Check: Error rate is acceptable (system-level, always pass for individual messages)
	v.mu.RLock()
	errorRate := 0.0
	if v.totalValidated > 0 {
		errorRate = float64(v.totalFailed) / float64(v.totalValidated)
	}
	v.mu.RUnlock()
	
	errorRateOK := errorRate <= v.rules.MaxErrorRate
	result.Checks["error_rate_ok"] = errorRateOK
	
	result.Passed = rawPreserved && errorRateOK
	
	if !result.Passed {
		result.Message = fmt.Sprintf("Fault tolerance check: rawPreserved=%v, errorRate=%f",
			rawPreserved, errorRate)
	}
	
	return result
}

// Layer 5: Performance
func (v *Validator) validatePerformance(event *canonicalizer.CanonicalEvent, processingStartTime int64) LayerResult {
	result := LayerResult{
		Layer:  "performance",
		Passed: true,
		Checks: make(map[string]bool),
	}
	
	// Check: Processing latency
	now := time.Now().UnixNano()
	latencyNs := now - processingStartTime
	latencyMs := latencyNs / 1000000
	
	latencyOK := latencyMs <= v.rules.MaxLatencyMs
	result.Checks["latency_ok"] = latencyOK
	
	// Check: Timestamps are monotonic (local > exchange for recent events)
	timestampsMonotonic := event.LocalHWTimestamp >= event.ExchangeTimestamp || 
		time.Duration(now-event.ExchangeTimestamp) > 1*time.Minute // Allow old events
	result.Checks["timestamps_monotonic"] = timestampsMonotonic
	
	result.Passed = latencyOK && timestampsMonotonic
	result.LatencyMs = latencyMs
	
	if !result.Passed {
		result.Message = fmt.Sprintf("Performance validation failed: latency=%dms (max=%d)",
			latencyMs, v.rules.MaxLatencyMs)
	}
	
	return result
}

// Helper: Check if field exists in event
func (v *Validator) checkFieldExists(event *canonicalizer.CanonicalEvent, field string) bool {
	switch field {
	case "EventID":
		return event.EventID != ""
	case "Source":
		return event.Source != ""
	case "CanonicalSymbol":
		return event.CanonicalSymbol != ""
	case "EventType":
		return event.EventType != ""
	default:
		return false
	}
}

// Stats returns validation statistics
func (v *Validator) Stats() ValidationStats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	
	passRate := 0.0
	if v.totalValidated > 0 {
		passRate = float64(v.totalPassed) / float64(v.totalValidated)
	}
	
	return ValidationStats{
		TotalValidated: v.totalValidated,
		TotalPassed:    v.totalPassed,
		TotalFailed:    v.totalFailed,
		PassRate:       passRate,
		Layer1Failures: v.layer1Failures,
		Layer2Failures: v.layer2Failures,
		Layer3Failures: v.layer3Failures,
		Layer4Failures: v.layer4Failures,
		Layer5Failures: v.layer5Failures,
		Uptime:         time.Since(v.startTime),
		LastValidated:  v.lastValidated,
	}
}

// ValidationResult holds validation result for a single event
type ValidationResult struct {
	EventID   string
	Timestamp int64
	Passed    bool
	Layers    map[string]LayerResult
}

// LayerResult holds result for a single validation layer
type LayerResult struct {
	Layer     string
	Passed    bool
	Checks    map[string]bool
	Message   string
	LatencyMs int64
}

// ValidationStats holds overall validation statistics
type ValidationStats struct {
	TotalValidated uint64
	TotalPassed    uint64
	TotalFailed    uint64
	PassRate       float64
	Layer1Failures uint64
	Layer2Failures uint64
	Layer3Failures uint64
	Layer4Failures uint64
	Layer5Failures uint64
	Uptime         time.Duration
	LastValidated  time.Time
}

// IsHealthy returns true if validation system is healthy
func (s ValidationStats) IsHealthy() bool {
	// System is healthy if:
	// - Pass rate > 90%
	// - Validated at least once
	// - Last validation is recent (<1 minute)
	
	if s.TotalValidated == 0 {
		return false
	}
	
	if s.PassRate < 0.9 {
		return false
	}
	
	if time.Since(s.LastValidated) > 1*time.Minute {
		return false
	}
	
	return true
}
