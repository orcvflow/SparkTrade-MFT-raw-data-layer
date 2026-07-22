// Package adapter provides interfaces and types for market data source adapters
package adapter

import (
	"context"
	"time"
)

// Adapter is the interface that all market data adapters must implement
// Each adapter (Binance, IB, NASDAQ, etc.) implements this interface
type Adapter interface {
	// Connect establishes connection to the data source
	Connect(ctx context.Context) error
	
	// Start begins receiving data and sending to output channel
	// The adapter runs in a goroutine and sends RawMessage to output
	Start(ctx context.Context, output chan<- RawMessage) error
	
	// Stop gracefully stops the adapter
	Stop() error
	
	// Name returns the adapter name (e.g., "BINANCE", "IB")
	Name() string
	
	// Health returns current health status
	Health() HealthStatus
}

// RawMessage represents a raw message received from a data source
// The payload is UNTOUCHED - byte-for-byte identical to wire data
type RawMessage struct {
	Source      string    // "BINANCE" | "IB" | "NASDAQ" | "CTP" | "CME"
	Payload     []byte    // Original message — NEVER MODIFIED
	ReceivedAt  int64     // Hardware timestamp (nanoseconds since epoch)
	SequenceNum uint64    // Source's sequence number (if available)
}

// HealthStatus represents the health of an adapter
type HealthStatus struct {
	Connected      bool
	LastMessage    time.Time
	MessagesRecv   uint64
	MessagesSent   uint64
	Errors         []error
	ReconnectCount int
	Uptime         time.Duration
}

// AdapterConfig holds common configuration for all adapters
type AdapterConfig struct {
	Enabled           bool
	ReconnectAttempts int
	BackoffSeconds    []int
	Timeout           time.Duration
}

// ErrorType categorizes adapter errors
type ErrorType int

const (
	ErrorUnknown ErrorType = iota
	ErrorConnection
	ErrorProtocol
	ErrorTimeout
	ErrorAuthentication
	ErrorRateLimit
)

// AdapterError represents an adapter-specific error
type AdapterError struct {
	Type    ErrorType
	Message string
	Source  string
	Err     error
}

func (e *AdapterError) Error() string {
	if e.Err != nil {
		return e.Source + ": " + e.Message + ": " + e.Err.Error()
	}
	return e.Source + ": " + e.Message
}

func (e *AdapterError) Unwrap() error {
	return e.Err
}

// NewAdapterError creates a new adapter error
func NewAdapterError(typ ErrorType, source, message string, err error) *AdapterError {
	return &AdapterError{
		Type:    typ,
		Message: message,
		Source:  source,
		Err:     err,
	}
}
