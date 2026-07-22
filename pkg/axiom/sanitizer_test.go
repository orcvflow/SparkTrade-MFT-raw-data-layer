package axiom

import (
	"math"
	"testing"
)

func TestMathSanitizer_SanitizePrice(t *testing.T) {
	sanitizer := NewMathSanitizer()

	tests := []struct {
		name     string
		input    float64
		expected float64
	}{
		{"Valid positive price", 100.5, 100.5},
		{"Negative price", -50.0, 0.0},
		{"NaN", math.NaN(), 0.0},
		{"Positive infinity", math.Inf(1), 0.0},
		{"Negative infinity", math.Inf(-1), 0.0},
		{"Zero", 0.0, 0.0},
		{"Very small positive", 0.0001, 0.0001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizer.SanitizePrice(tt.input)
			if math.IsNaN(tt.expected) {
				if !math.IsNaN(result) {
					t.Errorf("SanitizePrice() = %v, want NaN", result)
				}
			} else if result != tt.expected {
				t.Errorf("SanitizePrice() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMathSanitizer_DetectOverflow(t *testing.T) {
	sanitizer := NewMathSanitizer()

	tests := []struct {
		name     string
		input    float64
		expected bool
	}{
		{"Normal value", 100.5, false},
		{"Large but not overflow", 1e14, false},
		{"Overflow positive", 1e16, true},
		{"Overflow negative", -1e16, true},
		{"Exactly at threshold", 1e15, false},
		{"Just above threshold", 1.1e15, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizer.DetectOverflow(tt.input)
			if result != tt.expected {
				t.Errorf("DetectOverflow() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMathSanitizer_ClampPrice(t *testing.T) {
	sanitizer := NewMathSanitizer()

	tests := []struct {
		name     string
		price    float64
		min      float64
		max      float64
		expected float64
	}{
		{"Within range", 100.0, 50.0, 150.0, 100.0},
		{"Below minimum", 30.0, 50.0, 150.0, 50.0},
		{"Above maximum", 200.0, 50.0, 150.0, 150.0},
		{"At minimum", 50.0, 50.0, 150.0, 50.0},
		{"At maximum", 150.0, 50.0, 150.0, 150.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizer.ClampPrice(tt.price, tt.min, tt.max)
			if result != tt.expected {
				t.Errorf("ClampPrice() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMathSanitizer_SafeDivide(t *testing.T) {
	sanitizer := NewMathSanitizer()

	tests := []struct {
		name        string
		numerator   float64
		denominator float64
		expected    float64
	}{
		{"Normal division", 100.0, 2.0, 50.0},
		{"Division by zero", 100.0, 0.0, 0.0},
		{"Zero numerator", 0.0, 5.0, 0.0},
		{"NaN numerator", math.NaN(), 5.0, 0.0},
		{"NaN denominator", 100.0, math.NaN(), 0.0},
		{"Inf result", 1e308, 0.5, 0.0}, // Would overflow
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizer.SafeDivide(tt.numerator, tt.denominator)
			if math.IsNaN(tt.expected) {
				if !math.IsNaN(result) {
					t.Errorf("SafeDivide() = %v, want NaN", result)
				}
			} else if result != tt.expected {
				t.Errorf("SafeDivide() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Benchmark tests
func BenchmarkSanitizePrice(b *testing.B) {
	sanitizer := NewMathSanitizer()
	prices := []float64{100.5, -50.0, math.NaN(), math.Inf(1), 1e20}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, price := range prices {
			sanitizer.SanitizePrice(price)
		}
	}
}

func BenchmarkDetectOverflow(b *testing.B) {
	sanitizer := NewMathSanitizer()
	values := []float64{100.5, 1e10, 1e14, 1e16, 1e20}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, value := range values {
			sanitizer.DetectOverflow(value)
		}
	}
}
