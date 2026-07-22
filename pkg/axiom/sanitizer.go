// Package axiom provides Axle-Axiom inspired mathematical operations
// for paranoid data validation and statistical analysis
package axiom

import (
	"math"
)

// MathSanitizer provides paranoid mathematical sanitization
// Inspired by Axle-Axiom's rigorous validation approach
type MathSanitizer struct{}

// NewMathSanitizer creates a new mathematical sanitizer
func NewMathSanitizer() *MathSanitizer {
	return &MathSanitizer{}
}

// SanitizePrice ensures price is valid, non-negative, finite
// Returns 0.0 for NaN, Inf, or negative values
func (s *MathSanitizer) SanitizePrice(price float64) float64 {
	if math.IsNaN(price) || math.IsInf(price, 0) || price < 0 {
		return 0.0
	}
	return price
}

// SanitizeSize ensures size is valid, non-negative, finite
// Returns 0.0 for NaN, Inf, or negative values
func (s *MathSanitizer) SanitizeSize(size float64) float64 {
	if math.IsNaN(size) || math.IsInf(size, 0) || size < 0 {
		return 0.0
	}
	return size
}

// DetectOverflow checks if value is too large (> 1e15)
// This prevents overflow in downstream calculations
func (s *MathSanitizer) DetectOverflow(value float64) bool {
	return math.Abs(value) > 1e15
}

// ClampPrice restricts price to reasonable range [min, max]
func (s *MathSanitizer) ClampPrice(price, min, max float64) float64 {
	if price < min {
		return min
	}
	if price > max {
		return max
	}
	return price
}

// IsValid checks if a float64 value is finite and not NaN
func (s *MathSanitizer) IsValid(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

// SafeDivide performs division with zero-check
// Returns 0.0 if denominator is zero or result is invalid
func (s *MathSanitizer) SafeDivide(numerator, denominator float64) float64 {
	if denominator == 0.0 || !s.IsValid(numerator) || !s.IsValid(denominator) {
		return 0.0
	}
	result := numerator / denominator
	if !s.IsValid(result) {
		return 0.0
	}
	return result
}
