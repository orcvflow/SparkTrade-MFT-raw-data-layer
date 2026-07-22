// Package mapper provides symbol mapping between provider-specific
// and canonical symbol representations
package mapper

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SymbolMapper manages bidirectional symbol mapping
// Thread-safe for concurrent read access
type SymbolMapper struct {
	mu          sync.RWMutex
	toCanonical map[string]map[string]string // source -> (provider_symbol -> canonical)
	toProvider  map[string]map[string]string // source -> (canonical -> provider_symbol)
}

// NewSymbolMapper creates a new symbol mapper by loading all JSON files
// from the specified mappings directory
func NewSymbolMapper(mappingsDir string) (*SymbolMapper, error) {
	m := &SymbolMapper{
		toCanonical: make(map[string]map[string]string),
		toProvider:  make(map[string]map[string]string),
	}

	// Load all JSON files from mappings directory
	files, err := filepath.Glob(filepath.Join(mappingsDir, "*.json"))
	if err != nil {
		return nil, fmt.Errorf("failed to list mapping files: %w", err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no mapping files found in %s", mappingsDir)
	}

	for _, file := range files {
		// Extract source name from filename (e.g., "binance.json" -> "binance")
		source := filepath.Base(file)
		source = source[:len(source)-5] // Remove .json extension

		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", file, err)
		}

		var mapping map[string]string
		if err := json.Unmarshal(data, &mapping); err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", file, err)
		}

		m.toCanonical[source] = make(map[string]string)
		m.toProvider[source] = make(map[string]string)

		for providerSymbol, canonicalSymbol := range mapping {
			m.toCanonical[source][providerSymbol] = canonicalSymbol
			m.toProvider[source][canonicalSymbol] = providerSymbol
		}
	}

	return m, nil
}

// ToCanonical converts a provider-specific symbol to canonical format
// Returns "UNKNOWN" if symbol is not found in mappings
func (m *SymbolMapper) ToCanonical(source, providerSymbol string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if canonical, ok := m.toCanonical[source][providerSymbol]; ok {
		return canonical
	}

	// Unknown symbol - return "UNKNOWN" but don't crash
	return "UNKNOWN"
}

// ToProvider converts a canonical symbol to provider-specific format
// Returns empty string if symbol is not found in mappings
func (m *SymbolMapper) ToProvider(source, canonicalSymbol string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if providerSymbol, ok := m.toProvider[source][canonicalSymbol]; ok {
		return providerSymbol
	}

	return ""
}

// IsKnown checks if a provider symbol exists in the mappings
func (m *SymbolMapper) IsKnown(source, providerSymbol string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.toCanonical[source][providerSymbol]
	return ok
}

// GetAllSymbols returns all canonical symbols for a given source
func (m *SymbolMapper) GetAllSymbols(source string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	symbols := make([]string, 0, len(m.toCanonical[source]))
	for _, canonical := range m.toCanonical[source] {
		symbols = append(symbols, canonical)
	}
	return symbols
}

// GetSources returns all known sources (e.g., "binance", "ib")
func (m *SymbolMapper) GetSources() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sources := make([]string, 0, len(m.toCanonical))
	for source := range m.toCanonical {
		sources = append(sources, source)
	}
	return sources
}
