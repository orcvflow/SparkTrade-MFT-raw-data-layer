package mapper

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func setupTestMapper(t *testing.T) *SymbolMapper {
	tmpDir := t.TempDir()

	binanceData := []byte(`{
		"BTCUSDT": "BTC/USD",
		"ETHUSDT": "ETH/USD",
		"BNBUSDT": "BNB/USD"
	}`)
	ibData := []byte(`{
		"265598": "AAPL",
		"8314": "MSFT",
		"76792991": "GOOGL"
	}`)

	if err := os.WriteFile(filepath.Join(tmpDir, "binance.json"), binanceData, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "ib.json"), ibData, 0644); err != nil {
		t.Fatal(err)
	}

	m, err := NewSymbolMapper(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSymbolMapper_ToCanonical(t *testing.T) {
	m := setupTestMapper(t)

	tests := []struct {
		name     string
		source   string
		symbol   string
		expected string
	}{
		{"Binance BTC", "binance", "BTCUSDT", "BTC/USD"},
		{"Binance ETH", "binance", "ETHUSDT", "ETH/USD"},
		{"Binance BNB", "binance", "BNBUSDT", "BNB/USD"},
		{"IB AAPL", "ib", "265598", "AAPL"},
		{"IB MSFT", "ib", "8314", "MSFT"},
		{"IB GOOGL", "ib", "76792991", "GOOGL"},
		{"Unknown Binance", "binance", "FAKECOIN", "UNKNOWN"},
		{"Unknown IB", "ib", "999999", "UNKNOWN"},
		{"Unknown Source", "unknown_exchange", "SYMBOL", "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.ToCanonical(tt.source, tt.symbol)
			if result != tt.expected {
				t.Errorf("ToCanonical(%s, %s) = %s; want %s",
					tt.source, tt.symbol, result, tt.expected)
			}
		})
	}
}

// TestSymbolMapper_SourceCaseInsensitive is the regression test for the
// pre-existing Addım A/B case-mismatch bug: the mapper keys tables by the
// lowercase JSON filename ("binance") while adapters emit uppercase source
// identifiers on RawMessage.Source (BinanceAdapter sets "BINANCE"). Before
// normalizeSource, ToCanonical("BINANCE", "BTCUSDT") returned "UNKNOWN" for
// every Binance symbol. Every spelling must now collapse to the same table.
func TestSymbolMapper_SourceCaseInsensitive(t *testing.T) {
	m := setupTestMapper(t)

	cases := []string{"BINANCE", "Binance", "binance", "BiNaNcE", "IB", "Ib", "ib"}
	for _, src := range cases {
		got := m.ToCanonical(src, "BTCUSDT")
		// "BTCUSDT" only exists in the binance table; ib has "265598"->"AAPL".
		want := "BTC/USD"
		// For ib-spelled sources, BTCUSDT is not a known provider symbol there,
		// so only assert for binance-spelled sources. Keep this test focused on
		// the case-collapse property: any casing of "binance" must resolve.
		if isBinanceSpelling(src) && got != want {
			t.Errorf("ToCanonical(%q, BTCUSDT) = %q; want %q (source case should collapse)", src, got, want)
		}
	}

	// IB reverse: any casing of "ib" must reverse-map AAPL -> "265598".
	for _, src := range []string{"IB", "Ib", "ib", "iB"} {
		if got := m.ToProvider(src, "AAPL"); got != "265598" {
			t.Errorf("ToProvider(%q, AAPL) = %q; want 265598 (source case should collapse)", src, got)
		}
		if !m.IsKnown(src, "265598") {
			t.Errorf("IsKnown(%q, 265598) = false; want true (source case should collapse)", src)
		}
	}
}

// isBinanceSpelling reports whether src lowercases to "binance".
func isBinanceSpelling(src string) bool {
	return strings.EqualFold(src, "binance")
}

func TestSymbolMapper_ToProvider(t *testing.T) {
	m := setupTestMapper(t)

	tests := []struct {
		name      string
		source    string
		canonical string
		expected  string
	}{
		{"Binance BTC", "binance", "BTC/USD", "BTCUSDT"},
		{"Binance ETH", "binance", "ETH/USD", "ETHUSDT"},
		{"IB AAPL", "ib", "AAPL", "265598"},
		{"IB MSFT", "ib", "MSFT", "8314"},
		{"Unknown Canonical", "binance", "UNKNOWN_SYMBOL", ""},
		{"Unknown Source", "unknown_exchange", "AAPL", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.ToProvider(tt.source, tt.canonical)
			if result != tt.expected {
				t.Errorf("ToProvider(%s, %s) = %s; want %s", 
					tt.source, tt.canonical, result, tt.expected)
			}
		})
	}
}

func TestSymbolMapper_IsKnown(t *testing.T) {
	m := setupTestMapper(t)

	tests := []struct {
		name     string
		source   string
		symbol   string
		expected bool
	}{
		{"Known Binance", "binance", "BTCUSDT", true},
		{"Known IB", "ib", "265598", true},
		{"Unknown Binance", "binance", "FAKECOIN", false},
		{"Unknown IB", "ib", "999999", false},
		{"Unknown Source", "unknown", "SYMBOL", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.IsKnown(tt.source, tt.symbol)
			if result != tt.expected {
				t.Errorf("IsKnown(%s, %s) = %v; want %v", 
					tt.source, tt.symbol, result, tt.expected)
			}
		})
	}
}

func TestSymbolMapper_GetAllSymbols(t *testing.T) {
	m := setupTestMapper(t)

	binanceSymbols := m.GetAllSymbols("binance")
	if len(binanceSymbols) != 3 {
		t.Errorf("GetAllSymbols(binance) returned %d symbols; want 3", len(binanceSymbols))
	}

	ibSymbols := m.GetAllSymbols("ib")
	if len(ibSymbols) != 3 {
		t.Errorf("GetAllSymbols(ib) returned %d symbols; want 3", len(ibSymbols))
	}

	unknownSymbols := m.GetAllSymbols("unknown")
	if len(unknownSymbols) != 0 {
		t.Errorf("GetAllSymbols(unknown) returned %d symbols; want 0", len(unknownSymbols))
	}
}

func TestSymbolMapper_GetSources(t *testing.T) {
	m := setupTestMapper(t)

	sources := m.GetSources()
	if len(sources) != 2 {
		t.Errorf("GetSources() returned %d sources; want 2", len(sources))
	}

	// Check that both sources exist
	sourceMap := make(map[string]bool)
	for _, source := range sources {
		sourceMap[source] = true
	}

	if !sourceMap["binance"] || !sourceMap["ib"] {
		t.Errorf("GetSources() = %v; want [binance, ib]", sources)
	}
}

// TestSymbolMapper_RaceCondition tests concurrent access
// This is the mandatory race condition test from CLAUDE.md
func TestSymbolMapper_RaceCondition(t *testing.T) {
	m := setupTestMapper(t)

	// Simulate 10 goroutines accessing mapper concurrently
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < 1000; j++ {
				// Mix of read operations
				_ = m.ToCanonical("binance", "BTCUSDT")
				_ = m.ToProvider("binance", "BTC/USD")
				_ = m.IsKnown("ib", "265598")
				_ = m.GetAllSymbols("binance")
				_ = m.GetSources()
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check if any errors occurred
	for err := range errors {
		if err != nil {
			t.Errorf("Race condition detected: %v", err)
		}
	}
}

// TestSymbolMapper_EmptyDirectory tests error handling for empty directory
func TestSymbolMapper_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	_, err := NewSymbolMapper(tmpDir)
	if err == nil {
		t.Error("NewSymbolMapper with empty directory should return error")
	}
}

// TestSymbolMapper_InvalidJSON tests error handling for malformed JSON
func TestSymbolMapper_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()

	invalidJSON := []byte(`{"BTCUSDT": "BTC/USD", invalid`)
	if err := os.WriteFile(filepath.Join(tmpDir, "invalid.json"), invalidJSON, 0644); err != nil {
		t.Fatal(err)
	}

	_, err := NewSymbolMapper(tmpDir)
	if err == nil {
		t.Error("NewSymbolMapper with invalid JSON should return error")
	}
}

// Benchmark tests
func BenchmarkSymbolMapper_ToCanonical(b *testing.B) {
	tmpDir := b.TempDir()
	binanceData := []byte(`{"BTCUSDT": "BTC/USD"}`)
	os.WriteFile(filepath.Join(tmpDir, "binance.json"), binanceData, 0644)
	m, _ := NewSymbolMapper(tmpDir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.ToCanonical("binance", "BTCUSDT")
	}
}

func BenchmarkSymbolMapper_ToProvider(b *testing.B) {
	tmpDir := b.TempDir()
	binanceData := []byte(`{"BTCUSDT": "BTC/USD"}`)
	os.WriteFile(filepath.Join(tmpDir, "binance.json"), binanceData, 0644)
	m, _ := NewSymbolMapper(tmpDir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.ToProvider("binance", "BTC/USD")
	}
}

func BenchmarkSymbolMapper_Concurrent(b *testing.B) {
	tmpDir := b.TempDir()
	binanceData := []byte(`{"BTCUSDT": "BTC/USD"}`)
	os.WriteFile(filepath.Join(tmpDir, "binance.json"), binanceData, 0644)
	m, _ := NewSymbolMapper(tmpDir)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.ToCanonical("binance", "BTCUSDT")
		}
	})
}
