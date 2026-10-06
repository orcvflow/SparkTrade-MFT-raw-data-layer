#!/bin/bash
# MT5 Test Execution Script
# Run this in YOUR terminal (not Kiro agent terminal)

set -e

echo "=========================================="
echo "MT5 Integration Test Suite"
echo "=========================================="
echo ""

cd /home/main/Desktop/raw-data-layer

echo "📍 Current directory:"
pwd
echo ""

echo "=========================================="
echo "1. CANONICALIZER TESTS (13 tests)"
echo "=========================================="
go test ./pkg/canonicalizer -run TestMT5 -v
echo ""

echo "=========================================="
echo "2. ADAPTER TESTS (20 tests)"
echo "=========================================="
go test ./pkg/adapter -run TestMT5 -v
echo ""

echo "=========================================="
echo "3. COVERAGE ANALYSIS"
echo "=========================================="
echo "Canonicalizer coverage:"
go test ./pkg/canonicalizer -run TestMT5 -coverprofile=/tmp/canon_mt5.out
go tool cover -func=/tmp/canon_mt5.out | grep mt5
echo ""

echo "Adapter coverage:"
go test ./pkg/adapter -run TestMT5 -coverprofile=/tmp/adapter_mt5.out
go tool cover -func=/tmp/adapter_mt5.out | grep mt5
echo ""

echo "=========================================="
echo "4. RACE DETECTOR"
echo "=========================================="
go test ./pkg/canonicalizer ./pkg/adapter -race -run TestMT5
echo ""

echo "=========================================="
echo "5. BENCHMARKS"
echo "=========================================="
echo "Canonicalizer benchmarks:"
go test ./pkg/canonicalizer -bench=BenchmarkMT5 -benchmem -run=^$ | grep -E "(Benchmark|ns/op|allocs/op)"
echo ""

echo "Adapter benchmarks:"
go test ./pkg/adapter -bench=BenchmarkMT5 -benchmem -run=^$ | grep -E "(Benchmark|ns/op|allocs/op)"
echo ""

echo "=========================================="
echo "6. INTEGRATION TESTS (optional - requires ZMQ)"
echo "=========================================="
echo "Skipping integration tests (requires mock ZMQ setup)"
# go test -tags=integration ./test/integration -run TestIntegration_MT5 -v
echo ""

echo "=========================================="
echo "✅ TEST SUITE COMPLETE"
echo "=========================================="
echo ""
echo "Summary:"
echo "- Canonicalizer: Check if 13/13 tests passed"
echo "- Adapter: Check if 20/20 tests passed"
echo "- Coverage: Check if ≥85% for both"
echo "- Race: Check if 0 data races"
echo ""
echo "If ALL tests passed: MT5 is PRODUCTION-READY ✅"
echo "If ANY test failed: Report error to fix ❌"
