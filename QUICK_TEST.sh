#!/bin/bash
# Quick MT5 Test - Run this first
# Usage: ./QUICK_TEST.sh

cd /home/main/Desktop/raw-data-layer

echo "🧪 Quick MT5 Test"
echo ""
echo "1. Build test..."
if go test -c ./pkg/canonicalizer -o /tmp/mt5_test 2>&1; then
    echo "   ✅ Build SUCCESS"
else
    echo "   ❌ Build FAILED"
    exit 1
fi

echo ""
echo "2. Run 1 test (TestMT5_ParseL1Tick)..."
go test ./pkg/canonicalizer -run TestMT5_ParseL1Tick -v

echo ""
echo "3. Count all MT5 tests..."
echo "   Canonicalizer:"
go test ./pkg/canonicalizer -list TestMT5 | grep -c "^Test"
echo "   Adapter:"
go test ./pkg/adapter -list TestMT5 | grep -c "^Test"

echo ""
echo "✅ Quick test complete"
echo ""
echo "To run full suite: ./RUN_TESTS.sh"
