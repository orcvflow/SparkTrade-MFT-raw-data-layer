#!/bin/bash
# IMMEDIATE TEST - Run this now!

cd /home/main/Desktop/raw-data-layer

echo "🔬 Testing 2 Bug Fixes..."
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "BUG #1: UnknownSymbol"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
go test ./pkg/canonicalizer -run TestMT5_UnknownSymbol -v
BUG1=$?

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "BUG #2: ReconnectOnDisconnect"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
go test ./pkg/adapter -run TestMT5_ReconnectOnDisconnect -v
BUG2=$?

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "RESULT"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ $BUG1 -eq 0 ] && [ $BUG2 -eq 0 ]; then
    echo "✅ BOTH BUGS FIXED!"
    echo ""
    echo "Ready for full suite:"
    echo "  ./FINAL_TEST.sh"
    exit 0
else
    echo "❌ STILL FAILING:"
    [ $BUG1 -ne 0 ] && echo "  - Bug #1: UnknownSymbol"
    [ $BUG2 -ne 0 ] && echo "  - Bug #2: ReconnectOnDisconnect"
    exit 1
fi
