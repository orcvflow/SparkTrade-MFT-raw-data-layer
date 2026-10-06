#!/bin/bash
# Quick retest after bug fixes

cd /home/main/Desktop/raw-data-layer

echo "🔧 Retesting after bug fixes..."
echo ""

echo "1. TestMT5_UnknownSymbol (was failing):"
go test ./pkg/canonicalizer -run TestMT5_UnknownSymbol -v
echo ""

echo "2. TestMT5_ReconnectOnDisconnect (was failing):"
go test ./pkg/adapter -run TestMT5_ReconnectOnDisconnect -v
echo ""

echo "3. All Canonicalizer MT5 tests:"
go test ./pkg/canonicalizer -run TestMT5 -v
echo ""

echo "4. All Adapter MT5 tests:"
go test ./pkg/adapter -run TestMT5 -v
echo ""

echo "5. Coverage check:"
go test ./pkg/canonicalizer -run TestMT5 -coverprofile=/tmp/canon_final.out
go tool cover -func=/tmp/canon_final.out | grep mt5
echo ""

echo "✅ Retest complete!"
