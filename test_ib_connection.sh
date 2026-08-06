#!/bin/bash
# IB Gateway API Connection Test Script

set -e

echo "═══════════════════════════════════════════════════════════"
echo "  IB Gateway API Connection Test"
echo "═══════════════════════════════════════════════════════════"
echo ""

# 1. Check if IB Gateway is running
echo "🔍 Checking if IB Gateway is running..."
if ps aux | grep -i "ibgateway\|GWClient" | grep -v grep > /dev/null; then
    echo "✅ IB Gateway process is running"
    ps aux | grep -i "ibgateway\|GWClient" | grep -v grep | head -1
else
    echo "❌ IB Gateway is NOT running"
    echo "   Please start IB Gateway first"
    exit 1
fi

echo ""

# 2. Check if port 7497 is open
echo "🔍 Checking if API port 7497 is listening..."
if timeout 3 bash -c 'cat < /dev/null > /dev/tcp/localhost/7497' 2>/dev/null; then
    echo "✅ Port 7497 is OPEN"
else
    echo "❌ Port 7497 is CLOSED or not accessible"
    echo ""
    echo "📋 Action Required:"
    echo "   1. Open IB Gateway"
    echo "   2. Go to: Configure → Settings → API → Settings"
    echo "   3. Check: ✓ Enable ActiveX and Socket Clients"
    echo "   4. Set Socket port: 7497"
    echo "   5. Click: Apply → OK"
    echo "   6. Restart IB Gateway"
    echo ""
    echo "   See IB_GATEWAY_SETUP.md for detailed instructions"
    exit 1
fi

echo ""

# 3. Check configuration
echo "🔍 Checking config.yaml IB settings..."
if grep -q "port: 7497" config/config.yaml; then
    echo "✅ config.yaml has port: 7497"
else
    echo "⚠️  config.yaml port setting not found or different"
fi

if grep -q "enabled: true" config/config.yaml | grep -A 5 "ib:"; then
    echo "✅ IB adapter is enabled in config"
else
    echo "❌ IB adapter is disabled in config"
fi

echo ""

# 4. Test simple connection
echo "🧪 Testing raw TCP connection to IB Gateway..."
(echo "" | timeout 2 nc -v localhost 7497) 2>&1 | head -3 || true

echo ""

# 5. Build and quick test (if requested)
if [ "$1" == "--test-adapter" ]; then
    echo "🏗️  Building adapter..."
    go build -o bin/adapter ./cmd/adapter
    
    echo ""
    echo "🚀 Starting adapter (5 second test)..."
    echo "   Press Ctrl+C to stop early"
    echo ""
    
    timeout 5 ./bin/adapter --binance=false --ib=true 2>&1 | grep -E "level|IB|connect|health" || true
    
    echo ""
    echo "✅ Adapter test completed"
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Summary"
echo "═══════════════════════════════════════════════════════════"
echo "✅ IB Gateway is running"
echo "✅ Port 7497 is accessible"
echo ""
echo "Next steps:"
echo "  1. If port was closed, follow IB_GATEWAY_SETUP.md"
echo "  2. Run full adapter test:"
echo "     ./test_ib_connection.sh --test-adapter"
echo "  3. Or run manually:"
echo "     go run ./cmd/adapter/main.go"
echo ""
