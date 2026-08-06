#!/bin/bash
# DolphinDB Docker Setup Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "════════════════════════════════════════════════════════════"
echo "  DolphinDB Docker Setup"
echo "════════════════════════════════════════════════════════════"
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed"
    echo "   Install: https://docs.docker.com/engine/install/"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not installed"
    echo "   Install: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "✅ Docker and docker-compose found"
echo ""

# Check if DolphinDB is already running
if docker ps | grep -q dolphindb; then
    echo "⚠️  DolphinDB container is already running"
    read -p "   Stop and recreate? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🛑 Stopping existing container..."
        docker-compose -f "$SCRIPT_DIR/docker-compose.dolphindb.yml" down
    else
        echo "   Keeping existing container"
        exit 0
    fi
fi

# Start DolphinDB
echo "🚀 Starting DolphinDB..."
docker-compose -f "$SCRIPT_DIR/docker-compose.dolphindb.yml" up -d

# Wait for DolphinDB to be ready
echo ""
echo "⏳ Waiting for DolphinDB to start (max 60s)..."
for i in {1..60}; do
    if docker exec dolphindb curl -s -f http://localhost:8848 > /dev/null 2>&1; then
        echo "✅ DolphinDB is ready!"
        break
    fi
    echo -n "."
    sleep 1
done
echo ""

# Check if DolphinDB started successfully
if ! docker ps | grep -q dolphindb; then
    echo "❌ DolphinDB failed to start"
    echo "   Check logs: docker logs dolphindb"
    exit 1
fi

# Initialize schema
echo ""
echo "📊 Initializing database schema..."
sleep 2

# Execute init script via HTTP API
INIT_SCRIPT=$(cat "$SCRIPT_DIR/dolphindb/init/init_schema.dos")

# Use curl to execute the script
curl -s -X POST \
  http://localhost:8848/run \
  -H 'Content-Type: text/plain' \
  -d "$INIT_SCRIPT" > /tmp/dolphindb_init.log 2>&1

if [ $? -eq 0 ]; then
    echo "✅ Schema initialized successfully"
    cat /tmp/dolphindb_init.log
else
    echo "⚠️  Schema initialization may have issues"
    echo "   Check: /tmp/dolphindb_init.log"
fi

# Test connection
echo ""
echo "🧪 Testing connection..."
RESPONSE=$(curl -s -X POST http://localhost:8848/run -d "1+1" 2>&1)

if [ "$RESPONSE" = "2" ]; then
    echo "✅ Connection test passed"
else
    echo "⚠️  Connection test returned: $RESPONSE"
fi

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  DolphinDB Setup Complete!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "📋 Connection Details:"
echo "   Host: localhost"
echo "   Port: 8848 (HTTP API)"
echo "   Username: admin"
echo "   Password: 123456"
echo ""
echo "📊 Database:"
echo "   Name: dfs://raw_data"
echo "   Tables: raw_events, canonical_events"
echo ""
echo "🔍 Useful Commands:"
echo "   View logs:    docker logs -f dolphindb"
echo "   Stop:         docker-compose -f $SCRIPT_DIR/docker-compose.dolphindb.yml down"
echo "   Restart:      docker-compose -f $SCRIPT_DIR/docker-compose.dolphindb.yml restart"
echo "   Shell access: docker exec -it dolphindb bash"
echo ""
echo "🌐 Web Interface:"
echo "   http://localhost:8848"
echo ""
echo "✅ Ready to use with Raw Data Layer!"
echo "   Update config.yaml:"
echo "   storage.dolphindb.enabled: true"
echo ""
