#!/bin/bash
# DolphinDB Official Docker Setup (Quick Start)
# Based on: https://docs.dolphindb.com/en/Tutorials/docker_single_deployment.html

set -e

echo "════════════════════════════════════════════════════════════"
echo "  DolphinDB Official Docker Setup (Quick Start)"
echo "════════════════════════════════════════════════════════════"
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed"
    exit 1
fi

echo "✅ Docker found"
echo ""

# Check if DolphinDB is already running
if docker ps | grep -q dolphindb; then
    echo "⚠️  DolphinDB container is already running"
    read -p "   Stop and recreate? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🛑 Stopping existing container..."
        docker stop dolphindb 2>/dev/null || true
        docker rm dolphindb 2>/dev/null || true
    else
        echo "   Keeping existing container"
        docker ps | grep dolphindb
        exit 0
    fi
fi

# Pull official DolphinDB image
echo "🐋 Pulling DolphinDB official image (v2.00.10)..."
docker pull dolphindb/dolphindb:v2.00.10

echo ""
echo "🚀 Starting DolphinDB container..."

# Quick Start command (from official docs)
docker run -itd --name dolphindb \
  --hostname dolphindb-host \
  -p 8848:8848 \
  -v /etc:/dolphindb/etc \
  dolphindb/dolphindb:v2.00.10 \
  sh

echo ""
echo "⏳ Waiting for DolphinDB to start (max 30s)..."
for i in {1..30}; do
    if docker exec dolphindb curl -s -f http://localhost:8848 > /dev/null 2>&1; then
        echo "✅ DolphinDB is ready!"
        break
    fi
    echo -n "."
    sleep 1
done
echo ""

# Check if started successfully
if ! docker ps | grep -q dolphindb; then
    echo "❌ DolphinDB failed to start"
    echo "   Check logs: docker logs dolphindb"
    exit 1
fi

# Test connection
echo ""
echo "🧪 Testing connection..."
RESPONSE=$(docker exec dolphindb curl -s -X POST http://localhost:8848 -d "1+1" 2>&1 || echo "error")

if [ "$RESPONSE" = "2" ]; then
    echo "✅ Connection test passed (1+1=2)"
else
    echo "⚠️  Connection test returned: $RESPONSE"
    echo "   DolphinDB may still be initializing..."
fi

# Initialize schema
echo ""
echo "📊 Initializing database schema..."
sleep 2

# Create init script (inline)
INIT_SCRIPT='
login("admin", "123456")
try { dropDatabase("dfs://raw_data") } catch(ex) { print("Creating new database") }
db = database("dfs://raw_data", VALUE, 2020.01.01..2030.12.31)

// raw_events table
schema_raw = table(1:0, `event_id`timestamp`source`payload`sequence_num, [LONG, TIMESTAMP, SYMBOL, BLOB, LONG])
raw_events = db.createPartitionedTable(schema_raw, `raw_events, `timestamp)
print("Created table: raw_events")

// canonical_events table  
schema_canonical = table(1:0, `event_id`timestamp`event_type`canonical_symbol`provider_symbol`source`asset_class`price`size`exchange_timestamp`local_timestamp`metadata, [LONG, TIMESTAMP, SYMBOL, SYMBOL, SYMBOL, SYMBOL, SYMBOL, DOUBLE, DOUBLE, LONG, LONG, BLOB])
canonical_events = db.createPartitionedTable(schema_canonical, `canonical_events, `timestamp)
print("Created table: canonical_events")

print("Schema initialization complete!")
'

# Execute init script
echo "$INIT_SCRIPT" | docker exec -i dolphindb curl -s -X POST http://localhost:8848 -d @- > /tmp/dolphindb_init.log 2>&1

if [ $? -eq 0 ]; then
    echo "✅ Schema initialized successfully"
    cat /tmp/dolphindb_init.log | head -10
else
    echo "⚠️  Schema initialization may have issues"
    echo "   Check: /tmp/dolphindb_init.log"
fi

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  DolphinDB Setup Complete!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "📋 Connection Details:"
echo "   Host: localhost"
echo "   Port: 8848"
echo "   Username: admin"
echo "   Password: 123456"
echo ""
echo "📊 Database:"
echo "   Name: dfs://raw_data"
echo "   Tables: raw_events, canonical_events"
echo ""
echo "🔍 Useful Commands:"
echo "   View logs:    docker logs -f dolphindb"
echo "   Stop:         docker stop dolphindb"
echo "   Start:        docker start dolphindb"
echo "   Remove:       docker rm -f dolphindb"
echo "   Shell:        docker exec -it dolphindb bash"
echo ""
echo "🌐 HTTP API:"
echo "   curl -X POST http://localhost:8848 -d 'version()'"
echo ""
echo "✅ Ready to use with Raw Data Layer!"
echo ""
