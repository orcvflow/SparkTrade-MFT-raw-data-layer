#!/bin/bash
set -e

cd /home/main/Desktop/raw-data-layer

echo "=== Real Data Test (60 seconds) ==="
echo "Starting raw-data-layer..."

timeout 60 go run ./cmd/raw-data-layer/main.go \
  --binance=true \
  --ib=false \
  --zmq=false \
  --db=true \
  --log-level=info > /tmp/rdl_60s.log 2>&1 &

RDL_PID=$!
echo "PID: $RDL_PID"
echo ""

echo "Waiting 10s for initialization..."
sleep 10

echo "=== Health @ 10s ==="
wget -q -O- http://localhost:8080/health 2>/dev/null | head -100
echo ""

echo "Waiting 20s more..."
sleep 20

echo "=== Health @ 30s ==="
wget -q -O- http://localhost:8080/health 2>/dev/null | head -100
echo ""

echo "Waiting 30s more (total 60s)..."
sleep 30

echo "=== Final Health @ 60s ==="
wget -q -O- http://localhost:8080/health 2>/dev/null | head -100
echo ""

echo "=== WAL Files ==="
ls -lh data/wal/*.jsonl | tail -5
echo ""
wc -l data/wal/*.jsonl | tail -5
echo ""

echo "=== Last 20 Log Lines ==="
tail -20 /tmp/rdl_60s.log

echo ""
echo "Test complete. Stopping..."
kill $RDL_PID 2>/dev/null || true
wait $RDL_PID 2>/dev/null || true
echo "Done."
