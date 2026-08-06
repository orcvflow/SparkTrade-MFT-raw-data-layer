#!/bin/bash
set -e

cd /home/main/Desktop/raw-data-layer

echo "Starting raw-data-layer with DB enabled..."
timeout 30 go run ./cmd/raw-data-layer/main.go \
  --binance=true \
  --ib=false \
  --zmq=false \
  --db=true \
  --log-level=info > /tmp/rdl_test.log 2>&1 &

RDL_PID=$!
echo "PID: $RDL_PID"

echo "Waiting 10 seconds for startup..."
sleep 10

echo ""
echo "=== Health Check ==="
wget -q -O- http://localhost:8080/health 2>/dev/null | head -100 || echo "Failed"

echo ""
echo "=== Last 30 log lines ==="
tail -30 /tmp/rdl_test.log

echo ""
echo "Stopping..."
kill $RDL_PID 2>/dev/null || true
wait $RDL_PID 2>/dev/null || true
echo "Done"
