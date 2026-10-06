# DolphinDB Schema Application — DevOps Plan

**Project:** SparkTrade-MFT Raw Data Layer  
**Date:** 2026-10-03  
**Status:** 🔴 CRITICAL — Only remaining blocker for production data persistence  
**Estimated Time:** 10 minutes (5 min apply + 2 min verify + 3 min test)

---

## 📋 Executive Summary

The DolphinDB container is running on port 8848, but the database tables (`raw_events`, `canonical_events`) have not been created. **427 events are pending** in the DolphinDB writer batch queue — they will be lost if the process restarts before schema is applied.

**This plan provides a zero-risk, idempotent, observable procedure to apply the schema.**

---

## 🏗️ Architecture Context

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         RAW DATA LAYER DEPLOYMENT                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐               │
│   │   Binance    │     │    IB        │     │    MT5       │               │
│   │  WebSocket   │     │   Gateway    │     │   (ZMQ)      │               │
│   └──────┬───────┘     └──────┬───────┘     └──────┬───────┘               │
│          │                    │                    │                         │
│          ▼                    ▼                    ▼                         │
│   ┌─────────────────────────────────────────────────────────────────┐      │
│   │                    RAW DATA LAYER (Single Process)               │      │
│   │  ┌─────────┐  ┌───────────┐  ┌────────────┐  ┌─────────────┐   │      │
│   │  │Adapter  │→ │Worker Pool│→ │Canonicalizer│→ │ 5-Layer     │   │      │
│   │  │(Binance)│  │  (50)     │  │ (Axiom)    │  │ Validator   │   │      │
│   │  └─────────┘  └───────────┘  └────────────┘  └──────┬──────┘   │      │
│   │                                                     │            │      │
│   │  ┌─────────┐    ┌────────────┐    ┌───────────────▼────┐      │      │
│   │  │   WAL   │←───│  ZMQ Pub   │←───│  DolphinDB Writer  │      │      │
│   │  │(Lossless)│    │(Port 5555) │    │  (Batch 1000/1s) │      │      │
│   │  └─────────┘    └────────────┘    └────────┬──────────┘      │      │
│   └─────────────────────────────────────────────┼────────────────┘      │
│                                                 │                         │
│                                                 ▼                         │
│                                    ┌───────────────────────┐             │
│                                    │    DOLPHINDB          │             │
│                                    │  dfs://raw_data       │             │
│                                    │  ┌─────────────────┐  │             │
│                                    │  │ raw_events      │  │  ← SCHEMA  │
│                                    │  │ canonical_events│  │    MISSING │
│                                    │  └─────────────────┘  │             │
│                                    └───────────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## ✅ Pre-Flight Checklist (Run First — 1 Minute)

### 1. Verify Docker Environment
```bash
# Check Docker is available
docker version --format '{{.Server.Version}}'
# Expected: 24.x or 25.x+

# Check docker-compose
docker-compose version --short
# Expected: v2.x
```

### 2. Verify DolphinDB Container Status
```bash
# Check container is running
docker ps --filter "name=dolphindb" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Expected output:
# NAMES      STATUS         PORTS
# dolphindb  Up X minutes   0.0.0.0:8848->8848/tcp
```

### 3. Verify HTTP API Accessibility
```bash
# Test HTTP endpoint
curl -s -o /dev/null -w "%{http_code}" http://localhost:8848
# Expected: 200

# Test basic query
curl -s -X POST http://localhost:8848/run -d "1+1"
# Expected: 2
```

### 4. Verify Schema File Exists
```bash
ls -la docker/dolphindb/init/init_schema.dos
# Expected: File exists, ~3KB
```

### 5. Verify Config Alignment
```bash
# Check config.yaml matches container
grep -A 5 "dolphindb:" config/config.yaml
# Expected: host: "localhost", port: 8848, enabled: true
```

### 6. Check for Pending Events (Critical Context)
```bash
# Check health endpoint for pending count
curl -s http://localhost:8080/health 2>/dev/null | jq '.db' || echo "Health endpoint not available (app not running)"
# Expected: {"connected":true,"total_written":0,"pending":427}
```

---

## 🚀 Schema Application Procedure (Idempotent — 2 Minutes)

### Option A: Automated (Recommended — Uses setup script)
```bash
cd ~/Desktop/raw-data-layer/docker

# Run the setup script — it handles container check, wait, schema apply, verify
./setup_dolphindb.sh
```

### Option B: Manual Step-by-Step (For Control/Observability)

#### Step 1: Ensure Container Running
```bash
cd ~/Desktop/raw-data-layer/docker

# If using docker-compose.dolphindb.yml
docker-compose -f docker-compose.dolphindb.yml up -d

# Wait for healthcheck (max 60s)
for i in {1..60}; do
    if curl -s -f http://localhost:8848 > /dev/null 2>&1; then
        echo "✅ DolphinDB ready at $(date)"
        break
    fi
    sleep 1
done
```

#### Step 2: Apply Schema via HTTP API (Idempotent)
```bash
# The init_schema.dos script is idempotent — safe to run multiple times
echo "📊 Applying schema..."
curl -s -X POST http://localhost:8848/run \
  -H 'Content-Type: text/plain' \
  -d @../docker/dolphindb/init/init_schema.dos
```

**Expected Output:**
```
Step 1: Creating database...
✅ Database dfs://raw_data created (VALUE partition by month)

Step 2: Creating raw_events table...
✅ Table raw_events created
   Columns: event_id(STRING), source(SYMBOL), payload(BLOB), received_at(TIMESTAMP), sequence_num(LONG)
   Partition: received_at (monthly)

Step 3: Creating canonical_events table...
✅ Table canonical_events created
   Columns: event_id, canonical_symbol, exchange_timestamp, local_hw_timestamp, event_type, price, size, side, source, raw_event_id
   Partition: exchange_timestamp (monthly)

Step 4: Verifying tables...
✅ raw_events row count: 0
✅ canonical_events row count: 0

═══════════════════════════════════════════════════════════
  DolphinDB Schema Initialization Complete!
═══════════════════════════════════════════════════════════

Database: dfs://raw_data
Tables:
  1. raw_events (BLOB payload storage)
  2. canonical_events (normalized market data)

Ready to receive real market data from adapters!
═══════════════════════════════════════════════════════════
```

---

## ✅ Post-Application Verification (2 Minutes)

### 1. Verify Database Exists
```bash
curl -s -X POST http://localhost:8848/run \
  -d 'existsDatabase("dfs://raw_data")'
# Expected: true
```

### 2. Verify Tables Created
```bash
# raw_events
curl -s -X POST http://localhost:8848/run \
  -d "select count(*) from loadTable('dfs://raw_data', 'raw_events')"
# Expected: 0

# canonical_events
curl -s -X POST http://localhost:8848/run \
  -d "select count(*) from loadTable('dfs://raw_data', 'canonical_events')"
# Expected: 0
```

### 3. Verify Table Schemas
```bash
# raw_events schema
curl -s -X POST http://localhost:8848/run \
  -d "schema(loadTable('dfs://raw_data', 'raw_events'))"

# Expected columns: event_id(STRING), source(SYMBOL), payload(BLOB), received_at(TIMESTAMP), sequence_num(LONG)

# canonical_events schema
curl -s -X POST http://localhost:8848/run \
  -d "schema(loadTable('dfs://raw_data', 'canonical_events'))"

# Expected columns: event_id, canonical_symbol, exchange_timestamp, local_hw_timestamp, event_type, price, size, side, source, raw_event_id
```

### 4. Verify Partition Strategy
```bash
curl -s -X POST http://localhost:8848/run \
  -d 'schema(database("dfs://raw_data"))'
# Expected: VALUE partition, 2020.01M..2030.12M
```

---

## 🧪 End-to-End Integration Test (3 Minutes)

### 1. Start Raw Data Layer with DolphinDB Enabled
```bash
cd ~/Desktop/raw-data-layer

# Run for 60 seconds with Binance + DolphinDB
timeout 60 go run ./cmd/raw-data-layer/main.go \
  --binance=true \
  --ib=false \
  --db=true \
  --log-level=info
```

### 2. Monitor Real-Time Writes (in another terminal)
```bash
# Watch event count grow
watch -n 2 'curl -s -X POST http://localhost:8848/run -d "
select count(*) as total from loadTable(\"dfs://raw_data\", \"canonical_events\")
"'
```

### 3. Verify Health Endpoint
```bash
curl -s http://localhost:8080/health | jq
```

**Expected Health Output:**
```json
{
  "status": "healthy",
  "adapters": {
    "binance": {
      "connected": true,
      "messages_received": 50,
      "last_message_at": "2026-10-03T14:05:00Z"
    }
  },
  "worker_pool": {
    "active_workers": 50,
    "queue_depth": 0,
    "messages_processed": 50
  },
  "db": {
    "connected": true,
    "total_written": 50,
    "pending": 0
  },
  "wal": {
    "files": 1,
    "total_size": "12.3 KB",
    "current_file": "wal_20261003_140500_000001.jsonl"
  }
}
```

**Critical Success Indicator:** `"pending": 0` and `"total_written": > 0`

### 4. Query Sample Data
```bash
# Last 5 canonical events
curl -s -X POST http://localhost:8848/run -d "
select top 5 event_id, canonical_symbol, price, size, side, exchange_timestamp
from loadTable('dfs://raw_data', 'canonical_events')
order by exchange_timestamp desc
"

# Expected: BTC/USD, ETH/USD, BNB/USD trades with valid prices/sizes
```

---

## 🔄 Rollback Procedure (If Something Goes Wrong)

### Scenario 1: Schema Apply Fails
```bash
# Check DolphinDB logs
docker logs dolphindb --tail 100

# Common fix: Restart container and retry
docker-compose -f docker/docker-compose.dolphindb.yml restart

# Wait for health, then re-apply
sleep 10
curl -s -X POST http://localhost:8848/run \
  -H 'Content-Type: text/plain' \
  -d @../docker/dolphindb/init/init_schema.dos
```

### Scenario 2: Tables Created But Wrong Schema
```bash
# Drop and recreate (⚠️ DELETES DATA — only use if no production data)
curl -s -X POST http://localhost:8848/run -d "
login('admin', '123456')
dropTable('dfs://raw_data', 'raw_events')
dropTable('dfs://raw_data', 'canonical_events')
"

# Re-apply schema
curl -s -X POST http://localhost:8848/run \
  -H 'Content-Type: text/plain' \
  -d @../docker/dolphindb/init/init_schema.dos
```

### Scenario 3: Container Won't Start
```bash
# Full reset
docker-compose -f docker/docker-compose.dolphindb.yml down -v
docker-compose -f docker/docker-compose.dolphindb.yml up -d

# Wait and re-apply
sleep 30
curl -s -X POST http://localhost:8848/run \
  -H 'Content-Type: text/plain' \
  -d @../docker/dolphindb/init/init_schema.dos
```

---

## 📊 Monitoring & Alerting Setup (Post-Schema)

### 1. Prometheus Metrics to Watch
```yaml
# Key metrics from raw-data-layer
- raw_data_messages_received_total{source="binance"}
- raw_data_adapter_latency_microseconds{source="binance"}
- raw_data_queue_depth
- raw_data_backpressure_total
- raw_data_wal_writes_total
- raw_data_dolphindb_writes_total
- raw_data_dolphindb_write_errors_total
```

### 2. Critical Alert Rules
```yaml
groups:
  - name: raw-data-layer
    rules:
      - alert: DolphinDBWriteFailures
        expr: rate(raw_data_dolphindb_write_errors_total[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "DolphinDB write errors detected"

      - alert: DolphinDBPendingEvents
        expr: raw_data_dolphindb_pending_events > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "DolphinDB batch queue backing up"

      - alert: DolphinDBConnectionDown
        expr: up{job="raw-data-layer"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Raw Data Layer unreachable"
```

### 3. Grafana Dashboard Panels (Already in deployments/grafana/)
- Throughput (msg/s) — target >100K batched
- Latency p50/p99 — target p99 <500µs
- Queue depth — alert >8000
- WAL rotation rate
- DolphinDB write latency
- Pending batch size

---

## 🐳 Docker Compose Integration (Production)

### Combined Stack (DolphinDB + Raw Data Layer + Monitoring)
```bash
# Start everything
cd ~/Desktop/raw-data-layer/docker

# 1. Start DolphinDB first
docker-compose -f docker-compose.dolphindb.yml up -d

# 2. Wait and apply schema (automated in setup script)
./setup_dolphindb.sh

# 3. Start Raw Data Layer + Prometheus + Grafana
docker-compose up -d

# 4. Verify all healthy
docker-compose ps
curl http://localhost:8080/health
curl http://localhost:9090/-/healthy
curl http://localhost:3000/api/health
```

### Environment Variables (.env)
```bash
# Create .env for production
cat > .env <<'EOF'
DOLPHINDB_HOST=localhost
DOLPHINDB_PORT=8848
DOLPHINDB_USER=admin
DOLPHINDB_PASSWORD=CHANGE_ME_IN_PRODUCTION
BINANCE_ENDPOINT=wss://stream.binance.com:9443/ws
IB_HOST=your-ib-gateway-host
IB_PORT=7497
GRAFANA_USER=admin
GRAFANA_PASSWORD=CHANGE_ME_IN_PRODUCTION
EOF
```

---

## 🔐 Security Hardening (Production Checklist)

| Item | Status | Action |
|------|--------|--------|
| DolphinDB admin password | ⚠️ Default (123456) | **Change before production** |
| Network isolation | ✅ Bridge network | Use custom network |
| TLS for HTTP API | ❌ Not configured | Add reverse proxy (nginx) |
| Auth for health endpoint | ❌ Open | Add basic auth or VPN |
| Secrets management | ⚠️ .env file | Use Docker secrets / Vault |

### Change DolphinDB Password
```bash
# After schema applied, change password
curl -s -X POST http://localhost:8848/run -d "
login('admin', '123456')
setUserPassword('admin', 'NEW_SECURE_PASSWORD')
"

# Update config.yaml and .env
# Restart Raw Data Layer
docker-compose restart raw-data-layer
```

---

## 📝 Runbook: Schema Application

### Quick Reference Card

| Step | Command | Expected | Timeout |
|------|---------|----------|---------|
| 1. Check container | `docker ps | grep dolphindb` | Up, port 8848 | 10s |
| 2. Test API | `curl -X POST localhost:8848/run -d "1+1"` | `2` | 5s |
| 3. Apply schema | `curl -X POST localhost:8848/run -d @docker/dolphindb/init/init_schema.dos` | Tables created | 10s |
| 4. Verify tables | `curl -X POST localhost:8848/run -d "select count(*) from loadTable('dfs://raw_data', 'canonical_events')"` | `0` | 5s |
| 5. Test write | `timeout 60 go run ./cmd/raw-data-layer/main.go --binance=true --db=true` | Events written | 60s |
| 6. Health check | `curl localhost:8080/health \| jq .db` | `pending: 0` | 5s |

### Success Criteria Checklist
- [ ] DolphinDB container running and healthy
- [ ] HTTP API responds to `1+1` → `2`
- [ ] Schema script executes without errors
- [ ] `raw_events` table exists with correct schema
- [ ] `canonical_events` table exists with correct schema
- [ ] Monthly VALUE partitioning (2020.01M..2030.12M)
- [ ] Raw Data Layer writes events (health: `pending: 0`, `total_written > 0`)
- [ ] Query returns recent market data (BTC/USD, ETH/USD, etc.)
- [ ] WAL file created with new events
- [ ] Prometheus metrics incrementing

---

## 🚨 Troubleshooting Quick Reference

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| `curl: (7) Failed to connect` | Container not running / port wrong | `docker ps`, check port mapping |
| `existsDatabase` returns false | Schema not applied | Re-run schema script |
| `Table not found` error | Table creation failed | Check DolphinDB logs, re-apply |
| `pending` never decreases | Writer can't connect | Check config.yaml host/port |
| `Connection refused` from app | Network mismatch | Use `localhost` outside Docker, container name inside |
| Schema script timeout | DolphinDB busy | Wait 30s, retry |
| `raw_events` count stays 0 | Binance not sending data | Check Binance WS connection, symbols |

---

## 📚 Related Documentation

| Document | Purpose |
|----------|---------|
| `LAYIHE_ANALIZ.md` | Full project analysis (Azerbaijani) |
| `RAPORT_2026_08_06.md` | Execution report with 10-min action plan |
| `DOLPHINDB_QUICKSTART.md` | Connection & testing guide |
| `docker/dolphindb/README.md` | Complete Docker setup guide |
| `docker/dolphindb/init/init_schema.dos` | Schema script (source of truth) |
| `config/config.yaml` | System configuration |
| `PROGRESS.md` | Implementation progress (Step A — HTTP REST path) |

---

## 🎯 Sign-Off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| DevOps Engineer | | | |
| Backend Engineer | | | |
| Data Engineer | | | |

---

**Next Steps After Schema Applied:**
1. ✅ Verify real data persistence (this plan)
2. 🔧 Implement IB Gateway real protocol (2-3 days) — `hadrianl/ibapi`
3. 🚀 Load test 100K+ msg/s (1 day)
4. ☸️ Production deploy to Kubernetes (3-4 days) — Helm charts ready
5. 📊 Deploy Grafana dashboards (4 hours)
6. 🚨 Configure Alertmanager (1 day)

---

**Plan Version:** 1.0  
**Last Updated:** 2026-10-03  
**Status:** Ready for execution