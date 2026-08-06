# DolphinDB Docker Setup for Raw Data Layer

**Complete DolphinDB time-series database setup with Docker**

---

## 🚀 Quick Start

### 1. Start DolphinDB (Automated)
```bash
cd ~/Desktop/raw-data-layer/docker
./setup_dolphindb.sh
```

**What it does:**
- ✅ Pulls DolphinDB Docker image
- ✅ Starts DolphinDB container on port 8848
- ✅ Creates `dfs://raw_data` database
- ✅ Creates `raw_events` and `canonical_events` tables
- ✅ Tests connection

---

## 📋 Manual Setup (Alternative)

### 1. Start Container
```bash
cd ~/Desktop/raw-data-layer/docker
docker-compose -f docker-compose.dolphindb.yml up -d
```

### 2. Wait for Startup (30 seconds)
```bash
docker logs -f dolphindb
# Wait for: "DolphinDB server is running"
```

### 3. Initialize Schema
```bash
# Execute init script
docker exec -i dolphindb /DolphinDB/server/dolphindb \
  -home /data \
  -console 0 \
  -script /DolphinDB/server/init/init_schema.dos
```

---

## 🔍 Verification

### Check Container Status
```bash
docker ps | grep dolphindb
```

**Expected:**
```
CONTAINER ID   IMAGE                        STATUS         PORTS
abc123def456   dolphindb/dolphindb:latest   Up 2 minutes   0.0.0.0:8848->8848/tcp
```

### Test HTTP API
```bash
curl -X POST http://localhost:8848/run -d "1+1"
```

**Expected:** `2`

### Test Database
```bash
curl -X POST http://localhost:8848/run -d "existsDatabase(\"dfs://raw_data\")"
```

**Expected:** `true`

### Query Tables
```bash
# Check raw_events table
curl -X POST http://localhost:8848/run -d "select count(*) from loadTable(\"dfs://raw_data\", \"raw_events\")"

# Check canonical_events table
curl -X POST http://localhost:8848/run -d "select count(*) from loadTable(\"dfs://raw_data\", \"canonical_events\")"
```

---

## 🔧 Configuration

### Connection Details
| Parameter | Value |
|-----------|-------|
| Host | `localhost` |
| Port | `8848` (HTTP API) |
| Username | `admin` |
| Password | `123456` |
| Database | `dfs://raw_data` |

### Update Raw Data Layer Config
**config/config.yaml:**
```yaml
storage:
  dolphindb:
    enabled: true        # Enable DolphinDB
    host: "localhost"
    port: 8848
    username: "admin"
    password: "123456"
    database: "raw_data"
    batch_size: 1000
    batch_timeout: "1s"
```

### Update Environment Variables
**.env:**
```bash
DOLPHINDB_HOST=localhost
DOLPHINDB_PORT=8848
DOLPHINDB_USER=admin
DOLPHINDB_PASSWORD=123456
```

---

## 📊 Database Schema

### Table 1: `raw_events` (BLOB Storage)
Stores **untouched** raw payloads from adapters (paranoid: never lose data).

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | LONG | Unique event ID |
| `timestamp` | TIMESTAMP | Event timestamp (partition key) |
| `source` | SYMBOL | Data source (IB, Binance, etc.) |
| `payload` | BLOB | Raw binary payload (byte-for-byte) |
| `sequence_num` | LONG | Sequence number |

**Partition:** By `timestamp` (daily)

### Table 2: `canonical_events` (Structured Data)
Normalized, validated market data in canonical format.

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | LONG | Unique event ID |
| `timestamp` | TIMESTAMP | Event timestamp (partition key) |
| `event_type` | SYMBOL | TRADE, QUOTE, BOOK_UPDATE, etc. |
| `canonical_symbol` | SYMBOL | Normalized symbol (AAPL, BTCUSDT) |
| `provider_symbol` | SYMBOL | Original symbol from source |
| `source` | SYMBOL | Data source (IB, Binance) |
| `asset_class` | SYMBOL | EQUITY, CRYPTO, FOREX, FUTURE |
| `price` | DOUBLE | Sanitized price (Axle-Axiom validated) |
| `size` | DOUBLE | Sanitized size/volume |
| `exchange_timestamp` | LONG | Exchange-provided timestamp (ns) |
| `local_timestamp` | LONG | Local receive timestamp (ns) |
| `metadata` | BLOB | JSON metadata (exchange-specific) |

**Partition:** By `timestamp` (daily)

---

## 🧪 Testing with Raw Data Layer

### 1. Enable DolphinDB in Config
```bash
cd ~/Desktop/raw-data-layer
cat config/config.yaml | grep -A 10 "dolphindb:"
```

Set `enabled: true`

### 2. Run Adapter (IB or Binance)
```bash
# With DolphinDB enabled
go run ./cmd/raw-data-layer/main.go \
  --binance=true \
  --ib=false \
  --zmq=false \
  --db=true \
  --log-level=info
```

### 3. Monitor Writes
```bash
# Watch log
docker logs -f dolphindb | grep -i "write\|insert"

# Query event count
watch -n 2 'curl -s -X POST http://localhost:8848/run -d "select count(*) from loadTable(\"dfs://raw_data\", \"raw_events\")"'
```

### 4. Query Recent Events
```bash
# Last 10 raw events
curl -X POST http://localhost:8848/run -d "
select top 10 * from loadTable('dfs://raw_data', 'raw_events') 
order by timestamp desc
"

# Last 10 canonical events
curl -X POST http://localhost:8848/run -d "
select top 10 event_type, canonical_symbol, price, size, timestamp 
from loadTable('dfs://raw_data', 'canonical_events') 
order by timestamp desc
"
```

---

## 🛠️ Management

### View Logs
```bash
docker logs -f dolphindb
```

### Restart
```bash
docker-compose -f docker/docker-compose.dolphindb.yml restart
```

### Stop
```bash
docker-compose -f docker/docker-compose.dolphindb.yml down
```

### Stop + Delete Data
```bash
docker-compose -f docker/docker-compose.dolphindb.yml down -v
```

### Shell Access
```bash
docker exec -it dolphindb bash
```

### Connect via DolphinDB GUI (Optional)
```bash
# Start GUI in another container (if needed)
docker run -d -p 8850:8850 dolphindb/dolphindb-gui
# Open: http://localhost:8850
```

---

## 📈 Performance Monitoring

### Check Write Rate
```bash
# Monitor inserts per second
watch -n 1 'curl -s -X POST http://localhost:8848/run -d "
select count(*) as total_events 
from loadTable(\"dfs://raw_data\", \"canonical_events\")
"'
```

### Check Storage Size
```bash
docker exec dolphindb du -sh /data
```

### Memory Usage
```bash
docker stats dolphindb
```

---

## 🐛 Troubleshooting

### Problem 1: Container Won't Start
**Check:**
```bash
docker logs dolphindb
```

**Common causes:**
- Port 8848 already in use
- Insufficient disk space
- Corrupted data volume

**Fix:**
```bash
# Check port
sudo lsof -i :8848

# Clean start
docker-compose -f docker/docker-compose.dolphindb.yml down -v
./setup_dolphindb.sh
```

### Problem 2: Schema Init Failed
**Manually execute:**
```bash
curl -X POST http://localhost:8848/run -d "
login('admin', '123456')
existsDatabase('dfs://raw_data')
"
```

**Re-run init script:**
```bash
curl -X POST http://localhost:8848/run -d @docker/dolphindb/init/init_schema.dos
```

### Problem 3: Connection Refused from Raw Data Layer
**Check:**
```bash
# From host
curl http://localhost:8848

# From container network
docker exec dolphindb curl http://localhost:8848
```

**Fix:** Ensure `config.yaml` has correct host (`localhost` when running outside Docker)

### Problem 4: Write Errors
**Check DolphinDB logs:**
```bash
docker exec dolphindb tail -f /DolphinDB/server/log/dolphindb.log
```

**Check table schema:**
```bash
curl -X POST http://localhost:8848/run -d "
schema(loadTable('dfs://raw_data', 'raw_events'))
"
```

---

## 📚 References

- **DolphinDB Docs:** https://docs.dolphindb.com/
- **Docker Hub:** https://hub.docker.com/r/dolphindb/dolphindb
- **HTTP API:** https://docs.dolphindb.com/en/2.00.10/interfaces/http_api.html
- **Raw Data Layer PROGRESS.md:** `../PROGRESS.md` (Step A — DolphinDB HTTP REST write path)

---

## ✅ Success Checklist

- [ ] Container running (`docker ps | grep dolphindb`)
- [ ] Port 8848 accessible (`curl http://localhost:8848`)
- [ ] Database exists (`existsDatabase("dfs://raw_data")` → `true`)
- [ ] Tables created (`raw_events`, `canonical_events`)
- [ ] Raw Data Layer connects (no "connection refused" errors)
- [ ] Events written (count > 0)

**Status:** 🟢 Ready for production testing

