# DolphinDB Schema Setup - Addım-addım Təlimat

**Status:** Schema faylı hazırdır, apply etmək lazımdır  
**Vaxt:** 5-10 dəqiqə  
**Kritiklik:** ⚠️ YÜksək - Data yazılmır!

---

## 🎯 Məqsəd

DolphinDB-də 2 cədvəl yaratmaq:
1. `raw_events` - Orijinal payload (BLOB)
2. `canonical_events` - Normalized market data

---

## 📋 Addım 1: Docker-i Başlat

```bash
# Docker daemon-u başlat
sudo systemctl start docker

# DolphinDB container-ini yoxla
docker ps | grep dolphindb
```

**Gözlənilən çıxış:**
```
CONTAINER ID   IMAGE                        STATUS
abc123...      dolphindb/dolphindb:v2.00.10 Up 2 minutes
```

**Əgər container yoxdursa:**
```bash
cd ~/Desktop/raw-data-layer/docker
./setup_dolphindb_official.sh
```

---

## 📋 Addım 2: Schema Faylını Apply Et

### Method 1: Docker Exec (Tövsiyə edilir)

```bash
# 1. Schema faylını container-ə kopyala
cd ~/Desktop/raw-data-layer
docker cp docker/dolphindb/init/init_schema.dos dolphindb:/tmp/init_schema.dos

# 2. Script-i icra et
docker exec dolphindb /data/ddb/server/dolphindb \
  -home /data \
  -console 0 \
  -script /tmp/init_schema.dos
```

**Gözlənilən çıxış:**
```
Step 1: Creating database...
✅ Database dfs://raw_data created

Step 2: Creating raw_events table...
✅ Table raw_events created

Step 3: Creating canonical_events table...
✅ Table canonical_events created

Step 4: Verifying tables...
✅ raw_events row count: 0
✅ canonical_events row count: 0

═══════════════════════════════════════════════════════════
  DolphinDB Schema Initialization Complete!
═══════════════════════════════════════════════════════════
```

### Method 2: HTTP API (Alternative)

```bash
# Schema faylını HTTP POST ilə göndər
curl -X POST http://localhost:8848 \
  -H 'Content-Type: text/plain' \
  -d @docker/dolphindb/init/init_schema.dos
```

---

## 📋 Addım 3: Schema-nı Doğrula

### Test 1: Cədvəllərin olduğunu yoxla

```bash
# raw_events cədvəli
curl -s -X POST http://localhost:8848 \
  -H 'Content-Type: text/plain' \
  -d "select count(*) from loadTable('dfs://raw_data', 'raw_events')"
```

**Gözlənilən:** `0` (hələ data yoxdur)

```bash
# canonical_events cədvəli
curl -s -X POST http://localhost:8848 \
  -H 'Content-Type: text/plain' \
  -d "select count(*) from loadTable('dfs://raw_data', 'canonical_events')"
```

**Gözlənilən:** `0`

### Test 2: Manual test event yaz

```bash
curl -X POST http://localhost:8848 \
  -H 'Content-Type: text/plain' \
  -d '
    t = table(
        ["test_evt_001"] as event_id,
        ["MANUAL_TEST"] as source,
        [blob("test_payload_raw_bytes")] as payload,
        [now()] as received_at,
        [1] as sequence_num
    );
    loadTable("dfs://raw_data", "raw_events").append!(t);
    select count(*) from loadTable("dfs://raw_data", "raw_events")
  '
```

**Gözlənilən:** `1` (test event yazıldı)

### Test 3: Test event-i oxu

```bash
curl -X POST http://localhost:8848 \
  -H 'Content-Type: text/plain' \
  -d "select * from loadTable('dfs://raw_data', 'raw_events') limit 5"
```

**Gözlənilən:** Test event görünəcək

---

## 📋 Addım 4: Go Integration Test

```bash
cd ~/Desktop/raw-data-layer

# Real DolphinDB ilə integration test
go test ./pkg/storage/... -v -run DolphinDB
```

**Gözlənilən çıxış:**
```
=== RUN   TestDolphinDBWriter_Integration
--- PASS: TestDolphinDBWriter_Integration (2.34s)
PASS
ok      raw-data-layer/pkg/storage      2.345s
```

---

## 📋 Addım 5: Real Data Test

Real Binance data ilə test et:

```bash
# Background-da 60 saniyə işlət
cd ~/Desktop/raw-data-layer
timeout 60 go run ./cmd/raw-data-layer/main.go \
  --binance=true \
  --ib=false \
  --zmq=false \
  --db=true \
  --log-level=info &

# 10 saniyə gözlə
sleep 10

# DolphinDB-də event sayı yoxla
curl -X POST http://localhost:8848 \
  -H 'Content-Type: text/plain' \
  -d "select count(*) from loadTable('dfs://raw_data', 'canonical_events')"
```

**Gözlənilən:** `> 0` (real events yazıldı)

---

## 📋 Addım 6: Health Check

```bash
curl -s http://localhost:8080/health | jq '.db'
```

**Əvvəl (schema yoxdursa):**
```json
{
  "connected": true,
  "total_written": 0,
  "pending": 25
}
```

**Sonra (schema varsa):**
```json
{
  "connected": true,
  "total_written": 150,
  "pending": 0
}
```

---

## 🐛 Troubleshooting

### Problem 1: "Table not found"

**Səbəb:** Schema apply olmayıb

**Həll:**
```bash
# Schema-nı yenidən apply et
docker exec dolphindb /data/ddb/server/dolphindb \
  -home /data -console 0 \
  -script /tmp/init_schema.dos
```

### Problem 2: "Database not found"

**Səbəb:** Database yaradılmayıb

**Həll:**
```bash
# Database-i manual yarat
curl -X POST http://localhost:8848 \
  -H 'Content-Type: text/plain' \
  -d "login('admin','123456'); database('dfs://raw_data', VALUE, 2020.01M..2030.12M)"
```

### Problem 3: Docker container işləmir

**Həll:**
```bash
# Container-i yenidən başlat
docker restart dolphindb

# Və ya yenidən yarat
docker rm -f dolphindb
cd docker && ./setup_dolphindb_official.sh
```

### Problem 4: "Connection refused" (port 8848)

**Həll:**
```bash
# Container log-larına bax
docker logs dolphindb | tail -20

# DolphinDB başlayıbmı yoxla
docker exec dolphindb ps aux | grep dolphindb
```

---

## ✅ Uğur Meyarları

Schema düzgün apply olunubsa:

- [x] `dfs://raw_data` database mövcuddur
- [x] `raw_events` table mövcuddur (count = 0)
- [x] `canonical_events` table mövcuddur (count = 0)
- [x] Test event yazıb oxumaq mümkündür
- [x] Real Binance data DolphinDB-yə yazılır
- [x] Health check `pending: 0` göstərir

---

## 📊 Schema Strukturu

### Table 1: raw_events

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING | Unikal event ID |
| `source` | SYMBOL | Mənbə (BINANCE, IB, etc.) |
| `payload` | BLOB | Raw binary payload (base64) |
| `received_at` | TIMESTAMP | Server receive timestamp |
| `sequence_num` | LONG | Sequence number |

**Partition:** `received_at` (monthly)

### Table 2: canonical_events

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING | Unikal event ID |
| `canonical_symbol` | SYMBOL | Normalized symbol (BTC/USD) |
| `exchange_timestamp` | TIMESTAMP | Exchange timestamp |
| `local_hw_timestamp` | TIMESTAMP | Local HW timestamp |
| `event_type` | SYMBOL | TRADE, QUOTE, etc. |
| `price` | DOUBLE | Price |
| `size` | DOUBLE | Size/volume |
| `side` | SYMBOL | BUY, SELL |
| `source` | SYMBOL | Source adapter |
| `raw_event_id` | STRING | Link to raw_events |

**Partition:** `exchange_timestamp` (monthly)

---

## 📝 Commit Sonra

Schema apply olunduqdan və test edildikdən sonra:

```bash
cd ~/Desktop/raw-data-layer
git add docker/dolphindb/init/init_schema.dos SCHEMA_SETUP_GUIDE.md
git commit -m "feat: DolphinDB schema initialized - CRITICAL FIX

Tables created:
- raw_events (BLOB payload storage)
- canonical_events (normalized market data)

Partition: Monthly (VALUE partition)
Test: Manual test event written successfully
Status: Real data now writes to DolphinDB ✅

Fixes: 427 pending events issue
Time: 30 minutes
Priority: CRITICAL"

git push origin main
```

---

## 🎓 Nəticə

Schema apply olunduqdan sonra:

**Əvvəl:**
```
Binance → Adapter → WAL (452 events) ✅
                  → DolphinDB (0 events) ❌
                     ↳ pending: 427
```

**Sonra:**
```
Binance → Adapter → WAL (452 events) ✅
                  → DolphinDB (427 events) ✅
                     ↳ pending: 0
```

---

**İlkin addım:** Docker-i başlat və schema apply et! 🚀

