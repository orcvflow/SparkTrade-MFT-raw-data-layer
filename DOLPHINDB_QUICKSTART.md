# DolphinDB Quick Start

## ✅ DolphinDB Hazırdır!

**Container status:** Running on port 8848  
**Image:** dolphindb/dolphindb:v2.00.10

---

## 🚀 Start DolphinDB (Already Done!)

```bash
cd ~/Desktop/raw-data-layer/docker
./setup_dolphindb_official.sh
```

**Check status:**
```bash
docker ps | grep dolphindb
```

---

## 🔧 Connection Info

| Parameter | Value |
|-----------|-------|
| **Host** | `localhost` |
| **Port** | `8848` |
| **Username** | `admin` |
| **Password** | `123456` |
| **Default DB** | `dfs://raw_data` |

---

## 🧪 Test Connection

### From Raw Data Layer

Update `config/config.yaml`:
```yaml
storage:
  dolphindb:
    enabled: true
    host: "localhost"
    port: 8848
    username: "admin"
    password: "123456"
```

### Run with DolphinDB

```bash
cd ~/Desktop/raw-data-layer

# With IB Brokers (if gateway running)
go run ./cmd/raw-data-layer/main.go \
  --binance=false \
  --ib=true \
  --zmq=false \
  --db=true \
  --log-level=info

# Or with Binance
go run ./cmd/raw-data-layer/main.go \
  --binance=true \
  --ib=false \
  --zmq=false \
  --db=true \
  --log-level=info
```

**Expected log:**
```json
{"level":"INFO","msg":"DolphinDB connected","host":"localhost","port":8848}
```

---

## 📊 DolphinDB Web GUI

DolphinDB has a web interface:

**URL:** http://localhost:8848  
**Login:** admin / 123456

---

## 🛠️ Management Commands

### View Logs
```bash
docker logs -f dolphindb
```

### Stop
```bash
docker stop dolphindb
```

### Start (after stop)
```bash
docker start dolphindb
```

### Restart
```bash
docker restart dolphindb
```

### Remove (delete data)
```bash
docker rm -f dolphindb
```

### Shell Access
```bash
docker exec -it dolphindb bash
```

---

## 🐛 Troubleshooting

### Port 8848 Already in Use
```bash
# Find what's using the port
sudo lsof -i :8848

# Or use different port
docker run -itd --name dolphindb \
  -p 8849:8848 \
  dolphindb/dolphindb:v2.00.10 sh

# Update config.yaml port to 8849
```

### Connection Refused
```bash
# Check if container is running
docker ps | grep dolphindb

# Check if port is accessible
curl -s http://localhost:8848

# Restart container
docker restart dolphindb
```

### Schema Not Initialized
Schema will be auto-created on first write from Raw Data Layer adapter.

If you want to manually initialize:
1. Open web GUI: http://localhost:8848
2. Login: admin / 123456
3. Execute initialization script (see `docker/dolphindb/init/init_schema.dos`)

---

## ✅ Status

- ✅ DolphinDB container running
- ✅ Port 8848 accessible  
- ⏳ Schema will be created on first data write
- 🟢 Ready to receive market data from adapters

**Next:** Run Raw Data Layer adapter with `--db=true`

