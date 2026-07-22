# Raw Data Layer

**Multi-Asset Market Data Ingestion System with Axle-Axiom Mathematical Engine**

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 🎯 Overview

Raw Data Layer is a **paranoid, fault-tolerant** market data ingestion system that:

- **Ingests** real-time data from IB Gateway (multi-asset) + Binance (crypto)
- **Normalizes** heterogeneous payloads to canonical format
- **Validates** with 5-layer validation framework
- **Publishes** via ZeroMQ PUB/SUB
- **Stores** in DolphinDB with WAL backup
- **Never panics**, **never loses data**, **always recovers**

---

## 🚀 Quick Start

### Prerequisites

- **Go 1.23+**
- **Protocol Buffers** compiler (`protoc`)
- **ZeroMQ** library
- **DolphinDB** (optional for MVP — WAL-only mode works)
- **IB Gateway/TWS** (for IB adapter)

### Installation

```bash
# Clone repository
git clone https://github.com/yourusername/raw-data-layer.git
cd raw-data-layer

# Install dependencies
go mod download

# Generate Protobuf code
chmod +x scripts/generate_proto.sh
./scripts/generate_proto.sh

# Run tests
go test ./... -v

# Build
go build -o raw-data-layer ./cmd/raw-data-layer

# Run
./raw-data-layer -config config/config.yaml
```

---

## 📁 Project Structure

```
raw-data-layer/
├── cmd/raw-data-layer/          # Main entry point
├── proto/                        # Protobuf schemas
├── pkg/
│   ├── axiom/                    # ⭐ Axle-Axiom mathematical engine
│   ├── adapter/                  # Data source adapters (Binance, IB)
│   ├── mapper/                   # Symbol mapping
│   ├── workerpool/               # Bounded concurrency
│   ├── canonicalizer/            # Data normalization
│   ├── validation/               # 5-layer validation
│   ├── publisher/                # ZeroMQ distribution
│   ├── storage/                  # WAL + DolphinDB
│   └── health/                   # Health checks + metrics
├── mappings/                     # Symbol mappings (JSON)
├── config/                       # Configuration
├── test/                         # Unit + Integration + Chaos tests
└── docker/                       # Docker + Compose

---
## ✅ Status: MVP Complete (18/18 Tasks)

Raw Data Layer has reached MVP completion according to CLAUDE.md plan. All 18 tasks have been implemented:

### 📊 Implementation Progress
| Phase | Tasks | Status | Output |
|-------|-------|--------|--------|
| **1: Model** | 1.1-1.4 | ✅ 100% | Protobuf schema, code gen, mapper, config |
| **2: Adapters** | 2.1-2.8 | ✅ 100% | Binance WS + IB Gateway adapters, 36 tests |
| **3: Processing** | 3.1-3.3 | ✅ 100% | Worker pool, canonicalizer, 5-layer validation |
| **4: Distribution** | 4.1 | ✅ 100% | ZeroMQ PUB/SUB with heartbeat |
| **5: Storage** | 5.1-5.2 | ✅ 100% | WAL + DolphinDB batch writer |
| **6: Testing** | 6.1-6.3 | ✅ 100% | Death tests, integration, chaos |

### 🧪 Testing Pyramid
```
   ╱╲  (7) Chaos Tests     test/chaos/chaos_test.go
  ╱  ╲ (7) Integration     test/integration/pipeline_test.go  
 ╱────╲ (9) Death Tests    test/unit/death_test.go
╱ pkg ╲ (77) Unit Tests    pkg/**/*_test.go
```

---

## 🐳 Docker Deployment

### Docker Compose (Development)
```bash
cd raw-data-layer
docker-compose -f docker/docker-compose.yml up -d

# Check health
curl http://localhost:8080/health

# View logs
docker-compose logs -f raw-data-layer
```

### Docker Standalone
```bash
docker build -t raw-data-layer -f docker/Dockerfile .
docker run -d \
  --name raw-data-layer \
  -p 5555:5555 \
  -p 8080:8080 \
  -v ./data/wal:/var/log/raw_data/wal \
  raw-data-layer \
  --binance=true \
  --zmq=true \
  --db=false \
  --log-level=info
```

---

## 🚀 Production Deployment (systemd)

```bash
# 1. Install binary
sudo cp deployments/systemd/raw-data-layer.service /etc/systemd/system/
sudo mkdir -p /opt/raw-data-layer
sudo cp raw-data-layer mappings/ config/ /opt/raw-data-layer/

# 2. Create user
sudo useradd -r -s /bin/false quant
sudo chown -R quant:quant /opt/raw-data-layer /var/log/raw_data

# 3. Environment (optional)
echo 'BINANCE_ENDPOINT="wss://stream.binance.com:9443/ws"' | sudo tee /etc/raw-data-layer/env

# 4. Start
sudo systemctl daemon-reload
sudo systemctl enable raw-data-layer
sudo systemctl start raw-data-layer

# 5. Monitor
journalctl -fu raw-data-layer
curl http://localhost:8080/health
```

---

## 🔧 Configuration

### Key Configuration Options (`config/config.yaml`)
```yaml
# Binance Testnet (default safe)
binance:
  endpoint: "wss://testnet.binance.vision/ws"
  symbols: ["btcusdt", "ethusdt", "bnbusdt"]

# IB Gateway Paper Trading
ib:
  host: "localhost"
  port: 7497  # Paper (7496 live)

# Performance
worker_pool:
  workers: 50      # 4-core optimal (DataSea.cn benchmark)
  queue_size: 10000 # 10K burst buffer

# ZeroMQ
zmq:
  port: 5555
  heartbeat_interval: "5s"
```

### Environment Variables
```bash
export BINANCE_ENDPOINT="wss://stream.binance.com:9443/ws"
export IB_HOST="192.168.1.100"
export DOLPHINDB_HOST="dolphindb.prod.internal"
```

---

## 📊 Monitoring & Observability

### Health Endpoints
```
GET /health    # Full system health with stats
GET /ready     # Readiness probe (ActiveWorkers > 0)
GET /live      # Liveness probe (always 200)
GET /metrics   # Prometheus metrics (planned)
```

### Performance Targets (from CLAUDE.md)
- Adapter latency: <500μs (p50)
- Throughput: >100K msg/s
- Memory: <2GB under load
- WAL rotation: 100MB or 10K messages
- ZeroMQ latency: <1ms (Homalos)

### Critical Alerts
```yaml
alert_if:
  - queue_depth > 8000 (80% full)
  - drop_rate > 1% (backpressure)
  - WAL size > 10GB
  - last_write > 60s
  - error_rate > 10%
```

---

## 🧩 Architecture

### Core Principles (Şeytani Məntiq)
1. **Never panic** — Every goroutine has `defer recover()`
2. **Never lose data** — Raw payload byte-for-byte preserved
3. **Never hang** — Bounded queues with explicit backpressure
4. **Always observable** — Health checks, metrics, structured logging
5. **Always recoverable** — Exponential backoff (1,2,4,8,16,30s)

### Data Flow
```
[Binance WS] → [RawMessage] ─┐
[IB Gateway] → [RawMessage] ─┼→ [Worker Pool (50)] → [Canonicalizer]
                             │                             ↓
[Heartbeat] ←─ [ZMQ PUB] ←─ [Validator (5-layer)] → [WAL] → [DolphinDB]
```

### Axle-Axiom Integration
The system includes Axle-Axiom mathematical engine components:
- `pkg/axiom/sanitizer.go` — Paranoid math validation (NaN/Inf → 0.0)
- Batch sanitization with SIMD-inspired patterns
- Moving average, percentile tracking, time series math

---

## 🔬 Testing

### Mandatory Death Tests (CLAUDE.md section 16)
```bash
go test ./test/unit/ -v -run "Test_NilPayload|Test_OverflowPrice|Test_ChannelFull|Test_DBTimeout|Test_RaceCondition" -race
```

### Full Test Suite
```bash
# All tests (unit + integration + chaos)
go test ./... -v -timeout 300s

# Unit tests only
go test ./pkg/... -v

# Integration tests
go test ./test/integration/ -v -timeout 60s

# Chaos tests
go test ./test/chaos/ -v -timeout 120s

# Race detection
go test ./... -race -short

# Coverage
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Evidence-Based Benchmarks
| Component | Evidence Source | Performance |
|-----------|----------------|-------------|
| Worker Pool | DataSea.cn | 74.5% less memory, 88.5% less GC pause vs unlimited goroutines |
| ZeroMQ | ZeroMQ benchmarks | 3.2M msg/s, ~200μs latency |
| DolphinDB | DolphinDB vs pickle | 10x faster read speed, 4:1-10:1 compression |
| Binance adapter | project-chrono | <2ms latency, empty subscription fix |

---

## 🚧 Known Limitations (MVP Scope)

1. **IB Gateway adapter**: Simplified protocol (not full IB API)
2. **DolphinDB driver**: Not included (WAL-only mode by default)
3. **Protobuf generation**: Requires manual `protoc` installation
4. **Single process**: Not yet microservices (future: Homalos multi-process)
5. **No authentication**: For production, add API keys/auth headers

---

## 🔮 Future Extensions

### Additional Adapters (1 week each)
- [ ] NASDAQ ITCH (UDP multicast, binary protocol)
- [ ] CTP (Çin futures, binary protocol)
- [ ] CME MDP 3.0 (SBE encoding)
- [ ] FIX Protocol (Forex)
- [ ] Polygon.io (REST + WebSocket)
- [ ] Alpha Vantage

### Advanced Features
- [ ] Multi-process deployment (Homalos model)
- [ ] Kubernetes operator
- [ ] Grafana dashboards
- [ ] Alert manager integration
- [ ] Historical data replay
- [ ] Backtesting mode
- [ ] Machine learning pipeline integration

### Performance Optimizations
- [ ] SIMD for price parsing
- [ ] Memory pool for event allocation
- [ ] Zero-copy deserialization
- [ ] Lock-free data structures
- [ ] FPGA acceleration for critical path

---

## 📚 References

### Evidence Base
1. **CMDP (LangAlpha)** — Canonical model design
2. **fin-stream** — Tick normalization patterns
3. **market-tick-aggregator** — Go pipeline architecture
4. **ZeroMQ benchmark** — 3.2M msg/s, <1ms latency
5. **DataSea.cn** — Worker pool vs unlimited goroutines
6. **DolphinDB vs pickle** — 10x read speed improvement

### Academic Sources
- "FlatBuffers and Cap'n Proto underperform in distributed settings"
- Databento: "Most sophisticated traders use both raw and normalized"
- CoinAPI: "Dual timestamp required for latency measurement"

---

## 🤝 Contributing

1. Read CLAUDE.md thoroughly — it's the single source of truth
2. Follow paranoid design principles (never panic, never lose data)
3. Add mandatory death tests for new components
4. Provide evidence-based benchmarks for performance claims
5. Maintain 5-layer validation framework consistency

---

## 📄 License

MIT License — see [LICENSE](LICENSE) file.

---

**Raw Data Layer MVP Complete** 🎯  
*Never panic, never lose data, never hang.*
# OMNI-Trade-raw-data-layer
# OMNI-Trade-raw-data-layer
# OMNI-Trade-raw-data-layer
# OMNI--Trade-raw-data-layer
# OMNI--Trade-raw-data-layer
# OMNI--Trade-raw-data-layer
