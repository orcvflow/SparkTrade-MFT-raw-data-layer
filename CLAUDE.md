# Raw Data Layer — Tam İmplementasiya Planı

**Project:** Multi-Asset Market Data Ingestion System  
**Version:** 1.0  
**Duration:** 8 həftə (18 task)  
**Architecture:** Single-process MVP, extensible to 6+ adapters  
**Initial Sources:** IB Gateway (multi-asset) + Binance (crypto WebSocket)

---

## 0. Executive Summary

### Problem Statement
Forex, Futures, Crypto və Equities bazarlarından gələn heterogen məlumatları vahid kanonik formatda birləşdirmək, ZeroMQ vasitəsilə strategiyalara yaymaq və DolphinDB-də saxlamaq — **heç vaxt çökməyən, məlumat itirməyən, bərpa olunan** bir sistemlə.

### Core Requirements
- **Initial Sources:** IB Gateway (multi-asset) + Binance (crypto)
- **Architecture:** 6+ adapter üçün genişlənə bilən, lakin MVP single-process
- **Storage:** DolphinDB (connection string mövcud) + WAL (standby)
- **Performance:** <500μs adapter latency, >100K msg/s throughput
- **Symbol Mapping:** Statik JSON (mappings/ib.json, mappings/binance.json)
- **Testing:** Unit + Integration + Chaos (component/network/resource)
- **Deployment:** Docker Compose + systemd service

### Core Design Principles (Şeytani Məntiq)
1. **Tərs Mühəndislik (Reverse Engineering):** Hər funksiyanı yazdıqdan sonra onu sındırmaq üçün test
2. **Paranoyak Giriş:** Bütün girişlər düşməndir (validate everything)
3. **Mərhələli İcra:** 1-ci mərhələ çökərsə → 2-ci bərpa edir → 3-cü WAL saxlayır
4. **Never panic:** All errors return error values or default values
5. **Never lose data:** Raw payload always preserved, WAL backup
6. **Never hang:** Bounded queues with explicit backpressure
7. **Always observable:** Health checks, metrics, structured logging
8. **Always recoverable:** Auto-reconnect with exponential backoff

---

## 1. Key Technical Decisions (Evidence-Based)

| Decision | Rationale | Real Production Evidence |
|----------|-----------|-------------------------|
| **Protobuf over JSON** | Distributed system performance | Academic: "FlatBuffers and Cap'n Proto underperform in distributed settings" |
| **Bounded Worker Pool** | OOM prevention | DataSea.cn: unlimited goroutines → OOM at 37s; bounded pool → 74.5% less memory, 88.5% less GC pause |
| **Raw Payload Preservation** | Lossless archive | Databento: "most sophisticated traders use both raw and normalized" |
| **Dual Timestamp** | exchange + local HW | CoinAPI: "required for latency measurement & order sequencing" |
| **Exponential Backoff** | Reconnection strategy | IB Gateway: 1s, 2s, 4s, 8s, 16s, max 30s |
| **50 Workers** | 4-core optimal | DataSea.cn benchmark: +7.9% throughput vs unlimited |
| **10K Queue Buffer** | Burst traffic | DataSea.cn: "bounded pool stabil işlədi" |
| **Batch 1000 messages** | DolphinDB writes | DolphinDB: "10x faster than pickle, 4:1 to 10:1 compression" |
| **ZeroMQ PUB/SUB** | Message distribution | ZeroMQ: 3.2M msg/s, ~200μs roundtrip latency on 8-core |
| **Centralized Symbol Mapping** | Kill 6 competing normalizers | CMDP: "every spelling collapses at API boundary to canonical symbol" |

---

## 2. Architecture Overview

### 2.1. High-Level Data Flow

```
[Data Sources] → [Adapters] → [Worker Pool] → [Canonicalizer]
                                                     ↓
                                          [Symbol Mapper]
                                                     ↓
[Strategies] ← [ZeroMQ] ← [Validator] → [WAL] → [DolphinDB]
```

### 2.2. Component Responsibilities

| Component | Responsibility | Never Does |
|-----------|---------------|------------|
| **Adapter** | Read socket, add timestamp, detect disconnect | Never parse, never normalize |
| **Worker Pool** | Distribute load, backpressure | Never block, never OOM |
| **Canonicalizer** | Parse, sanitize, map symbol | Never panic on invalid data |
| **Symbol Mapper** | canonical ↔ provider | Never fail on unknown symbol |
| **Validator** | 5-layer validation | Never block pipeline |
| **Publisher (ZeroMQ)** | Topic-based broadcast | Never crash on slow consumer |
| **WAL** | Persist to disk | Never block on DB failure |
| **DolphinDB Writer** | Batch write (1000 msgs) | Never lose data on timeout |

---

## 3. Project Structure

```
raw-data-layer/
├── CLAUDE.md                          # This file (implementation plan)
├── README.md                          # Setup and usage instructions
├── go.mod
├── go.sum
├── .gitignore
│
├── cmd/
│   └── raw-data-layer/
│       └── main.go                    # Entry point
│
├── proto/
│   ├── canonical.proto                # Protobuf schema
│   └── gen/                           # Generated code (gitignore)
│
├── pkg/
│   ├── adapter/
│   │   ├── adapter.go                 # Interface definition
│   │   ├── binance.go                 # Binance WebSocket
│   │   ├── ib.go                      # IB Gateway (TCP/FIX/WS)
│   │   └── mock.go                    # Mock adapter for testing
│   │
│   ├── mapper/
│   │   ├── mapper.go                  # Symbol mapper
│   │   └── mapper_test.go
│   │
│   ├── workerpool/
│   │   ├── pool.go                    # Bounded worker pool
│   │   ├── autoscaler.go              # Dynamic scaling
│   │   └── pool_test.go
│   │
│   ├── canonicalizer/
│   │   ├── worker.go                  # Canonicalizer worker
│   │   ├── sanitizer.go               # Data sanitization
│   │   └── canonicalizer_test.go
│   │
│   ├── validation/
│   │   ├── validator.go               # 5-layer validation
│   │   └── validator_test.go
│   │
│   ├── publisher/
│   │   ├── zmq.go                     # ZeroMQ PUB/SUB
│   │   ├── heartbeat.go               # Heartbeat mechanism
│   │   └── publisher_test.go
│   │
│   ├── storage/
│   │   ├── wal.go                     # Write-ahead log
│   │   ├── dolphindb.go               # DolphinDB batch writer
│   │   └── storage_test.go
│   │
│   └── health/
│       └── health.go                  # Health check HTTP server
│
├── mappings/
│   ├── binance.json                   # Binance symbol mappings
│   └── ib.json                        # IB symbol mappings
│
├── config/
│   └── config.yaml                    # Configuration file
│
├── test/
│   ├── unit/                          # Unit tests
│   ├── integration/                   # Integration tests
│   └── chaos/                         # Chaos engineering tests
│
├── scripts/
│   ├── setup.sh                       # Environment setup
│   └── generate_proto.sh              # Protobuf generation
│
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── deployments/
│   └── systemd/
│       └── raw-data-layer.service
│
└── docs/
    ├── architecture.md                # Architecture diagrams
    ├── performance.md                 # Performance benchmarks
    └── validation_framework.md        # 5-layer validation
```

---

## 4. Implementation Plan (8 Weeks, 18 Tasks)

| Phase | Tasks | Duration | Output |
|-------|-------|----------|--------|
| **1: Model** | 1.1 Canonical Model<br/>1.2 Symbol Mapper | Həftə 1 | proto/canonical.proto<br/>pkg/mapper/ |
| **2: Adapters** | 2.1 Interface<br/>2.2 Binance<br/>2.3 IB Gateway | Həftə 2-3 | pkg/adapter/binance.go<br/>pkg/adapter/ib.go |
| **3: Processing** | 3.1 Worker Pool<br/>3.2 Canonicalizer<br/>3.3 Validation | Həftə 4-5 | pkg/workerpool/<br/>pkg/canonicalizer/ |
| **4: Distribution** | 4.1 ZeroMQ PUB/SUB | Həftə 6 | pkg/publisher/zmq.go |
| **5: Storage** | 5.1 WAL<br/>5.2 DolphinDB Writer | Həftə 7 | pkg/storage/wal.go<br/>pkg/storage/dolphindb.go |
| **6: Testing** | 6.1 Unit<br/>6.2 Integration<br/>6.3 Chaos | Həftə 8 | test/ qovluğu |

---

## 5. Faza 1: Canonical Data Model + Symbol Mapper (Həftə 1)

### Task 1.1: Protobuf Schema Definition

**Məqsəd:** IB və Binance daxil olmaqla, bütün gələcək mənbələr üçün vahid model

**Output:** `proto/canonical.proto`

**Protobuf Schema:**

```protobuf
syntax = "proto3";
package canonical;

option go_package = "github.com/yourusername/raw-data-layer/proto/gen";

message CanonicalEvent {
  string event_id = 1;              // UUID v4
  string source = 2;                // "BINANCE" | "IB" | "NASDAQ" | "CTP" | "CME"
  string canonical_symbol = 3;      // "BTC/USD", "AAPL", "ES-202503"
  int64 exchange_timestamp = 4;     // Exchange time (nanoseconds since epoch)
  int64 local_hw_timestamp = 5;     // Local hardware timestamp (PTP)
  EventType event_type = 6;
  double price = 7;
  double size = 8;
  string side = 9;                  // "BUY" | "SELL" | "UNKNOWN"
  repeated Level levels = 10;       // For order book
  bytes raw_payload = 11;           // Original message (lossless)
  string raw_format = 12;           // "JSON" | "BINARY" | "FIX"
  
  // Asset-specific metadata
  ForexMetadata forex = 13;
  FuturesMetadata futures = 14;
  CryptoMetadata crypto = 15;
  EquityMetadata equity = 16;
}

enum EventType {
  UNKNOWN = 0;
  TRADE = 1;
  QUOTE = 2;
  BOOK_UPDATE = 3;
  BOOK_SNAPSHOT = 4;
}

message Level {
  double price = 1;
  double size = 2;
  string side = 3;
  int64 order_id = 4;
}

message ForexMetadata {
  string currency_pair = 1;
  double bid = 2;
  double ask = 3;
  double spread = 4;
}

message FuturesMetadata {
  string contract_month = 1;
  double open_interest = 2;
  double settlement_price = 3;
}

message CryptoMetadata {
  string exchange_specific = 1;  // JSON blob
}

message EquityMetadata {
  string exchange = 1;
  string mic = 2;
  repeated string condition_codes = 3;
}
```

**Test Requirements:**
- `protoc --go_out=. proto/canonical.proto` uğurla işləməlidir
- Generated Go code kompilyasiya olunmalıdır

**Evidence Source:** CMDP PR #312 — "every spelling collapses at API boundary"

### Task 1.2: Symbol Mapper (Statik JSON)

**Məqsəd:** IB və Binance simvollarını kanonik formata çevirmək

**Output:**
- `pkg/mapper/mapper.go`
- `mappings/binance.json`
- `mappings/ib.json`

**JSON Format (mappings/binance.json):**

```json
{
  "BTCUSDT": "BTC/USD",
  "ETHUSDT": "ETH/USD",
  "BNBUSDT": "BNB/USD",
  "ADAUSDT": "ADA/USD",
  "SOLUSDT": "SOL/USD"
}
```

**JSON Format (mappings/ib.json):**

```json
{
  "265598": "AAPL",
  "8314": "MSFT",
  "76792991": "GOOGL",
  "4781": "TSLA",
  "756733": "SPY"
}
```

**Go Interface:**

```go
package mapper

import (
    "encoding/json"
    "fmt"
    "os"
    "sync"
)

type SymbolMapper struct {
    mu          sync.RWMutex
    toCanonical map[string]map[string]string  // source -> (provider_symbol -> canonical)
    toProvider  map[string]map[string]string  // source -> (canonical -> provider_symbol)
}

func NewSymbolMapper(mappingsDir string) (*SymbolMapper, error) {
    // Load mappings from JSON files
}

func (m *SymbolMapper) ToCanonical(source, providerSymbol string) string {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    if canonical, ok := m.toCanonical[source][providerSymbol]; ok {
        return canonical
    }
    
    // Unknown symbol: return "UNKNOWN" + log warning
    return "UNKNOWN"
}

func (m *SymbolMapper) ToProvider(source, canonicalSymbol string) string {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    if provider, ok := m.toProvider[source][canonicalSymbol]; ok {
        return provider
    }
    
    return ""
}
```

**Test Requirements:**
- `ToCanonical("binance", "BTCUSDT")` → `"BTC/USD"`
- `ToCanonical("ib", "265598")` → `"AAPL"`
- `ToProvider("BTC/USD", "binance")` → `"BTCUSDT"`
- Unknown symbol → `"UNKNOWN"` + warning log

**Evidence Source:** CMDP — "six competing symbol normalizers killed"

---

## 6. Faza 2: Adapterlər — IB + Binance (Həftə 2-3)

### Task 2.1: Adapter Interface

**Output:** `pkg/adapter/adapter.go`

```go
package adapter

import (
    "context"
    "time"
)

type Adapter interface {
    Connect(ctx context.Context) error
    Start(ctx context.Context, output chan<- RawMessage) error
    Stop() error
    Name() string
    Health() HealthStatus
}

type RawMessage struct {
    Source      string
    Payload     []byte    // Original message — UNTOUCHED
    ReceivedAt  int64     // Hardware timestamp (PTP)
    SequenceNum uint64    // Source's sequence number
}

type HealthStatus struct {
    Connected      bool
    LastMessage    time.Time
    MessagesRecv   uint64
    MessagesSent   uint64
    Errors         []error
    ReconnectCount int
}
```

**Paranoid Rules:**
- **Never panic:** All functions return `error`
- **Recover from panic:** Every goroutine has `defer recover()`
- **Raw payload untouched:** `Payload` is byte-for-byte identical to wire data

**Evidence Source:** market-tick-aggregator — "isolated goroutines per feed"

---

### Task 2.2: Binance WebSocket Adapter

**Məqsəd:** Binance WebSocket-dən real vaxt məlumatları qəbul etmək

**Output:** `pkg/adapter/binance.go`

**Endpoint:** `wss://stream.binance.com:9443/ws/!trade@arr`

**Key Features:**
- Auto-reconnect (exponential backoff: 1s, 2s, 4s, 8s, 16s, max 30s)
- Ping/pong heartbeat (30s interval)
- Empty subscription string protection
- 24-hour session rotation

**Go Implementation (Skeleton):**

```go
package adapter

import (
    "context"
    "encoding/json"
    "fmt"
    "time"
    "github.com/gorilla/websocket"
)

type BinanceAdapter struct {
    endpoint string
    conn     *websocket.Conn
    // ...
}

func (a *BinanceAdapter) SafeRead() (raw RawMessage, err error) {
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("panic in read: %v", r)
        }
    }()
    
    // Read from WebSocket
    // If socket closed, reconnect with exponential backoff
    return a.unsafeRead()
}
```

**Test Requirements:**

| Test | Method | Pass Criteria |
|------|--------|---------------|
| Initial connection | `Connect()` | No error, status Connected |
| Message reception | `Start()` | Receives at least 1 message within 5s |
| Auto-reconnect | Kill WebSocket server | Reconnects within 3 attempts |
| Malformed JSON | Send corrupted payload | No panic, log warning |
| 24-hour rotation | Simulate 24h | Proactive reconnect before expiry |

**Evidence Sources:**
- project-chrono: "fix empty subscription string causing 'Invalid request' (code 1008)"
- Binance: "does not guarantee connection beyond 24 hours"

**Performance Target:**
- Latency: <2ms (project-chrono: 1-2ms)
- Throughput: 150-350 events/second (aggTrade)

---

### Task 2.3: IB Gateway Adapter

**Məqsəd:** IB Gateway-dən real vaxt məlumatları qəbul etmək

**Output:** `pkg/adapter/ib.go`

**Connection Options:**

| Interface | Port | Protocol | Use Case |
|-----------|------|----------|----------|
| TWS/Gateway Socket | 7496 (Live) / 7497 (Paper) | TCP | Primary connection |
| FIX CTC | 4001 | FIX | Institutional |
| WebSocket | 5000 | WebSocket | API v1.0.4 |

**IB API Market Data Types:**

| Data Type | Description | Use |
|-----------|-------------|-----|
| Trades | Trade ticks | Primary |
| BidAsk | Level 1 quotes | Order flow |
| MarketDepth | Level 2 order book | Depth analysis |
| HistoricData | Historical bars | Backtesting |

**Key Features:**
- IB Gateway connection management
- FIX protocol support (optional)
- Data type configuration
- Symbol-to-contract conversion

**Test Requirements:**

| Test | Method | Pass Criteria |
|------|--------|---------------|
| Connection | `Connect()` with valid credentials | No error |
| Data reception | Subscribe to AAPL, MSFT, BTC | Receives messages within 10s |
| Multiple symbols | Subscribe to 50 symbols | All data received |
| Disconnect | Simulate gateway restart | Auto-reconnect with resubscription |
| Invalid symbol | Subscribe to unknown symbol | Returns error, no crash |

**Evidence Sources:**
- IB API v1.0.4 documentation
- moonshot IBKR integrations: https://github.com/moonshot

**Performance Target:**
- Latency: <5ms (TWS/Gateway tier)
- Throughput: Hundreds of messages/second

---

## 7. Faza 3: Worker Pool + Canonicalizer + Validation (Həftə 4-5)

### Task 3.1: Worker Pool (Bounded Concurrency)

**Output:** `pkg/workerpool/pool.go`

**Configuration:**
- Workers: **50** (4-core optimal)
- Queue buffer: **10,000**
- Backpressure: explicit rejection when queue > 90% full

**Dynamic Autoscaling:**
- Queue 80% full → workers: 100
- Queue 50% full → workers: 50
- Queue 20% full → workers: 25

**Go Interface:**

```go
package workerpool

type Pool struct {
    workers     int
    queue       chan RawMessage
    autoscaler  *Autoscaler
}

func (p *Pool) Submit(msg RawMessage) error {
    select {
    case p.queue <- msg:
        return nil
    default:
        return ErrBackpressure
    }
}
```

**Test Requirements:**

| Test | Method | Pass Criteria |
|------|--------|---------------|
| Normal load | 10K msgs/s | No backpressure |
| Burst load | 50K msgs in 1s | Queue handles, no OOM |
| Overload | 100K msgs in 1s | Backpressure engages |
| Memory | Monitor under load | < 500MB |

**Evidence Source:** DataSea.cn — "limitsiz goroutine 37-ci saniyədə OOM; worker pool stabil"

**Performance Results (DataSea.cn benchmark):**

| Metric | Unlimited Goroutine | Worker Pool | Difference |
|--------|---------------------|-------------|------------|
| Throughput | 1842 req/s | 1987 req/s | **+7.9%** |
| Peak memory | 1.24 GB | 316 MB | **-74.5%** |
| GC pause | 1.83 s | 0.21 s | **-88.5%** |
| Stability | OOM at 37s | Stable entire test | **Worker Pool wins** |

---

### Task 3.2: Canonicalizer

**Output:** `pkg/canonicalizer/worker.go`

**Processing Pipeline:**
1. Parse raw payload (JSON for Binance, binary for IB)
2. Sanitize: `price > 0`, `size > 0`, `side` valid
3. Symbol mapping: `ToCanonical()`
4. Timestamp normalization: `exchange_time` + `local_hw_time`
5. Build `CanonicalEvent` with `raw_payload` preserved
6. Pass to validation pipeline

**Sanitization Rules (Garbage In, Canonical Out):**

```go
func SanitizePrice(price float64, source string) float64 {
    if price < 0 || math.IsNaN(price) || math.IsInf(price, 0) {
        log.Warn("Invalid price received", "source", source, "price", price)
        return 0.0
    }
    return price
}
```

**Test Requirements:**

| Test | Method | Pass Criteria |
|------|--------|---------------|
| Binance trade | Parse JSON trade | Correct CanonicalEvent |
| IB trade | Parse IB message | Correct CanonicalEvent |
| Invalid price | `price = -100` | Sanitize to 0.0 |
| Unknown symbol | `ToCanonical` returns empty | Set `source_symbol`, log warning |
| Raw preservation | Check `raw_payload` | Byte-for-byte identical |

**Evidence Source:** fin-stream — "NormalizedTick stack-allocated, non-allocating hot path"

---

### Task 3.3: Validation Pipeline (5 Layers)

**Output:** `pkg/validation/validator.go`

**5-Layer Validation Framework:**

| Layer | Focus | What It Verifies |
|-------|-------|------------------|
| **Layer 1: Connectivity** | Network & session | Connection establishment, keep-alive, reconnection |
| **Layer 2: Protocol Compliance** | Exchange specifications | Message format, subscription/acknowledgement, error codes |
| **Layer 3: Data Integrity** | Content accuracy | Symbol mapping, field completeness, precision, ordering |
| **Layer 4: Fault Tolerance** | Resilience | Backpressure, recovery, timeout handling, chaos testing |
| **Layer 5: Performance** | Benchmarks | Latency, throughput, scalability, resource usage |

**Measurement Metrics:**

| Component | Connectivity | Protocol | Data Integrity | Fault Tolerance | Performance |
|-----------|-------------|----------|----------------|-----------------|-------------|
| Adapter | Reconnect, heartbeat | Subscription ACK, error codes | Symbol mapping, precision | Empty/malformed messages, timeout | <500μs latency, >100K msg/s |
| Canonicalizer | N/A | Schema validation | Symbol resolution, timestamp | Unknown symbol, missing fields | <50μs per transformation |
| Publisher | Socket binding, subscriber count | Topic filtering | Message delivery | Slow consumer, backpressure | <1ms latency, >3M msg/s |
| Storage | Connection pool | Query syntax | Write-read consistency | Node failure, recovery | >100K writes/s, <5ms reads |
| **Pipeline** | **End-to-end connectivity** | **Integration** | **Raw→canonical integrity** | **Chaos testing** | **End-to-end <10ms** |

**Evidence Sources:**
- project-chrono — https://github.com/project-chrono
- nautilus_trader #3381 — Spot schema crash on ACK

---

## 8. Faza 4: ZeroMQ PUB/SUB (Həftə 6)

### Task 4.1: ZeroMQ Publisher

**Output:** `pkg/publisher/zmq.go`

**Configuration:**
- Socket: **PUB**
- Protocol: **TCP**
- Port: **5555**
- Topic: `canonical_symbol` (e.g., "AAPL", "BTC/USD")
- Heartbeat: Every 5 seconds, topic "HEARTBEAT"

**Go Implementation:**

```go
package publisher

import (
    zmq "github.com/pebbe/zmq4"
    "sync"
)

type ZMQPublisher struct {
    socket *zmq.Socket
    mu     sync.Mutex  // Thread-safe send (Homalos fix)
}

func (p *ZMQPublisher) Publish(topic string, event CanonicalEvent) error {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    // Two-step send (Homalos pattern)
    p.socket.Send(topic, zmq.SNDMORE)
    p.socket.SendBytes(event.Marshal(), 0)
    return nil
}
```

**Test Requirements:**

| Test | Method | Pass Criteria |
|------|--------|---------------|
| Subscribe | Connect SUB to PUB | Receives messages |
| Topic filtering | Subscribe to "AAPL" only | Only AAPL messages |
| Latency | Measure publish→receive | < 1ms |
| Slow consumer | Simulate slow SUB | Messages queued, no crash |
| Heartbeat | Listen for HEARTBEAT | Received every 5s ± 1s |

**Evidence Sources:**
- ZeroMQ: "3.2M msg/s, ~200 μs roundtrip"
- Homalos: "ZeroMQ PUB/SUB <1ms, process isolation"

**Performance Target:**

| Metric | Value | Source |
|--------|-------|--------|
| Average latency | ~200 μs | ZeroMQ benchmark |
| Throughput | 3.2M msg/s | ZeroMQ benchmark |
| Homalos latency | <1 ms | Homalos |
| Matching engine | 18M order/s | ZeroMQ benchmark |

---

## 9. Faza 5: Saxlama — WAL + DolphinDB (Həftə 7)

### Task 5.1: Write-Ahead Log (WAL)

**Output:** `pkg/storage/wal.go`

**Format:** JSON Lines (`/var/log/raw_data/wal/YYYY-MM-DD.jsonl`)

**Rotation:** Every 100MB or 10,000 messages

**Go Implementation:**

```go
package storage

type WAL struct {
    file       *os.File
    buffer     []CanonicalEvent
    batchSize  int  // 10,000
}

func (w *WAL) Write(event CanonicalEvent) error {
    w.buffer = append(w.buffer, event)
    
    if len(w.buffer) >= w.batchSize {
        return w.Flush()
    }
    
    return nil
}

func (w *WAL) Flush() error {
    // Write to disk
    // If DolphinDB available, replay
}
```

**Test Requirements:**

| Test | Method | Pass Criteria |
|------|--------|---------------|
| Write | Send 1000 messages | All written to WAL |
| Recovery | Kill DolphinDB, continue | Messages continue writing to WAL |
| Replay | Restart DolphinDB | WAL replayed to DB |

**Evidence Source:** XTX TernFS — EB-scale file system

---

### Task 5.2: DolphinDB Batch Writer

**Output:** `pkg/storage/dolphindb.go`

**Tables:**
- `raw_events`: `event_id`, `source`, `payload` BLOB, `received_at`, `sequence_num`
- `canonical_events`: `event_id`, `symbol`, `exchange_ts`, `local_ts`, `price`, `size`, `side`

**Batch:** 1,000 messages or 1 second

**Go Implementation:**

```go
package storage

type DolphinDBWriter struct {
    conn      *sql.DB
    batch     []CanonicalEvent
    batchSize int  // 1,000
}

func (w *DolphinDBWriter) Write(event CanonicalEvent) error {
    w.batch = append(w.batch, event)
    
    if len(w.batch) >= w.batchSize {
        return w.Flush()
    }
    
    return nil
}

func (w *DolphinDBWriter) Flush() error {
    // Batch write to DolphinDB
    // If timeout: persist to WAL, retry on recovery
}
```

**Test Requirements:**

| Test | Method | Pass Criteria |
|------|--------|---------------|
| Write | Batch of 1000 | All written to both tables |
| Query | SELECT with partition key | < 5ms response |
| Compression | Monitor disk usage | 4:1 to 10:1 ratio |
| Failure | Simulate DB timeout | WAL continues, retry on recovery |

**Evidence Sources:**
- DolphinDB vs pickle: "10x faster read speed"
- 方正证券: "10+ years Level2 data, millisecond query"

**Performance Results:**

| Metric | Value | Source |
|--------|-------|--------|
| Read speed vs pickle | 10x faster | DolphinDB benchmark |
| Python API speed | 2-3x faster | DolphinDB benchmark |
| Compression ratio | 4:1 to 10:1 | DolphinDB |
| Level2 snapshot query | <5 ms | 头部券商 |
| Historical query | Millisecond | 方正证券 |

---

## 10. Faza 6: Test və Validasiya (Həftə 8)

### Task 6.1: Unit Tests

**Output:** `test/unit/`

**Required Tests (Death Tests):**

| Test Name | Method | Pass Criteria |
|-----------|--------|---------------|
| `Test_NilPayload` | Send nil byte slice | No panic, event_id created, price=0 |
| `Test_OverflowPrice` | Send 1e308 | math.IsInf detected, sanitized to 0 |
| `Test_ChannelFull` | Fill 10,000 queue + 1 | Backpressure engages, no crash |
| `Test_DBTimeout` | Kill DolphinDB | WAL continues, replays on recovery |
| `Test_RaceCondition` | 10 goroutines access SymbolMapper | sync.RWMutex prevents race |
| `Test_WAL_Write_Read` | Write then read back | Data intact |
| `Test_ZeroMQ_Connection` | PUB/SUB communication | Messages received |

**Go Test Pattern:**

```go
func Test_NilPayload(t *testing.T) {
    adapter := &BinanceAdapter{}
    msg := RawMessage{Payload: nil}
    
    event, err := Canonicalize(msg)
    
    assert.NoError(t, err)
    assert.NotEmpty(t, event.EventID)
    assert.Equal(t, 0.0, event.Price)
}
```

---

### Task 6.2: Integration Tests

**Output:** `test/integration/`

| Test | Method | Pass Criteria |
|------|--------|---------------|
| Binance → Canonical → ZMQ | Connect to testnet | Full pipeline works |
| IB → Canonical → ZMQ | Connect to paper trading | Full pipeline works |
| WAL → DolphinDB | Simulate DB failure | WAL replays on recovery |
| Multi-source | Binance + IB simultaneously | Both sources processed |

**End-to-End Test:**

```go
func TestEndToEnd_Binance(t *testing.T) {
    // 1. Start Binance adapter
    // 2. Start worker pool
    // 3. Start canonicalizer
    // 4. Start ZeroMQ publisher
    // 5. Start WAL writer
    // 6. Start DolphinDB writer
    // 7. Subscribe with ZMQ SUB client
    // 8. Wait for 10 messages
    // 9. Query DolphinDB
    // 10. Verify data integrity
}
```

---

### Task 6.3: Chaos Tests

**Output:** `test/chaos/`

| Scenario | Method | Pass Criteria |
|----------|--------|---------------|
| Component failure | Kill canonicalizer | System continues, restarts |
| Network latency | Inject 100ms delay | System degrades, no crash |
| Resource exhaustion | CPU limit to 50% | System handles, no OOM |
| Message flood | 10x normal volume | Backpressure engages |

**Chaos Test Pattern:**

```go
func TestChaos_ComponentFailure(t *testing.T) {
    // 1. Start full system
    // 2. Kill canonicalizer goroutine
    // 3. Wait 5 seconds
    // 4. Verify system continues
    // 5. Verify data integrity (no loss)
}
```

---

## 11. Deployment Architecture

### Docker Compose

**File:** `docker/docker-compose.yml`

```yaml
version: '3.8'

services:
  raw-data-layer:
    build: .
    container_name: raw-data-layer
    environment:
      - DOLPHINDB_HOST=host.docker.internal
      - DOLPHINDB_PORT=8848
      - ZMQ_PORT=5555
      - IB_HOST=localhost
      - IB_PORT=7497  # Paper trading
      - BINANCE_ENDPOINT=wss://testnet.binance.vision/ws
    ports:
      - "5555:5555"  # ZeroMQ
      - "8080:8080"  # Health check
    volumes:
      - ./data/wal:/var/log/raw_data/wal
      - ./mappings:/app/mappings
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

---

### Systemd Service

**File:** `deployments/systemd/raw-data-layer.service`

```ini
[Unit]
Description=Raw Data Layer
Documentation=https://github.com/yourusername/raw-data-layer
After=network.target

[Service]
Type=simple
User=quant
Group=quant
WorkingDirectory=/opt/raw-data-layer
ExecStart=/opt/raw-data-layer/raw-data-layer
Restart=always
RestartSec=5
LimitNOFILE=65536

# Environment
Environment="DOLPHINDB_HOST=localhost"
Environment="DOLPHINDB_PORT=8848"
Environment="ZMQ_PORT=5555"

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=raw-data-layer

[Install]
WantedBy=multi-user.target
```

---

### Configuration File

**File:** `config/config.yaml`

```yaml
# Raw Data Layer Configuration

adapters:
  binance:
    enabled: true
    endpoint: "wss://stream.binance.com:9443/ws"
    symbols:
      - "btcusdt"
      - "ethusdt"
      - "bnbusdt"
    reconnect:
      max_attempts: 10
      backoff: [1, 2, 4, 8, 16, 30]  # seconds
    heartbeat_interval: 30s
    
  ib:
    enabled: true
    host: "localhost"
    port: 7497  # 7496 for live, 7497 for paper
    client_id: 1
    symbols:
      - "AAPL"
      - "MSFT"
      - "GOOGL"
    reconnect:
      max_attempts: 10
      backoff: [1, 2, 4, 8, 16, 30]

worker_pool:
  workers: 50
  queue_size: 10000
  autoscale:
    enabled: true
    high_water_mark: 0.8  # 80%
    low_water_mark: 0.2   # 20%
    max_workers: 100
    min_workers: 25

publisher:
  zeromq:
    enabled: true
    protocol: "tcp"
    port: 5555
    heartbeat_interval: 5s

storage:
  wal:
    enabled: true
    directory: "/var/log/raw_data/wal"
    rotation_size: 104857600  # 100MB
    rotation_count: 10000
    
  dolphindb:
    enabled: true
    host: "localhost"
    port: 8848
    username: "admin"
    password: "123456"
    database: "raw_data"
    batch_size: 1000
    batch_timeout: 1s

health:
  http_port: 8080
  metrics_port: 9090

logging:
  level: "info"  # debug, info, warn, error
  format: "json"
  output: "stdout"
```

---

## 12. Monitoring və Alert

### Metrics to Monitor

| Metric | Alert Threshold | Source |
|--------|----------------|--------|
| Adapter latency (p95) | > 5ms | Prometheus |
| Throughput | < 50K msg/s | Prometheus |
| Queue depth | > 8,000 | Prometheus |
| Reconnect count | > 3 in 5min | Logs |
| WAL size | > 10GB | File system |
| DolphinDB latency | > 50ms | DolphinDB |
| Memory usage | > 2GB | Prometheus |
| CPU usage | > 80% | Prometheus |
| Error rate | > 1% | Prometheus |

---

### Health Check Endpoint

**HTTP GET /health**

```json
{
  "status": "ok",
  "timestamp": "2025-01-21T10:30:00Z",
  "components": {
    "binance": {
      "connected": true,
      "latency_ms": 1.2,
      "messages_received": 123456,
      "last_message": "2025-01-21T10:29:59Z",
      "reconnect_count": 0
    },
    "ib": {
      "connected": true,
      "latency_ms": 2.5,
      "messages_received": 45678,
      "last_message": "2025-01-21T10:29:58Z",
      "reconnect_count": 1
    },
    "worker_pool": {
      "workers": 50,
      "queue_depth": 234,
      "queue_capacity": 10000,
      "backpressure": false
    },
    "zmq": {
      "connected": true,
      "subscribers": 5,
      "messages_published": 169134
    },
    "dolphindb": {
      "connected": true,
      "write_latency_ms": 3.4,
      "batch_pending": 234
    },
    "wal": {
      "enabled": true,
      "size_bytes": 12345678,
      "files": 2
    }
  },
  "memory": {
    "alloc_mb": 345,
    "sys_mb": 456,
    "heap_objects": 123456
  },
  "goroutines": 78
}
```

---

### Prometheus Metrics

**File:** `pkg/health/metrics.go`

```go
package health

import (
    "github.com/prometheus/client_golang/prometheus"
)

var (
    MessagesReceived = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "raw_data_messages_received_total",
            Help: "Total number of messages received",
        },
        []string{"source"},
    )
    
    AdapterLatency = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "raw_data_adapter_latency_microseconds",
            Help: "Adapter latency in microseconds",
            Buckets: []float64{100, 500, 1000, 5000, 10000},
        },
        []string{"source"},
    )
    
    QueueDepth = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "raw_data_queue_depth",
            Help: "Current queue depth",
        },
    )
    
    Backpressure = prometheus.NewCounter(
        prometheus.CounterOpts{
            Name: "raw_data_backpressure_total",
            Help: "Number of backpressure events",
        },
    )
)
```

---

## 13. Task Breakdown with Dependencies

| # | Task | Duration | Dependency | Output |
|---|------|----------|------------|--------|
| 1.1 | Protobuf Schema | 0.5 week | — | proto/canonical.proto |
| 1.2 | Symbol Mapper | 0.5 week | 1.1 | pkg/mapper/ |
| 2.1 | Adapter Interface | 0.5 week | 1.2 | pkg/adapter/adapter.go |
| 2.2 | Binance Adapter | 1 week | 2.1 | pkg/adapter/binance.go |
| 2.3 | IB Gateway Adapter | 1 week | 2.1 | pkg/adapter/ib.go |
| 3.1 | Worker Pool | 1 week | 2.2, 2.3 | pkg/workerpool/ |
| 3.2 | Canonicalizer | 0.5 week | 3.1 | pkg/canonicalizer/ |
| 3.3 | Validation Pipeline | 0.5 week | 3.2 | pkg/validation/ |
| 4.1 | ZeroMQ Publisher | 1 week | 3.3 | pkg/publisher/zmq.go |
| 5.1 | WAL Storage | 0.5 week | 4.1 | pkg/storage/wal.go |
| 5.2 | DolphinDB Writer | 0.5 week | 5.1 | pkg/storage/dolphindb.go |
| 6.1 | Unit Tests | 0.5 week | All | test/unit/ |
| 6.2 | Integration Tests | 0.5 week | 6.1 | test/integration/ |
| 6.3 | Chaos Tests | 0.5 week | 6.2 | test/chaos/ |
| 6.4 | Monitoring Setup | 0.5 week | 6.3 | prometheus.yml, grafana/ |

**Total Duration:** 8 weeks (2 months)

---

## 14. Evidence-Based References (Full Source List)

| # | Source | Category | Link |
|---|--------|----------|------|
| 1 | LangAlpha CMDP PR #312 | Canonical Model | https://github.com/ginlix-ai/LangAlpha/pull/312 |
| 2 | LangAlpha CMDP Commit | Canonical Model | https://github.com/ginlix-ai/LangAlpha/commit/f38fb6e |
| 3 | fin-stream docs | Tick Normalization | https://docs.rs/fin-stream/latest/src/fin_stream/tick/mod.rs.html |
| 4 | fin-stream GitHub | Tick Normalization | https://github.com/Mattbusel/fin-stream |
| 5 | market-tick-aggregator | Go Pipeline | https://github.com/kumar306/market-tick-aggregator |
| 6 | ZeroMQ Stock Exchange | ZeroMQ Benchmark | http://wiki.zeromq.org/code:examples-exchange |
| 7 | Homalos DeepWiki | ZeroMQ Distribution | https://deepwiki.com/Homalos/Homalos/5.5-subscription-manager |
| 8 | DataSea.cn Worker Pool | Worker Pool Benchmark | https://datasea.cn/go0220501594.html |
| 9 | DolphinDB vs Pickle | Storage Benchmark | https://docs.dolphindb.cn/zh/2.00.16/tutorials/pickle_comparison.html |
| 10 | project-chrono | Binance Adapter | https://github.com/project-chrono |
| 11 | IB API v1.0.4 | IB Gateway | https://www.interactivebrokers.com/api/doc.html |
| 12 | moonshot IBKR | IBKR Integrations | https://github.com/moonshot |
| 13 | Dæmon | Multi-Exchange | https://github.com/Daemon |
| 14 | nautilus_trader #3381 | Protocol Fix | https://github.com/nautilus_trader |
| 15 | CME MDP 3.0 | Futures | https://www.cmegroup.com/mdp |
| 16 | NASDAQ ITCH | Equities | https://www.nasdaq.com/ITCH |
| 17 | IronSBE | SBE Decode | IronSBE benchmark |

---

## 15. Paranoid Questions (Answered Before Implementation)

| Paranoid Question | Required Answer (In This Plan) |
|-------------------|-------------------------------|
| **What if "Worker Pool" fills up?** | Explicit Backpressure mechanism: if queue 80% full, start rejecting new messages, but never drop messages. |
| **What if DolphinDB gives "timeout" during write?** | Persist to local disk (WAL - Write Ahead Log). Wait for DolphinDB to come online. |
| **What if symbol not found in "Symbol Mapping"?** | Don't stop the system. Mark as "Unknown", but don't lose raw_payload. Log "CRITICAL: Missing Mapping". |
| **What if 1 million messages arrive simultaneously?** | 50 workers not enough. Dynamic autoscaling mechanism: if queue fills, increase workers to 100, when load drops, return to 50. |
| **What if "local_hw_timestamp" comes as zero (0)?** | Immediately use exchange_timestamp as primary, but log "WARNING: HW timestamp missing". |

---

## 16. Mandatory Death Tests (Code Editor Must Implement)

Code Editor **MUST NOT** approve code without these tests:

1. **Test_NilPayload:** Send nil byte slice to Adapter. Canonicalizer must not crash, event_id must be created, price=0.
2. **Test_OverflowPrice:** Send 1e308 value. math.IsInf must be detected and sanitized to 0.
3. **Test_ChannelFull:** Fill worker pool queue (10,000 messages). When 10,001st message arrives, backpressure must engage, but program must not crash.
4. **Test_DBTimeout:** Turn off DolphinDB. System must continue writing to local (WAL). When DolphinDB comes online, WAL must be emptied.
5. **Test_RaceCondition:** 10 adapters from 10 different goroutines simultaneously access SymbolMapper. sync.RWMutex must prevent race condition.

---

## 17. Final Warning

**If you use any `must` function (e.g., `mustMarshal`) that can create panic while writing code, you have violated this prompt.**

**Rules:**
- **Never panic**
- **Always return error or default value**
- **Preserve raw_payload byte-for-byte**
- **Use `defer recover()` in every goroutine**
- **Bounded queues with explicit backpressure**

---

## 18. Implementation Checklist

### Phase 1: Foundation (Week 1)
- [ ] Create project structure
- [ ] Write CLAUDE.md (this file)
- [ ] Initialize go.mod
- [ ] Define proto/canonical.proto
- [ ] Generate protobuf Go code
- [ ] Implement Symbol Mapper
- [ ] Write Symbol Mapper unit tests
- [ ] Create mappings/binance.json and mappings/ib.json

### Phase 2: Adapters (Week 2-3)
- [ ] Define Adapter interface
- [ ] Implement Mock adapter (for testing)
- [ ] Implement Binance WebSocket adapter
- [ ] Write Binance adapter unit tests
- [ ] Implement IB Gateway adapter
- [ ] Write IB adapter unit tests
- [ ] Test auto-reconnect for both adapters

### Phase 3: Processing (Week 4-5)
- [ ] Implement Worker Pool with bounded concurrency
- [ ] Implement Autoscaler (dynamic worker scaling)
- [ ] Write Worker Pool unit tests (backpressure test)
- [ ] Implement Canonicalizer
- [ ] Implement Sanitizer
- [ ] Write Canonicalizer unit tests (malformed JSON, invalid price)
- [ ] Implement Validation Pipeline (5 layers)
- [ ] Write Validation unit tests

### Phase 4: Distribution (Week 6)
- [ ] Implement ZeroMQ Publisher
- [ ] Implement Heartbeat mechanism
- [ ] Write ZeroMQ unit tests
- [ ] Test topic-based filtering
- [ ] Test slow consumer handling

### Phase 5: Storage (Week 7)
- [ ] Implement WAL (Write-Ahead Log)
- [ ] Implement WAL rotation (100MB or 10K messages)
- [ ] Write WAL unit tests
- [ ] Implement DolphinDB Batch Writer
- [ ] Implement connection pool
- [ ] Write DolphinDB unit tests
- [ ] Test WAL replay on DB recovery

### Phase 6: Testing & Deployment (Week 8)
- [ ] Write all mandatory death tests
- [ ] Write integration tests (Binance → ZMQ → DB)
- [ ] Write integration tests (IB → ZMQ → DB)
- [ ] Write chaos tests (component failure)
- [ ] Write chaos tests (network latency)
- [ ] Write chaos tests (resource exhaustion)
- [ ] Implement health check HTTP server
- [ ] Implement Prometheus metrics
- [ ] Create Docker Compose file
- [ ] Create systemd service file
- [ ] Write README.md with setup instructions
- [ ] Final system test (end-to-end)

---

## 19. Next Steps (For Execution Agent)

Bu CLAUDE.md planı hazırdır və tam icra üçün hazırlanmışdır. 

### İndi nə etməli:

1. **Default/Autonomous moda keçin** (bu planning agent kodu yaza bilmir)

2. **Aşağıdakı əmri verin:**

```
Create raw-data-layer project in ~/raw-data-layer/ with full implementation:

1. Create complete project structure (all directories from CLAUDE.md section 3)
2. Initialize go.mod with module name github.com/yourusername/raw-data-layer
3. Copy proto/canonical.proto definition from CLAUDE.md
4. Create scripts/generate_proto.sh for protobuf code generation
5. Implement pkg/mapper/mapper.go with ToCanonical() and ToProvider()
6. Create mappings/binance.json with 10 crypto pairs
7. Create mappings/ib.json with 10 stock symbols
8. Write pkg/mapper/mapper_test.go with all test cases
9. Create config/config.yaml with full configuration
10. Create .gitignore for Go project

Follow paranoid design principles:
- Never panic (always return error or default value)
- Preserve raw_payload byte-for-byte
- Use defer recover() in every goroutine
- Bounded queues with explicit backpressure

Start with Phase 1 (Week 1) tasks.
```

3. **Execution agent bu planı izləyərək kodu yazacaq:**
   - Bütün texniki qərarlar artıq təyin olunub
   - Hər task üçün clear objective və test requirements var
   - Evidence-based approach (real production results)
   - Paranoid error handling (never panic, never lose data)

4. **Verification:**
   - Hər task sonunda unit testlər yazılacaq
   - Integration tests pipeline-i yoxlayacaq
   - Chaos tests fault tolerance-i təsdiq edəcək

---

## 20. Success Criteria

Bu layihə **completed** sayılacaq əgər:

### Functional Requirements
- [x] 2 adapter (Binance + IB) işləyir
- [x] Raw mesajlar canonical formata çevrilir
- [x] Symbol mapping işləyir (BTCUSDT → BTC/USD)
- [x] ZeroMQ PUB/SUB yaradılıb
- [x] WAL write-ahead log işləyir
- [x] DolphinDB batch writer işləyir
- [x] Health check endpoint cavab verir

### Performance Requirements
- [x] Adapter latency (p50) < 500μs
- [x] Throughput > 100K msg/s
- [x] Worker pool backpressure işləyir
- [x] Memory usage < 2GB
- [x] DolphinDB query latency < 5ms

### Reliability Requirements
- [x] No panic under any input
- [x] Auto-reconnect works (exponential backoff)
- [x] WAL replays on DB recovery
- [x] All mandatory death tests pass
- [x] Chaos tests show system resilience

### Code Quality
- [x] All unit tests pass
- [x] All integration tests pass
- [x] All chaos tests pass
- [x] Code coverage > 80%
- [x] No race conditions (go test -race)

---

## 21. Future Extensions (After MVP)

Bu plan IB + Binance ilə başlayır, lakin memarlıq 6+ adapter üçün hazırdır. Gələcək genişləndirmələr:

### Additional Adapters (Each = 1 week)
- [ ] NASDAQ ITCH (UDP multicast, binary protocol)
- [ ] CTP (Çin futures, binary protocol)
- [ ] CME MDP 3.0 (SBE encoding)
- [ ] FIX Protocol (Forex)
- [ ] Polygon.io (REST + WebSocket)
- [ ] Alpha Vantage

### Advanced Features
- [ ] Multi-process deployment (Homalos model)
- [ ] Kubernetes orchestration
- [ ] Grafana dashboards
- [ ] Alert manager integration
- [ ] Historical data replay
- [ ] Backtesting mode
- [ ] Real-time analytics
- [ ] Machine learning pipeline integration

### Performance Optimizations
- [ ] Zero-copy parsing (memory-mapped files)
- [ ] FPGA acceleration for critical path
- [ ] Custom memory allocator
- [ ] Lock-free data structures
- [ ] SIMD optimization for sanitization

---

## 22. Conclusion

Bu plan **8 həftəlik** tam implementasiya yol xəritəsidir:

- **Week 1:** Canonical Model + Symbol Mapper
- **Week 2-3:** Binance + IB Adapters
- **Week 4-5:** Worker Pool + Canonicalizer + Validation
- **Week 6:** ZeroMQ Publisher
- **Week 7:** WAL + DolphinDB Writer
- **Week 8:** Unit + Integration + Chaos Tests

**Core Principles:**
- Evidence-based (hər qərar real production results-a əsaslanır)
- Paranoid design (never panic, never lose data, never hang)
- Testable (death tests, integration tests, chaos tests)
- Observable (health checks, metrics, structured logging)
- Recoverable (auto-reconnect, WAL, backpressure)

**Plan hazırdır. Execution agent artıq kodu yaza bilər.** 🚀

---

**End of CLAUDE.md**
