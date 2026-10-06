# MT5 İnteqrasiyası + Monitoring — İmplementasiya Planı

**Tarix:** 2026-08-06  
**Status:** 3/10 Task Tamamlandı (30%)  
**Son İcra:** Task #1, #2, #3 (MT5 adapter + canonicalizer + mapper)  
**Növbəti:** Task #4 (Main Wiring)

---

## 📊 İCRA STATUSU

| # | Task | Status | Fayllar | Qeydlər |
|---|------|--------|---------|---------|
| ✅ 1 | MT5 ZeroMQ Adapter | **TAMAMLANDI** | `pkg/adapter/mt5_zmq.go` | Paranoid error handling, race-free |
| ✅ 2 | MT5 Canonicalizer | **TAMAMLANDI** | `pkg/canonicalizer/mt5.go` | L1 tick + L2 depth parser |
| ✅ 3 | MT5 Symbol Mapper | **TAMAMLANDI** | `mappings/mt5.json` | 18 forex/crypto/index pairs |
| ⏳ 4 | Adapter Main Wiring | **BAŞLANMAYIB** | `cmd/adapter/main.go` | Wire MT5 adapter to pipeline |
| ⏳ 5 | MQL5 EA Script | **BAŞLANMAYIB** | `scripts/mt5_zmq_bridge.mq5` | L1+L2 publisher (Wine/MT5) |
| ⏳ 6 | Monitoring Stack | **BAŞLANMAYIB** | `docker/docker-compose.monitoring.yml` | Prometheus + Grafana |
| ⏳ 7 | ZeroMQ Publisher | **BAŞLANMAYIB** | `config/config.yaml` | Set enabled=true |
| ⏳ 8 | IB Diagnostics | **BAŞLANMAYIB** | `docs/IB_DIAGNOSTICS.md` | Troubleshooting playbook |
| ⏳ 9 | Integration Tests | **BAŞLANMAYIB** | `test/integration/mt5_test.go` | End-to-end test |
| ⏳ 10 | Documentation | **BAŞLANMAYIB** | `README.md`, `NEXT_STEPS.md` | Setup guide |

---

## ✅ TASK #1: MT5 ZeroMQ Adapter (TAMAMLANDI)

### Yaradılan Fayl
- `pkg/adapter/mt5_zmq.go` (258 sətir)

### Xüsusiyyətlər
- ✅ ZeroMQ SUB client (tcp://localhost:5556)
- ✅ Paranoid error handling (`defer recover()` bütün metodlarda)
- ✅ Exponential backoff reconnect (1,2,4,8,16,30s — CLAUDE.md pattern)
- ✅ Non-blocking receive with 1s timeout
- ✅ raw_payload byte-for-byte qorunması
- ✅ Atomic state management (no races)
- ✅ Backpressure detection (output channel block → undo metrics)
- ✅ Health metrics (connected, last_message, messages_recv, errors)
- ✅ Max 10 errors saxlanılır

### Test Gözləyir
```bash
go test ./pkg/adapter/... -run MT5
```

### Növbəti Addım
Task #2 (MT5 Canonicalizer) — MQL5 EA-dan gələn L1_TICK və L2_DEPTH JSON-u parse etmək

---

## ⏳ TASK #2: MT5 Canonicalizer (NÖVBƏTI)

### Yaradılacaq Fayl
- `pkg/canonicalizer/mt5.go`

### Tələblər

#### 2.1 Parse L1_TICK JSON
```json
{
  "type": "L1_TICK",
  "symbol": "EURUSD",
  "bid": 1.08456,
  "ask": 1.08458,
  "last": 1.08457,
  "volume": 0.5,
  "time": 1722933771120,
  "source": "MT5",
  "timestamp": 1722933771120
}
```

**Çıxış:** `CanonicalEvent` (EventType_QUOTE, price=last, Quote{bid,ask})

#### 2.2 Parse L2_DEPTH JSON
```json
{
  "type": "L2_DEPTH",
  "symbol": "EURUSD",
  "bids": [
    {"price": 1.08456, "volume": 2.5},
    {"price": 1.08455, "volume": 1.0}
  ],
  "asks": [
    {"price": 1.08458, "volume": 3.0},
    {"price": 1.08459, "volume": 0.8}
  ],
  "source": "MT5"
}
```

**Çıxış:** `CanonicalEvent` (EventType_BOOK_SNAPSHOT, Levels array)

#### 2.3 Sanitization (CLAUDE.md Paranoid Principles)
- ✓ `SanitizePrice()` — NaN/Inf/negative → 0.0
- ✓ `SanitizeSize()` — NaN/Inf/negative → 0.0
- ✓ Symbol mapping via `mapper.ToCanonical("MT5", "EURUSD")`

#### 2.4 Kod Strukturu
```go
package canonicalizer

import (
	"encoding/json"
	"time"
	"github.com/google/uuid"
	"raw-data-layer/pkg/axiom"
	"raw-data-layer/pkg/mapper"
)

// MT5Tick — L1_TICK JSON structure
type MT5Tick struct {
	Type      string  `json:"type"`
	Symbol    string  `json:"symbol"`
	Bid       float64 `json:"bid"`
	Ask       float64 `json:"ask"`
	Last      float64 `json:"last"`
	Volume    float64 `json:"volume"`
	Time      int64   `json:"time"`
	Source    string  `json:"source"`
	Timestamp int64   `json:"timestamp"`
}

// MT5Depth — L2_DEPTH JSON structure
type MT5Depth struct {
	Type   string        `json:"type"`
	Symbol string        `json:"symbol"`
	Bids   []PriceLevel  `json:"bids"`
	Asks   []PriceLevel  `json:"asks"`
	Source string        `json:"source"`
}

type PriceLevel struct {
	Price  float64 `json:"price"`
	Volume float64 `json:"volume"`
}

// CanonicalizeMT5 — main parser function
func (c *Canonicalizer) parseMT5(raw []byte) (*CanonicalEvent, error) {
	// 1. Detect type (L1_TICK vs L2_DEPTH)
	// 2. Parse JSON
	// 3. Sanitize floats
	// 4. Map symbol
	// 5. Build CanonicalEvent
	// 6. Preserve raw_payload
}
```

#### 2.5 Integration Point
`pkg/canonicalizer/worker.go` içində:
```go
case "MT5":
	if err := c.parseMT5(raw.Payload, ev); err != nil {
		fillUnknown(ev, raw, "MT5", "UNKNOWN")
		return pm, fmt.Errorf("mt5 parse: %w", err)
	}
```

---

## ⏳ TASK #3: MT5 Symbol Mapper

### Yaradılacaq Fayl
- `mappings/mt5.json`

### Məzmun
```json
{
  "EURUSD": "EUR/USD",
  "GBPUSD": "GBP/USD",
  "USDJPY": "USD/JPY",
  "AUDUSD": "AUD/USD",
  "USDCAD": "USD/CAD",
  "USDCHF": "USD/CHF",
  "NZDUSD": "NZD/USD",
  "EURGBP": "EUR/GBP",
  "EURJPY": "EUR/JPY",
  "XAUUSD": "XAU/USD",
  "XAGUSD": "XAG/USD",
  "BTCUSD": "BTC/USD",
  "ETHUSD": "ETH/USD"
}
```

### Test
```go
mapper.ToCanonical("MT5", "EURUSD") // → "EUR/USD"
mapper.ToCanonical("MT5", "XAUUSD") // → "XAU/USD" (gold)
```

---

## ⏳ TASK #4: Adapter Main Wiring

### Modifikasiya Ediləcək Fayl
- `cmd/adapter/main.go`

### Əlavə Olunacaq Kod
```go
// MT5 adapter (if enabled in config)
if cfg.Adapters.MT5.Enabled {
	mt5Cfg := adapter.AdapterConfig{
		Enabled:           cfg.Adapters.MT5.Enabled,
		ReconnectAttempts: 10,
		BackoffSeconds:    []int{1, 2, 4, 8, 16, 30},
		Timeout:           10 * time.Second,
	}
	
	mt5Adapter := adapter.NewMT5ZMQAdapter(cfg.Adapters.MT5.Endpoint, mt5Cfg)
	
	// Connect
	if err := mt5Adapter.Connect(ctx); err != nil {
		log.Warn("mt5 adapter connect failed (continuing without MT5)", "error", err)
	} else {
		// Start in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := mt5Adapter.Start(ctx, rawChan); err != nil {
				log.Error("mt5 adapter failed", "error", err)
			}
		}()
		
		adapters = append(adapters, mt5Adapter)
		log.Info("mt5 adapter started", "endpoint", cfg.Adapters.MT5.Endpoint)
	}
}
```

### Config Əlavəsi
`config/config.yaml`:
```yaml
adapters:
  mt5:
    enabled: false  # Default false (Wine + MT5 setup required)
    endpoint: "tcp://localhost:5556"
    reconnect:
      max_attempts: 10
      backoff: [1, 2, 4, 8, 16, 30]
```

---

## ⏳ TASK #5: MQL5 EA Script

### Yaradılacaq Fayl
- `scripts/mt5_zmq_bridge.mq5`

### Tələblər
1. **ZeroMQ PUB** socket bind (tcp://*:5556)
2. **L1 Tick** publisher (50ms interval = 20 Hz)
3. **L2 Depth** publisher (MarketBookGet)
4. **JSON serialization** (Json.mqh kitabxanası)
5. **Symbol list** input parametri
6. **Paranoid error handling** (no crashes)

### Asılılıqlar
- `Zmq.mqh` (mql5-zmq kitabxanası)
- `Json.mqh` (MQL5 JSON library)

### Quraşdırma
```bash
# 1. Wine 10.2 (not 10.3)
# 2. MT5 terminal (XM broker)
# 3. Copy to: ~/.wine/drive_c/Program Files/MetaTrader 5/MQL5/Experts/
# 4. Copy libraries to: MQL5/Include/{Zmq,Json}.mqh
# 5. Compile in MT5 MetaEditor
# 6. Attach EA to chart (EURUSD H1)
```

### Test
```bash
# Go side
./bin/adapter --mt5=true --binance=false --ib=false

# Check logs
tail -f /var/log/raw_data/adapter.log | grep MT5

# Expected:
# MT5: connected
# MT5: received 10 messages (L1_TICK)
```

---

## ⏳ TASK #6: Monitoring Stack (Docker Compose)

### Yaradılacaq Fayllar
1. `docker/docker-compose.monitoring.yml`
2. `docker/prometheus/prometheus.yml`
3. `docker/grafana/provisioning/dashboards/dashboard.yaml`
4. `docker/grafana/provisioning/datasources/datasource.yaml`

### 6.1 Docker Compose
```yaml
version: '3.8'
services:
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    ports:
      - "9090:9090"
    restart: unless-stopped

  grafana:
    image: grafana/grafana:latest
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning
      - ./grafana/dashboards:/var/lib/grafana/dashboards
      - grafana_data:/var/lib/grafana
    ports:
      - "3000:3000"
    depends_on:
      - prometheus
    restart: unless-stopped

volumes:
  prometheus_data:
  grafana_data:
```

### 6.2 Prometheus Config
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'raw-data-layer'
    static_configs:
      - targets: ['host.docker.internal:8080']
    metrics_path: '/metrics'
```

### 6.3 Grafana Dashboard
**QEYD:** `deployments/grafana/raw-data-layer.json` artıq mövcuddur (Step E)!  
Sadəcə kopyala:
```bash
cp deployments/grafana/raw-data-layer.json docker/grafana/dashboards/
```

### İşə Salma
```bash
cd docker
docker-compose -f docker-compose.monitoring.yml up -d

# Access
# Grafana: http://localhost:3000 (admin/admin)
# Prometheus: http://localhost:9090
```

---

## ⏳ TASK #7: ZeroMQ Publisher Activation

### Modifikasiya Ediləcək Fayl
- `config/config.yaml`

### Dəyişiklik
```yaml
publisher:
  zeromq:
    enabled: true  # ← false-dan true-ya
    protocol: "tcp"
    bind_address: "*"
    port: 5555
    heartbeat_interval: 5s
```

### Test
```python
# Python subscriber
import zmq
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://localhost:5555")
socket.setsockopt_string(zmq.SUBSCRIBE, "BTC/USD")  # Topic filter

while True:
    topic = socket.recv_string()
    data = socket.recv_json()
    print(f"Topic: {topic}, Data: {data}")
```

**Gözlənilən:**
- Heartbeat hər 5 saniyədə (topic: "HEARTBEAT")
- Binance trades (topic: "BTC/USD")
- MT5 ticks (topic: "EUR/USD")

---

## ⏳ TASK #8: IB Gateway Diagnostics

### Yaradılacaq Fayl
- `docs/IB_DIAGNOSTICS.md`

### Məzmun
```markdown
# IB Gateway Data Gəlməməsi — Troubleshooting

## Problem
IB Gateway bağlıdır, API enabled, lakin data axını yoxdur.

## Səbəblər
1. Market data subscription yoxdur
2. TWS/Gateway API parametrləri səhvdir
3. Contract tərifi səhvdir
4. Bazar bağlıdır (non-trading hours)

## Troubleshooting Steps

### Step 1: Subscription Check
1. Login: https://www.interactivebrokers.com/portal
2. Settings → Market Data Subscriptions
3. Verify: "US Securities Snapshot" (delayed 15min, free)
4. Real-time: "US Equity and Options Add-On" ($10/month)

### Step 2: TWS/Gateway API Settings
1. Open TWS/Gateway
2. File → Global Configuration → API → Settings
3. ✓ Enable ActiveX and Socket Clients
4. ✓ Socket port: 7497 (paper) / 7496 (live)
5. ✓ Allow localhost
6. ✓ Master API client ID: 0

### Step 3: Contract Definition
```go
// Correct contract (AAPL stock)
contract := ibapi.Contract{
    Symbol:   "AAPL",
    SecType:  "STK",   // Stock
    Exchange: "SMART", // Smart routing
    Currency: "USD",
}

// Forex example (EUR/USD)
contract := ibapi.Contract{
    Symbol:   "EUR",
    SecType:  "CASH",  // Forex
    Exchange: "IDEALPRO",
    Currency: "USD",
}
```

### Step 4: Delayed Data Test
```go
// Request delayed data (15min lag, no subscription needed)
client.ReqMarketDataType(3) // 3 = delayed
client.ReqMktData(1, contract, "", false, false, nil)
```

### Step 5: Trading Hours
- NYSE: 9:30 AM - 4:00 PM ET (Mon-Fri)
- Forex: 24/5 (Sun 5pm - Fri 5pm ET)
- Check: https://www.interactivebrokers.com/en/index.php?f=563

### Step 6: Restart TWS/Gateway
1. Close TWS/Gateway completely
2. Wait 30 seconds
3. Reopen
4. Reconnect adapter

## Test Code
```go
// pkg/adapter/ib_test.go
func TestIB_LiveData(t *testing.T) {
    client := NewIBAdapter("localhost", 7497, config)
    
    contract := ibapi.Contract{
        Symbol:   "AAPL",
        SecType:  "STK",
        Exchange: "SMART",
        Currency: "USD",
    }
    
    // Delayed data
    client.ReqMarketDataType(3)
    client.ReqMktData(1, contract, "", false, false, nil)
    
    // Wait 10 seconds
    time.Sleep(10 * time.Second)
    
    // Check data received
    health := client.Health()
    assert.True(t, health.MessagesRecv > 0, "No data received")
}
```

## Known Issues
1. **Simplified IB adapter** (PROGRESS.md): Current adapter is stub protocol, not full IB API
2. **Real fix**: Integrate `hadrianl/ibapi` library (2-3 days work)
3. **Workaround**: Use Binance + MT5 until IB adapter upgraded
```

---

## ⏳ TASK #9: Integration Tests

### Yaradılacaq Fayl
- `test/integration/mt5_test.go`

### Test Scenarios

#### 9.1 MT5 → Pipeline → DolphinDB
```go
func TestIntegration_MT5_EndToEnd(t *testing.T) {
    // 1. Start MT5 adapter (mock ZMQ publisher)
    // 2. Send L1_TICK JSON
    // 3. Verify canonicalization
    // 4. Verify WAL write
    // 5. Verify DolphinDB write
}
```

#### 9.2 Multi-Source (Binance + MT5)
```go
func TestIntegration_MultiSource(t *testing.T) {
    // 1. Start Binance + MT5 adapters
    // 2. Both sources send data simultaneously
    // 3. Verify no message loss
    // 4. Verify symbol mapping (BTCUSDT → BTC/USD, EURUSD → EUR/USD)
}
```

#### 9.3 ZeroMQ Publisher
```go
func TestIntegration_ZMQPublisher(t *testing.T) {
    // 1. Enable ZMQ publisher
    // 2. Start SUB client
    // 3. Send canonical events
    // 4. Verify SUB receives messages
    // 5. Verify topic filtering (subscribe "BTC/USD" only)
}
```

---

## ⏳ TASK #10: Documentation

### Modifikasiya Ediləcək Fayllar
1. `README.md` — MT5 setup bölməsi
2. `NEXT_STEPS.md` — MT5 integration complete qeydi
3. `PROGRESS.md` — MT5 adapter əlavə et

### README.md Əlavəsi
```markdown
## MT5 Integration (Forex + CFD)

### Prerequisites
- Wine 10.2 (not 10.3)
- MetaTrader 5 terminal
- XM broker account (or any MT5 broker)
- ZeroMQ library for MQL5

### Installation
```bash
# 1. Install Wine
sudo apt install wine-stable=10.2

# 2. Install MT5
wine mt5setup.exe

# 3. Download mql5-zmq
git clone https://github.com/dingmaotu/mql-zmq.git
cp -r mql-zmq/MQL5/Include/Zmq ~/.wine/.../MQL5/Include/

# 4. Copy EA
cp scripts/mt5_zmq_bridge.mq5 ~/.wine/.../MQL5/Experts/

# 5. Compile in MetaEditor
# 6. Attach to chart (EURUSD H1)
```

### Configuration
```yaml
# config/config.yaml
adapters:
  mt5:
    enabled: true
    endpoint: "tcp://localhost:5556"
```

### Run
```bash
./bin/adapter --mt5=true
```
```

---

## 📋 NÖVBƏTI ADDIMLAR (Prioritet Sırasında)

### İndi (P0)
1. ✅ Task #1: MT5 Adapter (DONE)
2. ⏳ Task #2: MT5 Canonicalizer — **BAŞLA BURADAN**
3. ⏳ Task #3: Symbol Mapper

### Bu Həftə (P1)
4. Task #4: Main wiring
5. Task #5: MQL5 EA script
6. Task #6: Monitoring stack
7. Task #7: ZeroMQ activation

### Növbəti Sprint (P2)
8. Task #8: IB diagnostics
9. Task #9: Integration tests
10. Task #10: Documentation

---

## 🎯 UĞUR MƏYARİ (Hər Task Üçün)

| Meyar | Check |
|-------|-------|
| Kod yazıldı | ✓ |
| `go build ./...` clean | ✓ |
| `go test ./pkg/...` pass | ✓ |
| `go test ./... -race` clean | ✓ |
| Paranoid principles (never panic) | ✓ |
| raw_payload preserved | ✓ |
| Documentation updated | ✓ |

---

## 🚨 KNOWN ISSUES

1. **MT5 adapter yazıldı amma test olunmayıb** — real MT5 terminal + Wine lazımdır
2. **IB Gateway stub protocol** — real `hadrianl/ibapi` integration 2-3 gün
3. **DolphinDB schema hələ apply olunmayıb** — RAPORT_2026_08_06.md P0 task

---

**Növbəti addım:** Task #2 (MT5 Canonicalizer) başla!  
**Fayl:** `pkg/canonicalizer/mt5.go`  
**Vaxt:** ~30 dəqiqə
