# Raw Data Layer — Növbəti Addımlar

**Tarix:** 2026-08-06  
**Status:** IB Gateway hazırdır, adapter simplified protocol-dadır

---

## 🎯 Prioritet Sırası

### 1. Git Push (Dərhal) ✅
```bash
cd ~/Desktop/raw-data-layer
git push origin main --tags
```

**Status:** CI fix + IB setup faylları commit olunub

---

### 2. IB API Real Protocol (2 yol)

#### Yol A: Go IB API Library (Tövsiyə)
**hadrianl/ibapi** (ən aktiv Go IB wrapper):
```bash
go get github.com/hadrianl/ibapi
```

**Tətbiq:**
1. `pkg/adapter/ib.go`-da `import "github.com/hadrianl/ibapi"`
2. `NewIBAdapter` içində `ibapi.NewClient()` yarad
3. `Connect()` içində client-i connect et və contract-ları subscribe et
4. `receiveLoop()` içində `client.HandleMsg()` dinlə

**Üstünlüklər:**
- Real IB API protocol
- Market data events (Tick Price, Tick Size)
- Contract management
- Error handling

**Misal kod:**
```go
import "github.com/hadrianl/ibapi"

func (ib *IBAdapter) Connect(ctx context.Context) error {
    client := ibapi.NewClient(ib.host, ib.port, ib.clientID)
    if err := client.Connect(); err != nil {
        return err
    }
    
    // Subscribe to market data
    for _, symbol := range ib.symbols {
        contract := &ibapi.Contract{
            Symbol:   symbol,
            SecType:  "STK",
            Exchange: "SMART",
            Currency: "USD",
        }
        client.ReqMktData(reqID, contract, "", false, false, nil)
    }
    
    return nil
}
```

#### Yol B: Mock Data Generator (Sürətli Test)
Stub data generator ilə pipeline-i test et:

```bash
# Test data generator yarad
cat > test_mock_ib.go <<'EOF'
package main

import (
    "context"
    "time"
    "raw-data-layer/pkg/adapter"
)

func main() {
    output := make(chan adapter.RawMessage, 100)
    
    // Mock IB data generator
    go func() {
        ticker := time.NewTicker(100 * time.Millisecond)
        for range ticker.C {
            msg := adapter.RawMessage{
                Source:     "IB",
                Payload:    []byte(`{"symbol":"AAPL","price":178.45,"volume":1000}`),
                ReceivedAt: time.Now().UnixNano(),
            }
            output <- msg
        }
    }()
    
    // Process messages
    for msg := range output {
        println("Received:", string(msg.Payload))
    }
}
EOF

go run test_mock_ib.go
```

---

### 3. Binance Adapter Test (Alternativ)
IB-dən əvvəl Binance ilə test et (WebSocket daha sadədir):

```bash
cd ~/Desktop/raw-data-layer
go run ./cmd/raw-data-layer/main.go \
  --binance=true \
  --ib=false \
  --zmq=false \
  --db=false \
  --log-level=debug
```

**Gözlənilən:** Binance public WebSocket data axını

---

### 4. Multi-Process Test (Addım C Topology)
4 prosessi ayrı-ayrı işə sal və UDS ilə bağla:

```bash
# Terminal 1 - Storage
go run ./cmd/storage/main.go

# Terminal 2 - Publisher
go run ./cmd/publisher/main.go

# Terminal 3 - Canonicalizer
go run ./cmd/canonicalizer/main.go

# Terminal 4 - Adapter
go run ./cmd/adapter/main.go
```

**Health check:**
```bash
for port in 8081 8082 8083 8084; do
  curl -s http://localhost:$port/health | jq -r '.status'
done
```

---

### 5. Production Config (Addım F)
Batched WAL-ı production default-a çevir:

**config/config.yaml:**
```yaml
storage:
  wal:
    mode: "batched"  # sync → batched (148K msg/s)
    batch_timeout_ms: 50
```

---

## 🐛 Cari Məhdudiyyətlər

| Problem | Status | Həll |
|---------|--------|------|
| IB API simplified | ❌ Stub protocol | hadrianl/ibapi istifadə et |
| Binance not tested | ⏳ Gözləyir | `--binance=true` ilə test et |
| Multi-process untested | ⏳ Gözləyir | 4 terminal ilə test et |
| DolphinDB disabled | ⚠️ WAL-only | Live DolphinDB instance lazımdır |

---

## 📊 Verification Checklist

### IB API Integration
- [ ] `hadrianl/ibapi` install
- [ ] `pkg/adapter/ib.go` refactor (real protocol)
- [ ] Contract subscription (AAPL, MSFT, GOOGL)
- [ ] Tick data receive loop
- [ ] WAL-a write (>0 events)
- [ ] Health check (messages_received > 0)

### Binance Test
- [ ] `--binance=true` ilə işə sal
- [ ] WebSocket connection log
- [ ] Trade events axını
- [ ] WAL write
- [ ] Canonical events generate

### Multi-Process
- [ ] 4 prosess parallel işə sal
- [ ] UDS connection established
- [ ] Health endpoints (8081-8084) OK
- [ ] IPC message flow
- [ ] Graceful shutdown

---

## 🚀 Dərhal İşlədilə Biləcək Komandlar

```bash
# 1. Push to GitHub
git push origin main --tags

# 2. IB API library install
go get github.com/hadrianl/ibapi

# 3. Binance test (alternative)
go run ./cmd/raw-data-layer/main.go --binance=true --ib=false --log-level=debug

# 4. Check WAL events
tail -f data/wal/*.jsonl | jq '.'

# 5. Health monitoring
watch -n 2 'curl -s http://localhost:8080/health | jq ".pool, .wal"'
```

---

## 📚 Məlumat Mənbələri

- **IB API Go Wrapper:** https://github.com/hadrianl/ibapi
- **IB TWS API Docs:** https://interactivebrokers.github.io/tws-api/
- **Binance WebSocket:** https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams
- **Addım C (Multi-Process):** `ADDIM_C_PHASES.md`
- **Addım E (Deployment):** `STEP-E.md`

---

**Növbəti addım:** `git push` və ya IB API library integration-a başla.

