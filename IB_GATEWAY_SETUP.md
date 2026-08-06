# IB Gateway API Konfiqurasiyası

## 📋 Status
- ✅ IB Gateway işləyir (PID 12883)
- ❌ API Socket port 7497 **qapalıdır**
- 🔧 Port 7497-ni açmaq lazımdır

---

## 🚀 API Port-u Açmaq (Addım-addım)

### 1. IB Gateway GUI-da API Settings Aç

**IB Gateway pəncərəsində:**
```
Configure (Konfiqurasiya) → Settings (Parametrlər) → API → Settings (Tənzimləmələr)
```

### 2. API Ayarlarını Aktiv Et

Aşağıdakı qutucuqları **işarələ (✓)**:

#### ✅ Əsas Parametrlər:
- **Enable ActiveX and Socket Clients** ← Bu ən vacibdir!
- **Socket port:** `7497` (Paper Trading default)
- **Master API client ID:** `0` (default)
- **Read-Only API:** `NO` (unchecked) — market data üçün read-only kifayətdir, amma order göndərmək üçün NO olmalıdır

#### ✅ Güvənlik Parametrləri:
- **Allow connections from localhost only:** `YES` (✓) — təhlükəsizlik üçün
- **Trusted IP addresses:** `127.0.0.1` (localhost)

#### ⚠️ İxtiyari (Test üçün tövsiyə olunur):
- **Create API message log file:** `YES` — debug üçün
- **Include market data in API log:** `YES` — nə aldığını görmək üçün
- **Logging Level:** `Detail`

### 3. Apply və Restart

1. **Apply** düyməsinə bas
2. **OK** düyməsinə bas
3. IB Gateway-i **restart et** (File → Exit → yenidən aç)

---

## 🧪 Test: API Connection-u Yoxla

Restart-dan sonra port açıq olmalıdır:

```bash
# Port yoxlama
timeout 3 bash -c 'cat < /dev/null > /dev/tcp/localhost/7497' && \
  echo "✅ Port 7497 AÇIQ" || \
  echo "❌ Port 7497 QAPALI"
```

**Gözlənilən nəticə:**
```
✅ Port 7497 AÇIQ
```

---

## 🏃 Raw Data Layer IB Adapter Testi

API port açıldıqdan sonra adapter-i test et:

### Variant 1: Single-process (monolith)
```bash
cd ~/Desktop/raw-data-layer
go run ./cmd/raw-data-layer/main.go \
  --binance=false \
  --ib=true \
  --zmq=false \
  --db=false \
  --log-level=debug
```

### Variant 2: Multi-process (Addım C topology)
```bash
# Terminal 1 - Storage
go run ./cmd/storage/main.go

# Terminal 2 - Publisher
go run ./cmd/publisher/main.go

# Terminal 3 - Canonicalizer
go run ./cmd/canonicalizer/main.go

# Terminal 4 - Adapter (IB only)
go run ./cmd/adapter/main.go
```

**Gözlənilən output (debug log):**
```json
{"level":"info","adapter":"IB","msg":"connecting to localhost:7497"}
{"level":"info","adapter":"IB","msg":"connected successfully"}
{"level":"debug","adapter":"IB","msg":"handshake sent"}
{"level":"debug","adapter":"IB","msg":"received ticker data","symbol":"AAPL","price":178.45}
```

---

## 📊 Health Check

API işlədiyi zaman health endpoint response:

```bash
curl -s http://localhost:8080/health | jq '.adapters.ib'
```

**Gözlənilən:**
```json
{
  "status": "healthy",
  "connected": true,
  "messages_received": 42,
  "uptime_seconds": 120
}
```

---

## 🐛 Troubleshooting

### Problem 1: "Connection refused"
**Səbəb:** API port hələ də qapalıdır  
**Həll:** 
- IB Gateway-də Settings → API → Settings yoxla
- "Enable ActiveX and Socket Clients" işarələnməlidir
- Gateway-i restart et

### Problem 2: "Socket port 7497 is already in use"
**Səbəb:** Başqa proqram 7497-ni istifadə edir  
**Həll:**
```bash
# Hansı prosess istifadə edir?
sudo lsof -i :7497
# və ya
sudo ss -tulpn | grep 7497

# Prosessi öldür (PID-i əvəz et)
kill <PID>
```

### Problem 3: "Authentication failed"
**Səbəb:** Client ID conflict  
**Həll:** `config/config.yaml`-da `client_id: 1` olduğuna əmin ol (0 master üçündür)

### Problem 4: Market data yoxdur
**Səbəb:** Paper Trading hesabda market data subscription yoxdur  
**Həll:** 
- IB Paper Trading hesab default olaraq **delayed data** (15 min) verir
- Real-time data üçün live subscription lazımdır (ödənişli)
- Delayed data test üçün kifayətdir

---

## 📚 İlkin Konfiqurasiya (config.yaml)

Cari konfiqurasiya:
```yaml
adapters:
  ib:
    enabled: true
    host: "localhost"
    port: 7497  # Paper Trading
    client_id: 1
    symbols:
      - "AAPL"
      - "MSFT"
      - "GOOGL"
      - "TSLA"
      - "AMZN"
```

**Test üçün tövsiyə:** İlk öncə 1 simvol ilə test et:
```yaml
symbols: ["AAPL"]  # Sadə test
```

---

## ✅ API Port Açıldıqdan Sonra

1. ✅ Port 7497 açıqdır
2. ✅ Adapter connect olur
3. ✅ Market data axını başlayır
4. ✅ Canonical events generate olunur
5. ✅ WAL-a yazılır

**Növbəti:** Binance + IB eyni anda test et (multi-source ingestion)

