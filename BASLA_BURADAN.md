# 🎯 MT5 ZeroMQ Quraşdırma — BAŞLA BURADAN

**Xəta:** `'Zmq/Zmq.mqh' file does not exist`  
**Həll:** 1 komanda, 30 saniyə  
**Status:** ✅ Hazır (JSON kitabxanası artıq lazım deyil)

---

## ⚡ 1. İLK ADDIM — Zmq Kitabxanası Quraşdır

```bash
cd ~/Desktop/raw-data-layer
chmod +x QUICK_INSTALL_ZMQ.sh
./QUICK_INSTALL_ZMQ.sh
```

**Gözlənilən Çıxış:**
```
✅ Quraşdırıldı: Wine MT5
📍 Yer: ~/.wine/drive_c/Program Files/MetaTrader 5/MQL5/Include/Zmq
Zmq.mqh
Context.mqh
Socket.mqh
...
🎯 Növbəti: MT5-i restart edin və F7 ilə compile edin
```

---

## 🔧 2. MT5-i Restart Et

- **MT5 Terminal** açıqdırsa → bağla
- **MetaEditor** açıqdırsa → bağla
- Hər ikisini **yenidən aç**

---

## 🛠️ 3. EA-nı Compile Et

### MetaEditor-da:
1. **File → Open**
2. Fayl seç: `~/Desktop/raw-data-layer/scripts/mt5_zmq_bridge.mq5`
3. **F7** bas (Compile)

**Gözlənilən:**
```
Compilation successful
0 errors, 0 warnings
```

✅ **JSON kitabxanası artıq lazım deyil** — EA manual JSON yaradır

---

## 📋 4. EA-nı MT5-ə Köçür

```bash
cp ~/Desktop/raw-data-layer/scripts/mt5_zmq_bridge.mq5 \
   ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Experts/
```

---

## 🚀 5. EA-nı Chart-a Bağla

### MT5 Terminal-da:
1. **Navigator → Expert Advisors**
2. **F5** (Refresh)
3. `mt5_zmq_bridge` tapıb **EURUSD H1** chart-a sürükle
4. Parametrlər:
   - **Symbols:** `EURUSD,GBPUSD,XAUUSD`
   - **ZMQ Port:** `5556`
5. **OK** bas

---

## 🧪 6. Test Et

### MT5 Experts Tab (aşağıda):
```
ZeroMQ PUB socket bound to tcp://*:5556
MarketBookAdd OK for EURUSD
MarketBookAdd OK for GBPUSD
MT5 ZeroMQ Bridge EA initialized successfully.
```

### Go Adapter Başlat:
```bash
cd ~/Desktop/raw-data-layer
./bin/adapter --mt5=true --binance=false --ib=false
```

### Log Yoxla:
```bash
tail -f logs/adapter.log | grep MT5
```

**Gözlənilən:**
```
INFO  MT5: connecting to tcp://localhost:5556
INFO  MT5: connected
INFO  MT5: received message (type=L1_TICK, symbol=EURUSD)
INFO  MT5: received 10 messages in 1s
```

---

## ✅ Yoxlama

- [ ] `./QUICK_INSTALL_ZMQ.sh` işə salındı
- [ ] "✅ Quraşdırıldı" görüldü
- [ ] MT5 və MetaEditor restart oldu
- [ ] EA compile oldu (0 errors)
- [ ] EA MT5 Experts qovluğuna köçürüldü
- [ ] EA chart-a bağlandı
- [ ] Experts tab-da "initialized successfully" görüldü
- [ ] Go adapter başladıldı
- [ ] "MT5: connected" log-u görüldü

**Hamısı ✅ → MT5 tam işləyir!** 🎉

---

## 🚨 Problem Varsa

### Xəta 1: "MT5 tapılmadı"
```bash
# Manuel tap
find ~ -name "MetaTrader 5" -type d 2>/dev/null

# Tapılan yolu istifadə et
INSTALL_ZMQ_MQL5.sh # (ağıllı skript - bütün yolları yoxlayır)
```

### Xəta 2: "Permission denied"
```bash
sudo ./QUICK_INSTALL_ZMQ.sh
sudo chown -R $USER:$USER ~/.wine
```

### Xəta 3: "Hələ də Zmq.mqh tapılmır"
```bash
# Cache təmizlə
# MetaEditor → Tools → Options → Compiler → Clear cache
# F7 ilə yenidən compile et
```

### Ətraflı Həll
➡️ **ZMQ_QURASDIRMA_TELIMAT.md** (debugging guide)

---

## 📖 Sənədlər

| Fayl | Məzmun |
|------|--------|
| **BASLA_BURADAN.md** | Bu fayl (xülasə) |
| **ZMQ_XETASI_HELL.md** | 2 dəqiqə həll (TL;DR) |
| **ZMQ_QURASDIRMA_TELIMAT.md** | Ətraflı təlimat + debugging |
| **MT5_ZMQ_SETUP_README.md** | Texniki xülasə |
| **QUICK_INSTALL_ZMQ.sh** | Tez quraşdırma skripti |
| **INSTALL_ZMQ_MQL5.sh** | Ağıllı quraşdırma skripti |

---

## 🎯 İNDİ NƏ ETMƏLİ?

### 1. Skripti İşə Sal
```bash
./QUICK_INSTALL_ZMQ.sh
```

### 2. Nəticəni Bildir
✅ **Uğurlu:** "Zmq.mqh quraşdırıldı, compile oldu"  
❌ **Xəta:** Dəqiq xəta mesajını göndər

---

## 💡 Əsas Məqamlar

1. ✅ **JSON kitabxanası lazım deyil** — EA artıq manual JSON yaradır
2. ✅ **Sadəcə Zmq.mqh kitabxanası lazımdır** — 1 komanda ilə quraşdırılır
3. ✅ **EA compile-ready** — heç bir əlavə dependency yoxdur
4. ✅ **Test hazırdır** — Go adapter və MT5 EA hazır

---

## 🚀 TL;DR (5 Saniyə Versiya)

```bash
# 1. Zmq quraşdır
./QUICK_INSTALL_ZMQ.sh

# 2. MT5 restart et

# 3. EA compile et (F7)

# 4. EA chart-a bağla

# 5. Adapter başlat
./bin/adapter --mt5=true

# 6. Log yoxla
tail -f logs/adapter.log | grep MT5
```

**Bitdi! 🎉**

---

**✅ İNDİ BAŞLA:** `./QUICK_INSTALL_ZMQ.sh`  
**Bildirin:** "Zmq.mqh quraşdırıldı, compile oldu" 🚀
