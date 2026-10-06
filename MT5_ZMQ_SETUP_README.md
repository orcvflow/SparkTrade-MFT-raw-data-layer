# 🚀 MT5 ZeroMQ Quraşdırma — Tez Başlanğıc

**Məqsəd:** `Zmq.mqh` xətasını həll etmək və MT5 EA-nı işlətmək  
**Vaxt:** 2 dəqiqə  
**Status:** ✅ Hazır

---

## ⚡ 1 Komanda Həll

```bash
cd ~/Desktop/raw-data-layer
chmod +x QUICK_INSTALL_ZMQ.sh INSTALL_ZMQ_MQL5.sh
./QUICK_INSTALL_ZMQ.sh
```

**Uğurlu olduqda:**
```
✅ Quraşdırıldı: Wine MT5
📍 Yer: ~/.wine/drive_c/Program Files/MetaTrader 5/MQL5/Include/Zmq
🎯 Növbəti: MT5-i restart edin və F7 ilə compile edin
```

---

## 📂 Yaradılan Fayllar

| Fayl | Təyinat |
|------|---------|
| **QUICK_INSTALL_ZMQ.sh** | Tez quraşdırma (30s) |
| **INSTALL_ZMQ_MQL5.sh** | Ağıllı quraşdırma (debugging) |
| **ZMQ_XETASI_HELL.md** | Qısa həll təlimatı |
| **ZMQ_QURASDIRMA_TELIMAT.md** | Ətraflı təlimat + debugging |
| **MT5_ZMQ_SETUP_README.md** | Bu fayl (xülasə) |

---

## 🎯 Addımlar

### 1. ZMQ Kitabxanasını Quraşdırın
```bash
./QUICK_INSTALL_ZMQ.sh
```

### 2. MT5-i Restart Edin
- MT5 Terminal bağlayın → açın
- MetaEditor bağlayın → açın

### 3. EA-nı Compile Edin
- MetaEditor: **File → Open**
- Fayl: `scripts/mt5_zmq_bridge.mq5`
- **F7** (Compile)
- Gözlənilən: **0 errors** ✅

### 4. EA-nı MT5-ə Köçürün
```bash
cp scripts/mt5_zmq_bridge.mq5 \
   ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Experts/
```

### 5. EA-nı Chart-a Bağlayın
1. MT5: **Navigator → Expert Advisors**
2. **F5** (Refresh)
3. `mt5_zmq_bridge` → EURUSD H1 chart-a sürükləyin
4. **OK**

### 6. Test Edin
```bash
./bin/adapter --mt5=true --binance=false --ib=false
tail -f logs/adapter.log | grep MT5
```

---

## 🔍 Yoxlama

### ✅ Quraşdırma Uğurlu?
```bash
ls ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/Zmq/Zmq.mqh
```

**Çıxış olmalıdır:** `/home/.../Zmq.mqh` (xəta yoxdur)

### ✅ EA Compile Oldu?
MetaEditor-da compile etdikdən sonra:
```
Compilation successful
0 errors, 0 warnings
```

### ✅ EA İşləyir?
MT5 Experts tab (aşağıda):
```
ZMQ Publisher started on port 5556
Published 3 symbols (EURUSD, GBPUSD, BTCUSD)
```

### ✅ Go Adapter Bağlandı?
```bash
tail -f logs/adapter.log | grep MT5
```

Gözlənilən:
```
INFO  MT5: connected
INFO  MT5: received message (type=L1_TICK, symbol=EURUSD)
```

---

## 🚨 Problem Varsa

### "MT5 tapılmadı"
```bash
# Manuel tap
find ~ -name "MetaTrader 5" -type d 2>/dev/null

# Tapılan yolu istifadə et
cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq "/TAPILAN_YOL/MQL5/Include/"
```

### "Permission denied"
```bash
sudo cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq \
           ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/
sudo chown -R $USER:$USER ~/.wine
```

### "Hələ də Zmq.mqh xətası"
```bash
# MetaEditor cache təmizlə
# Tools → Options → Compiler → Clear cache
# Sonra F7 ilə yenidən compile et
```

### Ətraflı Həll
➡️ **ZMQ_QURASDIRMA_TELIMAT.md**

---

## 📊 Hazırlıq Statusu

- [x] ZMQ kitabxanası yükləndi (`/tmp/mql-zmq-master`)
- [x] Quraşdırma skriptləri hazırdır
- [x] EA faylı mövcuddur (`scripts/mt5_zmq_bridge.mq5`)
- [x] Təlimatlar yazılıb
- [ ] **SİZ:** Skripti işə salmalısınız
- [ ] **SİZ:** MT5-i restart etməlisiniz
- [ ] **SİZ:** EA-nı compile etməlisiniz

---

## 🎯 Növbəti Addım

**İNDİ İŞƏ SALIN:**
```bash
cd ~/Desktop/raw-data-layer
chmod +x QUICK_INSTALL_ZMQ.sh
./QUICK_INSTALL_ZMQ.sh
```

**Sonra bildirin:**
✅ "Zmq.mqh quraşdırıldı, compile oldu"

---

## 📖 Əlaqəli Sənədlər

1. **ZMQ_XETASI_HELL.md** — 2 dəqiqə həll (TL;DR)
2. **ZMQ_QURASDIRMA_TELIMAT.md** — Ətraflı təlimat + debugging
3. **MT5_FINAL_STATUS.md** — MT5 integration status
4. **MT5_IMPLEMENTATION_PLAN.md** — Tam plan

---

**Sual varsa, bu debugging məlumatını göndərin:**
```bash
find ~ -name "MetaTrader 5" -type d 2>/dev/null
ls -la ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/ 2>/dev/null
wine --version
```

**Uğurlar!** 🚀
