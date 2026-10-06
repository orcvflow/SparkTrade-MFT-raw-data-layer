# ⚡ ZMQ Xətası Həlli — 2 Dəqiqə Həll

**Xəta:** `'Zmq/Zmq.mqh' file does not exist`  
**Səbəb:** MQL5 kitabxanası quraşdırılmayıb  
**Həll:** Bir komanda

---

## 🚀 TEZ HƏLL (30 saniyə)

```bash
cd ~/Desktop/raw-data-layer
chmod +x QUICK_INSTALL_ZMQ.sh
./QUICK_INSTALL_ZMQ.sh
```

**Gözlənilən:**
```
✅ Quraşdırıldı: Wine MT5
📍 Yer: /home/.../.wine/.../MQL5/Include/Zmq
Zmq.mqh
Context.mqh
Socket.mqh
...

🎯 Növbəti: MT5-i restart edin və F7 ilə compile edin
```

---

## 📋 SONRA NƏ ETMƏLİ?

### 1. MT5-i Restart Edin
- MT5 Terminal açıqdırsa → bağlayın
- MetaEditor açıqdırsa → bağlayın
- Hər ikisini yenidən açın

### 2. EA-nı Compile Edin
1. MetaEditor açın
2. **File → Open** → `scripts/mt5_zmq_bridge.mq5` (layihədən)
3. **F7** basın (Compile)
4. **Gözlənilən:** "0 errors, 0 warnings" ✅

### 3. EA-nı MT5-ə Köçürün (avtomatik)
```bash
cp scripts/mt5_zmq_bridge.mq5 \
   ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Experts/
```

### 4. EA-nı Chart-a Bağlayın
1. MT5 terminalında **Navigator → Expert Advisors**
2. Yenilə (F5)
3. `mt5_zmq_bridge` tapın
4. EURUSD H1 chart-a sürükləyin
5. Parametrlər:
   - **Symbols:** "EURUSD,GBPUSD,BTCUSD"
   - **ZMQ Port:** 5556 (default)
6. **OK** basın

### 5. EA İşləyir? Yoxlayın
**MT5 Experts tab (aşağıda):**
```
2026.08.08 18:30:00   mt5_zmq_bridge EURUSD,H1: ZMQ Publisher started on port 5556
2026.08.08 18:30:01   mt5_zmq_bridge EURUSD,H1: Published 3 symbols (EURUSD, GBPUSD, BTCUSD)
```

---

## 🧪 TEST — Go Adapter Tərəfi

### Terminal 1: Go Adapter Başladın
```bash
cd ~/Desktop/raw-data-layer
./bin/adapter --mt5=true --binance=false --ib=false
```

### Terminal 2: Log İzləyin
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

## 🔧 PROBLEM HƏLLETMƏtMƏ

### Xəta 1: "MT5 tapılmadı"
```bash
# MT5 qovluğunu manuel tapın
find ~ -name "MetaTrader 5" -type d 2>/dev/null

# Tapılan yolu istifadə edin
cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq \
      "/TAPILAN_YOL/MQL5/Include/"
```

### Xəta 2: "Permission denied"
```bash
# Sudo ilə köçürün
sudo cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq \
           ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/

# Sahiblik dəyişin
sudo chown -R $USER:$USER ~/.wine
```

### Xəta 3: "wget: command not found"
```bash
sudo apt install wget unzip
```

### Xəta 4: Compile xətası (hələ də Zmq.mqh tapılmır)
**Səbəb:** MT5 köhnə cache istifadə edir

**Həll:**
1. MetaEditor-da **Tools → Options → Compiler**
2. **Clear cache** basın
3. **F7** ilə yenidən compile edin

---

## 📊 YOXLAMA LİSTİ

Hər addımı işarələyin:

- [ ] `./QUICK_INSTALL_ZMQ.sh` işə salındı
- [ ] "✅ Quraşdırıldı" mesajı görüldü
- [ ] `ls ~/.wine/.../Include/Zmq/` faylları göstərdi
- [ ] MT5 və MetaEditor restart olundu
- [ ] `mt5_zmq_bridge.mq5` compile oldu (0 errors)
- [ ] EA MT5 Experts qovluğuna köçürüldü
- [ ] EA chart-a bağlandı
- [ ] Experts tab-da "ZMQ Publisher started" yazısı görüldü
- [ ] Go adapter başladıldı və "MT5: connected" log-u görüldü

**Hamısı ✅ olarsa → MT5 integration tam işləyir!** 🎉

---

## 📖 ƏLAVƏ SƏNƏDLƏR

### Ətraflı Təlimat
➡️ **ZMQ_QURASDIRMA_TELIMAT.md** — tam debugging, bütün hallar

### Avtomatik Skript
➡️ **INSTALL_ZMQ_MQL5.sh** — ağıllı quraşdırma (error handling)

### EA Faylı
➡️ **scripts/mt5_zmq_bridge.mq5** — ZeroMQ publisher EA

---

## 🎯 TL;DR (Çox Qısa Versiya)

```bash
# 1. ZMQ quraşdır (30s)
./QUICK_INSTALL_ZMQ.sh

# 2. MT5 restart et

# 3. EA compile et (MetaEditor → F7)

# 4. EA chart-a bağla

# 5. Go adapter başlat
./bin/adapter --mt5=true

# 6. Yoxla
tail -f logs/adapter.log | grep MT5
```

**Bitdi!** 🚀

---

## 📞 KÖMƏYƏ EHTIYYAC?

1. Skripti işə salın və çıxışı göndərin
2. Xəta varsa, dəqiq mesajı paylaşın
3. Aşağıdakı komandaların çıxışını göndərin:

```bash
# Debugging məlumat
find ~ -name "MetaTrader 5" -type d 2>/dev/null
ls -la ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/ 2>/dev/null
wine --version
```

---

**✅ Bildirin:** "Zmq.mqh quraşdırıldı, compile oldu"  
**Sonra:** Növbəti testlərə keçərik! 🎉
