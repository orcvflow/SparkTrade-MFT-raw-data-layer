# 📚 MT5 ZeroMQ Quraşdırma — Fayl İndeksi

**Məqsəd:** `Zmq.mqh` xətasını həll etmək  
**Status:** ✅ Hazır (JSON artıq lazım deyil)  
**Vaxt:** 30 saniyə

---

## 🎯 BAŞLANĞIC NOQTƏSI

| # | Fayl | Təyinat | İstifadə |
|---|------|---------|----------|
| **1** | **BASLA_BURADAN.md** | **Əsas təlimat (başla buradan)** | **OXU İLK** |

---

## 🔧 QURAŞDIRMA SKRİPTLƏRİ

| Fayl | Təyinat | Komanda |
|------|---------|---------|
| **QUICK_INSTALL_ZMQ.sh** | Tez quraşdırma (30s, minimal) | `./QUICK_INSTALL_ZMQ.sh` |
| **INSTALL_ZMQ_MQL5.sh** | Ağıllı quraşdırma (error handling) | `./INSTALL_ZMQ_MQL5.sh` |

**Tövsiyə:** Əvvəl `QUICK_INSTALL_ZMQ.sh` işə sal, problem olarsa `INSTALL_ZMQ_MQL5.sh`

---

## 📖 TELİMAT SƏNƏDLƏRİ

| Fayl | Səviyyə | Məzmun | Kimlər Üçün |
|------|---------|--------|-------------|
| **BASLA_BURADAN.md** | ⭐⭐⭐ Ən Əsas | Xülasə, 6 addım, yoxlama | Hamı |
| **ZMQ_XETASI_HELL.md** | ⭐⭐ Qısa | 2 dəqiqə həll, TL;DR | Tez həll istəyənlər |
| **ZMQ_QURASDIRMA_TELIMAT.md** | ⭐ Ətraflı | Problem həlletmə, debugging | Xəta varsa |
| **MT5_ZMQ_SETUP_README.md** | Texniki | Arxitektura, fayllar | Tərtibatçılar |

---

## 🧪 TEST VƏ EA FAYLARI

| Fayl | Təyinat |
|------|---------|
| `scripts/mt5_zmq_bridge.mq5` | MT5 Expert Advisor (JSON artıq lazım deyil) |
| `bin/adapter` | Go adapter executable |
| `config/config.yaml` | MT5 adapter konfiqurasiyası |

---

## 📊 STATUS VƏ RAPORTLAR

| Fayl | Məzmun |
|------|--------|
| `MT5_FINAL_STATUS.md` | MT5 integration status (95% hazır) |
| `MT5_IMPLEMENTATION_PLAN.md` | Tam implementasiya planı |
| `MT5_IMPLEMENTATION_COMPLETE.md` | Tamamlanma raportu |

---

## 🚀 İŞƏ SALMA ARDIOILLIGI

### Addım 1: Quraşdır
```bash
chmod +x QUICK_INSTALL_ZMQ.sh INSTALL_ZMQ_MQL5.sh
./QUICK_INSTALL_ZMQ.sh
```

### Addım 2: Restart
- MT5 Terminal bağla → aç
- MetaEditor bağla → aç

### Addım 3: Compile
- MetaEditor: `scripts/mt5_zmq_bridge.mq5` aç
- **F7** bas

### Addım 4: Köçür
```bash
cp scripts/mt5_zmq_bridge.mq5 ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Experts/
```

### Addım 5: Bağla
- MT5: Navigator → Expert Advisors → mt5_zmq_bridge
- EURUSD H1 chart-a sürükle

### Addım 6: Test
```bash
./bin/adapter --mt5=true
tail -f logs/adapter.log | grep MT5
```

---

## 🔍 PROBLEM HƏLLETMə QAYDALARI

| Problem | Həll Faylı | Əlavə Komanda |
|---------|------------|---------------|
| Skript işləmir | `INSTALL_ZMQ_MQL5.sh` | Debugging çıxışı var |
| MT5 tapılmır | `ZMQ_QURASDIRMA_TELIMAT.md` | `find ~ -name "MetaTrader 5"` |
| Permission xətası | Həmin sənəd | `sudo chown -R $USER ~/.wine` |
| Compile xətası | `ZMQ_XETASI_HELL.md` | Cache təmizlə |
| JSON xətası | Yoxdur! | EA artıq JSON kitabxanası istəmir |

---

## ✅ ÜMUMİ YOXLAMA

- [ ] `BASLA_BURADAN.md` oxudum
- [ ] `QUICK_INSTALL_ZMQ.sh` işə saldım
- [ ] "✅ Quraşdırıldı" mesajı gördüm
- [ ] MT5 restart etdim
- [ ] EA compile oldu (0 errors)
- [ ] EA chart-a bağlandım
- [ ] "initialized successfully" log-u gördüm
- [ ] Go adapter başlatdım
- [ ] "MT5: connected" log-u gördüm

**Hamısı ✅ → MT5 tam işləyir!** 🎉

---

## 💡 ÖNƏMLİ QEYDLƏR

### ✅ JSON Kitabxanası Artıq Lazım Deyil
EA (`mt5_zmq_bridge.mq5`) artıq manual JSON yaradır:
- ❌ `#include <JAson.mqh>` — silindi
- ✅ `string json = "{...}"` — manual konstruksiya

**Nəticə:** Sadəcə `Zmq.mqh` lazımdır!

### ✅ Wine Uyğunluğu
- **Tövsiyə:** Wine 10.2 (not 10.3)
- **Test:** MT5 terminal normal açılır + EA compile olursa → OK

### ✅ Broker Tələbləri
- **Forex:** Bütün brokerlər (EURUSD, GBPUSD, ...)
- **DOM (L2_DEPTH):** Bəzi brokerlər dəstəkləmir (EA log-da xəbərdarlıq verəcək)

---

## 📞 KÖMƏYƏ EHTIYYAC?

### Debugging Məlumat Göndərin:
```bash
# MT5 qovluğu
find ~ -name "MetaTrader 5" -type d 2>/dev/null

# Zmq faylları
ls -la ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/Zmq/ 2>/dev/null

# Wine versiya
wine --version

# Skript çıxışı
./QUICK_INSTALL_ZMQ.sh 2>&1 | tee zmq_install.log
cat zmq_install.log
```

---

## 🎯 TL;DR — 10 Saniyə Xülasə

1. **Oku:** `BASLA_BURADAN.md`
2. **İşə sal:** `./QUICK_INSTALL_ZMQ.sh`
3. **Restart et:** MT5 + MetaEditor
4. **Compile et:** F7 (0 errors)
5. **Bağla:** EA chart-a
6. **Test et:** `./bin/adapter --mt5=true`

**Xəta varsa:** `ZMQ_QURASDIRMA_TELIMAT.md` 📖

---

**✅ İNDİ BAŞLA:**
```bash
cd ~/Desktop/raw-data-layer
./QUICK_INSTALL_ZMQ.sh
```

**Bildirin:** "Zmq.mqh quraşdırıldı, compile oldu" 🚀

---

**Son Yeniləmə:** 2026-08-08  
**Status:** ✅ Hazır  
**JSON Dependency:** ❌ Artıq lazım deyil
