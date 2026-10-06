# 🔧 Zmq.mqh Xətasının Həlli — Addım-Addım Təlimat

**Xəta:** `Cannot open include file 'Zmq/Zmq.mqh'`  
**Səbəb:** ZeroMQ kitabxanası MT5-ə quraşdırılmayıb  
**Həll Müddəti:** 2 dəqiqə

---

## 📋 **AVTOMATIK HƏLL (Tövsiyə)**

### Addım 1: Skripti İşə Salın
```bash
cd ~/Desktop/raw-data-layer
chmod +x INSTALL_ZMQ_MQL5.sh
./INSTALL_ZMQ_MQL5.sh
```

### Addım 2: Nəticəni Yoxlayın
Əgər "✅ Quraşdırma tamamlandı!" görürsənsə → **Hazırsınız!**

### Addım 3: MT5-i Restart Edin
- MT5 terminal açıqdırsa → bağlayın və yenidən açın
- MetaEditor açıqdırsa → bağlayın və yenidən açın

### Addım 4: EA-nı Compile Edin
1. MetaEditor-da `mt5_zmq_bridge.mq5` faylını açın
2. **F7** basın (Compile)
3. "Compilation successful" yazısı görünməlidir

---

## 🛠️ **MANUEL HƏLL (Əgər Skript İşləməzsə)**

### Addım 1: Kitabxananı Yükləyin
```bash
cd /tmp
wget https://github.com/dingmaotu/mql-zmq/archive/refs/heads/master.zip -O mql-zmq.zip
unzip mql-zmq.zip
```

### Addım 2: MT5 Qovluğunu Tapın
```bash
# Wine (standart quraşdırma)
ls ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include

# Snap Wine (alternativ)
ls ~/snap/wine/common/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include
```

**Hansı varsa, o yolu istifadə edin.**

### Addım 3: Zmq Qovluğunu Köçürün
```bash
# Wine üçün:
cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq \
      ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/

# Snap Wine üçün:
cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq \
      ~/snap/wine/common/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/
```

### Addım 4: Yoxlayın
```bash
# Faylların olduğunu təsdiq edin
ls ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/Zmq/
```

**Gözlənilən çıxış:**
```
Zmq.mqh
Context.mqh
Socket.mqh
...
```

---

## 🔍 **PROBLEM HƏLLETMƏ**

### Problem 1: "MT5 qovluğu tapılmadı"
**Səbəb:** MT5 quraşdırılmayıb və ya fərqli yerdədir

**Həll:**
```bash
# MT5 qovluğunu tapın
find ~ -name "MetaTrader 5" -type d 2>/dev/null

# Tapılan yolu istifadə edərək köçürün
cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq \
      "/TAP_EDILEN_YOL/MQL5/Include/"
```

### Problem 2: "Permission denied"
**Səbəb:** Yazma icazəsi yoxdur

**Həll:**
```bash
# Sudo ilə yenidən cəhd edin
sudo cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq \
           ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/

# Və ya sahibliyi dəyişin
sudo chown -R $USER:$USER ~/.wine
```

### Problem 3: "wget: command not found"
**Səbəb:** wget quraşdırılmayıb

**Həll:**
```bash
# Ubuntu/Debian
sudo apt install wget

# Və ya curl istifadə edin
cd /tmp
curl -L https://github.com/dingmaotu/mql-zmq/archive/refs/heads/master.zip -o mql-zmq.zip
unzip mql-zmq.zip
```

### Problem 4: "unzip: command not found"
**Səbəb:** unzip quraşdırılmayıb

**Həll:**
```bash
sudo apt install unzip
```

---

## 🧪 **TEST**

### Test 1: Faylları Yoxlayın
```bash
ls -lh ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/Zmq/
```

**Gözlənilən:** 5-10 .mqh faylı görünməlidir

### Test 2: MT5 Compile Test
1. MetaEditor açın
2. Yeni fayl yaradın: **File → New**
3. Expert Advisor seçin
4. Aşağıdakı kodu yapışdırın:

```mql5
//+------------------------------------------------------------------+
//|                                              test_zmq.mq5        |
//+------------------------------------------------------------------+
#property strict
#include <Zmq/Zmq.mqh>

void OnStart() {
    Print("ZMQ Test: OK");
}
```

5. **F7** basın (Compile)
6. **Gözlənilən:** "Compilation successful" (xəta yoxdur)

---

## 🎯 **ÜMUMİ QAYDALAR**

### Quraşdırma Yerləri
```
✅ Doğru yol:
   ~/.wine/drive_c/Program Files/MetaTrader 5/MQL5/Include/Zmq/

❌ Səhv yol:
   ~/.wine/drive_c/Program Files/MetaTrader 5/MQL5/Zmq/
   ~/.wine/drive_c/Program Files/MetaTrader 5/Include/Zmq/
```

### Fayl Strukturu
```
MQL5/
├── Include/
│   ├── Zmq/              ← Burası
│   │   ├── Zmq.mqh       ← Əsas fayl
│   │   ├── Context.mqh
│   │   ├── Socket.mqh
│   │   └── ...
│   └── ...
└── Experts/
    └── mt5_zmq_bridge.mq5  ← EA faylı
```

---

## 📊 **STATUS YOXLAMASİ**

Hər addımı tamamladıqdan sonra işarələyin:

- [ ] INSTALL_ZMQ_MQL5.sh işə salındı
- [ ] "✅ Quraşdırma tamamlandı!" mesajı görüldü
- [ ] `ls ~/.wine/.../MQL5/Include/Zmq/` faylları göstərdi
- [ ] MT5 restart olundu
- [ ] MetaEditor açıldı
- [ ] mt5_zmq_bridge.mq5 compile oldu (xətasız)

**Hamısı ✅ olarsa → Hazırsınız! MT5 EA-nı başlada bilərsiniz.**

---

## 🚀 **NÖVBƏTI ADDIMLAR**

### 1. EA-nı MT5-ə Əlavə Edin
```bash
# EA faylını köçürün
cp scripts/mt5_zmq_bridge.mq5 \
   ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Experts/
```

### 2. EA-nı Chart-a Bağlayın
1. MT5 terminalında **Navigator → Expert Advisors**
2. `mt5_zmq_bridge` tapın
3. EURUSD H1 chart-a sürükləyin
4. Parametrləri yoxlayın və **OK** basın

### 3. Log-ları Yoxlayın
```bash
# Go adapter tərəfi
tail -f logs/adapter.log | grep MT5

# MT5 tərəfi
# MetaEditor → Tools → Journal
```

### 4. Test Edin
```bash
# Raw-data-layer adapter-i başladın
./bin/adapter --mt5=true --binance=false --ib=false

# Gözlənilən log:
# INFO  MT5: connected to tcp://localhost:5556
# INFO  MT5: received 10 messages (L1_TICK)
```

---

## 📞 **KÖMƏYƏ EHTIYYAC VARSA**

### Addımlar
1. Skripti işə salın: `./INSTALL_ZMQ_MQL5.sh`
2. Çıxışı köçürün və göndərin
3. Xəta varsa, dəqiq xəta mesajını paylaşın

### Debugging Komandaları
```bash
# MT5 qovluğu harada?
find ~ -name "MetaTrader 5" -type d 2>/dev/null

# Include qovluğu varmı?
ls -la ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/

# Zmq quraşdırılıbmı?
ls -la ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/Zmq/

# Wine versiyası
wine --version

# İcazələr
ls -ld ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/
```

Bu məlumatları göndərərək problemi tez həll edə bilərik.

---

## 📚 **ƏLAVƏ MƏLUMAT**

### ZeroMQ Kitabxanası Haqqında
- **GitHub:** https://github.com/dingmaotu/mql-zmq
- **Sənədlər:** Repo README
- **MQL5 Forum:** https://www.mql5.com/en/forum (ZeroMQ mövzusu)

### MT5 API Məsləhətləri
- `#include <Zmq/Zmq.mqh>` — əsas ZMQ kitabxanası
- Context yaradın, socket açın, pub/sub edin
- JSON.mqh alternativ: CJAVal (built-in MQL5 JSON parser)

---

**✅ Quraşdırma tamamlandıqdan sonra bildirin:**
> "Zmq.mqh quraşdırıldı, compile oldu"

Sonra növbəti testlərə keçərik! 🚀
