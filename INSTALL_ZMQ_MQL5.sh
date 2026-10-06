#!/bin/bash
# =============================================================================
# MT5 ZeroMQ Kitabxanası Quraşdırma Skripti
# Məqsəd: mql5-zmq kitabxanasını MT5 Include qovluğuna əlavə etmək
# =============================================================================

set -e  # Xəta olduqda dayan

echo "🔧 MT5 ZeroMQ Kitabxanası Quraşdırılır..."
echo ""

# Step 1: MT5 qovluğunu tap
echo "📁 MT5 qovluğu axtarılır..."

# Wine yoxla
if [ -d ~/.wine/drive_c/Program\ Files/MetaTrader\ 5 ]; then
    MT5_DIR=~/.wine/drive_c/Program\ Files/MetaTrader\ 5
    echo "✅ Wine MT5 tapıldı: $MT5_DIR"
elif [ -d ~/snap/wine/common/.wine/drive_c/Program\ Files/MetaTrader\ 5 ]; then
    MT5_DIR=~/snap/wine/common/.wine/drive_c/Program\ Files/MetaTrader\ 5
    echo "✅ Snap Wine MT5 tapıldı: $MT5_DIR"
else
    echo "❌ XƏTA: MT5 qovluğu tapılmadı!"
    echo "   Yoxlanılan yollər:"
    echo "   - ~/.wine/drive_c/Program Files/MetaTrader 5"
    echo "   - ~/snap/wine/common/.wine/drive_c/Program Files/MetaTrader 5"
    echo ""
    echo "💡 Həll:"
    echo "   1. MT5 quraşdırılmışdır? wine mt5setup.exe ilə yükləyin"
    echo "   2. MT5 başqa yerdədir? Manuel olaraq köçürün:"
    echo "      cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq /YOL/TO/MT5/MQL5/Include/"
    exit 1
fi

# MQL5/Include qovluğunu yoxla
MQL5_INCLUDE="$MT5_DIR/MQL5/Include"
if [ ! -d "$MQL5_INCLUDE" ]; then
    echo "❌ XƏTA: MQL5/Include qovluğu tapılmadı: $MQL5_INCLUDE"
    exit 1
fi

# Step 2: mql5-zmq yüklə
echo ""
echo "📥 mql5-zmq kitabxanası yüklənir..."

cd /tmp

# Əgər artıq varsa, sil
if [ -d mql-zmq-master ]; then
    echo "   (əvvəlki versiya silindi)"
    rm -rf mql-zmq-master
fi

if [ -f mql-zmq.zip ]; then
    rm -f mql-zmq.zip
fi

# ZIP yüklə
if ! wget -q https://github.com/dingmaotu/mql-zmq/archive/refs/heads/master.zip -O mql-zmq.zip; then
    echo "❌ XƏTA: wget ilə yükləmə uğursuz oldu"
    echo "💡 Həll: Manuel olaraq yükləyin:"
    echo "   https://github.com/dingmaotu/mql-zmq/archive/refs/heads/master.zip"
    exit 1
fi

# ZIP aç (-o = overwrite without prompt)
if ! unzip -q -o mql-zmq.zip; then
    echo "❌ XƏTA: ZIP faylı açılmadı"
    exit 1
fi

echo "✅ mql5-zmq yükləndi"

# Step 3: Zmq qovluğunu köçür
echo ""
echo "📋 Zmq qovluğu köçürülür..."

SOURCE="/tmp/mql-zmq-master/MQL5/Include/Zmq"
TARGET="$MQL5_INCLUDE/Zmq"

if [ ! -d "$SOURCE" ]; then
    echo "❌ XƏTA: Zmq qovluğu tapılmadı: $SOURCE"
    exit 1
fi

# Köçür
if cp -r "$SOURCE" "$TARGET"; then
    echo "✅ Zmq köçürüldü: $TARGET"
else
    echo "❌ XƏTA: Köçürmə uğursuz oldu"
    echo "💡 Manuel cəhd: sudo cp -r $SOURCE $TARGET"
    exit 1
fi

# Step 4: Yoxla
echo ""
echo "✅ Quraşdırma tamamlandı!"
echo ""
echo "📂 Quraşdırılan fayllar:"
ls -lh "$TARGET" | head -10

echo ""
echo "🎯 Növbəti addımlar:"
echo "   1. MT5-i restart edin (əgər açıqdırsa)"
echo "   2. MetaEditor-da mt5_zmq_bridge.mq5 faylını açın"
echo "   3. F7 ilə compile edin"
echo "   4. Xəta yoxdursa → Hazırsınız!"
echo ""
echo "📍 EA faylı buradadır:"
echo "   scripts/mt5_zmq_bridge.mq5"
echo ""
echo "📍 Quraşdırma yeri:"
echo "   $TARGET"
echo ""
echo "✅ Hazırdır!"
