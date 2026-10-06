#!/bin/bash
# Tez ZMQ Quraşdırma - Bir Komanda Həll

echo "🚀 ZeroMQ kitabxanası quraşdırılır..."
echo ""

# mql-zmq yüklə
cd /tmp
echo "📥 Yüklənir..."

# Köhnə faylları sil
rm -rf mql-zmq-master mql-zmq.zip

wget -q https://github.com/dingmaotu/mql-zmq/archive/refs/heads/master.zip -O mql-zmq.zip || {
    echo "❌ Yükləmə uğursuz. İnternet bağlantınızı yoxlayın."
    exit 1
}

unzip -q -o mql-zmq.zip  # -o = overwrite without prompting

# MT5 tap və köçür
echo "📂 MT5 qovluğu axtarılır..."

if [ -d ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include ]; then
    TARGET=~/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/Zmq
    cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq "$TARGET"
    echo "✅ Quraşdırıldı: Wine MT5"
    echo "📍 Yer: $TARGET"
    ls "$TARGET" | head -5
    echo ""
    echo "🎯 Növbəti: MT5-i restart edin və F7 ilə compile edin"
    exit 0
fi

if [ -d ~/snap/wine/common/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include ]; then
    TARGET=~/snap/wine/common/.wine/drive_c/Program\ Files/MetaTrader\ 5/MQL5/Include/Zmq
    cp -r /tmp/mql-zmq-master/MQL5/Include/Zmq "$TARGET"
    echo "✅ Quraşdırıldı: Snap Wine MT5"
    echo "📍 Yer: $TARGET"
    ls "$TARGET" | head -5
    echo ""
    echo "🎯 Növbəti: MT5-i restart edin və F7 ilə compile edin"
    exit 0
fi

echo "❌ MT5 tapılmadı. Manuel quraşdırma tələb olunur."
echo "💡 Bax: ZMQ_QURASDIRMA_TELIMAT.md"
exit 1
