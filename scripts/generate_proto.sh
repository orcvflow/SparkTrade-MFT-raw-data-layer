#!/bin/bash
# Protobuf code generation script
# Generates Go code from proto/canonical.proto

set -e

PROTO_DIR="proto"
OUT_DIR="proto/gen"

echo "🔧 Generating Protobuf Go code..."

# Create output directory
mkdir -p $OUT_DIR

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo "❌ Error: protoc not found"
    echo "Install with: sudo apt install protobuf-compiler"
    exit 1
fi

# Generate Go code
protoc --go_out=$OUT_DIR \
       --go_opt=paths=source_relative \
       $PROTO_DIR/canonical.proto

if [ $? -eq 0 ]; then
    echo "✅ Protobuf generated in $OUT_DIR"
    ls -lh $OUT_DIR
else
    echo "❌ Protobuf generation failed"
    exit 1
fi
