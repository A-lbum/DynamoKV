#!/usr/bin/env bash

set -e

ROOT_DIR=$(pwd)
PROTO_DIR="$ROOT_DIR/proto"
OUT_DIR="$ROOT_DIR/proto/pkg"

# module path for distributed_core
GO_PREFIX_PATH="github.com/llllleeeewwwiis/distributed_core/proto/pkg"

echo "Generating distributed_core internal RPC protobuf..."
echo "PROTO_DIR = $PROTO_DIR"
echo "OUT_DIR   = $OUT_DIR"
echo "GO_PREFIX = $GO_PREFIX_PATH"

mkdir -p "$OUT_DIR"

for file in "$PROTO_DIR"/*.proto; do
    base=$(basename "$file" .proto)
    target_dir="$OUT_DIR/$base"

    echo "Processing $file → $target_dir"
    mkdir -p "$target_dir"

    protoc \
        -I "$PROTO_DIR" \
        -I "$ROOT_DIR/proto/include" \
        --go_out=paths=source_relative:"$target_dir" \
        --go-grpc_out=paths=source_relative:"$target_dir" \
        "$file"
done

echo "Done. Files generated under proto/pkg/"
