#!/usr/bin/env bash

# Modern protobuf code generation for standalone RawKV
# This script generates:
#   proto/pkg/<basename>/<basename>.pb.go
#   proto/pkg/<basename>/<basename>_grpc.pb.go

set -e

ROOT_DIR=$(pwd)
PROTO_DIR="$ROOT_DIR/proto"
OUT_DIR="$ROOT_DIR/pkg"

# Your module path
GO_PREFIX_PATH="github.com/llllleeeewwwiis/standalone/proto/pkg"

echo "Generating protobuf code..."
echo "PROTO_DIR = $PROTO_DIR"
echo "OUT_DIR   = $OUT_DIR"
echo "GO_PREFIX = $GO_PREFIX_PATH"

# Create output directory
mkdir -p "$OUT_DIR"

for file in "$PROTO_DIR"/*.proto; do
    base=$(basename "$file" .proto)
    target_dir="$OUT_DIR/$base"

    echo "Processing $file → $target_dir"

    mkdir -p "$target_dir"

    # Generate *.pb.go
    protoc \
        -I "$PROTO_DIR" \
        -I "$ROOT_DIR/proto/include" \
        --go_out=paths=source_relative:"$target_dir" \
        --go-grpc_out=paths=source_relative:"$target_dir" \
        "$file"

done

echo "Done. Generated files under proto/pkg/"
