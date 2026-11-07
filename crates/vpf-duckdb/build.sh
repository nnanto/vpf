#!/usr/bin/env bash
# Builds build/vpf.duckdb_extension for the DuckDB version in DUCKDB_VERSION.
#
#   DUCKDB_VERSION  DuckDB release to build against (default v2.0.0); must match the
#                   DuckDB that loads the extension.
#   DUCKDB_SRC      DuckDB source checkout (default: shallow clone into build/).
#   DUCKDB_PLATFORM Platform stamped into the extension (default: linux_amd64).
set -euo pipefail
cd "$(dirname "$0")"

DUCKDB_VERSION=${DUCKDB_VERSION:-v2.0.0}
DUCKDB_PLATFORM=${DUCKDB_PLATFORM:-linux_amd64}
DUCKDB_SRC=${DUCKDB_SRC:-build/duckdb-$DUCKDB_VERSION}
CXX=${CXX:-g++}
ROOT=$(cd ../.. && pwd)
TARGET_DIR=${CARGO_TARGET_DIR:-$ROOT/target}

mkdir -p build
if [ ! -d "$DUCKDB_SRC/src/include" ]; then
  git clone --quiet --depth 1 --branch "$DUCKDB_VERSION" https://github.com/duckdb/duckdb "$DUCKDB_SRC"
fi

cargo build --quiet --release -p vpf-ffi --manifest-path "$ROOT/Cargo.toml"

$CXX -std=c++17 -O2 -fPIC -fvisibility=hidden -c src/vpf_extension.cpp -o build/vpf_extension.o \
  -I"$DUCKDB_SRC/src/include" -I"$ROOT/crates/vpf-ffi/include"

# DuckDB symbols resolve against the loading process; Rust and SQLite stay private.
$CXX -shared -o build/vpf.so build/vpf_extension.o "$TARGET_DIR/release/libvpf_ffi.a" \
  -s -Wl,--exclude-libs,ALL -lpthread -ldl -lm

python3 append_metadata.py build/vpf.so build/vpf.duckdb_extension "$DUCKDB_PLATFORM" "$DUCKDB_VERSION"
echo "built $(pwd)/build/vpf.duckdb_extension"
