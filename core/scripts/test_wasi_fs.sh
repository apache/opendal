#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Test runner for wasi-fs service
# Requires: wasmtime, wasm32-wasip2 target

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_DIR="$(dirname "$SCRIPT_DIR")"
TEST_DIR="${TEST_DIR:-/tmp/opendal-wasi-fs-test}"

echo "=== WASI-FS Behavior Test Runner ==="
echo "Test directory: $TEST_DIR"

# Create test directory
mkdir -p "$TEST_DIR"

# Build behavior tests for wasm32-wasip2
echo "Building behavior tests for wasm32-wasip2..."
cd "$CORE_DIR"
cargo build --tests --target wasm32-wasip2 --features tests,services-wasi-fs

# Find the test binary
TEST_BINARY=$(find target/wasm32-wasip2/debug/deps -name "behavior-*.wasm" | head -1)

if [ -z "$TEST_BINARY" ]; then
    echo "Error: Could not find behavior test binary"
    exit 1
fi

echo "Running tests with wasmtime..."
echo "Binary: $TEST_BINARY"

# Run with wasmtime, granting access to test directory
OPENDAL_TEST=wasi-fs \
OPENDAL_WASI_FS_ROOT=/ \
wasmtime run \
    --dir "$TEST_DIR::/" \
    --env "OPENDAL_TEST=wasi-fs" \
    --env "OPENDAL_WASI_FS_ROOT=/" \
    "$TEST_BINARY"

echo "=== Tests completed ==="
