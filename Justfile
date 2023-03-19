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

# Load dotenv from .env file
set dotenv-load := true

export ROCKSDB_LIB_DIR := "/usr/lib"

[private]
default:
    @just --list --unsorted

# Check the whole project
check:
    cargo fmt --all -- --check
    cargo doc --lib --no-deps --all-features -p opendal
    cargo clippy --all-targets --all-features --workspace -- -D warnings

# Build all components
build_all:
    just build_core
    just build_bindings_python
    just build_bindings_nodejs

# Test all components
test_all:
    just test_core
    just test_bindings_python
    just test_bindings_nodejs

# Build the core library
build_core:
    cargo build -p opendal

# Test the core library
test_core:
    cargo nextest run --no-fail-fast && \
        cargo test --doc

[private]
doc_core:
    cargo doc --lib --no-deps --all-features -p opendal
    cp -r ./target/doc ./website/static/docs/rust

[private]
prepare_bindings_python:
    cd bindings/python && \
        PYTHON_CONFIGURE_OPTS="--enable-shared" python -m venv venv

# Build the bindings python library
build_bindings_python: prepare_bindings_python
    cd bindings/python && source venv/bin/activate && maturin develop

# Test the bindings python library
test_bindings_python: prepare_bindings_python
    cd bindings/python && \
        source venv/bin/activate && \
        maturin develop -E test && \
        behave tests

[private]
doc_bindings_python: prepare_bindings_python
    cd bindings/python && \
        source venv/bin/activate && \
        python -m pip install pdoc && \
        pdoc --output-dir ./docs opendal
    cp -r ./bindings/python/docs ./website/static/docs/python

[private]
prepare_bindings_nodejs:
    cd bindings/nodejs && yarn install

# Build the bindings nodejs library
build_bindings_nodejs: prepare_bindings_nodejs
    cd bindings/nodejs && yarn build:debug

# Test the bindings nodejs library
test_bindings_nodejs: prepare_bindings_nodejs
    cd bindings/nodejs && yarn test

[private]
doc_bindings_nodejs: prepare_bindings_nodejs
    cd bindings/nodejs && yarn docs
    cp -r ./bindings/nodejs/docs ./website/static/docs/nodejs

# Build website
build_website: doc_core doc_bindings_python doc_bindings_nodejs
    cd website && yarn install && yarn build
    cp .asf.yaml ./website/build/.asf.yaml

# Preview website
preview_website: doc_core doc_bindings_python doc_bindings_nodejs
    cd website && yarn install && yarn start
