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

C_BINDING_SRC_DIR=../c
SWIFT_PKG_DIR=./OpenDAL
ROOT_DIR=../..

INCLUDE_DIR=$(SWIFT_PKG_DIR)/Sources/COpenDAL/include
LIB_DIR=$(SWIFT_PKG_DIR)/Sources/COpenDAL/lib
DIRS=$(INCLUDE_DIR) $(LIB_DIR)

.PHONY: all
all: build-swift

.PHONY: build-c
build-c: $(DIRS)
	cargo build --manifest-path $(C_BINDING_SRC_DIR)/Cargo.toml
	cp $(C_BINDING_SRC_DIR)/include/* $(INCLUDE_DIR)
	cp $(C_BINDING_SRC_DIR)/target/debug/libopendal_c.a $(LIB_DIR)

.PHONY: build-swift
build-swift: build-c
	cd $(SWIFT_PKG_DIR) && swift build

.PHONY: test
test: build-c
	cd $(SWIFT_PKG_DIR) && swift test

$(DIRS):
	mkdir -p $@

.PHONY: clean
clean:
	cargo clean
	rm -rf $(INCLUDE_DIR)
	rm -rf $(LIB_DIR)
