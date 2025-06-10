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

.PHONY: tests clean help check-env generate-services run-tests

# Detect current OS and architecture
OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ARCH := $(shell uname -m)

# Normalize architecture names
ifeq ($(ARCH),x86_64)
    ARCH := amd64
endif
ifeq ($(ARCH),aarch64)
    ARCH := arm64
endif

# Define file extensions for different OS
ifeq ($(OS),linux)
    LIB_EXT := so
endif
ifeq ($(OS),darwin)
    LIB_EXT := dylib
endif
ifeq ($(findstring mingw,$(OS)),mingw)
    OS := windows
    LIB_EXT := dll
endif
ifeq ($(findstring msys,$(OS)),msys)
    OS := windows
    LIB_EXT := dll
endif

# Check if OPENDAL_TEST environment variable is set
check-env:
	@if [ -z "$(OPENDAL_TEST)" ]; then \
		echo "Error: OPENDAL_TEST environment variable is not set"; \
		echo "Please set OPENDAL_TEST to the service you want to test (e.g., fs, s3, etc.)"; \
		exit 1; \
	fi

# Other variables
SERVICE := $(OPENDAL_TEST)
GITHUB_WORKSPACE := $(PWD)/opendal-go-services
VERSION := latest
MATRIX := '{"build": [{"target":"$(OS)", "goos":"$(OS)", "goarch": "$(ARCH)"}], "service": ["$(SERVICE)"]}'

# Define library file paths
C_LIB_FILE := opendal/bindings/c/target/debug/libopendal_c.$(LIB_EXT)
SERVICE_PKG := $(shell echo $(SERVICE) | tr '-' '_')
FINAL_LIB_FILE := $(GITHUB_WORKSPACE)/$(SERVICE_PKG)/libopendal_c.$(OS).$(ARCH).$(LIB_EXT).zst

# Build C library only when source files change
$(C_LIB_FILE): opendal/bindings/c/src/*.rs opendal/bindings/c/Cargo.toml
	@echo "Building C library for service: $(SERVICE)"
	cd opendal/bindings/c && cargo build --features "opendal/services-$(SERVICE)"

# Create compressed library and generate services in one step
# This target depends on the final library file location, not intermediate
$(FINAL_LIB_FILE): $(C_LIB_FILE)
	@echo "Compressing library and generating services for $(SERVICE) on $(OS)..."
	@mkdir -p $(GITHUB_WORKSPACE)/libopendal_c_$(VERSION)_$(SERVICE)_$(OS)
	zstd -19 $< -o $(GITHUB_WORKSPACE)/libopendal_c_$(VERSION)_$(SERVICE)_$(OS)/libopendal_c.$(OS).$(LIB_EXT).zst
	@echo "Generating Go services..."
	cd opendal-go-services/internal/generate && \
	GITHUB_WORKSPACE="$(GITHUB_WORKSPACE)" \
	VERSION="$(VERSION)" \
	MATRIX=$(MATRIX) \
	go run generate.go
	@echo "Cleaning up intermediate directory..."
	@rm -rf $(GITHUB_WORKSPACE)/libopendal_c_$(VERSION)_$(SERVICE)_$(OS)

# Generate services target now just depends on the final file
generate-services: $(FINAL_LIB_FILE)

# Run tests
run-tests: generate-services
	@echo "Running behavior tests..."
	go test ./opendal/bindings/go/tests/behavior_tests -v -run TestBehavior

# Main target: run all tests
tests: check-env run-tests
	@echo "All tests completed successfully!"

# Clean all generated files
clean:
	@echo "Cleaning all generated files..."
	rm -rf opendal/bindings/c/target
	rm -rf opendal-go-services/*/libopendal_c.*
	rm -rf opendal-go-services/libopendal_c_*

# Help target
help:
	@echo "Available targets:"
	@echo "  tests        - Run all tests (requires OPENDAL_TEST environment variable)"
	@echo "  clean        - Clean all generated files"
	@echo "  help         - Show this help message"
	@echo ""
	@echo "Environment variables:"
	@echo "  OPENDAL_TEST - Required. The service to test (e.g., fs, s3, gcs, etc.)"
	@echo ""
	@echo "Example usage:"
	@echo "  OPENDAL_TEST=fs make tests"

