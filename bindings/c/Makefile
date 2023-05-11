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

RPATH=$(PWD)/../../target/debug
CXXFLAGS=-I./include -std=c++14
LDFLAGS=-L$(RPATH) -Wl,-rpath,$(RPATH)
LIBS=-lopendal_c -lgtest
OBJ_DIR=./build

.PHONY: all
all: build test

.PHONY: format
format:
	find . -name '*.cpp' -exec clang-format -i --style=WebKit --verbose {} \;

.PHONY: build
build:
	mkdir -p $(OBJ_DIR)
	cargo build

.PHONY: test
test:
	$(CXX) tests/bdd.cpp -o $(OBJ_DIR)/bdd $(CXXFLAGS) $(LDFLAGS) $(LIBS)
	$(OBJ_DIR)/bdd

.PHONY: clean
clean:
	cargo clean
	rm -rf $(OBJ_DIR)
