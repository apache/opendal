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

RPATH=$(PWD)/target/debug
OBJ_DIR=./build
DOC_DIR=./docs

CCFLAGS=-I./include
CXXFLAGS=-I./include -std=c++14
LDFLAGS=-L$(RPATH) -Wl,-rpath,$(RPATH)

LIBS=-lopendal_c -lgtest -lpthread

VALGRIND=valgrind --error-exitcode=1 --leak-check=full --

.PHONY: all
all: build test examples

.PHONY: format
format:
	cargo fmt
	find . -name '*.cpp' -exec clang-format -i --style=WebKit --verbose {} \;
	find . -name '*.c' -exec clang-format -i --style=WebKit --verbose {} \;

.PHONY: build
build:
	mkdir -p $(OBJ_DIR)
	cargo build --target-dir=$(RPATH)

.PHONY: test
test:
	$(CXX) tests/bdd.cpp -o $(OBJ_DIR)/bdd $(CXXFLAGS) $(LDFLAGS) $(LIBS)
	$(CXX) tests/list.cpp -o $(OBJ_DIR)/list $(CXXFLAGS) $(LDFLAGS) $(LIBS)
	$(CXX) tests/error_msg.cpp -o $(OBJ_DIR)/error_msg $(CXXFLAGS) $(LDFLAGS) $(LIBS)
	$(CXX) tests/opinfo.cpp -o $(OBJ_DIR)/opinfo $(CXXFLAGS) $(LDFLAGS) $(LIBS)
	$(OBJ_DIR)/bdd
	$(OBJ_DIR)/list
	$(OBJ_DIR)/error_msg
	$(OBJ_DIR)/opinfo

.PHONY: test_memory_leak
memory_leak:
	$(VALGRIND) $(OBJ_DIR)/bdd
	$(VALGRIND) $(OBJ_DIR)/list
	$(VALGRIND) $(OBJ_DIR)/error_msg

.PHONY: doc
doc:
	mkdir -p $(DOC_DIR)
	curl --proto '=https' --tlsv1.2 -sSf https://cdn.jsdelivr.net/gh/jothepro/doxygen-awesome-css@2.2.1/doxygen-awesome.min.css \
		-o $(DOC_DIR)/doxygen-awesome.css
	doxygen Doxyfile

# build examples begin
EXAMPLES=$(wildcard ./examples/*.c)
EXAMPLE_OBJECTS=$(EXAMPLES:.c=.o)
EXAMPLE_TARGETS=$(EXAMPLES:.c=)
.PHONY: examples
examples: $(EXAMPLE_TARGETS)

$(EXAMPLE_TARGETS): % : %.o
	$(CC) $(CCFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

%.o: %.c
	$(CC) $(CCFLAGS) -c $< -o $@
# build examples end

.PHONY: clean
clean:
	cargo clean
	rm -rf $(EXAMPLE_OBJECTS)
	rm -rf $(EXAMPLE_TARGETS)
	rm -rf $(OBJ_DIR)
	rm -rf $(DOC_DIR)

