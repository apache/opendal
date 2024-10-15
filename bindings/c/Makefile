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

DOC_DIR=./docs

.PHONY: format
format:
	cargo fmt
	find . -name '*.cpp' -exec clang-format -i --style=WebKit --verbose {} \;
	find . -name '*.c' -exec clang-format -i --style=WebKit --verbose {} \;

.PHONY: doc
doc:
	mkdir -p $(DOC_DIR)
	curl --proto '=https' --tlsv1.2 -sSf https://cdn.jsdelivr.net/gh/jothepro/doxygen-awesome-css@2.2.1/doxygen-awesome.min.css \
		-o $(DOC_DIR)/doxygen-awesome.css
	doxygen Doxyfile
