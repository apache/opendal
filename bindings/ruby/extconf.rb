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

require "mkmf"
# We use rb_sys for makefile generation only.
# We can use `RB_SYS_CARGO_PROFILE` to choose Cargo profile
# Read more https://github.com/oxidize-rb/rb-sys/blob/main/gem/README.md
require "rb_sys/mkmf"

create_rust_makefile("opendal_ruby/opendal_ruby")
