/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "opendal.hpp"
#include "lib.rs.h"

using namespace opendal;

Operator::Operator(std::string_view scheme,
                   const std::unordered_map<std::string, std::string> &config) {
  auto rust_map = rust::Vec<ffi::HashMapValue>();
  rust_map.reserve(config.size());
  for (const auto &[k, v] : config) {
    rust_map.push_back(ffi::HashMapValue{
        rust::String(k.data()),
        rust::String(v.data()),
    });
  }

  operator_ = opendal::ffi::new_operator(rust::Str(scheme.data()), rust_map);
}

bool Operator::available() const { return operator_.has_value(); }

std::vector<uint8_t> Operator::read(std::string_view path) {
  auto res = operator_.value()->read(rust::Str(path.data()));
  return std::vector<uint8_t>(res.data(), res.data() + res.size());
}

void Operator::write(std::string_view path, const std::vector<uint8_t> &data) {
  operator_.value()->write(
      rust::Str(path.data()),
      rust::Slice<const uint8_t>(data.data(), data.size()));
}