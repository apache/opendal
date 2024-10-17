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

#include "opendal_async.hpp"

#include <iterator>

#include "async.rs.h"
#include "async_defs.hpp"

#define RUST_STR(s) rust::Str(s.data(), s.size())
#define RUST_STRING(s) rust::String(s.data(), s.size())

using namespace opendal::async;

static rust::Box<opendal::ffi::async::Operator> new_operator(
    std::string_view scheme,
    const std::unordered_map<std::string, std::string> &config) {
  auto rust_map = rust::Vec<opendal::ffi::async::HashMapValue>();
  rust_map.reserve(config.size());
  for (auto &[k, v] : config) {
    rust_map.push_back({RUST_STRING(k), RUST_STRING(v)});
  }

  return opendal::ffi::async::new_operator(RUST_STR(scheme), rust_map);
}

Operator::Operator(std::string_view scheme,
                   const std::unordered_map<std::string, std::string> &config)
    : operator_(new_operator(scheme, config)) {}

Operator::ReadFuture Operator::read(std::string_view path) {
  return opendal::ffi::async::operator_read(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(path));
}

Operator::WriteFuture Operator::write(std::string_view path,
                                      std::span<uint8_t> data) {
  rust::Vec<uint8_t> vec;
  std::copy(data.begin(), data.end(), std::back_inserter(vec));

  return opendal::ffi::async::operator_write(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(path), vec);
}
