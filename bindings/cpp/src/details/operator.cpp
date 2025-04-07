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
#include "details/operator.hpp"

#include "details/common.hpp"
#include "lib.rs.h"

namespace opendal::details {

rust::Box<opendal::ffi::Operator> Operator::create_by_config(
    std::string_view scheme,
    const std::unordered_map<std::string, std::string> &config) {
  auto rust_map = rust::Vec<ffi::HashMapValue>();
  rust_map.reserve(config.size());
  for (auto &[k, v] : config) {
    rust_map.push_back({rust_string(k), rust_string(v)});
  }

  return opendal::ffi::new_operator(rust_str(scheme), rust_map);
}

std::string Operator::read(std::string_view path) {
  auto rust_vec = operator_->read(rust_str(path));
  return {rust_vec.data(), rust_vec.data() + rust_vec.size()};
}

void Operator::write(std::string_view path, std::string_view data) {
  operator_->write(rust_str(path), rust_slice<const uint8_t>(data));
}

rust::Box<ffi::Reader> Operator::reader(std::string_view path) {
  return operator_->reader(rust_str(path));
}

bool Operator::exists(std::string_view path) {
  return operator_->exists(rust_str(path));
}

void Operator::create_dir(std::string_view path) {
  operator_->create_dir(rust_str(path));
}

void Operator::copy(std::string_view src, std::string_view dst) {
  operator_->copy(rust_str(src), rust_str(dst));
}

void Operator::rename(std::string_view src, std::string_view dst) {
  operator_->rename(rust_str(src), rust_str(dst));
}

void Operator::remove(std::string_view path) {
  operator_->remove(rust_str(path));
}

ffi::Metadata Operator::stat(std::string_view path) {
  return operator_->stat(rust_str(path));
}

rust::Vec<ffi::Entry> Operator::list(std::string_view path) {
  return operator_->list(rust_str(path));
}

rust::Box<ffi::Lister> Operator::lister(std::string_view path) {
  return operator_->lister(rust_str(path));
}

}  // namespace opendal::details
