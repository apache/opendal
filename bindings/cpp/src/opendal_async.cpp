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

Operator::ListFuture Operator::list(std::string_view path) {
  return opendal::ffi::async::operator_list(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(path));
}

Operator::ExistsFuture Operator::exists(std::string_view path) {
  return opendal::ffi::async::operator_exists(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(path));
}

Operator::CreateDirFuture Operator::create_dir(std::string_view path) {
  return opendal::ffi::async::operator_create_dir(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(path));
}

Operator::CopyFuture Operator::copy(std::string_view from, std::string_view to) {
  return opendal::ffi::async::operator_copy(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(from),
      RUST_STRING(to));
}

Operator::RenameFuture Operator::rename(std::string_view from, std::string_view to) {
  return opendal::ffi::async::operator_rename(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(from),
      RUST_STRING(to));
}

Operator::DeleteFuture Operator::delete_path(std::string_view path) {
  return opendal::ffi::async::operator_delete(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(path));
}

Operator::RemoveAllFuture Operator::remove_all(std::string_view path) {
  return opendal::ffi::async::operator_remove_all(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(path));
}

Operator::ReaderFuture Operator::reader(std::string_view path) {
  return opendal::ffi::async::operator_reader(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(path));
}

Operator::ListerFuture Operator::lister(std::string_view path) {
  return opendal::ffi::async::operator_lister(
      opendal::ffi::async::OperatorPtr{&*operator_}, RUST_STRING(path));
}

// Reader implementation
Reader::Reader(size_t reader_id) noexcept : reader_id_(reader_id) {}

Reader::Reader(Reader &&other) noexcept : reader_id_(other.reader_id_) {
  other.reader_id_ = 0;
}

Reader &Reader::operator=(Reader &&other) noexcept {
  if (this != &other) {
    reader_id_ = other.reader_id_;
    other.reader_id_ = 0;
  }
  return *this;
}

Reader::~Reader() noexcept { destroy(); }

void Reader::destroy() noexcept {
  if (reader_id_ != 0) {
    opendal::ffi::async::delete_reader(
        opendal::ffi::async::ReaderPtr{reader_id_});
    reader_id_ = 0;
  }
}

Reader::ReadFuture Reader::read(uint64_t start, uint64_t len) {
  return opendal::ffi::async::reader_read(
      opendal::ffi::async::ReaderPtr{reader_id_}, start, len);
}

// Lister implementation
Lister::Lister(size_t lister_id) noexcept : lister_id_(lister_id) {}

Lister::Lister(Lister &&other) noexcept : lister_id_(other.lister_id_) {
  other.lister_id_ = 0;
}

Lister &Lister::operator=(Lister &&other) noexcept {
  if (this != &other) {
    lister_id_ = other.lister_id_;
    other.lister_id_ = 0;
  }
  return *this;
}

Lister::~Lister() noexcept { destroy(); }

void Lister::destroy() noexcept {
  if (lister_id_ != 0) {
    opendal::ffi::async::delete_lister(
        opendal::ffi::async::ListerPtr{lister_id_});
    lister_id_ = 0;
  }
}

Lister::NextFuture Lister::next() {
  return opendal::ffi::async::lister_next(
      opendal::ffi::async::ListerPtr{lister_id_});
}
