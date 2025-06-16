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

#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include "async.rs.h"
#include "async_defs.hpp"

namespace opendal::async {

class Operator {
 public:
  Operator(std::string_view scheme,
           const std::unordered_map<std::string, std::string> &config = {});

  // Disable copy and assign
  Operator(const Operator &) = delete;
  Operator &operator=(const Operator &) = delete;

  // Enable move
  Operator(Operator &&other) noexcept;
  Operator &operator=(Operator &&other) noexcept;
  ~Operator() noexcept;

  using ReadFuture = opendal::ffi::async::RustFutureRead;
  ReadFuture read(std::string_view path);

  using WriteFuture = opendal::ffi::async::RustFutureWrite;
  WriteFuture write(std::string_view path, std::span<uint8_t> data);

  using ListFuture = opendal::ffi::async::RustFutureList;
  ListFuture list(std::string_view path);

  using ExistsFuture = opendal::ffi::async::RustFutureBool;
  ExistsFuture exists(std::string_view path);

  using CreateDirFuture = opendal::ffi::async::RustFutureWrite;
  CreateDirFuture create_dir(std::string_view path);

  using CopyFuture = opendal::ffi::async::RustFutureWrite;
  CopyFuture copy(std::string_view from, std::string_view to);

  using RenameFuture = opendal::ffi::async::RustFutureWrite;
  RenameFuture rename(std::string_view from, std::string_view to);

  using DeleteFuture = opendal::ffi::async::RustFutureWrite;
  DeleteFuture delete_path(std::string_view path);

  using RemoveAllFuture = opendal::ffi::async::RustFutureWrite;
  RemoveAllFuture remove_all(std::string_view path);

  using ReaderFuture = opendal::ffi::async::RustFutureReaderId;
  ReaderFuture reader(std::string_view path);

  using ListerFuture = opendal::ffi::async::RustFutureListerId;
  ListerFuture lister(std::string_view path);

 private:
  void destroy() noexcept;
  
  size_t operator_id_{0};
};

/**
 * @class Reader
 * @brief Async Reader is designed to read data from a specific path in an
 * asynchronous manner.
 * @details It provides streaming read operations with range support.
 */
class Reader {
 public:
  // Disable copy and assign
  Reader(const Reader &) = delete;
  Reader &operator=(const Reader &) = delete;

  // Enable move
  Reader(Reader &&other) noexcept;
  Reader &operator=(Reader &&other) noexcept;
  ~Reader() noexcept;

  // Constructor from ID (for tests and advanced usage)
  explicit Reader(size_t reader_id) noexcept;

  using ReadFuture = opendal::ffi::async::RustFutureRead;

  /**
   * @brief Read data from the specified range
   * @param start Start offset in bytes
   * @param len Number of bytes to read
   * @return Future that resolves to the read data
   */
  ReadFuture read(uint64_t start, uint64_t len);

 private:
  friend class Operator;

  void destroy() noexcept;

  size_t reader_id_{0};
};

/**
 * @class Lister
 * @brief Async Lister is designed to list entries at a specified path in an
 * asynchronous manner.
 * @details It provides streaming iteration over directory entries.
 */
class Lister {
 public:
  // Disable copy and assign
  Lister(const Lister &) = delete;
  Lister &operator=(const Lister &) = delete;

  // Enable move
  Lister(Lister &&other) noexcept;
  Lister &operator=(Lister &&other) noexcept;
  ~Lister() noexcept;

  // Constructor from ID (for tests and advanced usage)
  explicit Lister(size_t lister_id) noexcept;

  using NextFuture = opendal::ffi::async::RustFutureEntryOption;

  /**
   * @brief Get the next entry in the listing
   * @return Future that resolves to the next entry path, or empty string if no
   * more entries
   */
  NextFuture next();

 private:
  friend class Operator;

  void destroy() noexcept;

  size_t lister_id_{0};
};

}  // namespace opendal::async
