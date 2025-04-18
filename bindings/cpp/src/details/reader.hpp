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

#include "lib.rs.h"

namespace opendal::details {
/**
 * @class Reader
 * @brief Reader is designed to read data from the operator.
 * @details It provides basic read and seek operations. If you want to use it
 * like a stream, you can use `ReaderStream` instead.
 * @code{.cpp}
 * opendal::ReaderStream stream(operator.reader("path"));
 * @endcode
 */
class Reader {
 public:
  Reader(rust::Box<opendal::ffi::Reader> &&reader)
      : reader_(std::move(reader)) {}

  Reader(Reader &&) = default;

  ~Reader() = default;

  std::size_t read(void *s, std::size_t n);

  std::uint64_t seek(std::uint64_t off, std::ios_base::seekdir dir);

 private:
  rust::Box<opendal::ffi::Reader> reader_;
};

}  // namespace opendal::details
