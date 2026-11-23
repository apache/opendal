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

#include <cstddef>
#include <limits>
#include <stdexcept>

#include "lib.rs.h"
#include "opendal.hpp"

namespace opendal {

void Reader::Destroy() noexcept {
  if (reader_) {
    ffi::delete_reader(reader_);
    reader_ = nullptr;
  }
}

Reader::Reader(ffi::Reader *reader) noexcept : reader_{reader} {}

Reader::Reader(Reader &&other) noexcept : reader_{other.reader_} {
  other.reader_ = nullptr;
}

Reader::~Reader() noexcept { Destroy(); }

std::streamsize Reader::Read(void *s, std::streamsize n) {
  if (n < 0) {
    throw std::invalid_argument("read size must be non-negative");
  }
  if (n == 0) {
    return 0;
  }
  if (s == nullptr) {
    throw std::invalid_argument("read buffer must not be null");
  }
  if constexpr (std::numeric_limits<std::streamsize>::max() >
                std::numeric_limits<std::ptrdiff_t>::max()) {
    if (n > std::numeric_limits<std::ptrdiff_t>::max()) {
      throw std::invalid_argument("read size exceeds maximum buffer length");
    }
  }
  return static_cast<std::streamsize>(reader_->read(
      rust::Slice<uint8_t>(static_cast<uint8_t *>(s),
                           static_cast<std::size_t>(n))));
}

std::streamsize Reader::ReadAt(void *s, std::streamsize n, uint64_t offset) {
  if (n < 0) {
    throw std::invalid_argument("read size must be non-negative");
  }
  if (n == 0) {
    return 0;
  }
  if (s == nullptr) {
    throw std::invalid_argument("read buffer must not be null");
  }
  if constexpr (std::numeric_limits<std::streamsize>::max() >
                std::numeric_limits<std::ptrdiff_t>::max()) {
    if (n > std::numeric_limits<std::ptrdiff_t>::max()) {
      throw std::invalid_argument("read size exceeds maximum buffer length");
    }
  }
  return static_cast<std::streamsize>(reader_->read_at(
      rust::Slice<uint8_t>(static_cast<uint8_t *>(s),
                           static_cast<std::size_t>(n)),
      offset));
}

ffi::SeekDir rust_seek_dir(std::ios_base::seekdir dir) {
  switch (dir) {
    case std::ios_base::beg:
      return ffi::SeekDir::Start;

    case std::ios_base::cur:
      return ffi::SeekDir::Current;

    case std::ios_base::end:
      return ffi::SeekDir::End;

    default:
      throw std::runtime_error("invalid seekdir");
  }
}

std::streampos Reader::Seek(std::streamoff off, std::ios_base::seekdir dir) {
  return reader_->seek(off, rust_seek_dir(dir));
}

}  // namespace opendal
