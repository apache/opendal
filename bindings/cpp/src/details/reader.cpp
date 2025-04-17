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

#include "details/reader.hpp"

namespace opendal::details {

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

std::size_t Reader::read(void *s, std::size_t n) {
  return reader_->read(rust::Slice<uint8_t>(static_cast<uint8_t *>(s), n));
}

std::uint64_t Reader::seek(std::uint64_t off, std::ios_base::seekdir dir) {
  return reader_->seek(off, rust_seek_dir(dir));
}

}  // namespace opendal::details
