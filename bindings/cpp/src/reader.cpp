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

#include "reader.hpp"

#include "lib.rs.h"

namespace opendal {

namespace {
ffi::SeekDir to_rust_seek_dir(std::ios_base::seekdir dir) {
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
}  // namespace

Reader::Reader(rust::Box<opendal::ffi::Reader> &&reader)
    : raw_reader_(std::move(reader)) {}

std::streamsize Reader::read(void *s, std::streamsize n) {
  auto rust_slice = rust::Slice<uint8_t>(reinterpret_cast<uint8_t *>(s), n);
  auto read_size = raw_reader_->read(rust_slice);
  return read_size;
}

std::streampos Reader::seek(std::streamoff off, std::ios_base::seekdir dir) {
  return raw_reader_->seek(off, to_rust_seek_dir(dir));
}

}  // namespace opendal
