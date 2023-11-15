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

#include <ios>
#include <utility>

#include "boost/iostreams/stream.hpp"
#include "lib.rs.h"

namespace opendal {

namespace ffi {
struct Reader;
}

/**
 * @class Reader
 * @brief Reader is designed to read data from the operator.
 * @details It provides basic read and seek operations. If you want to use it
 * like a stream, you can use `ReaderStream` instead.
 * @code{.cpp}
 * opendal::ReaderStream stream(operator.reader("path"));
 * @endcode
 */
class Reader
    : public boost::iostreams::device<boost::iostreams::input_seekable> {
 public:
  Reader(rust::Box<opendal::ffi::Reader> &&reader);

  std::streamsize read(void *s, std::streamsize n);
  std::streampos seek(std::streamoff off, std::ios_base::seekdir way);

 private:
  rust::Box<opendal::ffi::Reader> raw_reader_;
};

/**
 * @class ReaderStream
 * @brief ReaderStream is a stream wrapper of Reader which can provide
 * `iostream` interface. It will keep a Reader inside so that you can ignore the
 * lifetime of original Reader.
 */
class ReaderStream
    : public boost::iostreams::stream<boost::reference_wrapper<Reader>> {
 public:
  ReaderStream(Reader &&reader)
      : boost::iostreams::stream<boost::reference_wrapper<Reader>>(
            boost::ref(reader_)),
        reader_(std::move(reader)) {}

 private:
  Reader reader_;
};

}  // namespace opendal
