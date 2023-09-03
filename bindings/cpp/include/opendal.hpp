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

#include <boost/date_time/posix_time/posix_time.hpp>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace opendal {

constexpr int BUFFER_SIZE = 1024 * 1024;

/**
 * @enum EntryMode
 * @brief The mode of the entry
 */
enum EntryMode {
  FILE = 1,
  DIR = 2,
  UNKNOWN = 0,
};

/**
 * @struct Metadata
 * @brief The metadata of a file or directory
 */
struct Metadata {
  EntryMode type;
  std::uint64_t content_length;
  std::optional<std::string> cache_control;
  std::optional<std::string> content_disposition;
  std::optional<std::string> content_md5;
  std::optional<std::string> content_type;
  std::optional<std::string> etag;
  std::optional<boost::posix_time::ptime> last_modified;

  Metadata(ffi::Metadata &&);
};

/**
 * @struct Entry
 * @brief The entry of a file or directory
 */
struct Entry {
  std::string path;

  Entry(ffi::Entry &&);
};

class ReaderStream;

/**
 * @class Operator
 * @brief Operator is the entry for all public APIs.
 */
class Operator {
public:
  Operator() = default;

  /**
   * @brief Construct a new Operator object
   *
   * @param scheme The scheme of the operator, same as the name of rust doc
   * @param config The configuration of the operator, same as the service doc
   */
  Operator(std::string_view scheme,
           const std::unordered_map<std::string, std::string> &config = {});

  // Disable copy and assign
  Operator(const Operator &) = delete;
  Operator &operator=(const Operator &) = delete;

  // Enable move
  Operator(Operator &&) = default;
  Operator &operator=(Operator &&) = default;
  ~Operator() = default;

  /**
   * @brief Check if the operator is available
   *
   * @return true if the operator is available, false otherwise
   */
  bool available() const;

  /**
   * @brief Read data from the operator
   * @note The operation will make unnecessary copy. So we recommend to use the
   * `reader` method.
   *
   * @param path The path of the data
   * @return The data read from the operator
   */
  std::vector<uint8_t> read(std::string_view path);

  /**
   * @brief Write data to the operator
   *
   * @param path The path of the data
   * @param data The data to write
   */
  void write(std::string_view path, const std::vector<uint8_t> &data);

  /**
   * @brief Read data from the operator
   *
   * @param path The path of the data
   * @return The reader of the data
   */
  ReaderStream reader(std::string_view path);

  /**
   * @brief Check if the path exists
   *
   * @param path The path to check
   * @return true if the path exists, false otherwise
   */
  bool is_exist(std::string_view path);

  /**
   * @brief Create a directory
   *
   * @param path The path of the directory
   */
  void create_dir(std::string_view path);

  /**
   * @brief Copy a file from src to dst.
   *
   * @param src The source path
   * @param dst The destination path
   */
  void copy(std::string_view src, std::string_view dst);

  /**
   * @brief Rename a file from src to dst.
   *
   * @param src The source path
   * @param dst The destination path
   */
  void rename(std::string_view src, std::string_view dst);

  /**
   * @brief Remove a file or directory
   *
   * @param path The path of the file or directory
   */
  void remove(std::string_view path);

  /**
   * @brief Get the metadata of a file or directory
   *
   * @param path The path of the file or directory
   * @return The metadata of the file or directory
   */
  Metadata stat(std::string_view path);

  /**
   * @brief List the entries of a directory
   * @note The returned entries are sorted by name.
   *
   * @param path The path of the directory
   * @return The entries of the directory
   */
  std::vector<Entry> list(std::string_view path);

private:
  std::optional<rust::Box<opendal::ffi::Operator>> operator_;
};

using Reader = rust::Box<opendal::ffi::Reader>;

class ReaderStreamBuf : public std::streambuf {
public:
  ReaderStreamBuf(Reader &&reader) : reader_(std::move(reader)) {}

protected:
  int_type underflow() override;
  pos_type seekoff(off_type off, std::ios_base::seekdir dir,
                   std::ios_base::openmode which) override;
  pos_type seekpos(pos_type pos, std::ios_base::openmode which) override;

private:
  Reader reader_;
};

class ReaderStream : public std::istream {
public:
  ReaderStream(Reader &&reader)
      : std::istream(&buf_), buf_(std::move(reader)) {}

private:
  ReaderStreamBuf buf_;
};

} // namespace opendal