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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/date_time/posix_time/posix_time.hpp"
#include "boost/iostreams/concepts.hpp"
#include "boost/iostreams/stream.hpp"
#include "lib.rs.h"

namespace opendal {

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

class Reader;
class Lister;

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
  Reader reader(std::string_view path);

  /**
   * @brief Check if the path exists
   *
   * @param path The path to check
   * @return true if the path exists, false otherwise
   */
  [[deprecated("Use exists() instead.")]]
  bool is_exist(std::string_view path);

  /**
   * @brief Check if the path exists
   *
   * @param path The path to check
   * @return true if the path exists, false otherwise
   */
  bool exists(std::string_view path);

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

  Lister lister(std::string_view path);

 private:
  std::optional<rust::Box<opendal::ffi::Operator>> operator_;
};

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
  Reader(rust::Box<opendal::ffi::Reader> &&reader)
      : raw_reader_(std::move(reader)) {}

  std::streamsize read(void *s, std::streamsize n);
  std::streampos seek(std::streamoff off, std::ios_base::seekdir way);

 private:
  rust::Box<opendal::ffi::Reader> raw_reader_;
};

// Boost IOStreams requires it to be copyable. So we need to use
// `reference_wrapper` in ReaderStream. More details can be seen at
// https://lists.boost.org/Archives/boost/2005/10/95939.php

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

/**
 * @class Lister
 * @brief Lister is designed to list the entries of a directory.
 * @details It provides next operation to get the next entry. You can also use
 * it like an iterator.
 * @code{.cpp}
 * auto lister = operator.lister("dir/");
 * for (const auto &entry : lister) {
 *   // Do something with entry
 * }
 * @endcode
 */
class Lister {
 public:
  Lister(rust::Box<opendal::ffi::Lister> &&lister)
      : raw_lister_(std::move(lister)) {}

  /**
   * @class ListerIterator
   * @brief ListerIterator is an iterator of Lister.
   * @note It's an undefined behavior to make multiple iterators from one
   * Lister.
   */
  class ListerIterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Entry;
    using difference_type = std::ptrdiff_t;
    using pointer = Entry *;
    using reference = Entry &;

    ListerIterator(Lister &lister) : lister_(lister) {
      current_entry_ = lister_.next();
    }

    Entry operator*() { return current_entry_.value(); }

    ListerIterator &operator++() {
      if (current_entry_) {
        current_entry_ = lister_.next();
      }
      return *this;
    }

    bool operator!=(const ListerIterator &other) const {
      return current_entry_ != std::nullopt ||
             other.current_entry_ != std::nullopt;
    }

   protected:
    // Only used for end iterator
    ListerIterator(Lister &lister, bool /*end*/) : lister_(lister) {}

   private:
    Lister &lister_;
    std::optional<Entry> current_entry_;

    friend class Lister;
  };

  /**
   * @brief Get the next entry of the lister
   *
   * @return The next entry of the lister
   */
  std::optional<Entry> next();

  ListerIterator begin() { return ListerIterator(*this); }
  ListerIterator end() { return ListerIterator(*this, true); }

 private:
  rust::Box<opendal::ffi::Lister> raw_lister_;
};
}  // namespace opendal
