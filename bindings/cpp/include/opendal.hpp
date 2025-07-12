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

#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "data_structure.hpp"

namespace opendal {

namespace ffi {
class Operator;
class Reader;
class Lister;
}  // namespace ffi

class Reader;
class Lister;

/**
 * @class Operator
 * @brief Operator is the entry for all public APIs.
 */
class Operator {
 public:
  Operator() noexcept;

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
  Operator(Operator &&other) noexcept;
  Operator &operator=(Operator &&other) noexcept;

  ~Operator() noexcept;

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
  std::string read(std::string_view path);

  /**
   * @brief Write data to the operator
   *
   * @param path The path of the data
   * @param data The data to write
   */
  void write(std::string_view path, std::string_view data);

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
  void destroy() noexcept;

  ffi::Operator *operator_{nullptr};
};

/**
 * @class Reader
 * @brief Reader is designed to read data from the operator.
 * @details It provides basic read and seek operations with a stream-like
 * interface.
 * @code{.cpp}
 * opendal::ReaderStream stream(operator.reader("path"));
 * @endcode
 */
class Reader {
 public:
  Reader(Reader &&other) noexcept;

  ~Reader() noexcept;

  std::streamsize read(void *s, std::streamsize n);

  std::streampos seek(std::streamoff off, std::ios_base::seekdir way);

 private:
  friend class Operator;

  Reader(ffi::Reader *pointer) noexcept;

  void destroy() noexcept;

  ffi::Reader *reader_{nullptr};
};

/**
 * @class ReaderStream
 * @brief ReaderStream is a stream wrapper of Reader which provides
 * `iostream` interface. It wraps the Reader to provide standard stream
 * operations.
 */
class ReaderStream : public std::istream {
 public:
  class ReaderStreamBuf : public std::streambuf {
   public:
    ReaderStreamBuf(Reader &&reader)
        : reader_(std::move(reader)), buffer_start_pos_(0) {
      setg(buffer_, buffer_, buffer_);
    }

   protected:
    std::streamsize xsgetn(char *s, std::streamsize count) override {
      std::streamsize total_read = 0;

      while (total_read < count) {
        if (gptr() < egptr()) {
          std::streamsize available = egptr() - gptr();
          std::streamsize to_copy = std::min(available, count - total_read);
          std::memcpy(s + total_read, gptr(), to_copy);
          gbump(to_copy);
          total_read += to_copy;
          continue;
        }

        if (underflow() == traits_type::eof()) break;
      }

      return total_read;
    }

    int_type underflow() override {
      if (gptr() < egptr()) {
        return traits_type::to_int_type(*gptr());
      }

      // Update the buffer start position to current reader position
      buffer_start_pos_ += (egptr() - eback());

      std::streamsize n = reader_.read(buffer_, sizeof(buffer_));
      if (n <= 0) {
        return traits_type::eof();
      }

      setg(buffer_, buffer_, buffer_ + n);
      return traits_type::to_int_type(*gptr());
    }

    int_type uflow() override {
      int_type result = underflow();
      if (result != traits_type::eof()) {
        gbump(1);
      }
      return result;
    }

    std::streampos seekoff(std::streamoff off, std::ios_base::seekdir dir,
                           std::ios_base::openmode which) override {
      if (dir == std::ios_base::cur && off == 0) {
        // tellg() case - return current position
        return buffer_start_pos_ + (gptr() - eback());
      }

      // Actual seek operation
      std::streampos new_pos = reader_.seek(off, dir);
      if (new_pos != std::streampos(-1)) {
        buffer_start_pos_ = new_pos;
        setg(buffer_, buffer_, buffer_);
      }
      return new_pos;
    }

    std::streampos seekpos(std::streampos pos,
                           std::ios_base::openmode which) override {
      return seekoff(pos, std::ios_base::beg, which);
    }

   private:
    Reader reader_;
    char buffer_[8192];
    std::streampos buffer_start_pos_;
  };

  ReaderStream(Reader &&reader)
      : std::istream(&buf_), buf_(std::move(reader)) {}

 private:
  ReaderStreamBuf buf_;
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
  Lister(Lister &&other) noexcept;

  ~Lister() noexcept;

  /**
   * @class ListerIterator
   * @brief ListerIterator is an iterator of Lister.
   * @note It's an undefined behavior to make multiple iterators from one
   * Lister.
   */
  class Iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Entry;
    using difference_type = std::ptrdiff_t;
    using pointer = Entry *;
    using reference = Entry &;

    Iterator(Lister &lister) : lister_{lister} {
      current_entry_ = lister_.next();
    }

    Entry operator*() { return current_entry_.value(); }

    Iterator &operator++() {
      if (current_entry_) {
        current_entry_ = lister_.next();
      }
      return *this;
    }

    bool operator!=(const Iterator &other) const {
      return current_entry_ != std::nullopt ||
             other.current_entry_ != std::nullopt;
    }

   protected:
    // Only used for end iterator
    Iterator(Lister &lister, bool /*end*/) noexcept : lister_(lister) {}

   private:
    friend class Lister;

    Lister &lister_;
    std::optional<Entry> current_entry_;
  };

  /**
   * @brief Get the next entry of the lister
   *
   * @return The next entry of the lister
   */
  std::optional<Entry> next();

  Iterator begin() { return Iterator(*this); }
  Iterator end() { return Iterator(*this, true); }

 private:
  friend class Operator;

  Lister(ffi::Lister *pointer) noexcept;

  void destroy() noexcept;

  ffi::Lister *lister_{nullptr};
};

}  // namespace opendal
