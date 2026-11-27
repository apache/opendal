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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

#include "capability.hpp"
#include "metadata.hpp"

namespace opendal {

/**
 * @struct Entry
 * @brief The entry of a file or directory
 */
class Entry {
 public:
  std::string path;
};

/**
 * @struct ReadRange
 * @brief Byte range used by read operations.
 */
struct ReadRange {
  enum class Type : std::uint8_t {
    FULL = 0,
    OFFSET = 1,
    RANGE = 2,
    SUFFIX = 3,
  };

  Type type{Type::FULL};
  std::uint64_t start{0};
  std::uint64_t end{0};

  static ReadRange Full() { return {}; }

  static ReadRange Offset(std::uint64_t offset) {
    return {Type::OFFSET, offset, 0};
  }

  static ReadRange Range(std::uint64_t start, std::uint64_t end) {
    return {Type::RANGE, start, end};
  }

  static ReadRange Suffix(std::uint64_t size) {
    return {Type::SUFFIX, 0, size};
  }
};

/**
 * @struct StatOptions
 * @brief Options for stat operations.
 */
struct StatOptions {
  std::optional<std::string> version;
  std::optional<std::string> if_match;
  std::optional<std::string> if_none_match;
  std::optional<std::chrono::system_clock::time_point> if_modified_since;
  std::optional<std::chrono::system_clock::time_point> if_unmodified_since;
  std::optional<std::string> override_content_type;
  std::optional<std::string> override_cache_control;
  std::optional<std::string> override_content_disposition;
};

/**
 * @struct ReadOptions
 * @brief Options for read operations.
 */
struct ReadOptions {
  ReadRange range;
  std::optional<std::string> version;
  std::optional<std::string> if_match;
  std::optional<std::string> if_none_match;
  std::optional<std::chrono::system_clock::time_point> if_modified_since;
  std::optional<std::chrono::system_clock::time_point> if_unmodified_since;
  std::optional<std::uint64_t> content_length_hint;
  std::size_t concurrent{0};
  std::optional<std::size_t> chunk;
  std::optional<std::size_t> gap;
  std::optional<std::string> override_content_type;
  std::optional<std::string> override_cache_control;
  std::optional<std::string> override_content_disposition;
};

/**
 * @struct ReaderOptions
 * @brief Options for creating readers.
 */
struct ReaderOptions {
  std::optional<std::string> version;
  std::optional<std::string> if_match;
  std::optional<std::string> if_none_match;
  std::optional<std::chrono::system_clock::time_point> if_modified_since;
  std::optional<std::chrono::system_clock::time_point> if_unmodified_since;
  std::optional<std::uint64_t> content_length_hint;
  std::size_t concurrent{0};
  std::optional<std::size_t> chunk;
  std::optional<std::size_t> gap;
  std::size_t prefetch{0};
};

/**
 * @struct WriteOptions
 * @brief Options for write and writer operations.
 */
struct WriteOptions {
  bool append{false};
  std::optional<std::string> cache_control;
  std::optional<std::string> content_type;
  std::optional<std::string> content_disposition;
  std::optional<std::string> content_encoding;
  std::optional<std::unordered_map<std::string, std::string>> user_metadata;
  std::optional<std::string> if_match;
  std::optional<std::string> if_none_match;
  bool if_not_exists{false};
  std::size_t concurrent{0};
  std::optional<std::size_t> chunk;
};

/**
 * @struct CopyOptions
 * @brief Options for copy operations.
 */
struct CopyOptions {
  bool if_not_exists{false};
  std::optional<std::string> if_match;
  std::optional<std::string> source_version;
  std::optional<std::uint64_t> source_content_length_hint;
  std::size_t concurrent{0};
  std::optional<std::size_t> chunk;
};

/**
 * @struct RenameOptions
 * @brief Options for rename operations.
 */
struct RenameOptions {
  bool if_not_exists{false};
};

/**
 * @struct DeleteOptions
 * @brief Options for delete operations.
 */
struct DeleteOptions {
  std::optional<std::string> version;
  bool recursive{false};
};

/**
 * @struct ListOptions
 * @brief Options for list and lister operations.
 */
struct ListOptions {
  std::optional<std::size_t> limit;
  std::optional<std::string> start_after;
  bool recursive{false};
  bool versions{false};
  bool deleted{false};
};

}  // namespace opendal
