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
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "boost/date_time/posix_time/posix_time.hpp"
#include "lib.rs.h"

namespace opendal {
namespace ffi {
class Metadata;
}

/**
 * @enum class EntryMode
 * @brief The mode of the entry
 */
enum class EntryMode {
  kFile = 0,
  kDir = 1,
  kUnknown = 2,
};

/**
 * @struct Metadata
 * @brief The metadata of a file or directory
 */
class Metadata final {
 public:
  Metadata(ffi::Metadata &&ffi_metadata);
  ~Metadata() noexcept;

  [[nodiscard]] EntryMode mode() const noexcept;
  [[nodiscard]] uint64_t content_length() const noexcept;
  [[nodiscard]] std::optional<std::string_view> cache_control() const noexcept;
  [[nodiscard]] std::optional<std::string_view> content_disposition()
      const noexcept;
  [[nodiscard]] std::optional<std::string_view> content_md5() const noexcept;
  [[nodiscard]] std::optional<std::string_view> content_type() const noexcept;
  [[nodiscard]] std::optional<std::string_view> etag() const noexcept;
  [[nodiscard]] std::optional<boost::posix_time::ptime> last_modified()
      const noexcept;

 private:
  struct Rep;
  std::unique_ptr<Rep> rep_;
};
}  // namespace opendal
