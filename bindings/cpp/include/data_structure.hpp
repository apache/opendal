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
#include <optional>
#include <string>

#include "boost/date_time/posix_time/ptime.hpp"

namespace opendal {

/**
 * @enum EntryMode
 * @brief The mode of the entry
 */
enum class EntryMode : int {
  FILE = 1,
  DIR = 2,
  UNKNOWN = 0,
};

/**
 * @struct Metadata
 * @brief The metadata of a file or directory
 */
class Metadata {
 public:
  EntryMode type;
  std::uint64_t content_length;
  std::optional<std::string> cache_control;
  std::optional<std::string> content_disposition;
  std::optional<std::string> content_md5;
  std::optional<std::string> content_type;
  std::optional<std::string> etag;
  std::optional<boost::posix_time::ptime> last_modified;
};

/**
 * @struct Entry
 * @brief The entry of a file or directory
 */
class Entry {
 public:
  std::string path;
};

}  // namespace opendal
