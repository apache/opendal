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

#include <chrono>
#include <cstdio>
#include <ctime>

#include "lib.rs.h"
#include "opendal.hpp"
#include "utils/ffi_converter.hpp"
#include "utils/rust_converter.hpp"

namespace opendal {

std::optional<std::string> parse_optional_string(ffi::OptionalString &&s) {
  if (s.has_value) {
    return std::string(std::move(s.value));
  } else {
    return std::nullopt;
  }
}

Metadata parse_meta_data(ffi::Metadata &&meta) {
  Metadata metadata{
      .type = static_cast<EntryMode>(meta.mode),
      .content_length = meta.content_length,
      .cache_control = parse_optional_string(std::move(meta.cache_control)),
      .content_disposition =
          parse_optional_string(std::move(meta.content_disposition)),
      .content_md5 = parse_optional_string(std::move(meta.content_md5)),
      .content_type = parse_optional_string(std::move(meta.content_type)),
      .etag = parse_optional_string(std::move(meta.etag)),
  };

  auto last_modified_str = parse_optional_string(std::move(meta.last_modified));
  if (last_modified_str.has_value()) {
    // Parse ISO 8601 string to time_point using strptime to avoid locale lock
    std::tm tm = {};
    const char *str = last_modified_str.value().c_str();

    // Parse ISO 8601 format: YYYY-MM-DDTHH:MM:SS
    int year, month, day, hour, minute, second;
    if (sscanf(str, "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &minute,
               &second) == 6) {
      tm.tm_year = year - 1900;  // years since 1900
      tm.tm_mon = month - 1;     // months since January (0-11)
      tm.tm_mday = day;
      tm.tm_hour = hour;
      tm.tm_min = minute;
      tm.tm_sec = second;
      tm.tm_isdst = -1;  // let mktime determine DST

      std::time_t time_t_value = std::mktime(&tm);
      if (time_t_value != -1) {
        metadata.last_modified =
            std::chrono::system_clock::from_time_t(time_t_value);
      }
    }
  }

  return metadata;
}

Operator::Operator() noexcept = default;

void Operator::destroy() noexcept {
  if (operator_) {
    ffi::delete_operator(operator_);
    operator_ = nullptr;
  }
}

Operator::Operator(std::string_view scheme,
                   const std::unordered_map<std::string, std::string> &config) {
  auto rust_map = rust::Vec<ffi::HashMapValue>();
  rust_map.reserve(config.size());

  for (auto &[k, v] : config) {
    rust_map.push_back({utils::rust_string(k), utils::rust_string(v)});
  }

  operator_ = ffi::new_operator(utils::rust_str(scheme), rust_map);
}

Operator::~Operator() noexcept { destroy(); }

Operator::Operator(Operator &&other) noexcept : operator_(other.operator_) {
  other.operator_ = nullptr;
}

Operator &Operator::operator=(Operator &&other) noexcept {
  if (this != &other) {
    destroy();

    operator_ = other.operator_;
    other.operator_ = nullptr;
  }

  return *this;
}

bool Operator::available() const { return operator_ != nullptr; }

// We can't avoid copy, because std::vector hides the internal structure.
// std::vector doesn't support init from a pointer without copy.
std::string Operator::read(std::string_view path) {
  auto rust_vec = operator_->read(utils::rust_str(path));
  return {rust_vec.begin(), rust_vec.end()};
}

void Operator::write(std::string_view path, std::string_view data) {
  operator_->write(utils::rust_str(path),
                   utils::rust_slice<const uint8_t>(data));
}

bool Operator::exists(std::string_view path) {
  return operator_->exists(utils::rust_str(path));
}

bool Operator::is_exist(std::string_view path) { return exists(path); }

void Operator::create_dir(std::string_view path) {
  operator_->create_dir(utils::rust_str(path));
}

void Operator::copy(std::string_view src, std::string_view dst) {
  operator_->copy(utils::rust_str(src), utils::rust_str(dst));
}

void Operator::rename(std::string_view src, std::string_view dst) {
  operator_->rename(utils::rust_str(src), utils::rust_str(dst));
}

void Operator::remove(std::string_view path) {
  operator_->remove(utils::rust_str(path));
}

Metadata Operator::stat(std::string_view path) {
  return parse_meta_data(operator_->stat(utils::rust_str(path)));
}

std::vector<Entry> Operator::list(std::string_view path) {
  auto rust_vec = operator_->list(utils::rust_str(path));

  std::vector<Entry> entries;
  entries.reserve(rust_vec.size());
  for (auto &&entry : rust_vec) {
    entries.emplace_back(utils::parse_entry(std::move(entry)));
  }

  return entries;
}

Lister Operator::lister(std::string_view path) {
  return operator_->lister(utils::rust_str(path));
}

Reader Operator::reader(std::string_view path) {
  return operator_->reader(utils::rust_str(path));
}

}  // namespace opendal
