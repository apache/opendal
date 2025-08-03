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

std::optional<bool> parse_optional_bool(ffi::OptionalBool &&b) {
  if (b.has_value) {
    return b.value;
  } else {
    return std::nullopt;
  }
}

Metadata parse_meta_data(ffi::Metadata &&meta) {
  Metadata metadata;

  // Basic information
  metadata.type = static_cast<EntryMode>(meta.mode);
  metadata.content_length = meta.content_length;

  // HTTP-style headers
  metadata.cache_control = parse_optional_string(std::move(meta.cache_control));
  metadata.content_disposition =
      parse_optional_string(std::move(meta.content_disposition));
  metadata.content_md5 = parse_optional_string(std::move(meta.content_md5));
  metadata.content_type = parse_optional_string(std::move(meta.content_type));
  metadata.content_encoding =
      parse_optional_string(std::move(meta.content_encoding));
  metadata.etag = parse_optional_string(std::move(meta.etag));

  // Versioning information
  metadata.version = parse_optional_string(std::move(meta.version));
  metadata.is_current = parse_optional_bool(std::move(meta.is_current));
  metadata.is_deleted = meta.is_deleted;

  // Parse last_modified timestamp
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

void Operator::Destroy() noexcept {
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

Operator::~Operator() noexcept { Destroy(); }

Operator::Operator(Operator &&other) noexcept : operator_(other.operator_) {
  other.operator_ = nullptr;
}

Operator &Operator::operator=(Operator &&other) noexcept {
  if (this != &other) {
    Destroy();

    operator_ = other.operator_;
    other.operator_ = nullptr;
  }

  return *this;
}

bool Operator::Available() const { return operator_ != nullptr; }

// We can't avoid copy, because std::vector hides the internal structure.
// std::vector doesn't support init from a pointer without copy.
std::string Operator::Read(std::string_view path) {
  auto rust_vec = operator_->read(utils::rust_str(path));
  return {rust_vec.begin(), rust_vec.end()};
}

void Operator::Write(std::string_view path, std::string_view data) {
  operator_->write(utils::rust_str(path),
                   utils::rust_slice<const uint8_t>(data));
}

bool Operator::Exists(std::string_view path) {
  return operator_->exists(utils::rust_str(path));
}

bool Operator::IsExist(std::string_view path) { return Exists(path); }

void Operator::CreateDir(std::string_view path) {
  operator_->create_dir(utils::rust_str(path));
}

void Operator::Copy(std::string_view src, std::string_view dst) {
  operator_->copy(utils::rust_str(src), utils::rust_str(dst));
}

void Operator::Rename(std::string_view src, std::string_view dst) {
  operator_->rename(utils::rust_str(src), utils::rust_str(dst));
}

void Operator::Remove(std::string_view path) {
  operator_->remove(utils::rust_str(path));
}

Metadata Operator::Stat(std::string_view path) {
  return parse_meta_data(operator_->stat(utils::rust_str(path)));
}

std::vector<Entry> Operator::List(std::string_view path) {
  auto rust_vec = operator_->list(utils::rust_str(path));

  std::vector<Entry> entries;
  entries.reserve(rust_vec.size());
  for (auto &&entry : rust_vec) {
    entries.emplace_back(utils::parse_entry(std::move(entry)));
  }

  return entries;
}

Lister Operator::GetLister(std::string_view path) {
  return operator_->lister(utils::rust_str(path));
}

Reader Operator::GetReader(std::string_view path) {
  return operator_->reader(utils::rust_str(path));
}

}  // namespace opendal
