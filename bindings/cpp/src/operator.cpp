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

#include "boost/date_time/posix_time/time_parsers.hpp"
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
    try {
      // RFC3339 format from Rust needs to be converted to boost::posix_time
      // Try parsing as ISO extended string first (supports RFC3339 format)
      std::string timestamp = last_modified_str.value();
      
      // Convert RFC3339 'T' separator to space for boost parsing
      size_t t_pos = timestamp.find('T');
      if (t_pos != std::string::npos) {
        timestamp[t_pos] = ' ';
      }
      
      // Remove timezone suffix (Z or +HH:MM) for boost parsing
      size_t z_pos = timestamp.find('Z');
      if (z_pos != std::string::npos) {
        timestamp = timestamp.substr(0, z_pos);
      } else {
        size_t plus_pos = timestamp.find('+');
        if (plus_pos != std::string::npos) {
          timestamp = timestamp.substr(0, plus_pos);
        } else {
          size_t minus_pos = timestamp.rfind('-');
          if (minus_pos != std::string::npos && minus_pos > 10) { // Make sure it's not part of date
            timestamp = timestamp.substr(0, minus_pos);
          }
        }
      }
      
      metadata.last_modified = boost::posix_time::time_from_string(timestamp);
    } catch (const std::exception& e) {
      // If parsing fails, leave last_modified unset
      metadata.last_modified = std::nullopt;
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

void Operator::remove_all(std::string_view path) {
  operator_->remove_all(utils::rust_str(path));
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
