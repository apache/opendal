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

#include "opendal.hpp"

using namespace opendal;

#define RUST_STR(s) rust::Str(s.data(), s.size())
#define RUST_STRING(s) rust::String(s.data(), s.size())

Operator::Operator(std::string_view scheme,
                   const std::unordered_map<std::string, std::string> &config) {
  auto rust_map = rust::Vec<ffi::HashMapValue>();
  rust_map.reserve(config.size());
  for (auto &[k, v] : config) {
    rust_map.push_back({RUST_STRING(k), RUST_STRING(v)});
  }

  operator_ = opendal::ffi::new_operator(RUST_STR(scheme), rust_map);
}

bool Operator::available() const { return operator_.has_value(); }

// We can't avoid copy, because std::vector hides the internal structure.
// std::vector doesn't support init from a pointer without copy.
std::vector<uint8_t> Operator::read(std::string_view path) {
  auto rust_vec = operator_.value()->read(RUST_STR(path));

  return {rust_vec.data(), rust_vec.data() + rust_vec.size()};
}

void Operator::write(std::string_view path, const std::vector<uint8_t> &data) {
  operator_.value()->write(
      RUST_STR(path), rust::Slice<const uint8_t>(data.data(), data.size()));
}

bool Operator::is_exist(std::string_view path) {
  return operator_.value()->is_exist(RUST_STR(path));
}

void Operator::create_dir(std::string_view path) {
  operator_.value()->create_dir(RUST_STR(path));
}

void Operator::copy(std::string_view src, std::string_view dst) {
  operator_.value()->copy(RUST_STR(src), RUST_STR(dst));
}

void Operator::rename(std::string_view src, std::string_view dst) {
  operator_.value()->rename(RUST_STR(src), RUST_STR(dst));
}

void Operator::remove(std::string_view path) {
  operator_.value()->remove(RUST_STR(path));
}

Metadata Operator::stat(std::string_view path) {
  return {operator_.value()->stat(RUST_STR(path))};
}

std::vector<Entry> Operator::list(std::string_view path) {
  auto rust_vec = operator_.value()->list(RUST_STR(path));

  std::vector<Entry> entries;
  entries.reserve(rust_vec.size());
  for (auto &&entry : rust_vec) {
    entries.push_back(std::move(entry));
  }
  return entries;
}

Metadata::Metadata(ffi::Metadata &&other) {
  if (other.tag & 1) {
    type = EntryMode::FILE;
  } else if (other.tag & 0b10) {
    type = EntryMode::DIR;
  } else {
    type = EntryMode::UNKNOWN;
  }

  content_length = other.content_length;

  if (other.tag & 0b100) {
    cache_control = std::string(std::move(other.cache_control));
  }

  if (other.tag & 0b1000) {
    content_disposition = std::string(std::move(other.content_disposition));
  }

  if (other.tag & 0b10000) {
    content_md5 = std::string(std::move(other.content_md5));
  }

  if (other.tag & 0b100000) {
    content_type = std::string(std::move(other.content_type));
  }

  if (other.tag & 0b1000000) {
    etag = std::string(std::move(other.etag));
  }

  if (other.tag & 0b10000000) {
    last_modified = boost::posix_time::from_iso_string(
        std::string(std::move(other.last_modified)));
  }
}

Entry::Entry(ffi::Entry &&other) : path(std::move(other.path)) {}