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

// Operator

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

bool Operator::exists(std::string_view path) {
  return operator_.value()->exists(RUST_STR(path));
}

bool Operator::is_exist(std::string_view path) {
  return exists(path);
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

Lister Operator::lister(std::string_view path) {
  return operator_.value()->lister(RUST_STR(path));
}

Reader Operator::reader(std::string_view path) {
  return operator_.value()->reader(RUST_STR(path));
}

// Reader

std::streamsize Reader::read(void *s, std::streamsize n) {
  auto rust_slice = rust::Slice<uint8_t>(reinterpret_cast<uint8_t *>(s), n);
  auto read_size = raw_reader_->read(rust_slice);
  return read_size;
}

ffi::SeekDir to_rust_seek_dir(std::ios_base::seekdir dir);

std::streampos Reader::seek(std::streamoff off, std::ios_base::seekdir dir) {
  return raw_reader_->seek(off, to_rust_seek_dir(dir));
}

// Lister

std::optional<Entry> Lister::next() {
  auto entry = raw_lister_->next();
  if (entry.has_value) {
    return std::move(entry.value);
  } else {
    return std::nullopt;
  }
}

// Metadata

std::optional<std::string> parse_optional_string(ffi::OptionalString &&s);

Metadata::Metadata(ffi::Metadata &&other) {
  type = static_cast<EntryMode>(other.mode);
  content_length = other.content_length;
  cache_control = parse_optional_string(std::move(other.cache_control));
  content_disposition =
      parse_optional_string(std::move(other.content_disposition));
  content_type = parse_optional_string(std::move(other.content_type));
  content_md5 = parse_optional_string(std::move(other.content_md5));
  etag = parse_optional_string(std::move(other.etag));
  auto last_modified_str =
      parse_optional_string(std::move(other.last_modified));
  if (last_modified_str.has_value()) {
    last_modified =
        boost::posix_time::from_iso_string(last_modified_str.value());
  }
}

// Entry

Entry::Entry(ffi::Entry &&other) : path(std::move(other.path)) {}

// helper functions

std::optional<std::string> parse_optional_string(ffi::OptionalString &&s) {
  if (s.has_value) {
    return std::string(std::move(s.value));
  } else {
    return std::nullopt;
  }
}

ffi::SeekDir to_rust_seek_dir(std::ios_base::seekdir dir) {
  switch (dir) {
    case std::ios_base::beg:
      return ffi::SeekDir::Start;
    case std::ios_base::cur:
      return ffi::SeekDir::Current;
    case std::ios_base::end:
      return ffi::SeekDir::End;
    default:
      throw std::runtime_error("invalid seekdir");
  }
}