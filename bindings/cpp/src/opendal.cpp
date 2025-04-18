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

#include <memory>

#include "boost/date_time/posix_time/time_parsers.hpp"
#include "details/lister.hpp"
#include "details/operator.hpp"
#include "details/reader.hpp"

namespace opendal {

std::optional<std::string> parse_optional_string(ffi::OptionalString &&s) {
  if (s.has_value) {
    return std::string(std::move(s.value));
  } else {
    return std::nullopt;
  }
}

Entry parse_entry(ffi::Entry &&other) {
  return Entry{
      .path = std::string(std::move(other.path)),
  };
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
    metadata.last_modified =
        boost::posix_time::from_iso_string(last_modified_str.value());
  }

  return metadata;
}

Operator::Operator() = default;

Operator::Operator(std::string_view scheme,
                   const std::unordered_map<std::string, std::string> &config) {
  operator_ = std::make_unique<details::Operator>(scheme, config);
}

Operator::~Operator() = default;

Operator::Operator(Operator &&) = default;
Operator &Operator::operator=(Operator &&) = default;

bool Operator::available() const { return operator_ != nullptr; }

// We can't avoid copy, because std::vector hides the internal structure.
// std::vector doesn't support init from a pointer without copy.
std::string Operator::read(std::string_view path) {
  return operator_->read(path);
}

void Operator::write(std::string_view path, std::string_view data) {
  operator_->write(path, data);
}

bool Operator::exists(std::string_view path) { return operator_->exists(path); }

bool Operator::is_exist(std::string_view path) { return exists(path); }

void Operator::create_dir(std::string_view path) {
  operator_->create_dir(path);
}

void Operator::copy(std::string_view src, std::string_view dst) {
  operator_->copy(src, dst);
}

void Operator::rename(std::string_view src, std::string_view dst) {
  operator_->rename(src, dst);
}

void Operator::remove(std::string_view path) { operator_->remove(path); }

Metadata Operator::stat(std::string_view path) {
  return parse_meta_data(operator_->stat(path));
}

std::vector<Entry> Operator::list(std::string_view path) {
  auto rust_vec = operator_->list(path);

  std::vector<Entry> entries;
  entries.reserve(rust_vec.size());
  for (auto &&entry : rust_vec) {
    entries.emplace_back(parse_entry(std::move(entry)));
  }

  return entries;
}

Lister Operator::lister(std::string_view path) {
  return std::make_unique<details::Lister>(operator_->lister(path));
}

Reader Operator::reader(std::string_view path) {
  return std::make_unique<details::Reader>(operator_->reader(path));
}

Lister::Lister(std::unique_ptr<details::Lister> lister)
    : raw_lister_{std::move(lister)} {};

Lister::Lister(Lister &&) = default;

Lister::~Lister() = default;

std::optional<Entry> Lister::next() {
  auto entry = raw_lister_->next();

  if (!entry.has_value) {
    return std::nullopt;
  }

  return parse_entry(std::move(entry.value));
}

Reader::Reader(std::unique_ptr<details::Reader> &&reader)
    : raw_reader_{std::move(reader)} {}

Reader::Reader(Reader &&) = default;

Reader::~Reader() = default;

std::streamsize Reader::read(void *s, std::streamsize n) {
  return raw_reader_->read(s, n);
}

std::streampos Reader::seek(std::streamoff off, std::ios_base::seekdir dir) {
  return raw_reader_->seek(off, dir);
}

}  // namespace opendal
