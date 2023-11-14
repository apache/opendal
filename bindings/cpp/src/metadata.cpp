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

#include "metadata.hpp"

#include <memory>

#include "lib.rs.h"
#include "utils/from.hpp"

namespace opendal {

namespace {
EntryMode from(ffi::EntryMode mode) {
  switch (mode) {
    case ffi::EntryMode::File:
      return EntryMode::kFile;
    case ffi::EntryMode::Dir:
      return EntryMode::kDir;
    case ffi::EntryMode::Unknown:
      return EntryMode::kUnknown;
    default:
      return EntryMode::kUnknown;
  }
}

std::optional<std::string_view> from(const std::optional<std::string> &s) {
  if (s.has_value()) {
    return std::string_view(s.value());
  } else {
    return std::nullopt;
  }
}
}  // namespace

struct Metadata::Rep {
  EntryMode mode;
  std::uint64_t content_length;
  std::optional<std::string> cache_control;
  std::optional<std::string> content_disposition;
  std::optional<std::string> content_md5;
  std::optional<std::string> content_type;
  std::optional<std::string> etag;
  std::optional<boost::posix_time::ptime> last_modified;

  static std::unique_ptr<Rep> create(ffi::Metadata &&ffil_metadata);
};

std::unique_ptr<Metadata::Rep> Metadata::Rep::create(
    ffi::Metadata &&ffil_metadata) {
  auto rep = std::make_unique<Metadata::Rep>();

  rep->mode = from(ffil_metadata.mode);
  rep->content_length = ffil_metadata.content_length;
  rep->cache_control = from(std::move(ffil_metadata.cache_control));
  rep->content_disposition = from(std::move(ffil_metadata.content_disposition));
  rep->content_type = from(std::move(ffil_metadata.content_type));
  rep->content_md5 = from(std::move(ffil_metadata.content_md5));
  rep->etag = from(std::move(ffil_metadata.etag));
  auto last_modified_str = from(std::move(ffil_metadata.last_modified));
  if (last_modified_str.has_value()) {
    rep->last_modified =
        boost::posix_time::from_iso_string(last_modified_str.value());
  }

  return rep;
}

Metadata::Metadata(ffi::Metadata &&ffi_metadata)
    : rep_(Rep::create(std::move(ffi_metadata))) {}

Metadata::~Metadata() = default;

EntryMode Metadata::mode() const noexcept { return rep_->mode; }
uint64_t Metadata::content_length() const noexcept {
  return rep_->content_length;
}

std::optional<std::string_view> Metadata::cache_control() const noexcept {
  return from(rep_->cache_control);
}

std::optional<std::string_view> Metadata::content_disposition() const noexcept {
  return from(rep_->content_disposition);
}

std::optional<std::string_view> Metadata::content_md5() const noexcept {
  return from(rep_->content_md5);
}

std::optional<std::string_view> Metadata::content_type() const noexcept {
  return from(rep_->content_type);
}

std::optional<std::string_view> Metadata::etag() const noexcept {
  return from(rep_->etag);
}

std::optional<boost::posix_time::ptime> Metadata::last_modified()
    const noexcept {
  return rep_->last_modified;
}

}  // namespace opendal
