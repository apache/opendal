#include "opendal.hpp"

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
}  // namespace

Metadata::Metadata(ffi::Metadata &&other) {
  mode_ = from(other.mode);
  content_length = other.content_length;
  cache_control = from(std::move(other.cache_control));
  content_disposition = from(std::move(other.content_disposition));
  content_type = from(std::move(other.content_type));
  content_md5 = from(std::move(other.content_md5));
  etag = from(std::move(other.etag));
  auto last_modified_str = from(std::move(other.last_modified));
  if (last_modified_str.has_value()) {
    last_modified =
        boost::posix_time::from_iso_string(last_modified_str.value());
  }
}

}  // namespace opendal
