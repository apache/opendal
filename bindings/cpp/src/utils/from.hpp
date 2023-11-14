#pragma once

#include <optional>
#include <string>

#include "lib.rs.h"

namespace opendal {

std::optional<std::string> from(ffi::OptionalString &&s) {
  if (s.has_value) {
    return std::string(std::move(s.value));
  } else {
    return std::nullopt;
  }
}

} // namespace opendal
