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

#include "lister.hpp"

#include "lib.rs.h"

namespace opendal {

Lister::Lister(rust::Box<opendal::ffi::Lister> &&lister) noexcept
    : raw_lister_(std::move(lister)) {}

std::optional<Entry> Lister::next() {
  if (auto entry = raw_lister_->next(); entry.has_value) {
    return std::move(entry.value);
  }
  return std::nullopt;
}

}  // namespace opendal
