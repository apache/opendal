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

#pragma once

#include "lib.rs.h"

/**
 * @class Lister
 * @brief Lister is designed to list the entries of a directory.
 * @details It provides next operation to get the next entry. You can also use
 * it like an iterator.
 * @code{.cpp}
 * auto lister = operator.lister("dir/");
 * for (const auto &entry : lister) {
 *   // Do something with entry
 * }
 * @endcode
 */
namespace opendal::details {

class Lister {
 public:
  Lister(rust::Box<opendal::ffi::Lister> &&lister)
      : raw_lister_{std::move(lister)} {}

  Lister(Lister &&) = default;

  ~Lister() = default;

  /**
   * @brief Get the next entry of the lister
   *
   * @return The next entry of the lister
   */
  ffi::OptionalEntry next() { return raw_lister_->next(); }

 private:
  rust::Box<opendal::ffi::Lister> raw_lister_;
};

}  // namespace opendal::details
