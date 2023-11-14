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

#include <optional>

#include "entry.hpp"
#include "lib.rs.h"

namespace opendal {

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
class Lister {
 public:
  Lister(rust::Box<opendal::ffi::Lister> &&lister)
      : raw_lister_(std::move(lister)) {}

  /**
   * @class ListerIterator
   * @brief ListerIterator is an iterator of Lister.
   * @note It's an undefined behavior to make multiple iterators from one
   * Lister.
   */
  class ListerIterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Entry;
    using difference_type = std::ptrdiff_t;
    using pointer = Entry *;
    using reference = Entry &;

    ListerIterator(Lister &lister) : lister_(lister) {
      current_entry_ = lister_.next();
    }

    Entry operator*() { return current_entry_.value(); }

    ListerIterator &operator++() {
      if (current_entry_) {
        current_entry_ = lister_.next();
      }
      return *this;
    }

    bool operator!=(const ListerIterator &other) const {
      return current_entry_ != std::nullopt ||
             other.current_entry_ != std::nullopt;
    }

   protected:
    // Only used for end iterator
    ListerIterator(Lister &lister, bool /*end*/) : lister_(lister) {}

   private:
    Lister &lister_;
    std::optional<Entry> current_entry_;

    friend class Lister;
  };

  /**
   * @brief Get the next entry of the lister
   *
   * @return The next entry of the lister
   */
  std::optional<Entry> next();

  ListerIterator begin() { return ListerIterator(*this); }
  ListerIterator end() { return ListerIterator(*this, true); }

 private:
  rust::Box<opendal::ffi::Lister> raw_lister_;
};

}  // namespace opendal
