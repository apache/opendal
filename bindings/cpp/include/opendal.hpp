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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace opendal {

class Operator : std::enable_shared_from_this<Operator> {
public:
  Operator() = default;
  Operator(std::string_view scheme,
           const std::unordered_map<std::string, std::string> &config = {});

  // Disable copy and assign
  Operator(const Operator &) = delete;
  Operator &operator=(const Operator &) = delete;

  // Enable move
  Operator(Operator &&) = default;
  Operator &operator=(Operator &&) = default;
  ~Operator() = default;

  bool available() const;
  std::vector<uint8_t> read(std::string_view path);
  void write(std::string_view path, const std::vector<uint8_t> &data);

private:
  std::optional<rust::Box<opendal::ffi::Operator>> operator_;
};

} // namespace opendal