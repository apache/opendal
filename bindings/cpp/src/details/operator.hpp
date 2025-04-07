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

namespace opendal::details {

class Operator {
 public:
  Operator(std::string_view scheme,
           const std::unordered_map<std::string, std::string> &config = {})
      : operator_(create_by_config(scheme, config)) {}

  Operator(const Operator &) = delete;
  Operator &operator=(const Operator &) = delete;

  Operator(Operator &&) = default;
  Operator &operator=(Operator &&) = default;
  ~Operator() = default;

  std::string read(std::string_view path);

  void write(std::string_view path, std::string_view data);

  rust::Box<ffi::Reader> reader(std::string_view path);

  bool exists(std::string_view path);

  void create_dir(std::string_view path);

  void copy(std::string_view src, std::string_view dst);

  void rename(std::string_view src, std::string_view dst);

  void remove(std::string_view path);

  ffi::Metadata stat(std::string_view path);

  rust::Vec<ffi::Entry> list(std::string_view path);

  rust::Box<ffi::Lister> lister(std::string_view path);

 private:
  using OperatorBox = rust::Box<opendal::ffi::Operator>;
  using ConfigMap = std::unordered_map<std::string, std::string>;

  OperatorBox create_by_config(std::string_view scheme,
                               const ConfigMap &config);

  OperatorBox operator_;
};

}  // namespace opendal::details
