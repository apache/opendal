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
#include <string_view>
#include <unordered_map>
#include <vector>

#include "lib.rs.h"
#include "metadata.hpp"
#include "reader.hpp"

namespace opendal {

/**
 * @class Operator
 * @brief Operator is the entry for all public APIs.
 */
class Operator {
 public:
  Operator() = default;

  /**
   * @brief Construct a new Operator object
   *
   * @param scheme The scheme of the operator, same as the name of rust doc
   * @param config The configuration of the operator, same as the service doc
   */
  Operator(std::string_view scheme,
           const std::unordered_map<std::string, std::string> &config = {});

  // Disable copy and assign
  Operator(const Operator &) = delete;
  Operator &operator=(const Operator &) = delete;

  // Enable move
  Operator(Operator &&) = default;
  Operator &operator=(Operator &&) = default;
  ~Operator() = default;

  /**
   * @brief Check if the operator is available
   *
   * @return true if the operator is available, false otherwise
   */
  bool available() const;

  /**
   * @brief Read data from the operator
   * @note The operation will make unnecessary copy. So we recommend to use the
   * `reader` method.
   *
   * @param path The path of the data
   * @return The data read from the operator
   */
  std::vector<uint8_t> read(std::string_view path);

  /**
   * @brief Write data to the operator
   *
   * @param path The path of the data
   * @param data The data to write
   */
  void write(std::string_view path, const std::vector<uint8_t> &data);

  /**
   * @brief Read data from the operator
   *
   * @param path The path of the data
   * @return The reader of the data
   */
  Reader reader(std::string_view path);

  /**
   * @brief Check if the path exists
   *
   * @param path The path to check
   * @return true if the path exists, false otherwise
   */
  bool is_exist(std::string_view path);

  /**
   * @brief Create a directory
   *
   * @param path The path of the directory
   */
  void create_dir(std::string_view path);

  /**
   * @brief Copy a file from src to dst.
   *
   * @param src The source path
   * @param dst The destination path
   */
  void copy(std::string_view src, std::string_view dst);

  /**
   * @brief Rename a file from src to dst.
   *
   * @param src The source path
   * @param dst The destination path
   */
  void rename(std::string_view src, std::string_view dst);

  /**
   * @brief Remove a file or directory
   *
   * @param path The path of the file or directory
   */
  void remove(std::string_view path);

  /**
   * @brief Get the metadata of a file or directory
   *
   * @param path The path of the file or directory
   * @return The metadata of the file or directory
   */
  Metadata stat(std::string_view path);

  /**
   * @brief List the entries of a directory
   * @note The returned entries are sorted by name.
   *
   * @param path The path of the directory
   * @return The entries of the directory
   */
  std::vector<Entry> list(std::string_view path);

  Lister lister(std::string_view path);

 private:
  std::optional<rust::Box<opendal::ffi::Operator>> operator_;
};

}  // namespace opendal
