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

#include <cstddef>
#include <string>

#include "gtest/gtest.h"
#include "opendal.hpp"
#include "utils/options_converter.hpp"

TEST(OptionsTest, ListOptionsPositionalInitializerReachesBridge) {
  // Every member in the order `data_structure.hpp` declared them before
  // `ListOptions` was generated.
  const opendal::ListOptions options{
      /*limit=*/16,
      /*start_after=*/"start_after",
      /*recursive=*/true,
      /*versions=*/true,
      /*deleted=*/true,
  };

  const auto ffi = opendal::utils::ToFfiOptions(options);

  EXPECT_TRUE(ffi.limit.has_value);
  EXPECT_EQ(ffi.limit.value, std::size_t{16});
  EXPECT_TRUE(ffi.start_after.has_value);
  EXPECT_EQ(std::string(ffi.start_after.value), "start_after");
  EXPECT_TRUE(ffi.recursive);
  EXPECT_TRUE(ffi.versions);
  EXPECT_TRUE(ffi.deleted);
}

TEST(OptionsTest, ListOptionsStructuredBindingsKeepMemberOrder) {
  opendal::ListOptions options;
  auto &[limit, start_after, recursive, versions, deleted] = options;

  EXPECT_EQ(&limit, &options.limit);
  EXPECT_EQ(&start_after, &options.start_after);
  EXPECT_EQ(&recursive, &options.recursive);
  EXPECT_EQ(&versions, &options.versions);
  EXPECT_EQ(&deleted, &options.deleted);
}

TEST(OptionsTest, ListOptionsKeepExplicitZeroLimit) {
  opendal::ListOptions options;
  options.limit = 0;

  const auto ffi = opendal::utils::ToFfiOptions(options);

  EXPECT_TRUE(ffi.limit.has_value);
  EXPECT_EQ(ffi.limit.value, std::size_t{0});
}

TEST(OptionsTest, DefaultListOptionsReachBridgeAbsent) {
  // A const object can be default-initialized only while each scalar member
  // has a default member initializer.
  const opendal::ListOptions options;

  const auto ffi = opendal::utils::ToFfiOptions(options);

  EXPECT_FALSE(ffi.limit.has_value);
  EXPECT_FALSE(ffi.start_after.has_value);
  EXPECT_FALSE(ffi.recursive);
  EXPECT_FALSE(ffi.versions);
  EXPECT_FALSE(ffi.deleted);
}
