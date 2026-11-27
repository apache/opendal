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
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "opendal.hpp"

namespace {

// Positional aggregate initialization must keep the meaning it had before
// `opendal::Capability` was generated: the historical members come first, in
// their original order.
TEST(CapabilityTest, PositionalInitializerKeepsHistoricalOrder) {
  opendal::Capability caps{false, false, false, true};
  EXPECT_FALSE(caps.stat);
  EXPECT_FALSE(caps.stat_with_if_match);
  EXPECT_FALSE(caps.stat_with_if_none_match);
  EXPECT_TRUE(caps.read);
  EXPECT_FALSE(caps.stat_with_if_version_match);
}

constexpr std::size_t kHistoricalMembers = 32;

// Reads the members `opendal::Capability` declared before it was generated, in
// their original declaration order.
std::vector<std::size_t> HistoricalMembers(const opendal::Capability& c) {
  return {c.stat,
          c.stat_with_if_match,
          c.stat_with_if_none_match,
          c.read,
          c.read_with_if_match,
          c.read_with_if_none_match,
          c.read_with_override_cache_control,
          c.read_with_override_content_disposition,
          c.read_with_override_content_type,
          c.write,
          c.write_can_multi,
          c.write_can_empty,
          c.write_can_append,
          c.write_with_content_type,
          c.write_with_content_disposition,
          c.write_with_cache_control,
          c.write_multi_max_size,
          c.write_multi_min_size,
          c.write_total_max_size,
          c.create_dir,
          c.delete_feature,
          c.copy,
          c.rename,
          c.list,
          c.list_with_limit,
          c.list_with_start_after,
          c.list_with_recursive,
          c.presign,
          c.presign_read,
          c.presign_stat,
          c.presign_write,
          c.shared};
}

// Positionally initializes the first `sizeof...(I)` members; only the one at
// `pos` is true.
template <std::size_t... I>
opendal::Capability OnlyTrueAt(std::size_t pos, std::index_sequence<I...>) {
  return opendal::Capability{(I == pos)...};
}

TEST(CapabilityTest, PositionalInitializerSetsEachHistoricalMember) {
  for (std::size_t pos = 0; pos < kHistoricalMembers; ++pos) {
    std::vector<std::size_t> expected(kHistoricalMembers, 0);
    expected[pos] = 1;
    EXPECT_EQ(HistoricalMembers(OnlyTrueAt(
                  pos, std::make_index_sequence<kHistoricalMembers>())),
              expected)
        << "only positional initializer " << pos << " is true";
  }
}

TEST(CapabilityTest, PositionalInitializerReachesHistoricalLimits) {
  // stat .. write_with_cache_control are the first 16 members, followed by
  // write_multi_max_size, write_multi_min_size, and write_total_max_size.
  opendal::Capability caps{false, false, false, false, false, false, false,
                           false, false, true,  false, false, false, false,
                           false, false, 1,     2,     3,     true};
  EXPECT_TRUE(caps.write);
  EXPECT_EQ(caps.write_multi_max_size, std::size_t{1});
  EXPECT_EQ(caps.write_multi_min_size, std::size_t{2});
  EXPECT_EQ(caps.write_total_max_size, std::size_t{3});
  EXPECT_TRUE(caps.create_dir);
}

TEST(CapabilityTest, OperatorReportsCapability) {
  opendal::Operator op("memory");
  const auto caps = op.Info();
  EXPECT_TRUE(caps.read);
  EXPECT_TRUE(caps.write);
  EXPECT_TRUE(caps.delete_feature);
}

}  // namespace
