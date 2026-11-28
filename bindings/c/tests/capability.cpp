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

extern "C" {
#include "opendal.h"
}

namespace {

// A positional initializer written against the C binding 0.47.8 header
// (OpenDAL v0.59.5) must keep its meaning: the 57 fields released there come
// first, in their released order.
TEST(CapabilityTest, PositionalInitializerKeepsReleasedOrder)
{
    // stat .. stat_with_version are the first 9 fields, followed by read.
    opendal_capability cap = { true, false, false, false, false, false, false, false, false, true };
    EXPECT_TRUE(cap.stat);
    EXPECT_FALSE(cap.stat_with_if_match);
    EXPECT_FALSE(cap.stat_with_version);
    EXPECT_TRUE(cap.read);
    EXPECT_FALSE(cap.stat_with_if_version_match);
}

TEST(CapabilityTest, PositionalInitializerReachesReleasedLimits)
{
    // write .. write_with_user_metadata are fields 18 to 29, followed by
    // write_multi_max_size, write_multi_min_size, and write_total_max_size.
    opendal_capability cap = { false, false, false, false, false, false, false, false, false,
        false, false, false, false, false, false, false, false, false,
        true, false, false, false, false, false, false, false, false, false, false, false,
        1, 2, 3, true };
    EXPECT_TRUE(cap.write);
    EXPECT_EQ(cap.write_multi_max_size, 1u);
    EXPECT_EQ(cap.write_multi_min_size, 2u);
    EXPECT_EQ(cap.write_total_max_size, 3u);
    EXPECT_TRUE(cap.create_dir);
    EXPECT_FALSE(cap.delete_);
}

constexpr std::size_t kFrozenFields = 98;

// Reads every frozen opendal_capability field by name, in declaration order.
// The first 57 are the fields released in C binding 0.47.8 (OpenDAL v0.59.5);
// the rest were added after that release. New fields are only appended after
// these.
std::vector<std::size_t> FrozenFields(const opendal_capability& c)
{
    return {
        c.stat,
        c.stat_with_if_match,
        c.stat_with_if_none_match,
        c.stat_with_if_modified_since,
        c.stat_with_if_unmodified_since,
        c.stat_with_override_cache_control,
        c.stat_with_override_content_disposition,
        c.stat_with_override_content_type,
        c.stat_with_version,
        c.read,
        c.read_with_if_match,
        c.read_with_if_none_match,
        c.read_with_override_cache_control,
        c.read_with_override_content_disposition,
        c.read_with_override_content_type,
        c.read_with_if_modified_since,
        c.read_with_if_unmodified_since,
        c.read_with_version,
        c.write,
        c.write_can_multi,
        c.write_can_empty,
        c.write_can_append,
        c.write_with_content_type,
        c.write_with_content_disposition,
        c.write_with_content_encoding,
        c.write_with_cache_control,
        c.write_with_if_match,
        c.write_with_if_none_match,
        c.write_with_if_not_exists,
        c.write_with_user_metadata,
        c.write_multi_max_size,
        c.write_multi_min_size,
        c.write_total_max_size,
        c.create_dir,
        c.delete_,
        c.delete_with_version,
        c.delete_with_recursive,
        c.copy,
        c.copy_with_if_not_exists,
        c.copy_with_if_match,
        c.copy_with_source_version,
        c.copy_can_multi,
        c.copy_multi_max_size,
        c.copy_multi_min_size,
        c.rename,
        c.list,
        c.list_with_limit,
        c.list_with_start_after,
        c.list_with_recursive,
        c.list_with_versions,
        c.list_with_deleted,
        c.presign,
        c.presign_read,
        c.presign_stat,
        c.presign_write,
        c.presign_delete,
        c.shared,
        c.stat_with_if_version_match,
        c.stat_with_if_version_not_match,
        c.read_with_if_version_match,
        c.read_with_if_version_not_match,
        c.read_with_suffix,
        c.read_with_checksum,
        c.write_can_copy_from,
        c.write_with_content_language,
        c.write_with_if_version_match,
        c.write_with_if_version_not_match,
        c.write_with_expires,
        c.write_with_checksum,
        c.delete_with_if_match,
        c.delete_with_if_none_match,
        c.delete_with_if_version_match,
        c.delete_with_if_version_not_match,
        c.delete_max_size,
        c.copy_with_if_none_match,
        c.copy_with_if_version_match,
        c.copy_with_if_version_not_match,
        c.copy_with_user_metadata,
        c.copy_with_content_type,
        c.copy_with_content_language,
        c.compose,
        c.compose_with_content_type,
        c.compose_with_content_disposition,
        c.compose_with_content_encoding,
        c.compose_with_cache_control,
        c.compose_with_user_metadata,
        c.compose_with_if_match,
        c.compose_with_if_none_match,
        c.compose_with_if_version_match,
        c.compose_with_if_version_not_match,
        c.compose_with_if_not_exists,
        c.compose_with_source_version,
        c.compose_with_source_if_match,
        c.restore,
        c.restore_with_version,
        c.restore_with_if_not_exists,
        c.rename_with_if_not_exists,
        c.list_with_glob
    };
}

// Positionally initializes the first `sizeof...(I)` fields; only the one at
// `pos` is nonzero.
template <std::size_t... I>
opendal_capability OnlyTrueAt(std::size_t pos, std::index_sequence<I...>)
{
    return opendal_capability { (I == pos)... };
}

TEST(CapabilityTest, PositionalInitializerSetsEachFrozenField)
{
    for (std::size_t pos = 0; pos < kFrozenFields; ++pos) {
        std::vector<std::size_t> expected(kFrozenFields, 0);
        expected[pos] = 1;
        EXPECT_EQ(FrozenFields(OnlyTrueAt(pos, std::make_index_sequence<kFrozenFields>())), expected)
            << "only positional initializer " << pos << " is set";
    }
}

} // namespace
