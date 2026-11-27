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

#include <cstring>
#include <type_traits>
#include "gtest/gtest.h"

extern "C" {
#include "opendal.h"
}

// The metadata functions released before metadata.rs was generated from core.
// The generator must keep each of them with this exact signature.
#define ASSERT_SIGNATURE(fn, type) \
    static_assert(std::is_same<decltype(&fn), type>::value, #fn " changed its signature")

ASSERT_SIGNATURE(opendal_metadata_free, void (*)(struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_mode, uint8_t (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_content_length, uint64_t (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_is_file, bool (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_is_dir, bool (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_is_current, uint8_t (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_is_deleted, bool (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_cache_control, char* (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_content_disposition, char* (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_content_md5, char* (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_content_type, char* (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_content_encoding, char* (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_etag, char* (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_version, char* (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_get_user_metadata,
    struct opendal_metadata_user_metadata* (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_last_modified_ms, int64_t (*)(const struct opendal_metadata*));
ASSERT_SIGNATURE(opendal_metadata_user_metadata_pairs,
    const struct opendal_metadata_user_metadata_pair* (*)(const struct opendal_metadata_user_metadata*));
ASSERT_SIGNATURE(opendal_metadata_user_metadata_len,
    uintptr_t (*)(const struct opendal_metadata_user_metadata*));
ASSERT_SIGNATURE(opendal_metadata_user_metadata_free, void (*)(struct opendal_metadata_user_metadata*));

static void expect_owned_string(char* value, const char* expected)
{
    ASSERT_NE(value, nullptr);
    EXPECT_STREQ(value, expected);
    opendal_string_free(value);
}

class OpendalMetadataTest : public ::testing::Test {
protected:
    opendal_operator* p;

    void SetUp() override
    {
        opendal_operator_options* options = opendal_operator_options_new();
        opendal_operator_options_set(options, "root", "/myroot");

        opendal_operator* op = nullptr;
        opendal_error* error = opendal_operator_new("memory", options, &op);
        EXPECT_EQ(error, nullptr);

        this->p = op;
        EXPECT_TRUE(this->p);

        opendal_operator_options_free(options);
    }

    void TearDown() override { opendal_operator_free(this->p); }
};

TEST_F(OpendalMetadataTest, StatReturnsWrittenMetadata)
{
    const char* path = "metadata.txt";
    std::string content = "Hello, World!";
    const opendal_bytes data = {
        .data = (uint8_t*)content.c_str(),
        .len = content.length(),
    };
    const opendal_write_user_metadata_pair pairs[] = { { "location", "everywhere" } };

    opendal_write_options* opts = opendal_write_options_new();
    opendal_write_options_set_cache_control(opts, "max-age=60");
    opendal_write_options_set_content_type(opts, "text/plain");
    opendal_write_options_set_content_disposition(opts, "attachment");
    opendal_write_options_set_content_encoding(opts, "identity");
    opendal_write_options_set_user_metadata(opts, pairs, 1);
    opendal_error* error = opendal_operator_write_with(this->p, path, &data, opts);
    opendal_write_options_free(opts);
    ASSERT_EQ(error, nullptr);

    opendal_metadata* meta = nullptr;
    error = opendal_operator_stat(this->p, path, &meta);
    ASSERT_EQ(error, nullptr);
    ASSERT_NE(meta, nullptr);

    EXPECT_EQ(opendal_metadata_mode(meta), 1);
    EXPECT_TRUE(opendal_metadata_is_file(meta));
    EXPECT_FALSE(opendal_metadata_is_dir(meta));
    EXPECT_EQ(opendal_metadata_content_length(meta), content.length());
    EXPECT_EQ(opendal_metadata_is_current(meta), 2);
    EXPECT_FALSE(opendal_metadata_is_deleted(meta));

    expect_owned_string(opendal_metadata_cache_control(meta), "max-age=60");
    expect_owned_string(opendal_metadata_content_type(meta), "text/plain");
    expect_owned_string(opendal_metadata_content_disposition(meta), "attachment");
    expect_owned_string(opendal_metadata_content_encoding(meta), "identity");
    EXPECT_EQ(opendal_metadata_content_md5(meta), nullptr);
    EXPECT_EQ(opendal_metadata_etag(meta), nullptr);
    EXPECT_EQ(opendal_metadata_version(meta), nullptr);
    // The C write options cannot set Content-Language.
    EXPECT_EQ(opendal_metadata_content_language(meta), nullptr);

    opendal_metadata_user_metadata* user_metadata = opendal_metadata_get_user_metadata(meta);
    ASSERT_NE(user_metadata, nullptr);
    ASSERT_EQ(opendal_metadata_user_metadata_len(user_metadata), 1u);
    const opendal_metadata_user_metadata_pair* pair = opendal_metadata_user_metadata_pairs(user_metadata);
    EXPECT_STREQ(pair->key, "location");
    EXPECT_STREQ(pair->value, "everywhere");
    opendal_metadata_user_metadata_free(user_metadata);

    opendal_metadata_free(meta);
}
