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

#include "common.hpp"
#include "gtest/gtest.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

extern "C" {
#include "opendal.h"
}

// opendal_list_options as released before options.rs was generated from core.
// Its members are C ABI, so the generator must keep their order, types, and
// offsets, and these functions with these exact prototypes.
#define ASSERT_SIGNATURE(fn, type) \
    static_assert(std::is_same<decltype(&fn), type>::value, #fn " changed its signature")
#define ASSERT_MEMBER_TYPE(member, type)                                             \
    static_assert(std::is_same<decltype(opendal_list_options::member), type>::value, \
        "opendal_list_options." #member " changed its type")

ASSERT_MEMBER_TYPE(recursive, bool);
ASSERT_MEMBER_TYPE(limit, uintptr_t);
ASSERT_MEMBER_TYPE(start_after, char*);
ASSERT_MEMBER_TYPE(versions, bool);
ASSERT_MEMBER_TYPE(deleted, bool);

#if UINTPTR_MAX == UINT64_MAX
static_assert(sizeof(opendal_list_options) == 32, "opendal_list_options changed its size");
static_assert(offsetof(opendal_list_options, recursive) == 0, "recursive moved");
static_assert(offsetof(opendal_list_options, limit) == 8, "limit moved");
static_assert(offsetof(opendal_list_options, start_after) == 16, "start_after moved");
static_assert(offsetof(opendal_list_options, versions) == 24, "versions moved");
static_assert(offsetof(opendal_list_options, deleted) == 25, "deleted moved");
#endif

ASSERT_SIGNATURE(opendal_list_options_new, struct opendal_list_options* (*)(void));
ASSERT_SIGNATURE(opendal_list_options_set_recursive, void (*)(struct opendal_list_options*, bool));
ASSERT_SIGNATURE(opendal_list_options_set_limit, void (*)(struct opendal_list_options*, uintptr_t));
ASSERT_SIGNATURE(opendal_list_options_set_start_after,
    void (*)(struct opendal_list_options*, const char*));
ASSERT_SIGNATURE(opendal_list_options_set_versions, void (*)(struct opendal_list_options*, bool));
ASSERT_SIGNATURE(opendal_list_options_set_deleted, void (*)(struct opendal_list_options*, bool));
ASSERT_SIGNATURE(opendal_list_options_free, void (*)(struct opendal_list_options*));

class OpendalListTest : public ::testing::Test {
protected:
    const opendal_operator* p;

    // set up a brand new operator
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

// Basic use case of list
TEST_F(OpendalListTest, ListDirTest)
{
    std::string dname = "some_random_dir_name_152312";
    std::string fname = "some_random_file_name_21389";

    // 4 MiB of random bytes
    uintptr_t nbytes = 4 * 1024 * 1024;
    auto rand = generateRandomBytes(4 * 1024 * 1024);
    auto random_bytes = reinterpret_cast<uint8_t*>(rand.data());

    std::string path = dname + "/" + fname;
    opendal_bytes data = {
        .data = random_bytes,
        .len = nbytes,
    };

    // write must succeed
    EXPECT_EQ(opendal_operator_write(this->p, path.c_str(), &data),
        nullptr);

    // list must succeed since the write succeeded
    opendal_lister* lister = nullptr;
    opendal_error* err = opendal_operator_list(this->p, (dname + "/").c_str(), &lister);
    EXPECT_EQ(err, nullptr);
    ASSERT_NE(lister, nullptr);

    // start checking the lister's result
    bool found = false;

    opendal_entry* entry = nullptr;
    err = opendal_lister_next(lister, &entry);
    EXPECT_EQ(err, nullptr);
    while (entry) {
        char* de_path = opendal_entry_path(entry);

        // stat must succeed
        opendal_metadata* meta = nullptr;
        err = opendal_operator_stat(this->p, de_path, &meta);
        EXPECT_EQ(err, nullptr);
        ASSERT_NE(meta, nullptr);

        if (!strcmp(de_path, path.c_str())) {
            found = true;

            // the path we found has to be a file, and the length must be coherent
            EXPECT_TRUE(opendal_metadata_is_file(meta));
            EXPECT_EQ(opendal_metadata_content_length(meta), nbytes);
        }

        opendal_string_free(de_path);
        opendal_metadata_free(meta);
        opendal_entry_free(entry);

        err = opendal_lister_next(lister, &entry);
        EXPECT_EQ(err, nullptr);
    }

    // we must have found the file we wrote
    EXPECT_TRUE(found);

    // delete
    EXPECT_EQ(opendal_operator_delete(this->p, path.c_str()),
        nullptr);

    opendal_lister_free(lister);
}

// todo: Try list an empty directory
TEST_F(OpendalListTest, ListEmptyDirTest) { }

// todo: Try list a directory that does not exist
TEST_F(OpendalListTest, ListNotExistDirTest) { }
