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

#include <limits>
#include "gtest/gtest.h"

extern "C" {
#include "opendal.h"
}

class OpendalBddTest : public ::testing::Test {
protected:
    const opendal_operator* p;

    std::string scheme;
    std::string path;
    std::string content;

    void SetUp() override
    {
        this->scheme = std::string("memory");
        this->path = std::string("test");
        this->content = std::string("Hello, World!");

        opendal_operator_options* options = opendal_operator_options_new();
        opendal_operator_options_set(options, "root", "/myroot");

        // Given A new OpenDAL Blocking Operator
        opendal_operator* op = nullptr;
        opendal_error* error = opendal_operator_new(scheme.c_str(), options, &op);
        EXPECT_EQ(error, nullptr);

        this->p = op;
        EXPECT_TRUE(this->p);

        opendal_operator_options_free(options);
    }

    void TearDown() override { opendal_operator_free(this->p); }
};

// Scenario: OpenDAL Blocking Operations
TEST_F(OpendalBddTest, FeatureTest)
{
    // When Blocking write path "test" with content "Hello, World!"
    const opendal_bytes data = {
        .data = (uint8_t*)this->content.c_str(),
        .len = this->content.length(),
    };
    opendal_error* error = opendal_operator_check(this->p);
    EXPECT_EQ(error, nullptr);

    error = opendal_operator_write(this->p, this->path.c_str(), &data);
    EXPECT_EQ(error, nullptr);

    // The blocking file "test" should exist
    bool exists = false;
    error = opendal_operator_exists(this->p, this->path.c_str(), &exists);
    EXPECT_EQ(error, nullptr);
    EXPECT_TRUE(exists);

    // The blocking file "test" entry mode must be file
    opendal_metadata* meta = nullptr;
    error = opendal_operator_stat(this->p, this->path.c_str(), &meta);
    EXPECT_EQ(error, nullptr);
    ASSERT_NE(meta, nullptr);
    EXPECT_TRUE(opendal_metadata_is_file(meta));

    // The blocking file "test" content length must be 13
    EXPECT_EQ(opendal_metadata_content_length(meta), 13);

    // the blocking file "test" last modified time must not be -1
    EXPECT_FALSE(opendal_metadata_last_modified_ms(meta) != -1);
    opendal_metadata_free(meta);

    // The blocking file "test" must have content "Hello, World!"
    opendal_bytes r_data = { nullptr, 0, 0 };
    error = opendal_operator_read(this->p, this->path.c_str(), &r_data);
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(r_data.len, this->content.length());
    for (int i = 0; i < r_data.len; i++) {
        EXPECT_EQ(this->content[i], (char)(r_data.data[i]));
    }

    // The blocking file should be deleted
    error = opendal_operator_delete(this->p, this->path.c_str());
    EXPECT_EQ(error, nullptr);
    error = opendal_operator_exists(this->p, this->path.c_str(), &exists);
    EXPECT_EQ(error, nullptr);
    EXPECT_FALSE(exists);

    opendal_writer* writer = nullptr;
    error = opendal_operator_writer(this->p, this->path.c_str(), &writer);
    EXPECT_EQ(error, nullptr);
    ASSERT_NE(writer, nullptr);
    uintptr_t written_size = 0;
    error = opendal_writer_write(writer, &data, &written_size);
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(written_size, this->content.length());
    opendal_error* close_err = opendal_writer_close(writer);
    EXPECT_EQ(close_err, nullptr);
    opendal_writer_free(writer);

    // The blocking file "test" must have content "Hello, World!" and read into buffer
    int length = this->content.length();
    unsigned char buffer[this->content.length()];
    opendal_reader* reader = nullptr;
    error = opendal_operator_reader(this->p, this->path.c_str(), &reader);
    EXPECT_EQ(error, nullptr);
    ASSERT_NE(reader, nullptr);
    uintptr_t read_size = 0;
    error = opendal_reader_read(reader, buffer, length, &read_size);
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(read_size, length);
    for (int i = 0; i < this->content.length(); i++) {
        EXPECT_EQ(this->content[i], buffer[i]);
    }
    opendal_reader_free(reader);

    // The deletion operation should be idempotent
    error = opendal_operator_delete(this->p, this->path.c_str());
    EXPECT_EQ(error, nullptr);

    opendal_bytes_free(&r_data);

    // The directory "tmpdir/" should exist and should be a directory
    error = opendal_operator_create_dir(this->p, "tmpdir/");
    EXPECT_EQ(error, nullptr);
    opendal_metadata* dir_meta = nullptr;
    error = opendal_operator_stat(this->p, "tmpdir/", &dir_meta);
    EXPECT_EQ(error, nullptr);
    ASSERT_NE(dir_meta, nullptr);
    EXPECT_TRUE(opendal_metadata_is_dir(dir_meta));
    EXPECT_FALSE(opendal_metadata_is_file(dir_meta));
    opendal_metadata_free(dir_meta);
    error = opendal_operator_delete(this->p, "tmpdir/");
    EXPECT_EQ(error, nullptr);
}

TEST_F(OpendalBddTest, WriteEmptyNullBytes)
{
    const opendal_bytes empty = {
        .data = nullptr,
        .len = 0,
        .capacity = 0,
    };

    opendal_error* error = opendal_operator_write(this->p, "empty", &empty);
    EXPECT_EQ(error, nullptr);

    opendal_bytes read_data = { nullptr, 0, 0 };
    error = opendal_operator_read(this->p, "empty", &read_data);
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(read_data.len, 0);
    EXPECT_EQ(read_data.data, nullptr);
    EXPECT_EQ(read_data.capacity, 0);

    // An empty read result must satisfy the write input contract.
    error = opendal_operator_write(this->p, "empty-round-trip", &read_data);
    EXPECT_EQ(error, nullptr);
    opendal_bytes_free(&read_data);

    // Cover the separate write_with entry point that shares this validation.
    error = opendal_operator_write_with(this->p, "empty-with-options", &empty, nullptr);
    EXPECT_EQ(error, nullptr);

    error = opendal_operator_read(this->p, "empty-with-options", &read_data);
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(read_data.len, 0);
    opendal_bytes_free(&read_data);

    opendal_writer* writer = nullptr;
    error = opendal_operator_writer(this->p, "empty-writer", &writer);
    EXPECT_EQ(error, nullptr);
    ASSERT_NE(writer, nullptr);

    uintptr_t write_size = 0;
    error = opendal_writer_write(writer, &empty, &write_size);
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(write_size, 0);

    error = opendal_writer_close(writer);
    EXPECT_EQ(error, nullptr);
    opendal_writer_free(writer);

    error = opendal_operator_read(this->p, "empty-writer", &read_data);
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(read_data.len, 0);
    opendal_bytes_free(&read_data);

    EXPECT_EQ(opendal_operator_delete(this->p, "empty"), nullptr);
    EXPECT_EQ(opendal_operator_delete(this->p, "empty-round-trip"), nullptr);
    EXPECT_EQ(opendal_operator_delete(this->p, "empty-with-options"), nullptr);
    EXPECT_EQ(opendal_operator_delete(this->p, "empty-writer"), nullptr);
}

TEST_F(OpendalBddTest, RejectNonEmptyNullBytes)
{
    const opendal_bytes invalid = {
        .data = nullptr,
        .len = 1,
        .capacity = 0,
    };

    opendal_error* error = opendal_operator_write(this->p, "invalid", &invalid);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_UNEXPECTED);
    opendal_error_free(error);

    error = opendal_operator_write_with(this->p, "invalid-with-options", &invalid, nullptr);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_UNEXPECTED);
    opendal_error_free(error);

    opendal_writer* writer = nullptr;
    error = opendal_operator_writer(this->p, "invalid-writer", &writer);
    EXPECT_EQ(error, nullptr);
    ASSERT_NE(writer, nullptr);

    uintptr_t write_size = 99;
    error = opendal_writer_write(writer, &invalid, &write_size);
    EXPECT_EQ(write_size, 0);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_UNEXPECTED);
    opendal_error_free(error);
    opendal_writer_free(writer);
}

TEST_F(OpendalBddTest, RejectEmptyNonNullBytes)
{
    uint8_t sentinel = 0;
    const opendal_bytes invalid = {
        .data = &sentinel,
        .len = 0,
        .capacity = 0,
    };

    opendal_error* error = opendal_operator_write(this->p, "invalid-empty", &invalid);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_UNEXPECTED);
    opendal_error_free(error);

    error = opendal_operator_write_with(this->p, "invalid-empty-with-options", &invalid, nullptr);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_UNEXPECTED);
    opendal_error_free(error);

    opendal_writer* writer = nullptr;
    error = opendal_operator_writer(this->p, "invalid-empty-writer", &writer);
    EXPECT_EQ(error, nullptr);
    ASSERT_NE(writer, nullptr);

    uintptr_t write_size = 99;
    error = opendal_writer_write(writer, &invalid, &write_size);
    EXPECT_EQ(write_size, 0);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_UNEXPECTED);
    opendal_error_free(error);
    opendal_writer_free(writer);
}

TEST_F(OpendalBddTest, OutputParametersAndFailureInitTest)
{
    // 1. opendal_operator_new / opendal_operator_new_with_layers failure initializes out to NULL
    opendal_operator* bad_op = reinterpret_cast<opendal_operator*>(0x1);
    opendal_error* error = opendal_operator_new("not-a-valid-scheme", nullptr, &bad_op);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(bad_op, nullptr);
    opendal_error_free(error);

    bad_op = reinterpret_cast<opendal_operator*>(0x1);
    error = opendal_operator_new_with_layers("not-a-valid-scheme", nullptr, nullptr, &bad_op);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(bad_op, nullptr);
    opendal_error_free(error);

    // 2. opendal_operator_write_with_metadata on failure and success
    const opendal_bytes invalid_bytes = { .data = nullptr, .len = 1, .capacity = 0 };
    opendal_metadata* meta = reinterpret_cast<opendal_metadata*>(0x1);
    error = opendal_operator_write_with_metadata(this->p, "meta-write", &invalid_bytes, nullptr, &meta);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(meta, nullptr);
    opendal_error_free(error);

    const opendal_bytes valid_bytes = {
        .data = (uint8_t*)this->content.c_str(),
        .len = this->content.length(),
        .capacity = 0,
    };
    error = opendal_operator_write_with_metadata(this->p, "meta-write", &valid_bytes, nullptr, &meta);
    EXPECT_EQ(error, nullptr);
    ASSERT_NE(meta, nullptr);
    EXPECT_TRUE(opendal_metadata_is_file(meta));
    opendal_metadata_free(meta);

    // 3. opendal_operator_read / opendal_operator_read_with on failure resets uninitialized/sentinel bytes without dropping
    uint8_t stack_byte = 42;
    opendal_bytes read_out = { .data = &stack_byte, .len = 123, .capacity = 456 };
    error = opendal_operator_read(this->p, "non-existent-file", &read_out);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_NOT_FOUND);
    EXPECT_EQ(read_out.data, nullptr);
    EXPECT_EQ(read_out.len, 0);
    EXPECT_EQ(read_out.capacity, 0);
    opendal_error_free(error);

    read_out = { .data = &stack_byte, .len = 123, .capacity = 456 };
    error = opendal_operator_read_with(this->p, "non-existent-file", nullptr, &read_out);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_NOT_FOUND);
    EXPECT_EQ(read_out.data, nullptr);
    EXPECT_EQ(read_out.len, 0);
    EXPECT_EQ(read_out.capacity, 0);
    opendal_error_free(error);

    // 4. opendal_operator_stat / opendal_operator_stat_with on failure resets meta to NULL
    meta = reinterpret_cast<opendal_metadata*>(0x1);
    error = opendal_operator_stat(this->p, "non-existent-file", &meta);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_NOT_FOUND);
    EXPECT_EQ(meta, nullptr);
    opendal_error_free(error);

    meta = reinterpret_cast<opendal_metadata*>(0x1);
    error = opendal_operator_stat_with(this->p, "non-existent-file", nullptr, &meta);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_NOT_FOUND);
    EXPECT_EQ(meta, nullptr);
    opendal_error_free(error);

    // 5. opendal_operator_is_exist and opendal_operator_exists
    bool is_exist = true;
    error = opendal_operator_is_exist(this->p, "non-existent-file", &is_exist);
    EXPECT_EQ(error, nullptr);
    EXPECT_FALSE(is_exist);
    error = opendal_operator_is_exist(this->p, "meta-write", &is_exist);
    EXPECT_EQ(error, nullptr);
    EXPECT_TRUE(is_exist);

    // 6. opendal_writer_close_with_metadata (valid and null) and copier_next through completion
    opendal_writer* writer = nullptr;
    error = opendal_operator_writer_with(this->p, "writer-meta", nullptr, &writer);
    EXPECT_EQ(error, nullptr);
    ASSERT_NE(writer, nullptr);
    uintptr_t written = 0;
    error = opendal_writer_write(writer, &valid_bytes, &written);
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(written, valid_bytes.len);
    meta = nullptr;
    error = opendal_writer_close_with_metadata(writer, &meta);
    EXPECT_EQ(error, nullptr);
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(opendal_metadata_content_length(meta), valid_bytes.len);
    opendal_metadata_free(meta);
    opendal_writer_free(writer);

    meta = reinterpret_cast<opendal_metadata*>(0x1);
    error = opendal_writer_close_with_metadata(nullptr, &meta);
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(meta, nullptr);

    opendal_copier* copier = reinterpret_cast<opendal_copier*>(0x1);
    error = opendal_operator_copier(this->p, "meta-write", "meta-write-copy", &copier);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_UNSUPPORTED);
    EXPECT_EQ(copier, nullptr);
    opendal_error_free(error);

    opendal_operator_options* fs_opts = opendal_operator_options_new();
    opendal_operator_options_set(fs_opts, "root", "/tmp");
    opendal_operator* fs_op = nullptr;
    error = opendal_operator_new("fs", fs_opts, &fs_op);
    opendal_operator_options_free(fs_opts);
    ASSERT_EQ(error, nullptr);
    ASSERT_NE(fs_op, nullptr);

    EXPECT_EQ(opendal_operator_write(fs_op, "opendal_c_copier_src", &valid_bytes), nullptr);
    copier = nullptr;
    error = opendal_operator_copier(fs_op, "opendal_c_copier_src", "opendal_c_copier_dst", &copier);
    ASSERT_EQ(error, nullptr);
    ASSERT_NE(copier, nullptr);
    bool has_next = true;
    uintptr_t copy_size = 0;
    while (has_next) {
        error = opendal_copier_next(copier, &copy_size, &has_next);
        ASSERT_EQ(error, nullptr);
    }
    EXPECT_EQ(copy_size, 0);
    EXPECT_FALSE(has_next);
    opendal_copier_free(copier);
    EXPECT_EQ(opendal_operator_delete(fs_op, "opendal_c_copier_src"), nullptr);
    EXPECT_EQ(opendal_operator_delete(fs_op, "opendal_c_copier_dst"), nullptr);
    opendal_operator_free(fs_op);

    // 7. Presign failure on memory backend initializes req output to NULL
    opendal_presigned_request* req = reinterpret_cast<opendal_presigned_request*>(0x1);
    error = opendal_operator_presign_read(this->p, "meta-write", 60, &req);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, OPENDAL_UNSUPPORTED);
    EXPECT_EQ(req, nullptr);
    opendal_error_free(error);

    EXPECT_EQ(opendal_operator_delete(this->p, "meta-write"), nullptr);
    EXPECT_EQ(opendal_operator_delete(this->p, "writer-meta"), nullptr);
}

TEST_F(OpendalBddTest, OptionStringSelfAliasingSafe)
{
    opendal_list_options* list_opts = opendal_list_options_new();
    ASSERT_NE(list_opts, nullptr);
    opendal_list_options_set_start_after(list_opts, "dir/file_a");
    ASSERT_NE(list_opts->start_after, nullptr);
    opendal_list_options_set_start_after(list_opts, list_opts->start_after);
    EXPECT_STREQ(list_opts->start_after, "dir/file_a");
    opendal_list_options_set_start_after(list_opts, list_opts->start_after + 4);
    EXPECT_STREQ(list_opts->start_after, "file_a");
    opendal_list_options_set_start_after(list_opts, nullptr);
    EXPECT_EQ(list_opts->start_after, nullptr);
    opendal_list_options_free(list_opts);

    opendal_delete_options* del_opts = opendal_delete_options_new();
    ASSERT_NE(del_opts, nullptr);
    opendal_delete_options_set_version(del_opts, "v1/rev42");
    ASSERT_NE(del_opts->version, nullptr);
    opendal_delete_options_set_version(del_opts, del_opts->version);
    EXPECT_STREQ(del_opts->version, "v1/rev42");
    opendal_delete_options_set_version(del_opts, del_opts->version + 3);
    EXPECT_STREQ(del_opts->version, "rev42");
    opendal_delete_options_set_version(del_opts, nullptr);
    EXPECT_EQ(del_opts->version, nullptr);
    opendal_delete_options_free(del_opts);
}

TEST_F(OpendalBddTest, NullPointerGuards)
{
    EXPECT_EQ(opendal_presigned_request_method(nullptr), nullptr);
    EXPECT_EQ(opendal_presigned_request_uri(nullptr), nullptr);
    EXPECT_EQ(opendal_presigned_request_headers(nullptr), nullptr);
    EXPECT_EQ(opendal_presigned_request_headers_len(nullptr), 0);

    opendal_operator_options_set(nullptr, "root", "/myroot");
    opendal_operator_options* opts = opendal_operator_options_new();
    opendal_operator_options_set(opts, nullptr, "/myroot");
    opendal_operator_options_set(opts, "root", nullptr);
    opendal_operator_options_free(opts);

    opendal_operator_layers_add_retry(nullptr, true, 2.0f, 1000, 10000, 3);
    opendal_operator_layers_add_timeout(nullptr, 1000, 2000);
}

TEST_F(OpendalBddTest, RejectOutOfRangeConditionalTimestamps)
{
    const int64_t max_ts = std::numeric_limits<int64_t>::max();
    const int64_t min_ts = std::numeric_limits<int64_t>::min();

    opendal_stat_options* stat_opts = opendal_stat_options_new();
    opendal_stat_options_set_if_modified_since(stat_opts, max_ts);
    opendal_metadata* meta = reinterpret_cast<opendal_metadata*>(0x1);
    opendal_error* err = opendal_operator_stat_with(this->p, "any", stat_opts, &meta);
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(err->code, OPENDAL_CONFIG_INVALID);
    EXPECT_EQ(meta, nullptr);
    opendal_error_free(err);

    opendal_presigned_request* req = reinterpret_cast<opendal_presigned_request*>(0x1);
    err = opendal_operator_presign_stat_with(this->p, "any", 60, stat_opts, &req);
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(err->code, OPENDAL_CONFIG_INVALID);
    EXPECT_EQ(req, nullptr);
    opendal_error_free(err);
    opendal_stat_options_free(stat_opts);

    opendal_read_options* read_opts = opendal_read_options_new();
    opendal_read_options_set_if_unmodified_since(read_opts, min_ts);
    opendal_bytes bytes = {
        .data = reinterpret_cast<uint8_t*>(0x1),
        .len = 99,
        .capacity = 99,
    };
    err = opendal_operator_read_with(this->p, "any", read_opts, &bytes);
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(err->code, OPENDAL_CONFIG_INVALID);
    EXPECT_EQ(bytes.data, nullptr);
    EXPECT_EQ(bytes.len, 0);
    opendal_error_free(err);

    req = reinterpret_cast<opendal_presigned_request*>(0x1);
    err = opendal_operator_presign_read_with(this->p, "any", 60, read_opts, &req);
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(err->code, OPENDAL_CONFIG_INVALID);
    EXPECT_EQ(req, nullptr);
    opendal_error_free(err);
    opendal_read_options_free(read_opts);

    opendal_reader_options* reader_opts = opendal_reader_options_new();
    opendal_reader_options_set_if_modified_since(reader_opts, max_ts);
    opendal_reader* reader = reinterpret_cast<opendal_reader*>(0x1);
    err = opendal_operator_reader_with(this->p, "any", reader_opts, &reader);
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(err->code, OPENDAL_CONFIG_INVALID);
    EXPECT_EQ(reader, nullptr);
    opendal_error_free(err);
    opendal_reader_options_free(reader_opts);
}
