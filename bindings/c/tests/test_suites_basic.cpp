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

#include "test_framework.h"

// Test: Basic check operation
void test_check(opendal_test_context* ctx)
{
    // Get operator capabilities first
    opendal_operator_info* info = opendal_operator_info_new(ctx->config->operator_instance);
    OPENDAL_ASSERT_NOT_NULL(info, "Should be able to get operator info");

    opendal_capability cap;
    opendal_operator_info_get_capability(info, &cap);

    if (cap.list) {
        // Only perform the standard check if list operations are supported
        opendal_error* error = opendal_operator_check(ctx->config->operator_instance);
        OPENDAL_ASSERT_NO_ERROR(error, "Check operation should succeed");
    } else {
        // For KV adapters that don't support list, the operator creation itself is
        // the check If we got here, the operator is working properly
        printf("Note: Skipping list-based check for KV adapter\n");
    }

    opendal_operator_info_free(info);
}

// Test: Basic write and read operation
void test_write_read(opendal_test_context* ctx)
{
    const char* path = "test_write_read.txt";
    const char* content = "Hello, OpenDAL!";

    // Write data
    opendal_bytes data;
    data.data = (uint8_t*)content;
    data.len = strlen(content);
    data.capacity = strlen(content);

    opendal_error* error = opendal_operator_write(ctx->config->operator_instance, path, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write operation should succeed");

    // Read data back
    opendal_bytes read_data;
    error = opendal_operator_read(ctx->config->operator_instance, path, &read_data);
    OPENDAL_ASSERT_NO_ERROR(error, "Read operation should succeed");
    OPENDAL_ASSERT_EQ(strlen(content), read_data.len,
        "Read data length should match written data");

    // Verify content
    OPENDAL_ASSERT(memcmp(content, read_data.data, read_data.len) == 0,
        "Read content should match written content");

    // Cleanup
    opendal_bytes_free(&read_data);
    opendal_operator_delete(ctx->config->operator_instance, path);
}

// Test: Exists operation
void test_exists(opendal_test_context* ctx)
{
    const char* path = "test_exists.txt";
    const char* content = "test";

    // File should not exist initially
    bool exists = false;
    opendal_error* error = opendal_operator_exists(ctx->config->operator_instance, path, &exists);
    OPENDAL_ASSERT_NO_ERROR(error, "Exists operation should succeed");
    OPENDAL_ASSERT(!exists, "File should not exist initially");

    // Write file
    opendal_bytes data;
    data.data = (uint8_t*)content;
    data.len = strlen(content);
    data.capacity = strlen(content);

    error = opendal_operator_write(ctx->config->operator_instance, path, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write operation should succeed");

    // File should exist now
    error = opendal_operator_exists(ctx->config->operator_instance, path, &exists);
    OPENDAL_ASSERT_NO_ERROR(error, "Exists operation should succeed");
    OPENDAL_ASSERT(exists, "File should exist after write");

    // Cleanup
    opendal_operator_delete(ctx->config->operator_instance, path);
}

// Test: Stat operation
void test_stat(opendal_test_context* ctx)
{
    const char* path = "test_stat.txt";
    const char* content = "Hello, World!";

    // Write file
    opendal_bytes data;
    data.data = (uint8_t*)content;
    data.len = strlen(content);
    data.capacity = strlen(content);

    opendal_error* error = opendal_operator_write(ctx->config->operator_instance, path, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write operation should succeed");

    // Stat file
    opendal_metadata* meta = NULL;
    error = opendal_operator_stat(ctx->config->operator_instance, path, &meta);
    OPENDAL_ASSERT_NO_ERROR(error, "Stat operation should succeed");
    OPENDAL_ASSERT_NOT_NULL(meta, "Metadata should not be null");

    // Check file properties
    OPENDAL_ASSERT(opendal_metadata_is_file(meta),
        "Should be identified as file");
    OPENDAL_ASSERT(!opendal_metadata_is_dir(meta),
        "Should not be identified as directory");
    OPENDAL_ASSERT_EQ(strlen(content),
        opendal_metadata_content_length(meta),
        "Content length should match");

    // Cleanup
    opendal_metadata_free(meta);
    opendal_operator_delete(ctx->config->operator_instance, path);
}

// Test: Delete operation
void test_delete(opendal_test_context* ctx)
{
    const char* path = "test_delete.txt";
    const char* content = "to be deleted";

    // Write file
    opendal_bytes data;
    data.data = (uint8_t*)content;
    data.len = strlen(content);
    data.capacity = strlen(content);

    opendal_error* error = opendal_operator_write(ctx->config->operator_instance, path, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write operation should succeed");

    // Verify file exists
    bool exists = false;
    error = opendal_operator_exists(ctx->config->operator_instance, path, &exists);
    OPENDAL_ASSERT_NO_ERROR(error,
        "Exists operation should succeed");
    OPENDAL_ASSERT(exists, "File should exist before deletion");

    // Delete file
    error = opendal_operator_delete(ctx->config->operator_instance, path);
    OPENDAL_ASSERT_NO_ERROR(error, "Delete operation should succeed");

    // Verify file no longer exists
    error = opendal_operator_exists(ctx->config->operator_instance, path, &exists);
    OPENDAL_ASSERT_NO_ERROR(error,
        "Exists operation should succeed");
    OPENDAL_ASSERT(!exists, "File should not exist after deletion");

    // Delete should be idempotent
    error = opendal_operator_delete(ctx->config->operator_instance, path);
    OPENDAL_ASSERT_NO_ERROR(error, "Delete operation should be idempotent");
}

// Test: Create directory operation
void test_create_dir(opendal_test_context* ctx)
{
    const char* dir_path = "test_dir/";

    // Create directory
    opendal_error* error = opendal_operator_create_dir(ctx->config->operator_instance, dir_path);
    OPENDAL_ASSERT_NO_ERROR(error, "Create dir operation should succeed");

    // Verify directory exists
    bool exists = false;
    error = opendal_operator_exists(ctx->config->operator_instance, dir_path, &exists);
    OPENDAL_ASSERT_NO_ERROR(error, "Exists operation should succeed");
    OPENDAL_ASSERT(exists, "Directory should exist after creation");

    // Stat directory
    opendal_metadata* meta = NULL;
    error = opendal_operator_stat(ctx->config->operator_instance, dir_path, &meta);
    OPENDAL_ASSERT_NO_ERROR(error, "Stat operation should succeed");
    OPENDAL_ASSERT(opendal_metadata_is_dir(meta),
        "Should be identified as directory");
    OPENDAL_ASSERT(!opendal_metadata_is_file(meta),
        "Should not be identified as file");

    // Cleanup
    opendal_metadata_free(meta);
    opendal_operator_delete(ctx->config->operator_instance, dir_path);
}

// Define the basic test suite
opendal_test_case basic_tests[] = {
    { "check", test_check, NO_CAPABILITY },
    { "write_read", test_write_read, make_capability_read_write() },
    { "exists", test_exists, make_capability_write() },
    { "stat", test_stat, make_capability_write_stat() },
    { "delete", test_delete, make_capability_write_delete() },
    { "create_dir", test_create_dir, make_capability_create_dir() },
};

opendal_test_suite basic_suite = {
    "Basic Operations", // name
    basic_tests, // tests
    sizeof(basic_tests) / sizeof(basic_tests[0]) // test_count
};
