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

// Test: Default options read operation
void test_read_options_default(opendal_test_context* ctx)
{
    const char* path = "test_read_options.txt";
    const char* content = "Hello, OpenDAL Read Options!";
    size_t content_len = strlen(content);

    // Write test data first
    opendal_bytes data = {
        .data = (uint8_t*)content,
        .len = content_len,
        .capacity = content_len
    };

    opendal_error* error = opendal_operator_write(ctx->config->operator_instance, path, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write operation should succeed");

    // Read datat back with default options
    opendal_operator_options_read opts = {};
    opendal_result_read result = opendal_operator_read_options(ctx->config->operator_instance, path, &opts);
    OPENDAL_ASSERT_NO_ERROR(result.error, "Options Read operation should succeed");
    OPENDAL_ASSERT_EQ(content_len, result.data.len,
        "Read data length should match written data");

    // Verify content
    OPENDAL_ASSERT(memcmp(content, result.data.data, result.data.len) == 0,
        "Read content should match written content");

    // Cleanup
    opendal_bytes_free(&result.data);
    opendal_operator_delete(ctx->config->operator_instance, path);
}

// Test: range options read operation
void test_read_options_range(opendal_test_context* ctx)
{
    const char* path = "test_read_options.txt";
    const char* content = "Hello, OpenDAL Read Options!";
    size_t content_len = strlen(content);

    // Write test data first
    opendal_bytes data = {
        .data = (uint8_t*)content,
        .len = content_len,
        .capacity = content_len
    };

    opendal_error* error = opendal_operator_write(ctx->config->operator_instance, path, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write operation should succeed");

    // Read datat back with default options
    opendal_operator_options_read opts = {};
    uint64_t range[2] = { 0, 14 };
    opts.range = range;
    opendal_result_read result = opendal_operator_read_options(ctx->config->operator_instance, path, &opts);
    OPENDAL_ASSERT_NO_ERROR(result.error, "Options Read operation should succeed");
    OPENDAL_ASSERT_EQ(14, result.data.len,
        "Read data length should match written data");

    // Verify content
    OPENDAL_ASSERT(memcmp(content, result.data.data, result.data.len) == 0,
        "Read content should match written content");

    // Cleanup
    opendal_bytes_free(&result.data);
    opendal_operator_delete(ctx->config->operator_instance, path);
}

// Test: range options read operation
void test_read_options_timestamp(opendal_test_context* ctx)
{
    const char* path = "test_read_options.txt";
    const char* content = "Hello, OpenDAL Read Options!";
    size_t content_len = strlen(content);

    if (strcmp(ctx->config->scheme, "memory") == 0 || strcmp(ctx->config->scheme, "fs") == 0) {
        opendal_operator_delete(ctx->config->operator_instance, path);

        return;
    }

    // Write test data first
    opendal_bytes data = {
        .data = (uint8_t*)content,
        .len = content_len,
        .capacity = content_len
    };

    opendal_error* error = opendal_operator_write(ctx->config->operator_instance, path, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write operation should succeed");

    // Read datat back with default options
    opendal_operator_options_read opts = {};
    opts.if_modified_since = "2022-03-13T07:20:04Z";
    opendal_result_read result = opendal_operator_read_options(ctx->config->operator_instance, path, &opts);
    OPENDAL_ASSERT_NO_ERROR(result.error, "Options Read operation should succeed");
    OPENDAL_ASSERT_EQ(14, result.data.len,
        "Read data length should match written data");

    // Verify content
    OPENDAL_ASSERT(memcmp(content, result.data.data, result.data.len) == 0,
        "Read content should match written content");

    // Cleanup
    opendal_bytes_free(&result.data);
    opendal_operator_delete(ctx->config->operator_instance, path);
}

// Define the options read/write test suite
opendal_test_case options_tests[] = {
    { "read_options_default", test_read_options_default, make_capability_read_write() },
    { "read_options_range", test_read_options_range, make_capability_read_write() },
    { "read_options_timestamp", test_read_options_timestamp, make_capability_read_write() },
};

opendal_test_suite options_suite = {
    "Options Operations", // name
    options_tests, // tests
    sizeof(options_tests) / sizeof(options_tests[0]) // test_count
};
