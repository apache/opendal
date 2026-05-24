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

void test_operator_with_timeout_layer(opendal_test_context* ctx)
{
    opendal_operator_layers* layers = opendal_operator_layers_new();
    OPENDAL_ASSERT_NOT_NULL(layers, "Should create operator layers");

    opendal_operator_layers_add_timeout(layers, 60ULL * 1000 * 1000 * 1000, 10ULL * 1000 * 1000 * 1000);

    opendal_result_operator_new result = opendal_operator_new_with_layers(
        ctx->config->scheme, ctx->config->options, layers);
    opendal_operator_layers_free(layers);

    OPENDAL_ASSERT_NO_ERROR(result.error, "Operator with timeout layer should be created");
    OPENDAL_ASSERT_NOT_NULL(result.op, "Operator with timeout layer should not be null");

    opendal_operator_info* info = opendal_operator_info_new(result.op);
    OPENDAL_ASSERT_NOT_NULL(info, "Layered operator info should not be null");
    opendal_operator_info_free(info);

    const char* path = "test_timeout_layer.txt";
    const char* content = "Hello, OpenDAL timeout layer!";
    opendal_bytes data;
    data.data = (uint8_t*)content;
    data.len = strlen(content);
    data.capacity = strlen(content);

    opendal_error* error = opendal_operator_write(result.op, path, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write with timeout layer should succeed");

    opendal_result_read read_result = opendal_operator_read(result.op, path);
    OPENDAL_ASSERT_NO_ERROR(read_result.error, "Read with timeout layer should succeed");
    OPENDAL_ASSERT_EQ(strlen(content), read_result.data.len, "Read data length should match");
    OPENDAL_ASSERT(memcmp(content, read_result.data.data, read_result.data.len) == 0,
        "Read content should match");

    opendal_bytes_free(&read_result.data);
    opendal_operator_delete(result.op, path);
    opendal_operator_free(result.op);
}

void test_operator_with_retry_layer(opendal_test_context* ctx)
{
    opendal_operator_layers* layers = opendal_operator_layers_new();
    OPENDAL_ASSERT_NOT_NULL(layers, "Should create operator layers");

    opendal_operator_layers_add_retry(layers, 1, 2.0f, 1000ULL * 1000 * 1000, 60ULL * 1000 * 1000 * 1000, 3);

    opendal_result_operator_new result = opendal_operator_new_with_layers(
        ctx->config->scheme, ctx->config->options, layers);
    opendal_operator_layers_free(layers);

    OPENDAL_ASSERT_NO_ERROR(result.error, "Operator with retry layer should be created");
    OPENDAL_ASSERT_NOT_NULL(result.op, "Operator with retry layer should not be null");

    opendal_operator_info* info = opendal_operator_info_new(result.op);
    OPENDAL_ASSERT_NOT_NULL(info, "Layered operator info should not be null");
    opendal_operator_info_free(info);

    const char* path = "test_retry_layer.txt";
    const char* content = "Hello, OpenDAL retry layer!";
    opendal_bytes data;
    data.data = (uint8_t*)content;
    data.len = strlen(content);
    data.capacity = strlen(content);

    opendal_error* error = opendal_operator_write(result.op, path, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write with retry layer should succeed");

    opendal_result_read read_result = opendal_operator_read(result.op, path);
    OPENDAL_ASSERT_NO_ERROR(read_result.error, "Read with retry layer should succeed");
    OPENDAL_ASSERT_EQ(strlen(content), read_result.data.len, "Read data length should match");
    OPENDAL_ASSERT(memcmp(content, read_result.data.data, read_result.data.len) == 0,
        "Read content should match");

    opendal_bytes_free(&read_result.data);
    opendal_operator_delete(result.op, path);
    opendal_operator_free(result.op);
}

opendal_test_case layer_tests[] = {
    { "operator_with_retry_layer", test_operator_with_retry_layer, make_capability_read_write() },
    { "operator_with_timeout_layer", test_operator_with_timeout_layer, make_capability_read_write() },
};

opendal_test_suite layer_suite = {
    "Layer",
    layer_tests,
    sizeof(layer_tests) / sizeof(layer_tests[0])
};
