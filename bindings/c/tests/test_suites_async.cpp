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

#include <cstring>
#include <unistd.h>

static void poll_until_ready(opendal_future_status (*poll_fn)(void*, void*),
    void* future,
    void* out,
    opendal_future_status* final_status)
{
    const int max_attempts = 1000;
    int attempts = 0;
    opendal_future_status status;
    do {
        status = poll_fn(future, out);
        if (status == OPENDAL_FUTURE_PENDING) {
            usleep(1000); // yield briefly while waiting for runtime
        } else {
            break;
        }
    } while (++attempts < max_attempts);
    *final_status = status;
}

static void test_async_stat_await(opendal_test_context* ctx)
{
    opendal_result_operator_new async_res =
        opendal_async_operator_from_operator(ctx->config->operator_instance);
    OPENDAL_ASSERT_NO_ERROR(async_res.error, "Async operator creation should succeed");
    OPENDAL_ASSERT_NOT_NULL(async_res.op, "Async operator pointer must not be NULL");
    const opendal_async_operator* async_op = (const opendal_async_operator*)async_res.op;
    const char* path = "async_stat_await.txt";
    const char* content = "async stat await";

    opendal_bytes data;
    data.data = (uint8_t*)content;
    data.len = strlen(content);
    data.capacity = strlen(content);

    opendal_result_future_write wf = opendal_async_operator_write(async_op, path, &data);
    OPENDAL_ASSERT_NO_ERROR(wf.error, "Async write setup should succeed");
    opendal_error* write_err = opendal_future_write_await(wf.future);
    OPENDAL_ASSERT_NO_ERROR(write_err, "Awaiting async write should succeed");

    opendal_result_future_stat fut = opendal_async_operator_stat(async_op, path);
    OPENDAL_ASSERT_NO_ERROR(fut.error, "opendal_async_operator_stat should succeed");
    OPENDAL_ASSERT_NOT_NULL(fut.future, "Stat future pointer should not be NULL");

    opendal_result_stat stat = opendal_future_stat_await(fut.future);
    OPENDAL_ASSERT_NO_ERROR(stat.error, "Awaiting stat future should not fail");
    OPENDAL_ASSERT_NOT_NULL(stat.meta, "Stat metadata should be available");
    OPENDAL_ASSERT(opendal_metadata_is_file(stat.meta), "Metadata should mark the entry as file");
    OPENDAL_ASSERT_EQ(strlen(content), opendal_metadata_content_length(stat.meta),
        "Content length should match async stat await test payload");

    opendal_metadata_free(stat.meta);

    opendal_result_future_delete df = opendal_async_operator_delete(async_op, path);
    OPENDAL_ASSERT_NO_ERROR(df.error, "Async delete future should be created successfully");
    opendal_error* delete_err = opendal_future_delete_await(df.future);
    OPENDAL_ASSERT_NO_ERROR(delete_err, "Async delete should succeed");

    opendal_async_operator_free(async_op);
}

static opendal_future_status stat_poll_bridge(void* fut, void* out)
{
    return opendal_future_stat_poll((struct opendal_future_stat*)fut, (struct opendal_result_stat*)out);
}

static opendal_future_status read_poll_bridge(void* fut, void* out)
{
    return opendal_future_read_poll((struct opendal_future_read*)fut, (struct opendal_result_read*)out);
}

static opendal_future_status write_poll_bridge(void* fut, void* err_out)
{
    return opendal_future_write_poll((struct opendal_future_write*)fut, (struct opendal_error**)err_out);
}

static opendal_future_status poll_stat_ready(struct opendal_future_stat* future, opendal_result_stat* out)
{
    opendal_future_status status;
    poll_until_ready(stat_poll_bridge, future, out, &status);
    return status;
}

static void test_async_stat_poll_not_found(opendal_test_context* ctx)
{
    opendal_result_operator_new async_res =
        opendal_async_operator_from_operator(ctx->config->operator_instance);
    OPENDAL_ASSERT_NO_ERROR(async_res.error, "Async operator creation should succeed");
    OPENDAL_ASSERT_NOT_NULL(async_res.op, "Async operator pointer must not be NULL");
    const opendal_async_operator* async_op = (const opendal_async_operator*)async_res.op;
    const char* path = "async_stat_poll_missing.txt";

    opendal_result_future_stat fut = opendal_async_operator_stat(async_op, path);
    OPENDAL_ASSERT_NO_ERROR(fut.error, "Stat future creation should succeed");
    opendal_result_stat stat_out;
    memset(&stat_out, 0, sizeof(stat_out));
    opendal_future_status status = poll_stat_ready(fut.future, &stat_out);

    OPENDAL_ASSERT(status == OPENDAL_FUTURE_READY, "Stat poll should eventually become ready");
    OPENDAL_ASSERT_NULL(stat_out.meta, "Metadata should be NULL for missing object");
    OPENDAL_ASSERT_NOT_NULL(stat_out.error, "Missing object should report an error");
    opendal_error_free(stat_out.error);
    opendal_future_stat_free(fut.future);
    opendal_async_operator_free(async_op);
}

static opendal_future_status poll_read_ready(struct opendal_future_read* future, opendal_result_read* out)
{
    opendal_future_status status;
    poll_until_ready(read_poll_bridge, future, out, &status);
    return status;
}

static opendal_future_status poll_write_ready(struct opendal_future_write* future, opendal_error** error_out)
{
    opendal_future_status status;
    poll_until_ready(write_poll_bridge, future, error_out, &status);
    return status;
}

static void test_async_read_write_poll(opendal_test_context* ctx)
{
    opendal_result_operator_new async_res =
        opendal_async_operator_from_operator(ctx->config->operator_instance);
    OPENDAL_ASSERT_NO_ERROR(async_res.error, "Async operator creation should succeed");
    OPENDAL_ASSERT_NOT_NULL(async_res.op, "Async operator pointer must not be NULL");
    const opendal_async_operator* async_op = (const opendal_async_operator*)async_res.op;
    const char* path = "async_read_write_poll.txt";
    const char* payload = "async read/write poll payload";

    opendal_bytes data;
    data.data = (uint8_t*)payload;
    data.len = strlen(payload);
    data.capacity = strlen(payload);

    opendal_result_future_write wf = opendal_async_operator_write(async_op, path, &data);
    OPENDAL_ASSERT_NO_ERROR(wf.error, "Async write future creation should succeed");
    opendal_error* write_result = NULL;
    opendal_future_status write_status = poll_write_ready(wf.future, &write_result);
    OPENDAL_ASSERT(write_status == OPENDAL_FUTURE_READY, "Write poll should finish with READY");
    OPENDAL_ASSERT_NULL(write_result, "Write operation should succeed");
    opendal_future_write_free(wf.future);

    opendal_result_future_read rf = opendal_async_operator_read(async_op, path);
    OPENDAL_ASSERT_NO_ERROR(rf.error, "Async read future creation should succeed");

    opendal_result_read read_result;
    memset(&read_result, 0, sizeof(read_result));
    opendal_future_status read_status = poll_read_ready(rf.future, &read_result);
    OPENDAL_ASSERT(read_status == OPENDAL_FUTURE_READY, "Read poll should finish with READY");
    OPENDAL_ASSERT_NO_ERROR(read_result.error, "Async read should succeed");
    OPENDAL_ASSERT_EQ(data.len, read_result.data.len, "Read length should match written payload");
    OPENDAL_ASSERT(memcmp(read_result.data.data, data.data, data.len) == 0, "Read payload mismatch");

    opendal_bytes_free(&read_result.data);
    opendal_future_read_free(rf.future);
    opendal_result_future_delete df = opendal_async_operator_delete(async_op, path);
    OPENDAL_ASSERT_NO_ERROR(df.error, "Async delete future should be created successfully");
    opendal_error* del_err = opendal_future_delete_await(df.future);
    OPENDAL_ASSERT_NO_ERROR(del_err, "Async delete should succeed");

    opendal_async_operator_free(async_op);
}

opendal_test_case async_tests[] = {
    { "stat_await", test_async_stat_await, make_capability_write_stat() },
    { "stat_poll_not_found", test_async_stat_poll_not_found, make_capability_write_stat() },
    { "read_write_poll", test_async_read_write_poll, make_capability_read_write() },
};

opendal_test_suite async_suite = {
    "Async APIs",
    async_tests,
    sizeof(async_tests) / sizeof(async_tests[0]),
};
