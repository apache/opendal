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

#include "gtest/gtest.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// Include the generated OpenDAL C header
extern "C" {
#include "opendal.h"
}

class OpendalAsyncStatTest : public ::testing::Test {
protected:
    const opendal_async_operator* op;

    void SetUp() override
    {
        opendal_result_operator_new result_op = opendal_async_operator_new("memory", NULL);
        EXPECT_TRUE(result_op.error == nullptr);
        EXPECT_TRUE(result_op.op != nullptr);

        // Cast is necessary because opendal_async_operator_new reuses opendal_result_operator_new
        this->op = reinterpret_cast<const opendal_async_operator*>(result_op.op);
        EXPECT_TRUE(this->op);
    }

    void TearDown() override
    {
        opendal_async_operator_free(this->op); // Use the async free function
    }
};

TEST_F(OpendalAsyncStatTest, AsyncStatAwaitStyle)
{
    const char* path = "non_existent_file.txt";
    opendal_result_future_stat future_result = opendal_async_operator_stat(this->op, path);
    ASSERT_TRUE(future_result.error == nullptr);
    ASSERT_TRUE(future_result.future != nullptr);

    opendal_result_stat stat_result = opendal_future_stat_await(future_result.future);
    EXPECT_TRUE(stat_result.meta == nullptr);
    ASSERT_TRUE(stat_result.error != nullptr);
    EXPECT_EQ(stat_result.error->code, OPENDAL_NOT_FOUND);
    opendal_error_free(stat_result.error);
}

TEST_F(OpendalAsyncStatTest, AsyncStatFreeFuture)
{
    const char* path = "non_existent_file.txt";
    opendal_result_future_stat future_result = opendal_async_operator_stat(this->op, path);
    ASSERT_TRUE(future_result.error == nullptr);
    ASSERT_TRUE(future_result.future != nullptr);

    opendal_future_stat_free(future_result.future);
    // Nothing to assert beyond not crashing; the future is cancelled.
}

TEST_F(OpendalAsyncStatTest, AsyncWriteThenRead)
{
    const char* path = "async_write_read.txt";
    const char* payload = "hello async";
    opendal_bytes data = {
        .data = (uint8_t*)payload,
        .len = strlen(payload),
        .capacity = strlen(payload),
    };

    opendal_result_future_write write_result = opendal_async_operator_write(this->op, path, &data);
    ASSERT_TRUE(write_result.error == nullptr);
    ASSERT_TRUE(write_result.future != nullptr);

    opendal_error* write_err = opendal_future_write_await(write_result.future);
    ASSERT_TRUE(write_err == nullptr);

    opendal_result_future_read read_result = opendal_async_operator_read(this->op, path);
    ASSERT_TRUE(read_result.error == nullptr);
    ASSERT_TRUE(read_result.future != nullptr);

    opendal_result_read read_out = opendal_future_read_await(read_result.future);
    ASSERT_TRUE(read_out.error == nullptr);
    ASSERT_EQ(read_out.data.len, strlen(payload));
    EXPECT_EQ(memcmp(read_out.data.data, payload, read_out.data.len), 0);
    opendal_bytes_free(&read_out.data);

    opendal_result_future_delete delete_result = opendal_async_operator_delete(this->op, path);
    ASSERT_TRUE(delete_result.error == nullptr);
    opendal_error* delete_err = opendal_future_delete_await(delete_result.future);
    ASSERT_TRUE(delete_err == nullptr);
}

TEST_F(OpendalAsyncStatTest, AsyncDeleteMakesStatReturnNotFound)
{
    const char* path = "async_delete.txt";
    const char* payload = "cleanup";
    opendal_bytes data = {
        .data = (uint8_t*)payload,
        .len = strlen(payload),
        .capacity = strlen(payload),
    };

    // Write first so the delete has work to do.
    opendal_result_future_write write_result = opendal_async_operator_write(this->op, path, &data);
    ASSERT_TRUE(write_result.error == nullptr);
    ASSERT_TRUE(opendal_future_write_await(write_result.future) == nullptr);

    opendal_result_future_delete delete_result = opendal_async_operator_delete(this->op, path);
    ASSERT_TRUE(delete_result.error == nullptr);
    ASSERT_TRUE(opendal_future_delete_await(delete_result.future) == nullptr);

    // Stat should now report not found.
    opendal_result_future_stat stat_future = opendal_async_operator_stat(this->op, path);
    ASSERT_TRUE(stat_future.error == nullptr);
    opendal_result_stat stat_result = opendal_future_stat_await(stat_future.future);
    ASSERT_TRUE(stat_result.meta == nullptr);
    ASSERT_TRUE(stat_result.error != nullptr);
    EXPECT_EQ(stat_result.error->code, OPENDAL_NOT_FOUND);
    opendal_error_free(stat_result.error);
}
