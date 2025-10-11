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
