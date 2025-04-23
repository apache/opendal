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
#include <atomic> // For std::atomic_bool
#include <condition_variable> // For synchronization
#include <mutex> // For synchronization
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h> // For usleep
// Include the generated OpenDAL C header
extern "C" {
#include "opendal.h"
}

// Structure to hold callback results and synchronization primitives
typedef struct {
    std::atomic_bool completed;
    std::mutex mtx;
    std::condition_variable cv;
    opendal_result_stat result; // Store the result here
} callback_args_t;

// C-style callback function
extern "C" void test_stat_callback(opendal_result_stat result, void* user_data)
{
    callback_args_t* args = static_cast<callback_args_t*>(user_data);

    // Store the result
    args->result = result; // Shallow copy is okay here

    // Notify the waiting thread
    {
        std::lock_guard<std::mutex> lock(args->mtx);
        args->completed.store(true);
    }
    args->cv.notify_one();
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

TEST_F(OpendalAsyncStatTest, AsyncStatCallbackTest)
{
    // Initialize callback arguments and synchronization primitives
    callback_args_t cb_args;
    cb_args.completed.store(false);
    cb_args.result.meta = nullptr; // Initialize result fields
    cb_args.result.error = nullptr;

    // Call async stat for a non-existent file with the callback
    const char* path = "non_existent_file.txt";
    opendal_async_operator_stat_with_callback(this->op, path, test_stat_callback, &cb_args);

    // Wait for the callback to complete
    {
        std::unique_lock<std::mutex> lock(cb_args.mtx);
        cb_args.cv.wait(lock, [&cb_args] { return cb_args.completed.load(); });
    }

    // Verify the result received by the callback
    EXPECT_TRUE(cb_args.result.meta == nullptr); // Meta should be NULL for error
    EXPECT_TRUE(cb_args.result.error != nullptr); // Error should be non-NULL

    if (cb_args.result.error) {
        EXPECT_EQ(cb_args.result.error->code, OPENDAL_NOT_FOUND);
        // Free the error received by the callback
        opendal_error_free(cb_args.result.error);
    }
    // No metadata to free in case of error
}
