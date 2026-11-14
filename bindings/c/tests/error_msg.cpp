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
extern "C" {
#include "opendal.h"
}

class OpendalErrorTest : public ::testing::Test {
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

// Test no memory leak of error message
TEST_F(OpendalErrorTest, ErrorReadTest)
{
    // Initialize a operator for "memory" backend, with no options
    // The read is supposed to fail
    opendal_bytes data = { nullptr, 0, 0 };
    opendal_error* error = opendal_operator_read(this->p, "/testpath", &data);
    ASSERT_NE(error, nullptr);
    ASSERT_EQ(error->code, OPENDAL_NOT_FOUND);

    // Lets check the error message out
    struct opendal_bytes* error_msg = &error->message;
    ASSERT_NE(error_msg->data, nullptr);
    ASSERT_GT(error_msg->len, 0);

    // the opendal_bytes read is heap allocated, please free it
    opendal_bytes_free(&data);

    // free the error
    opendal_error_free(error);
}
