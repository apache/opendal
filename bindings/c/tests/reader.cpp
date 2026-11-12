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

class OpendalReaderTest : public ::testing::Test {
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

// Test seek
TEST_F(OpendalReaderTest, SeekTest)
{
    std::string payload = "ABCDEFGabcdefg1234567";
    opendal_bytes data = {
        .data = (uint8_t*)(payload.c_str()),
        .len = payload.length(),
    };

    // Prepare the file to be read immediately
    opendal_error* err = opendal_operator_write(this->p, "/testseek", &data);
    EXPECT_EQ(err, nullptr);

    opendal_reader* reader = nullptr;
    err = opendal_operator_reader(this->p, "/testseek", &reader);
    EXPECT_EQ(err, nullptr);
    ASSERT_NE(reader, nullptr);

    //  Test seek set
    uint64_t pos = 0;
    err = opendal_reader_seek(reader, 6, OPENDAL_SEEK_SET, &pos);
    EXPECT_EQ(pos, 6);
    EXPECT_EQ(err, nullptr);

    char buf1[64] = { 0 };
    uintptr_t read_size = 0;
    err = opendal_reader_read(reader, (uint8_t*)buf1, 7, &read_size);
    EXPECT_EQ(err, nullptr);
    EXPECT_EQ(read_size, 7);
    EXPECT_EQ(std::string(buf1), "Gabcdef");

    // Test seek cur, now we step on '3'
    err = opendal_reader_seek(reader, 3, OPENDAL_SEEK_CUR, &pos);
    EXPECT_EQ(pos, 16);
    EXPECT_EQ(err, nullptr);

    char buf2[64] = { 0 };
    err = opendal_reader_read(reader, (uint8_t*)buf2, 32 /* no more 32 bytes*/, &read_size);
    EXPECT_EQ(err, nullptr);
    EXPECT_EQ(read_size, 5);
    EXPECT_EQ(std::string(buf2), "34567");

    // Test seek end, now we step on 'g'
    err = opendal_reader_seek(reader, -8, OPENDAL_SEEK_END, &pos);
    EXPECT_EQ(pos, 13);
    EXPECT_EQ(err, nullptr);

    char buf3[64] = { 0 };
    err = opendal_reader_read(reader, (uint8_t*)buf3, 32 /* no more 32 bytes*/, &read_size);
    EXPECT_EQ(err, nullptr);
    EXPECT_EQ(read_size, 8);
    EXPECT_EQ(std::string(buf3), "g1234567");

    // Test invalid whence initializes pos to 0 and returns error
    pos = 999;
    err = opendal_reader_seek(reader, 0, 99, &pos);
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(pos, 0);
    opendal_error_free(err);

    opendal_reader_free(reader);
}
