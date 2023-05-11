/**
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

class OpendalBddTest : public ::testing::Test {
protected:
    // Setup code for the fixture
    opendal_operator_ptr p;
    std::string scheme;
    std::string path;
    std::string content;

    // the fixture setup is an operator write, which will be
    // run at the beginning of every tests
    void SetUp() override
    {
        // construct the memory operator
        this->scheme = std::string("memory");
        this->path = std::string("test");
        this->content = std::string("Hello, World!");

        this->p = opendal_operator_new(scheme.c_str());

        EXPECT_TRUE(this->p);

        const opendal_bytes data = {
            .data = (uint8_t*)this->content.c_str(),
            .len = this->content.length(),
        };

        opendal_code code = opendal_operator_blocking_write(this->p, this->path.c_str(), data);

        EXPECT_EQ(code, OPENDAL_OK);
    }

    // Teardown code for the fixture, free the operator
    void TearDown() override
    {
        opendal_operator_free(&this->p);
    }
};

// do nothing, the fixture does the Write Test
TEST_F(OpendalBddTest, Write)
{
}

// The path must exist
TEST_F(OpendalBddTest, Exist)
{
    opendal_result_is_exist r = opendal_operator_is_exist(this->p, this->path.c_str());

    EXPECT_EQ(r.code, OPENDAL_OK);
    EXPECT_TRUE(r.is_exist);
}

// The entry mode must be file
TEST_F(OpendalBddTest, EntryMode)
{
    opendal_result_stat r = opendal_operator_stat(this->p, this->path.c_str());
    EXPECT_EQ(r.code, OPENDAL_OK);

    opendal_metadata meta = r.meta;
    EXPECT_TRUE(opendal_metadata_is_file(&meta));

    opendal_metadata_free(&meta);
}

// The content length must be consistent
TEST_F(OpendalBddTest, ContentLength)
{
    opendal_result_stat r = opendal_operator_stat(this->p, this->path.c_str());
    EXPECT_EQ(r.code, OPENDAL_OK);

    opendal_metadata meta = r.meta;
    EXPECT_EQ(opendal_metadata_content_length(&meta), 13);

    opendal_metadata_free(&meta);
}

// We must read the correct content
TEST_F(OpendalBddTest, Read)
{
    struct opendal_result_read r = opendal_operator_blocking_read(this->p, this->path.c_str());

    EXPECT_EQ(r.code, OPENDAL_OK);
    EXPECT_EQ(r.data->len, this->content.length());

    for (int i = 0; i < r.data->len; i++) {
        EXPECT_EQ(this->content[i], (char)(r.data->data[i]));
    }

    // free the bytes's heap memory
    opendal_bytes_free(r.data);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
