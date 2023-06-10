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

class OpendalListTest : public ::testing::Test {
protected:
  opendal_operator_ptr p;

  // set up a brand new operator
  void SetUp() override {
    opendal_operator_options options = opendal_operator_options_new();
    opendal_operator_options_set(&options, "root", "/myroot");

    this->p = opendal_operator_new("memory", &options);
    EXPECT_TRUE(this->p.ptr);

    opendal_operator_options_free(&options);
  }

  void TearDown() override { opendal_operator_free(&this->p); }
};

// Basic usecase of list
TEST_F(OpendalListTest, ListDirTest) {}

// Try list an empty directory
TEST_F(OpendalListTest, ListEmptyDirTest) {}

// Try list a directory that does not exist
TEST_F(OpendalListTest, ListNotExistDirTest) {}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
