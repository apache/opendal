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

#include "opendal.hpp"
#include "gtest/gtest.h"
#include <string>
#include <unordered_map>

class OpendalTest : public ::testing::Test {
protected:
  opendal::Operator op;

  std::string scheme;
  std::unordered_map<std::string, std::string> config;

  void SetUp() override {
    this->scheme = "memory";
    op = opendal::Operator(this->scheme, this->config);

    EXPECT_TRUE(this->op.available());
  }
};

// Scenario: OpenDAL Blocking Operations
TEST_F(OpendalTest, BasicTest) {
  std::string path = "test";
  std::vector<uint8_t> data = {1, 2, 3, 4, 5};

  op.write("test", data);

  auto res = op.read("test");
  EXPECT_EQ(res, data);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
