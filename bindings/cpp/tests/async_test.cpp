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

#include <random>

#include "cppcoro/sync_wait.hpp"
#include "cppcoro/task.hpp"
#include "gtest/gtest.h"
#include "opendal_async.hpp"

class AsyncOpendalTest : public ::testing::Test {
 protected:
  std::optional<opendal::async::Operator> op;

  std::string scheme;
  std::unordered_map<std::string, std::string> config;

  // random number generator
  std::mt19937 rng;

  void SetUp() override {
    scheme = "memory";
    rng.seed(time(nullptr));

    op = opendal::async::Operator(scheme, config);
  }
};

TEST_F(AsyncOpendalTest, BasicTest) {
  auto path = "test_path";
  std::vector<uint8_t> data{1, 2, 3, 4, 5};
  cppcoro::sync_wait(op->write(path, data));
  auto d = cppcoro::sync_wait(op->read(path));
  for (size_t i = 0; i < data.size(); ++i) EXPECT_EQ(data[i], d[i]);
}
