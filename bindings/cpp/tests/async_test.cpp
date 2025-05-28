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
  auto res = cppcoro::sync_wait(op->read(path));
  for (size_t i = 0; i < data.size(); ++i) EXPECT_EQ(data[i], res[i]);

  path = "test_path2";
  cppcoro::sync_wait([&]() -> cppcoro::task<void> {
    co_await op->write(path, data);
    auto res = co_await op->read(path);
    for (size_t i = 0; i < data.size(); ++i) EXPECT_EQ(data[i], res[i]);
    co_return;
  }());
}

TEST_F(AsyncOpendalTest, AsyncOperationsTest) {
  cppcoro::sync_wait([&]() -> cppcoro::task<void> {
    const auto dir_path = "test_async_dir/";
    const auto file_path = "test_async_dir/test_file.txt";
    std::vector<uint8_t> file_content = {1, 2, 3, 4, 5};

    // Create directory and check existence
    co_await op->create_dir(dir_path);
    auto dir_exists = co_await op->exists(dir_path);
    EXPECT_TRUE(dir_exists);

    // Check non-existent file
    auto non_existent_exists = co_await op->exists("non_existent_file");
    EXPECT_FALSE(non_existent_exists);

    // Write a file and check its existence
    co_await op->write(file_path, file_content);
    auto file_exists = co_await op->exists(file_path);
    EXPECT_TRUE(file_exists);

    // Copy file and check existence and content
    const auto copied_file_path = "test_async_dir/copied_file.txt";
    // co_await op->copy(file_path, copied_file_path);
    // auto copied_file_exists = co_await op->exists(copied_file_path);
    // EXPECT_TRUE(copied_file_exists);
    // auto copied_content_rust_vec = co_await op->read(copied_file_path);
    // EXPECT_EQ(file_content.size(), copied_content_rust_vec.size());
    // for (size_t i = 0; i < file_content.size(); ++i) {
    //   EXPECT_EQ(file_content[i], copied_content_rust_vec[i]);
    // }
    co_await op->write(copied_file_path, file_content);

    // Rename file and check old path non-existence and new path existence
    const auto renamed_file_path = "test_async_dir/renamed_file.txt";
    co_await op->rename(copied_file_path, renamed_file_path);
    auto old_copied_exists = co_await op->exists(copied_file_path);
    EXPECT_FALSE(old_copied_exists);
    auto renamed_file_exists = co_await op->exists(renamed_file_path);
    EXPECT_TRUE(renamed_file_exists);
    auto renamed_content_rust_vec = co_await op->read(renamed_file_path);
    EXPECT_EQ(file_content.size(), renamed_content_rust_vec.size());
    for (size_t i = 0; i < file_content.size(); ++i) {
      EXPECT_EQ(file_content[i], renamed_content_rust_vec[i]);
    }

    // Delete the renamed file and check non-existence
    co_await op->delete_path(renamed_file_path);
    auto deleted_exists = co_await op->exists(renamed_file_path);
    EXPECT_FALSE(deleted_exists);

    // Create another file in the directory
    const auto another_file_path = "test_async_dir/another_file.txt";
    co_await op->write(another_file_path, file_content);
    auto another_file_exists = co_await op->exists(another_file_path);
    EXPECT_TRUE(another_file_exists);

    // Remove the entire directory and check non-existence of the directory and its contents
    co_await op->remove_all(dir_path);
    auto dir_after_remove_exists = co_await op->exists(dir_path);
    EXPECT_FALSE(dir_after_remove_exists);
    auto file_in_removed_dir_exists = co_await op->exists(file_path); // Original file path
    EXPECT_FALSE(file_in_removed_dir_exists);
    auto another_file_in_removed_dir_exists = co_await op->exists(another_file_path);
    EXPECT_FALSE(another_file_in_removed_dir_exists);

    // Test listing after cleaning up
    const auto list_dir_path = "test_list_dir/";
    const auto list_file1_path = "test_list_dir/file1.txt";
    const auto list_file2_path = "test_list_dir/file2.txt";
    const auto list_subdir_path = "test_list_dir/subdir/";

    co_await op->create_dir(list_dir_path);
    co_await op->write(list_file1_path, file_content);
    co_await op->write(list_file2_path, file_content);
    co_await op->create_dir(list_subdir_path);

    auto listed_entries = co_await op->list(list_dir_path);
    EXPECT_EQ(listed_entries.size(), 3); // file1.txt, file2.txt, subdir/

    bool found_file1 = false;
    bool found_file2 = false;
    bool found_subdir = false;
    for (const auto& entry : listed_entries) {
      if (entry == "file1.txt") found_file1 = true;
      if (entry == "file2.txt") found_file2 = true;
      if (entry == "subdir/") found_subdir = true;
    }
    EXPECT_TRUE(found_file1);
    EXPECT_TRUE(found_file2);
    EXPECT_TRUE(found_subdir);

    // Clean up list test directory
    co_await op->remove_all(list_dir_path);
    co_return;
  }());
}
