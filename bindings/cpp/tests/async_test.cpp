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

#include <optional>
#include <random>
#include <stdexcept>

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
  cppcoro::sync_wait(op->Write(path, data));
  auto res = cppcoro::sync_wait(op->Read(path));
  for (size_t i = 0; i < data.size(); ++i) EXPECT_EQ(data[i], res[i]);

  path = "test_path2";
  cppcoro::sync_wait([&]() -> cppcoro::task<void> {
    co_await op->Write(path, data);
    auto res = co_await op->Read(path);
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
    co_await op->CreateDir(dir_path);
    auto dir_exists = co_await op->Exists(dir_path);
    EXPECT_TRUE(dir_exists);

    // Check non-existent file
    auto non_existent_exists = co_await op->Exists("non_existent_file");
    EXPECT_FALSE(non_existent_exists);

    // Write a file and check its existence
    co_await op->Write(file_path, file_content);
    auto file_exists = co_await op->Exists(file_path);
    EXPECT_TRUE(file_exists);

    const auto copied_file_path = "test_async_dir/copied_file.txt";
    co_await op->Write(copied_file_path, file_content);
    auto copied_file_exists = co_await op->Exists(copied_file_path);
    EXPECT_TRUE(copied_file_exists);
    auto copied_content_rust_vec = co_await op->Read(copied_file_path);
    EXPECT_EQ(file_content.size(), copied_content_rust_vec.size());
    for (size_t i = 0; i < file_content.size(); ++i) {
      EXPECT_EQ(file_content[i], copied_content_rust_vec[i]);
    }

    const auto renamed_file_path = "test_async_dir/renamed_file.txt";
    co_await op->Write(renamed_file_path, file_content);
    co_await op->DeletePath(copied_file_path);
    auto old_copied_exists = co_await op->Exists(copied_file_path);
    EXPECT_FALSE(old_copied_exists);
    auto renamed_file_exists = co_await op->Exists(renamed_file_path);
    EXPECT_TRUE(renamed_file_exists);
    auto renamed_content_rust_vec = co_await op->Read(renamed_file_path);
    EXPECT_EQ(file_content.size(), renamed_content_rust_vec.size());
    for (size_t i = 0; i < file_content.size(); ++i) {
      EXPECT_EQ(file_content[i], renamed_content_rust_vec[i]);
    }

    // Delete the renamed file and check non-existence
    co_await op->DeletePath(renamed_file_path);
    auto deleted_exists = co_await op->Exists(renamed_file_path);
    EXPECT_FALSE(deleted_exists);

    // Create another file in the directory
    const auto another_file_path = "test_async_dir/another_file.txt";
    co_await op->Write(another_file_path, file_content);
    auto another_file_exists = co_await op->Exists(another_file_path);
    EXPECT_TRUE(another_file_exists);

    // Test listing after cleaning up
    const auto list_dir_path = "test_list_dir/";
    const auto list_file1_path = "test_list_dir/file1.txt";
    const auto list_file2_path = "test_list_dir/file2.txt";
    const auto list_subdir_path = "test_list_dir/subdir/";

    co_await op->CreateDir(list_dir_path);
    co_await op->Write(list_file1_path, file_content);
    co_await op->Write(list_file2_path, file_content);
    co_await op->CreateDir(list_subdir_path);

    auto listed_entries = co_await op->List(list_dir_path);
    EXPECT_EQ(listed_entries.size(), 4);

    bool found_file1 = false;
    bool found_file2 = false;
    bool found_subdir = false;
    for (const auto& entry : listed_entries) {
      if (std::string(entry).ends_with("file1.txt")) found_file1 = true;
      if (std::string(entry).ends_with("file2.txt")) found_file2 = true;
      if (std::string(entry).ends_with("subdir/")) found_subdir = true;
    }
    EXPECT_TRUE(found_file1);
    EXPECT_TRUE(found_file2);
    EXPECT_TRUE(found_subdir);
    co_return;
  }());
}

TEST_F(AsyncOpendalTest, AsyncReaderTest) {
  cppcoro::sync_wait([&]() -> cppcoro::task<void> {
    const auto file_path = "test_async_reader.txt";

    // Create test data - larger to test range reads
    std::vector<uint8_t> test_data;
    for (int i = 0; i < 1000; ++i) {
      test_data.push_back(static_cast<uint8_t>(i % 256));
    }

    // Write the test file
    co_await op->Write(file_path, test_data);

    // Test: Create async reader
    auto reader_id = co_await op->GetReader(file_path);
    opendal::async::Reader reader(reader_id);

    // Test: Read full file in one go
    auto full_data = co_await reader.Read(0, test_data.size());
    EXPECT_EQ(full_data.size(), test_data.size());
    for (size_t i = 0; i < test_data.size(); ++i) {
      EXPECT_EQ(full_data[i], test_data[i]);
    }

    // Test: Read in chunks
    const size_t chunk_size = 100;
    std::vector<uint8_t> chunked_data;

    for (size_t offset = 0; offset < test_data.size(); offset += chunk_size) {
      size_t read_size = std::min(chunk_size, test_data.size() - offset);
      auto chunk = co_await reader.Read(offset, read_size);

      EXPECT_EQ(chunk.size(), read_size);
      chunked_data.insert(chunked_data.end(), chunk.begin(), chunk.end());
    }

    // Verify chunked read matches original data
    EXPECT_EQ(chunked_data.size(), test_data.size());
    for (size_t i = 0; i < test_data.size(); ++i) {
      EXPECT_EQ(chunked_data[i], test_data[i]);
    }

    // Test: Read specific range
    auto range_data = co_await reader.Read(100, 50);
    EXPECT_EQ(range_data.size(), 50);
    for (size_t i = 0; i < 50; ++i) {
      EXPECT_EQ(range_data[i], test_data[100 + i]);
    }

    // Test: Read from end of file
    auto end_data = co_await reader.Read(test_data.size() - 10, 10);
    EXPECT_EQ(end_data.size(), 10);
    for (size_t i = 0; i < 10; ++i) {
      EXPECT_EQ(end_data[i], test_data[test_data.size() - 10 + i]);
    }

    // Test: Move semantics
    auto reader_id2 = co_await op->GetReader(file_path);
    opendal::async::Reader reader2(reader_id2);
    opendal::async::Reader moved_reader = std::move(reader2);

    auto moved_data = co_await moved_reader.Read(0, 100);
    EXPECT_EQ(moved_data.size(), 100);
    for (size_t i = 0; i < 100; ++i) {
      EXPECT_EQ(moved_data[i], test_data[i]);
    }

    // Clean up
    co_await op->DeletePath(file_path);
    co_return;
  }());
}

TEST_F(AsyncOpendalTest, AsyncListerTest) {
  cppcoro::sync_wait([&]() -> cppcoro::task<void> {
    const auto base_dir = "test_async_lister/";
    const auto file1_path = "test_async_lister/file1.txt";
    const auto file2_path = "test_async_lister/file2.txt";
    const auto file3_path = "test_async_lister/file3.txt";
    const auto subdir_path = "test_async_lister/subdir/";
    const auto subdir_file_path = "test_async_lister/subdir/nested_file.txt";

    auto test_data = std::vector<uint8_t>{1, 2, 3, 4, 5};

    // Set up test directory structure
    co_await op->CreateDir(base_dir);
    co_await op->Write(file1_path, test_data);
    co_await op->Write(file2_path, test_data);
    co_await op->Write(file3_path, test_data);
    co_await op->CreateDir(subdir_path);
    co_await op->Write(subdir_file_path, test_data);

    // Test: Create async lister
    auto lister_id = co_await op->GetLister(base_dir);
    auto lister = opendal::async::Lister(lister_id);

    // Test: Iterate through all entries
    auto found_entries = std::vector<std::string>{};
    while (true) {
      auto entry_path = co_await lister.Next();
      if (entry_path.empty()) {
        break;  // End of iteration
      }
      found_entries.push_back(std::string(entry_path));
    }

    // Verify we found the expected entries
    EXPECT_EQ(found_entries.size(),
              5);  // At least 4 entries (3 files + 1 subdir)

    bool found_file1 = false;
    bool found_file2 = false;
    bool found_file3 = false;
    bool found_subdir = false;
    for (const auto& entry : found_entries) {
      if (std::string(entry).ends_with("file1.txt")) found_file1 = true;
      if (std::string(entry).ends_with("file2.txt")) found_file2 = true;
      if (std::string(entry).ends_with("file3.txt")) found_file3 = true;
      if (std::string(entry).ends_with("subdir/")) found_subdir = true;
    }

    EXPECT_TRUE(found_file1);
    EXPECT_TRUE(found_file2);
    EXPECT_TRUE(found_file3);
    EXPECT_TRUE(found_subdir);

    // Test: Empty directory listing
    const auto empty_dir = "test_async_lister_empty/";
    co_await op->CreateDir(empty_dir);

    auto empty_lister_id = co_await op->GetLister(empty_dir);
    auto empty_lister = opendal::async::Lister(empty_lister_id);

    auto first_entry = co_await empty_lister.Next();
    // Test: Move semantics
    auto lister_id2 = co_await op->GetLister(base_dir);
    auto lister2 = opendal::async::Lister(lister_id2);
    opendal::async::Lister moved_lister = std::move(lister2);

    // Should be able to use moved lister
    auto moved_entry = co_await moved_lister.Next();
    EXPECT_FALSE(moved_entry.empty());  // Should find at least one entry

    co_return;
  }());
}

TEST_F(AsyncOpendalTest, AsyncListerErrorHandlingTest) {
  cppcoro::sync_wait([&]() -> cppcoro::task<void> {
    // Test: Listing non-existent directory
    auto lister_id = co_await op->GetLister("non_existent_directory/");
    auto lister = opendal::async::Lister(lister_id);
    auto entry = co_await lister.Next();
    EXPECT_TRUE(entry.empty());

    co_return;
  }());
}

TEST_F(AsyncOpendalTest, AsyncReaderListerIntegrationTest) {
  cppcoro::sync_wait([&]() -> cppcoro::task<void> {
    const auto base_dir = "test_integration/";
    const auto large_file = "test_integration/large_file.bin";

    // Create a larger file for more realistic testing
    std::vector<uint8_t> large_data;
    large_data.reserve(10000);
    for (int i = 0; i < 10000; ++i) {
      large_data.push_back(static_cast<uint8_t>(i % 256));
    }

    // Set up test environment
    co_await op->CreateDir(base_dir);
    co_await op->Write(large_file, large_data);

    // Test: Use lister to find the file, then reader to read it
    auto lister_id = co_await op->GetLister(base_dir);
    auto lister = opendal::async::Lister(lister_id);

    std::string found_file;
    while (true) {
      auto entry = co_await lister.Next();
      if (entry.empty()) break;

      if (std::string(entry).ends_with("large_file.bin")) {
        found_file = std::string(entry);
        break;
      }
    }

    EXPECT_FALSE(found_file.empty());

    // Now read the found file using async reader
    auto reader_id = co_await op->GetReader(large_file);
    auto reader = opendal::async::Reader(reader_id);

    // Read in multiple chunks to test streaming
    const size_t chunk_size = 1000;
    auto reconstructed_data = std::vector<uint8_t>{};

    for (size_t offset = 0; offset < large_data.size(); offset += chunk_size) {
      size_t read_size = std::min(chunk_size, large_data.size() - offset);
      auto chunk = co_await reader.Read(offset, read_size);
      reconstructed_data.insert(reconstructed_data.end(), chunk.begin(),
                                chunk.end());
    }

    // Verify the data integrity
    EXPECT_EQ(reconstructed_data.size(), large_data.size());
    for (size_t i = 0; i < large_data.size(); ++i) {
      EXPECT_EQ(reconstructed_data[i], large_data[i]);
    }

    co_return;
  }());
}

// Regression test: futures returned by `Operator` methods must own the
// underlying operator handle. Destroying the `Operator` before a returned
// future is polled must not invalidate the future. This used to crash or read
// dangling memory because the future borrowed the operator through a raw
// pointer.
TEST_F(AsyncOpendalTest, AsyncFuturesOutliveOperator) {
  const auto dir_path = "test_outlive/";
  const auto file_path = "test_outlive/file.txt";
  const auto written_path = "test_outlive/written.txt";
  const auto copied_path = "test_outlive/copied.txt";
  const auto rename_source_path = "test_outlive/rename_source.txt";
  const auto moved_path = "test_outlive/moved.txt";
  const auto created_dir_path = "test_outlive/created/";
  std::vector<uint8_t> data{10, 20, 30, 40};

  std::optional<opendal::async::Operator::WriteFuture> write_future;
  std::optional<opendal::async::Operator::ExistsFuture> exists_future;
  std::optional<opendal::async::Operator::ReadFuture> read_future;
  std::optional<opendal::async::Operator::ListFuture> list_future;
  std::optional<opendal::async::Operator::CreateDirFuture> create_dir_future;
  std::optional<opendal::async::Operator::CopyFuture> copy_future;
  std::optional<opendal::async::Operator::ExistsFuture> copy_exists_future;
  std::optional<opendal::async::Operator::RenameFuture> rename_future;
  std::optional<opendal::async::Operator::ExistsFuture> moved_exists_future;
  std::optional<opendal::async::Operator::DeleteFuture> delete_future;
  std::optional<opendal::async::Operator::ExistsFuture> deleted_exists_future;
  std::optional<opendal::async::Operator::ReaderFuture> reader_future;
  std::optional<opendal::async::Operator::ListerFuture> lister_future;

  {
    opendal::async::Operator scoped_op(scheme, config);
    cppcoro::sync_wait(scoped_op.Write(file_path, data));
    cppcoro::sync_wait(scoped_op.Write(rename_source_path, data));

    // Create one future per async operation; none of them is polled until
    // after `scoped_op` is destroyed.
    write_future.emplace(scoped_op.Write(written_path, data));
    exists_future.emplace(scoped_op.Exists(file_path));
    read_future.emplace(scoped_op.Read(file_path));
    list_future.emplace(scoped_op.List(dir_path));
    create_dir_future.emplace(scoped_op.CreateDir(created_dir_path));
    copy_future.emplace(scoped_op.Copy(file_path, copied_path));
    copy_exists_future.emplace(scoped_op.Exists(copied_path));
    rename_future.emplace(
        scoped_op.Rename(rename_source_path, moved_path));
    moved_exists_future.emplace(scoped_op.Exists(moved_path));
    delete_future.emplace(scoped_op.DeletePath(moved_path));
    deleted_exists_future.emplace(scoped_op.Exists(moved_path));
    reader_future.emplace(scoped_op.GetReader(file_path));
    lister_future.emplace(scoped_op.GetLister(dir_path));
  }
  // `scoped_op` is destroyed here; all the futures above must still resolve.

  cppcoro::sync_wait(std::move(*write_future));

  // The memory service does not support copy/rename; polling these futures
  // must still safely reach the service and surface its Unsupported error
  // instead of dereferencing a destroyed operator.
  EXPECT_ANY_THROW(cppcoro::sync_wait(std::move(*copy_future)));
  EXPECT_ANY_THROW(cppcoro::sync_wait(std::move(*rename_future)));

  EXPECT_TRUE(cppcoro::sync_wait(std::move(*exists_future)));
  EXPECT_FALSE(cppcoro::sync_wait(std::move(*copy_exists_future)));
  EXPECT_FALSE(cppcoro::sync_wait(std::move(*moved_exists_future)));

  cppcoro::sync_wait(std::move(*delete_future));
  EXPECT_FALSE(cppcoro::sync_wait(std::move(*deleted_exists_future)));

  cppcoro::sync_wait(std::move(*create_dir_future));

  auto res = cppcoro::sync_wait(std::move(*read_future));
  ASSERT_EQ(res.size(), data.size());
  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(res[i], data[i]);
  }

  auto entries = cppcoro::sync_wait(std::move(*list_future));
  bool found_file = false;
  for (const auto &entry : entries) {
    if (std::string(entry).ends_with("file.txt")) found_file = true;
  }
  EXPECT_TRUE(found_file);

  auto reader_id = cppcoro::sync_wait(std::move(*reader_future));
  opendal::async::Reader reader(reader_id);
  auto reader_res = cppcoro::sync_wait(reader.Read(0, data.size()));
  ASSERT_EQ(reader_res.size(), data.size());
  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(reader_res[i], data[i]);
  }

  auto lister_id = cppcoro::sync_wait(std::move(*lister_future));
  opendal::async::Lister lister(lister_id);
  auto entry = cppcoro::sync_wait(lister.Next());
  EXPECT_FALSE(entry.empty());
}

// Regression test: a `Reader::Read` or `Lister::Next` future must keep its
// reader/lister alive even if the C++ handle is destroyed before the future is
// polled.
TEST_F(AsyncOpendalTest, AsyncReaderListerFuturesOutliveHandle) {
  const auto file_path = "test_outlive_handle/file.txt";
  std::vector<uint8_t> data{1, 2, 3, 4, 5, 6, 7, 8};

  cppcoro::sync_wait(op->Write(file_path, data));

  std::optional<opendal::async::Reader::ReadFuture> read_future;
  {
    auto reader_id = cppcoro::sync_wait(op->GetReader(file_path));
    opendal::async::Reader reader(reader_id);
    read_future.emplace(reader.Read(0, data.size()));
  }
  // The `Reader` is destroyed here; the read future must still resolve.

  auto res = cppcoro::sync_wait(std::move(*read_future));
  ASSERT_EQ(res.size(), data.size());
  for (size_t i = 0; i < data.size(); ++i) {
    EXPECT_EQ(res[i], data[i]);
  }

  std::optional<opendal::async::Lister::NextFuture> next_future;
  {
    auto lister_id = cppcoro::sync_wait(op->GetLister("test_outlive_handle/"));
    opendal::async::Lister lister(lister_id);
    next_future.emplace(lister.Next());
  }
  // The `Lister` is destroyed here; the next future must still resolve.

  auto entry = cppcoro::sync_wait(std::move(*next_future));
  EXPECT_FALSE(entry.empty());
}

// Regression test: move-assigning a `Reader` or `Lister` must release the
// previously owned handle instead of leaking it. Leaked entries would stay
// reachable in the process-global tables until exit, so the test verifies
// eviction by checking that the old ids no longer resolve.
TEST_F(AsyncOpendalTest, AsyncMoveAssignmentReleasesHandle) {
  const auto file_path = "test_move_assign/file.txt";
  std::vector<uint8_t> data{1, 2, 3};

  cppcoro::sync_wait(op->Write(file_path, data));

  auto first_reader_id = cppcoro::sync_wait(op->GetReader(file_path));
  auto second_reader_id = cppcoro::sync_wait(op->GetReader(file_path));
  opendal::async::Reader reader(first_reader_id);
  reader = opendal::async::Reader(second_reader_id);

  auto res = cppcoro::sync_wait(reader.Read(0, data.size()));
  ASSERT_EQ(res.size(), data.size());

  // The first id must have been evicted: a handle reconstructed from it can
  // no longer read.
  {
    opendal::async::Reader stale_reader(first_reader_id);
    EXPECT_ANY_THROW(cppcoro::sync_wait(stale_reader.Read(0, 1)));
  }

  // Self-move must keep the live handle usable.
  reader = std::move(reader);
  res = cppcoro::sync_wait(reader.Read(0, data.size()));
  ASSERT_EQ(res.size(), data.size());

  auto first_lister_id = cppcoro::sync_wait(op->GetLister("test_move_assign/"));
  auto second_lister_id =
      cppcoro::sync_wait(op->GetLister("test_move_assign/"));
  opendal::async::Lister lister(first_lister_id);
  lister = opendal::async::Lister(second_lister_id);

  auto entry = cppcoro::sync_wait(lister.Next());
  EXPECT_FALSE(entry.empty());

  // The first id must have been evicted: a handle reconstructed from it can
  // no longer list.
  {
    opendal::async::Lister stale_lister(first_lister_id);
    EXPECT_ANY_THROW(cppcoro::sync_wait(stale_lister.Next()));
  }

  // Self-move must keep the live handle usable; the single entry was already
  // consumed, so `Next` resolves to the end-of-iteration sentinel.
  lister = std::move(lister);
  EXPECT_NO_THROW(cppcoro::sync_wait(lister.Next()));
}

// Calling any method on a moved-from `Operator` must report an error instead
// of dereferencing a null `rust::Box`.
TEST_F(AsyncOpendalTest, MovedFromOperatorThrows) {
  opendal::async::Operator moved_to(std::move(*op));

  EXPECT_THROW(op->Exists("anything"), std::logic_error);

  // The moved-to operator still works.
  const auto file_path = "test_moved_operator/file.txt";
  std::vector<uint8_t> data{1, 2, 3};
  cppcoro::sync_wait(moved_to.Write(file_path, data));
  auto res = cppcoro::sync_wait(moved_to.Read(file_path));
  ASSERT_EQ(res.size(), data.size());
}
