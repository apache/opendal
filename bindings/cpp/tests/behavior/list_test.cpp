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

#include "../framework/test_framework.hpp"
#include <set>
#include <thread>
#include <atomic>

namespace opendal::test {

class ListBehaviorTest : public OpenDALTest {
protected:
    void SetUp() override {
        OpenDALTest::SetUp();
    }
};

// Test listing empty directory
OPENDAL_TEST_F(ListBehaviorTest, ListEmptyDirectory) {
    auto dir_path = random_dir_path();
    
    // Create empty directory
    op_.create_dir(dir_path);
    
    // List the directory
    auto entries = op_.list(dir_path);
    
    // Should contain at least the directory itself
    EXPECT_GE(entries.size(), 1);
    
    bool found_dir = false;
    for (const auto& entry : entries) {
        if (entry.path == dir_path) {
            found_dir = true;
            auto metadata = op_.stat(entry.path);
            EXPECT_EQ(metadata.type, opendal::EntryMode::DIR);
        }
    }
    EXPECT_TRUE(found_dir);
}

// Test listing directory with files
OPENDAL_TEST_F(ListBehaviorTest, ListDirectoryWithFiles) {
    auto dir_path = random_dir_path();
    auto file1_path = dir_path + "file1.txt";
    auto file2_path = dir_path + "file2.txt";
    auto file3_path = dir_path + "file3.txt";
    
    // Create directory and files
    op_.create_dir(dir_path);
    op_.write(file1_path, random_string(100));
    op_.write(file2_path, random_string(200));
    op_.write(file3_path, random_string(300));
    
    // List the directory
    auto entries = op_.list(dir_path);
    
    // Should contain directory and 3 files
    EXPECT_EQ(entries.size(), 4);
    
    std::set<std::string> expected_paths = {dir_path, file1_path, file2_path, file3_path};
    std::set<std::string> actual_paths;
    
    for (const auto& entry : entries) {
        actual_paths.insert(entry.path);
        
        auto metadata = op_.stat(entry.path);
        if (entry.path == dir_path) {
            EXPECT_EQ(metadata.type, opendal::EntryMode::DIR);
        } else {
            EXPECT_EQ(metadata.type, opendal::EntryMode::FILE);
        }
    }
    
    EXPECT_EQ(actual_paths, expected_paths);
}

// Test listing nested directories
OPENDAL_TEST_F(ListBehaviorTest, ListNestedDirectories) {
    auto base_dir = random_dir_path();
    auto sub_dir = base_dir + "subdir/";
    auto file1 = base_dir + "file1.txt";
    auto file2 = sub_dir + "file2.txt";
    
    // Create nested structure
    op_.create_dir(base_dir);
    op_.create_dir(sub_dir);
    op_.write(file1, random_string(100));
    op_.write(file2, random_string(200));
    
    // List base directory
    auto entries = op_.list(base_dir);
    
    std::set<std::string> expected_paths = {base_dir, sub_dir, file1};
    std::set<std::string> actual_paths;
    
    for (const auto& entry : entries) {
        actual_paths.insert(entry.path);
    }
    
    EXPECT_EQ(actual_paths, expected_paths);
    
    // List subdirectory
    auto sub_entries = op_.list(sub_dir);
    
    std::set<std::string> sub_expected_paths = {sub_dir, file2};
    std::set<std::string> sub_actual_paths;
    
    for (const auto& entry : sub_entries) {
        sub_actual_paths.insert(entry.path);
    }
    
    EXPECT_EQ(sub_actual_paths, sub_expected_paths);
}

// Test listing non-existent directory
OPENDAL_TEST_F(ListBehaviorTest, ListNonExistentDirectory) {
    auto dir_path = random_dir_path();
    
    // Ensure directory doesn't exist
    EXPECT_FALSE(op_.exists(dir_path));
    
    // Listing non-existent directory should throw or return empty
    EXPECT_THROW({
        auto entries = op_.list(dir_path);
    }, std::exception);
}

// Test listing with many files
OPENDAL_TEST_F(ListBehaviorTest, ListManyFiles) {
    auto dir_path = random_dir_path();
    const int num_files = 100;
    std::vector<std::string> file_paths;
    
    // Create directory
    op_.create_dir(dir_path);
    
    // Create many files
    for (int i = 0; i < num_files; ++i) {
        auto file_path = dir_path + "file_" + std::to_string(i) + ".txt";
        op_.write(file_path, random_string(50));
        file_paths.push_back(file_path);
    }
    
    // List the directory
    auto entries = op_.list(dir_path);
    
    // Should contain directory + num_files
    EXPECT_EQ(entries.size(), num_files + 1);
    
    // Verify all files are present
    std::set<std::string> expected_paths(file_paths.begin(), file_paths.end());
    expected_paths.insert(dir_path);
    
    std::set<std::string> actual_paths;
    for (const auto& entry : entries) {
        actual_paths.insert(entry.path);
    }
    
    EXPECT_EQ(actual_paths, expected_paths);
}

// Test listing with special character names
OPENDAL_TEST_F(ListBehaviorTest, ListSpecialCharNames) {
    auto dir_path = random_dir_path();
    auto file1 = dir_path + "file-with.special_chars.txt";
    auto file2 = dir_path + "file with spaces.txt";
    auto file3 = dir_path + "file_123.txt";
    
    // Create directory and files
    op_.create_dir(dir_path);
    op_.write(file1, random_string(100));
    op_.write(file2, random_string(100));
    op_.write(file3, random_string(100));
    
    // List the directory
    auto entries = op_.list(dir_path);
    
    std::set<std::string> expected_paths = {dir_path, file1, file2, file3};
    std::set<std::string> actual_paths;
    
    for (const auto& entry : entries) {
        actual_paths.insert(entry.path);
    }
    
    EXPECT_EQ(actual_paths, expected_paths);
}

// Test using lister iterator
OPENDAL_TEST_F(ListBehaviorTest, ListerIterator) {
    auto dir_path = random_dir_path();
    auto file1_path = dir_path + "file1.txt";
    auto file2_path = dir_path + "file2.txt";
    
    // Create directory and files
    op_.create_dir(dir_path);
    op_.write(file1_path, random_string(100));
    op_.write(file2_path, random_string(200));
    
    // Use lister
    auto lister = op_.lister(dir_path);
    
    std::set<std::string> expected_paths = {dir_path, file1_path, file2_path};
    std::set<std::string> actual_paths;
    
    for (const auto& entry : lister) {
        actual_paths.insert(entry.path);
        
        auto metadata = op_.stat(entry.path);
        if (entry.path == dir_path) {
            EXPECT_EQ(metadata.type, opendal::EntryMode::DIR);
        } else {
            EXPECT_EQ(metadata.type, opendal::EntryMode::FILE);
        }
    }
    
    EXPECT_EQ(actual_paths, expected_paths);
}

// Test listing root directory
OPENDAL_TEST_F(ListBehaviorTest, ListRootDirectory) {
    // List root directory (empty path or "/")
    auto entries = op_.list("/");
    
    // Should not throw and return some entries
    EXPECT_NO_THROW({
        auto entries = op_.list("/");
    });
}

// Test metadata in list results
OPENDAL_TEST_F(ListBehaviorTest, ListMetadata) {
    auto dir_path = random_dir_path();
    auto file_path = dir_path + "test_file.txt";
    auto content = random_string(1000);
    
    // Create directory and file
    op_.create_dir(dir_path);
    op_.write(file_path, content);
    
    // List the directory
    auto entries = op_.list(dir_path);
    
    for (const auto& entry : entries) {
        auto metadata = op_.stat(entry.path);
        if (entry.path == file_path) {
            EXPECT_EQ(metadata.type, opendal::EntryMode::FILE);
            EXPECT_EQ(metadata.content_length, content.size());
        } else if (entry.path == dir_path) {
            EXPECT_EQ(metadata.type, opendal::EntryMode::DIR);
        }
    }
}

// Test concurrent listing
OPENDAL_TEST_F(ListBehaviorTest, ConcurrentListing) {
    auto dir_path = random_dir_path();
    
    // Create directory with some files
    op_.create_dir(dir_path);
    for (int i = 0; i < 10; ++i) {
        auto file_path = dir_path + "file_" + std::to_string(i) + ".txt";
        op_.write(file_path, random_string(100));
    }
    
    const int num_threads = 5;
    std::vector<std::thread> threads;
    std::vector<std::vector<opendal::Entry>> results(num_threads);
    std::atomic<int> error_count{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            try {
                results[i] = op_.list(dir_path);
            } catch (const std::exception& e) {
                error_count++;
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(error_count, 0);
    
    // All results should be the same
    for (int i = 1; i < num_threads; ++i) {
        EXPECT_EQ(results[0].size(), results[i].size());
    }
}

} // namespace opendal::test 