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

namespace opendal::test {

class DeleteBehaviorTest : public OpenDALTest {
protected:
    void SetUp() override {
        OpenDALTest::SetUp();
    }
};

// Test deleting existing file
OPENDAL_TEST_F(DeleteBehaviorTest, DeleteExistingFile) {
    auto path = random_path();
    auto content = random_string(100);
    
    // Create the file
    op_.write(path, content);
    EXPECT_TRUE(op_.exists(path));
    
    // Delete the file
    op_.remove(path);
    EXPECT_FALSE(op_.exists(path));
}

// Test deleting non-existent file (should not error)
OPENDAL_TEST_F(DeleteBehaviorTest, DeleteNonExistentFile) {
    auto path = random_path();
    
    // Ensure file doesn't exist
    EXPECT_FALSE(op_.exists(path));
    
    // Delete non-existent file should not throw
    EXPECT_NO_THROW(op_.remove(path));
}

// Test deleting empty directory
OPENDAL_TEST_F(DeleteBehaviorTest, DeleteEmptyDirectory) {
    auto dir_path = random_dir_path();
    
    // Create the directory
    op_.create_dir(dir_path);
    EXPECT_TRUE(op_.exists(dir_path));
    
    // Delete the directory
    op_.remove(dir_path);
    EXPECT_FALSE(op_.exists(dir_path));
}

// Test removing all (recursive delete)
OPENDAL_TEST_F(DeleteBehaviorTest, RemoveAllRecursive) {
    auto base_dir = random_dir_path();
    auto sub_dir = base_dir + "subdir/";
    auto file1 = base_dir + "file1.txt";
    auto file2 = sub_dir + "file2.txt";
    
    // Create directory structure with files
    op_.create_dir(base_dir);
    op_.create_dir(sub_dir);
    op_.write(file1, random_string(100));
    op_.write(file2, random_string(100));
    
    // Verify structure exists
    EXPECT_TRUE(op_.exists(base_dir));
    EXPECT_TRUE(op_.exists(sub_dir));
    EXPECT_TRUE(op_.exists(file1));
    EXPECT_TRUE(op_.exists(file2));
    
    // Remove all
    op_.remove_all(base_dir);
    
    // Verify everything is gone
    EXPECT_FALSE(op_.exists(base_dir));
    EXPECT_FALSE(op_.exists(sub_dir));
    EXPECT_FALSE(op_.exists(file1));
    EXPECT_FALSE(op_.exists(file2));
}

// Test deleting multiple files
OPENDAL_TEST_F(DeleteBehaviorTest, DeleteMultipleFiles) {
    std::vector<std::string> paths;
    
    // Create multiple files
    for (int i = 0; i < 10; ++i) {
        auto path = random_path();
        auto content = random_string(100);
        
        op_.write(path, content);
        EXPECT_TRUE(op_.exists(path));
        paths.push_back(path);
    }
    
    // Delete all files
    for (const auto& path : paths) {
        op_.remove(path);
        EXPECT_FALSE(op_.exists(path));
    }
}

// Test deleting files with special characters
OPENDAL_TEST_F(DeleteBehaviorTest, DeleteSpecialCharFile) {
    auto path = "test_with-special.chars_123/file-name_with.special.txt";
    auto content = random_string(100);
    
    // Create the file
    op_.write(path, content);
    EXPECT_TRUE(op_.exists(path));
    
    // Delete the file
    op_.remove(path);
    EXPECT_FALSE(op_.exists(path));
}

// Test deleting large file
OPENDAL_TEST_F(DeleteBehaviorTest, DeleteLargeFile) {
    auto path = random_path();
    auto content = random_string(1024 * 1024); // 1MB
    
    // Create large file
    op_.write(path, content);
    EXPECT_TRUE(op_.exists(path));
    
    // Delete the file
    op_.remove(path);
    EXPECT_FALSE(op_.exists(path));
}

// Test concurrent deletes
OPENDAL_TEST_F(DeleteBehaviorTest, ConcurrentDeletes) {
    const int num_threads = 10;
    std::vector<std::thread> threads;
    std::vector<std::string> paths(num_threads);
    std::atomic<int> error_count{0};
    
    // Create files
    for (int i = 0; i < num_threads; ++i) {
        paths[i] = random_path();
        op_.write(paths[i], random_string(100));
        EXPECT_TRUE(op_.exists(paths[i]));
    }
    
    // Delete concurrently
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            try {
                op_.remove(paths[i]);
            } catch (const std::exception& e) {
                error_count++;
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(error_count, 0);
    
    // Verify all files are deleted
    for (const auto& path : paths) {
        EXPECT_FALSE(op_.exists(path));
    }
}

// Test delete after read
OPENDAL_TEST_F(DeleteBehaviorTest, DeleteAfterRead) {
    auto path = random_path();
    auto content = random_string(100);
    
    // Create and read file
    op_.write(path, content);
    auto result = op_.read(path);
    EXPECT_EQ(result, content);
    
    // Delete the file
    op_.remove(path);
    EXPECT_FALSE(op_.exists(path));
    
    // Try to read again (should fail)
    EXPECT_THROW({
        auto content2 = op_.read(path);
    }, std::exception);
}

// Test delete and recreate
OPENDAL_TEST_F(DeleteBehaviorTest, DeleteAndRecreate) {
    auto path = random_path();
    auto original_content = random_string(100);
    auto new_content = random_string(200);
    
    // Create file
    op_.write(path, original_content);
    EXPECT_TRUE(op_.exists(path));
    auto result1 = op_.read(path);
    EXPECT_EQ(result1, original_content);
    
    // Delete file
    op_.remove(path);
    EXPECT_FALSE(op_.exists(path));
    
    // Recreate with different content
    op_.write(path, new_content);
    EXPECT_TRUE(op_.exists(path));
    auto result2 = op_.read(path);
    EXPECT_EQ(result2, new_content);
    EXPECT_NE(result2, original_content);
}

// Test deleting nested structure
OPENDAL_TEST_F(DeleteBehaviorTest, DeleteNestedStructure) {
    std::string base_dir = "test_nested/";
    auto level1_dir = base_dir + "level1/";
    auto level2_dir = level1_dir + "level2/";
    auto level3_dir = level2_dir + "level3/";
    auto deep_file = level3_dir + "deep_file.txt";
    
    // Create nested structure
    op_.create_dir(base_dir);
    op_.create_dir(level1_dir);
    op_.create_dir(level2_dir);
    op_.create_dir(level3_dir);
    op_.write(deep_file, random_string(100));
    
    // Verify structure exists
    EXPECT_TRUE(op_.exists(base_dir));
    EXPECT_TRUE(op_.exists(level1_dir));
    EXPECT_TRUE(op_.exists(level2_dir));
    EXPECT_TRUE(op_.exists(level3_dir));
    EXPECT_TRUE(op_.exists(deep_file));
    
    // Remove all from base
    op_.remove_all(base_dir);
    
    // Verify everything is gone
    EXPECT_FALSE(op_.exists(deep_file));
    EXPECT_FALSE(op_.exists(level3_dir));
    EXPECT_FALSE(op_.exists(level2_dir));
    EXPECT_FALSE(op_.exists(level1_dir));
    EXPECT_FALSE(op_.exists(base_dir));
}

} // namespace opendal::test 