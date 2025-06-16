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
#include <thread>
#include <atomic>

namespace opendal::test {

class ReadBehaviorTest : public OpenDALTest {
protected:
    void SetUp() override {
        OpenDALTest::SetUp();
    }
};

// Test reading non-existent file
OPENDAL_TEST_F(ReadBehaviorTest, ReadNonExistentFile) {
    auto path = random_path();
    
    EXPECT_THROW({
        auto content = op_.read(path);
    }, std::exception);
}

// Test reading empty file
OPENDAL_TEST_F(ReadBehaviorTest, ReadEmptyFile) {
    auto path = random_path();
    std::string empty_content = "";
    
    // Write empty file
    op_.write(path, empty_content);
    
    // Read it back
    auto result = op_.read(path);
    EXPECT_EQ(result, empty_content);
}

// Test reading small file
OPENDAL_TEST_F(ReadBehaviorTest, ReadSmallFile) {
    auto path = random_path();
    auto content = random_string(100);
    
    // Write the file
    op_.write(path, content);
    
    // Read it back
    auto result = op_.read(path);
    EXPECT_EQ(result, content);
}

// Test reading large file
OPENDAL_TEST_F(ReadBehaviorTest, ReadLargeFile) {
    auto path = random_path();
    auto content = random_string(1024 * 1024); // 1MB
    
    // Write the file
    op_.write(path, content);
    
    // Read it back
    auto result = op_.read(path);
    EXPECT_EQ(result, content);
}

// Test reading binary data
OPENDAL_TEST_F(ReadBehaviorTest, ReadBinaryData) {
    auto path = random_path();
    auto content = random_bytes(1000);
    
    // Write binary data
    std::string content_str(content.begin(), content.end());
    op_.write(path, content_str);
    
    // Read it back
    auto result = op_.read(path);
    std::vector<uint8_t> result_bytes(result.begin(), result.end());
    EXPECT_EQ(result_bytes, content);
}

// Test reading with special characters in path
OPENDAL_TEST_F(ReadBehaviorTest, ReadSpecialCharPath) {
    auto path = "test_with-special.chars_123/file.txt";
    auto content = random_string(100);
    
    // Create directory first
    op_.create_dir("test_with-special.chars_123/");
    
    // Write the file
    op_.write(path, content);
    
    // Read it back
    auto result = op_.read(path);
    EXPECT_EQ(result, content);
}

// Test reading multiple files
OPENDAL_TEST_F(ReadBehaviorTest, ReadMultipleFiles) {
    std::vector<std::string> paths;
    std::vector<std::string> contents;
    
    for (int i = 0; i < 10; ++i) {
        auto path = random_path();
        auto content = random_string(100 + i * 10);
        
        paths.push_back(path);
        contents.push_back(content);
        
        op_.write(path, content);
    }
    
    // Read all files back
    for (size_t i = 0; i < paths.size(); ++i) {
        auto result = op_.read(paths[i]);
        EXPECT_EQ(result, contents[i]);
    }
}

// Test reading after modification
OPENDAL_TEST_F(ReadBehaviorTest, ReadAfterModification) {
    auto path = random_path();
    auto original_content = random_string(100);
    auto modified_content = random_string(150);
    
    // Write original content
    op_.write(path, original_content);
    
    // Verify original content
    auto result1 = op_.read(path);
    EXPECT_EQ(result1, original_content);
    
    // Modify the file
    op_.write(path, modified_content);
    
    // Verify modified content
    auto result2 = op_.read(path);
    EXPECT_EQ(result2, modified_content);
}

// Test concurrent reads (if threading is available)
OPENDAL_TEST_F(ReadBehaviorTest, ConcurrentReads) {
    auto path = random_path();
    auto content = random_string(1000);
    
    // Write the file
    op_.write(path, content);
    
    const int num_threads = 10;
    std::vector<std::thread> threads;
    std::vector<std::string> results(num_threads);
    std::atomic<int> error_count{0};
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            try {
                results[i] = op_.read(path);
            } catch (const std::exception& e) {
                error_count++;
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(error_count, 0);
    for (const auto& result : results) {
        if (!result.empty()) {
            EXPECT_EQ(result, content);
        }
    }
}

} // namespace opendal::test 