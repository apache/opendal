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

class WriteBehaviorTest : public OpenDALTest {
protected:
    void SetUp() override {
        OpenDALTest::SetUp();
    }
};

// Test writing empty content
OPENDAL_TEST_F(WriteBehaviorTest, WriteEmptyContent) {
    auto path = random_path();
    std::string empty_content = "";
    
    op_.write(path, empty_content);
    
    // Verify the file exists and is empty
    EXPECT_TRUE(op_.exists(path));
    auto result = op_.read(path);
    EXPECT_EQ(result, empty_content);
}

// Test writing small content
OPENDAL_TEST_F(WriteBehaviorTest, WriteSmallContent) {
    auto path = random_path();
    auto content = random_string(100);
    
    op_.write(path, content);
    
    // Verify content
    EXPECT_TRUE(op_.exists(path));
    auto result = op_.read(path);
    EXPECT_EQ(result, content);
}

// Test writing large content
OPENDAL_TEST_F(WriteBehaviorTest, WriteLargeContent) {
    auto path = random_path();
    auto content = random_string(1024 * 1024); // 1MB
    
    op_.write(path, content);
    
    // Verify content
    EXPECT_TRUE(op_.exists(path));
    auto result = op_.read(path);
    EXPECT_EQ(result, content);
}

// Test writing binary data
OPENDAL_TEST_F(WriteBehaviorTest, WriteBinaryData) {
    auto path = random_path();
    auto content = random_bytes(1000);
    
    // Convert to string for writing
    std::string content_str(content.begin(), content.end());
    op_.write(path, content_str);
    
    // Verify content
    EXPECT_TRUE(op_.exists(path));
    auto result = op_.read(path);
    std::vector<uint8_t> result_bytes(result.begin(), result.end());
    EXPECT_EQ(result_bytes, content);
}

// Test overwriting existing file
OPENDAL_TEST_F(WriteBehaviorTest, OverwriteExistingFile) {
    auto path = random_path();
    auto original_content = random_string(100);
    auto new_content = random_string(200);
    
    // Write original content
    op_.write(path, original_content);
    auto result1 = op_.read(path);
    EXPECT_EQ(result1, original_content);
    
    // Overwrite with new content
    op_.write(path, new_content);
    auto result2 = op_.read(path);
    EXPECT_EQ(result2, new_content);
    EXPECT_NE(result2, original_content);
}

// Test writing to nested path (should create intermediate directories)
OPENDAL_TEST_F(WriteBehaviorTest, WriteToNestedPath) {
    auto path = "nested/deep/directory/file.txt";
    auto content = random_string(100);
    
    op_.write(path, content);
    
    // Verify content
    EXPECT_TRUE(op_.exists(path));
    auto result = op_.read(path);
    EXPECT_EQ(result, content);
}

// Test writing with special characters in filename
OPENDAL_TEST_F(WriteBehaviorTest, WriteSpecialCharFilename) {
    auto path = "test_with-special.chars_123/file-name_with.special.txt";
    auto content = random_string(100);
    
    op_.write(path, content);
    
    // Verify content
    EXPECT_TRUE(op_.exists(path));
    auto result = op_.read(path);
    EXPECT_EQ(result, content);
}

// Test writing multiple files
OPENDAL_TEST_F(WriteBehaviorTest, WriteMultipleFiles) {
    std::vector<std::string> paths;
    std::vector<std::string> contents;
    
    for (int i = 0; i < 10; ++i) {
        auto path = random_path();
        auto content = random_string(100 + i * 10);
        
        paths.push_back(path);
        contents.push_back(content);
        
        op_.write(path, content);
    }
    
    // Verify all files
    for (size_t i = 0; i < paths.size(); ++i) {
        EXPECT_TRUE(op_.exists(paths[i]));
        auto result = op_.read(paths[i]);
        EXPECT_EQ(result, contents[i]);
    }
}

// Test writing with unicode content
OPENDAL_TEST_F(WriteBehaviorTest, WriteUnicodeContent) {
    auto path = random_path();
    std::string unicode_content = "Hello 世界 🌍 Здравствуй мир";
    
    op_.write(path, unicode_content);
    
    // Verify content
    EXPECT_TRUE(op_.exists(path));
    auto result = op_.read(path);
    EXPECT_EQ(result, unicode_content);
}

// Test concurrent writes to different files
OPENDAL_TEST_F(WriteBehaviorTest, ConcurrentWritesDifferentFiles) {
    const int num_threads = 10;
    std::vector<std::thread> threads;
    std::vector<std::string> paths(num_threads);
    std::vector<std::string> contents(num_threads);
    std::atomic<int> error_count{0};
    
    for (int i = 0; i < num_threads; ++i) {
        paths[i] = random_path();
        contents[i] = random_string(100 + i);
    }
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            try {
                op_.write(paths[i], contents[i]);
            } catch (const std::exception& e) {
                error_count++;
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(error_count, 0);
    
    // Verify all files were written correctly
    for (int i = 0; i < num_threads; ++i) {
        EXPECT_TRUE(op_.exists(paths[i]));
        auto result = op_.read(paths[i]);
        EXPECT_EQ(result, contents[i]);
    }
}

// Test writing with different content sizes
OPENDAL_TEST_F(WriteBehaviorTest, WriteDifferentSizes) {
    std::vector<size_t> sizes = {0, 1, 10, 100, 1024, 10240, 102400};
    
    for (auto size : sizes) {
        auto path = random_path();
        auto content = random_string(size);
        
        op_.write(path, content);
        
        // Verify content
        EXPECT_TRUE(op_.exists(path));
        auto result = op_.read(path);
        EXPECT_EQ(result.size(), size);
        EXPECT_EQ(result, content);
    }
}

// Test appending behavior (if supported)
OPENDAL_TEST_F(WriteBehaviorTest, WriteAppendBehavior) {
    auto path = random_path();
    auto content1 = random_string(100);
    auto content2 = random_string(100);
    
    // Write first content
    op_.write(path, content1);
    auto result1 = op_.read(path);
    EXPECT_EQ(result1, content1);
    
    // Write second content (should overwrite, not append for normal write)
    op_.write(path, content2);
    auto result2 = op_.read(path);
    EXPECT_EQ(result2, content2);
    EXPECT_NE(result2, content1 + content2); // Should not be appended
}

} // namespace opendal::test 