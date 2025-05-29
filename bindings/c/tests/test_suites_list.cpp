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

#include "test_framework.h"
#include <set>
#include <string>

// Test: Basic list operation
void test_list_basic(opendal_test_context* ctx) {
    const char* dir_path = "test_list_dir/";
    
    // Create directory
    opendal_error* error = opendal_operator_create_dir(ctx->config->operator_instance, dir_path);
    OPENDAL_ASSERT_NO_ERROR(error, "Create dir operation should succeed");
    
    // Create some test files
    const char* test_files[] = {"test_list_dir/file1.txt", "test_list_dir/file2.txt", "test_list_dir/file3.txt"};
    const size_t num_files = sizeof(test_files) / sizeof(test_files[0]);
    
    for (size_t i = 0; i < num_files; i++) {
        opendal_bytes data;
        data.data = (uint8_t*)"test content";
        data.len = 12;
        data.capacity = 12;
        error = opendal_operator_write(ctx->config->operator_instance, test_files[i], &data);
        OPENDAL_ASSERT_NO_ERROR(error, "Write operation should succeed");
    }
    
    // List directory
    opendal_result_list list_result = opendal_operator_list(ctx->config->operator_instance, dir_path);
    OPENDAL_ASSERT_NO_ERROR(list_result.error, "List operation should succeed");
    OPENDAL_ASSERT_NOT_NULL(list_result.lister, "Lister should not be null");
    
    // Collect all entries
    std::set<std::string> found_paths;
    while (true) {
        opendal_result_lister_next next_result = opendal_lister_next(list_result.lister);
        if (next_result.error) {
            OPENDAL_ASSERT_NO_ERROR(next_result.error, "Lister next should not fail");
            break;
        }
        
        if (!next_result.entry) {
            // End of list
            break;
        }
        
        char* path = opendal_entry_path(next_result.entry);
        OPENDAL_ASSERT_NOT_NULL(path, "Entry path should not be null");
        found_paths.insert(std::string(path));
        
        free(path);
        opendal_entry_free(next_result.entry);
    }
    
    // Verify we found the directory and all files
    OPENDAL_ASSERT_EQ(num_files + 1, found_paths.size(), "Should find directory + all files");
    OPENDAL_ASSERT(found_paths.count(dir_path) > 0, "Should find the directory itself");
    
    for (size_t i = 0; i < num_files; i++) {
        OPENDAL_ASSERT(found_paths.count(test_files[i]) > 0, "Should find all test files");
    }
    
    // Cleanup
    opendal_lister_free(list_result.lister);
    for (size_t i = 0; i < num_files; i++) {
        opendal_operator_delete(ctx->config->operator_instance, test_files[i]);
    }
    opendal_operator_delete(ctx->config->operator_instance, dir_path);
}

// Test: List empty directory
void test_list_empty_dir(opendal_test_context* ctx) {
    const char* dir_path = "test_empty_dir/";
    
    // Create directory
    opendal_error* error = opendal_operator_create_dir(ctx->config->operator_instance, dir_path);
    OPENDAL_ASSERT_NO_ERROR(error, "Create dir operation should succeed");
    
    // List directory
    opendal_result_list list_result = opendal_operator_list(ctx->config->operator_instance, dir_path);
    OPENDAL_ASSERT_NO_ERROR(list_result.error, "List operation should succeed");
    OPENDAL_ASSERT_NOT_NULL(list_result.lister, "Lister should not be null");
    
    // Should only find the directory itself
    std::set<std::string> found_paths;
    while (true) {
        opendal_result_lister_next next_result = opendal_lister_next(list_result.lister);
        if (next_result.error) {
            OPENDAL_ASSERT_NO_ERROR(next_result.error, "Lister next should not fail");
            break;
        }
        
        if (!next_result.entry) {
            break;
        }
        
        char* path = opendal_entry_path(next_result.entry);
        found_paths.insert(std::string(path));
        free(path);
        opendal_entry_free(next_result.entry);
    }
    
    // Should find only the directory itself
    OPENDAL_ASSERT_EQ(1, found_paths.size(), "Should find only the directory");
    OPENDAL_ASSERT(found_paths.count(dir_path) > 0, "Should find the directory itself");
    
    // Cleanup
    opendal_lister_free(list_result.lister);
    opendal_operator_delete(ctx->config->operator_instance, dir_path);
}

// Test: List nested directories
void test_list_nested(opendal_test_context* ctx) {
    const char* base_dir = "test_nested/";
    const char* sub_dir = "test_nested/subdir/";
    const char* file_in_base = "test_nested/base_file.txt";
    const char* file_in_sub = "test_nested/subdir/sub_file.txt";
    
    // Create directories
    opendal_error* error = opendal_operator_create_dir(ctx->config->operator_instance, base_dir);
    OPENDAL_ASSERT_NO_ERROR(error, "Create base dir should succeed");
    
    error = opendal_operator_create_dir(ctx->config->operator_instance, sub_dir);
    OPENDAL_ASSERT_NO_ERROR(error, "Create sub dir should succeed");
    
    // Create files
    opendal_bytes data;
    data.data = (uint8_t*)"test content";
    data.len = 12;
    data.capacity = 12;
    
    error = opendal_operator_write(ctx->config->operator_instance, file_in_base, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write to base dir should succeed");
    
    error = opendal_operator_write(ctx->config->operator_instance, file_in_sub, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write to sub dir should succeed");
    
    // List base directory
    opendal_result_list list_result = opendal_operator_list(ctx->config->operator_instance, base_dir);
    OPENDAL_ASSERT_NO_ERROR(list_result.error, "List operation should succeed");
    
    std::set<std::string> found_paths;
    while (true) {
        opendal_result_lister_next next_result = opendal_lister_next(list_result.lister);
        if (next_result.error) {
            OPENDAL_ASSERT_NO_ERROR(next_result.error, "Lister next should not fail");
            break;
        }
        
        if (!next_result.entry) {
            break;
        }
        
        char* path = opendal_entry_path(next_result.entry);
        found_paths.insert(std::string(path));
        free(path);
        opendal_entry_free(next_result.entry);
    }
    
    // Should find base dir, sub dir, and file in base
    OPENDAL_ASSERT_EQ(3, found_paths.size(), "Should find 3 items in base directory");
    OPENDAL_ASSERT(found_paths.count(base_dir) > 0, "Should find base directory");
    OPENDAL_ASSERT(found_paths.count(sub_dir) > 0, "Should find sub directory");
    OPENDAL_ASSERT(found_paths.count(file_in_base) > 0, "Should find file in base directory");
    // Should NOT find file in subdirectory when listing base directory non-recursively
    OPENDAL_ASSERT(found_paths.count(file_in_sub) == 0, "Should not find file in subdirectory");
    
    // Cleanup
    opendal_lister_free(list_result.lister);
    opendal_operator_delete(ctx->config->operator_instance, file_in_sub);
    opendal_operator_delete(ctx->config->operator_instance, file_in_base);
    opendal_operator_delete(ctx->config->operator_instance, sub_dir);
    opendal_operator_delete(ctx->config->operator_instance, base_dir);
}

// Test: Entry name vs path
void test_entry_name_path(opendal_test_context* ctx) {
    const char* dir_path = "test_entry_names/";
    const char* file_path = "test_entry_names/test_file.txt";
    
    // Create directory and file
    opendal_error* error = opendal_operator_create_dir(ctx->config->operator_instance, dir_path);
    OPENDAL_ASSERT_NO_ERROR(error, "Create dir should succeed");
    
    opendal_bytes data;
    data.data = (uint8_t*)"test";
    data.len = 4;
    data.capacity = 4;
    error = opendal_operator_write(ctx->config->operator_instance, file_path, &data);
    OPENDAL_ASSERT_NO_ERROR(error, "Write should succeed");
    
    // List directory
    opendal_result_list list_result = opendal_operator_list(ctx->config->operator_instance, dir_path);
    OPENDAL_ASSERT_NO_ERROR(list_result.error, "List operation should succeed");
    
    bool found_file = false;
    while (true) {
        opendal_result_lister_next next_result = opendal_lister_next(list_result.lister);
        if (next_result.error) {
            OPENDAL_ASSERT_NO_ERROR(next_result.error, "Lister next should not fail");
            break;
        }
        
        if (!next_result.entry) {
            break;
        }
        
        char* path = opendal_entry_path(next_result.entry);
        char* name = opendal_entry_name(next_result.entry);
        
        if (strcmp(path, file_path) == 0) {
            found_file = true;
            OPENDAL_ASSERT_STR_EQ("test_file.txt", name, "Entry name should be just the filename");
            OPENDAL_ASSERT_STR_EQ(file_path, path, "Entry path should be the full path");
        }
        
        free(path);
        free(name);
        opendal_entry_free(next_result.entry);
    }
    
    OPENDAL_ASSERT(found_file, "Should have found the test file");
    
    // Cleanup
    opendal_lister_free(list_result.lister);
    opendal_operator_delete(ctx->config->operator_instance, file_path);
    opendal_operator_delete(ctx->config->operator_instance, dir_path);
}

// Define the list test suite
opendal_test_case list_tests[] = {
    {"list_basic", test_list_basic, make_capability_write_create_dir_list()},
    {"list_empty_dir", test_list_empty_dir, make_capability_create_dir_list()},
    {"list_nested", test_list_nested, make_capability_write_create_dir_list()},
    {"entry_name_path", test_entry_name_path, make_capability_write_create_dir_list()},
};

opendal_test_suite list_suite = {
    "List Operations",            // name
    list_tests,                   // tests
    sizeof(list_tests) / sizeof(list_tests[0])  // test_count
}; 