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

#include "opendal.h"
#include <stdio.h>
#include <string.h>

int main() {
    // Initialize the logger.
    // Set the RUST_LOG environment variable to control log levels, e.g.,
    // RUST_LOG=trace,opendal_c=debug
    opendal_init_logger();

    printf("OpenDAL C RUST_LOG logging example initialized.\n");
    printf("Try running with RUST_LOG=trace ./logging_example\n");

    // Create an operator for the "memory" scheme
    // No options are needed for the memory backend for this example.
    opendal_operator_options *options = opendal_operator_options_new();
    opendal_result_operator_new result_op_new = opendal_operator_new("memory", options);
    opendal_operator_options_free(options);

    if (result_op_new.error) {
        fprintf(stderr, "Failed to create operator: %.*s\n", (int)result_op_new.error->message.len, (char*)result_op_new.error->message.data);
        opendal_error_free(result_op_new.error);
        return 1;
    }

    opendal_operator *op = result_op_new.op;
    printf("Memory operator created successfully.\n");

    // Perform an operation that should generate logs
    const char *path = "test_logging.txt";
    printf("Checking if path '%s' exists...\n", path);
    opendal_result_exists result_exists = opendal_operator_exists(op, path);

    if (result_exists.error) {
        fprintf(stderr, "Error checking path existence: %.*s\n", (int)result_exists.error->message.len, (char*)result_exists.error->message.data);
        opendal_error_free(result_exists.error);
    } else {
        if (result_exists.exists) {
            printf("Path '%s' exists.\n", path);
        } else {
            printf("Path '%s' does not exist.\n", path);
        }
    }
    
    // Example of writing a small file to see more logs
    const char* content = "Hello from OpenDAL C logging example!";
    opendal_bytes data_to_write = {
        .data = (uint8_t*)content,
        .len = strlen(content)
    };
    printf("Writing to path '%s'\n", path);
    opendal_error* write_err = opendal_operator_write(op, path, &data_to_write);
    if (write_err) {
        fprintf(stderr, "Error writing to path: %.*s\n", (int)write_err->message.len, (char*)write_err->message.data);
        opendal_error_free(write_err);
    } else {
        printf("Successfully wrote to '%s'\n", path);
    }


    // Free the operator
    opendal_operator_free(op);
    printf("Operator freed.\n");

    printf("Logging example finished. Check console for logs if RUST_LOG was set.\n");

    return 0;
} 