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

// Example C callback function that mimics a glog-style logger.
// It prints the log to stdout.
void my_glog_logger(int level, const char* file, unsigned int line, const char* message) {
    const char* level_str = "UNKNOWN";
    switch (level) {
        case 0: level_str = "DEBUG/TRACE"; break; // Or map to glog VLOG
        case 1: level_str = "INFO"; break;
        case 2: level_str = "WARNING"; break;
        case 3: level_str = "ERROR"; break;
        default: level_str = "FATAL/UNKNOWN"; break; // glog also has FATAL
    }

    if (file) {
        printf("[%s] %s:%u: %s\n", level_str, file, line, message);
    } else {
        printf("[%s] %s\n", level_str, message);
    }
    fflush(stdout); // Ensure logs are printed immediately
}

int main() {
    printf("OpenDAL C glog integration example initializing.\n");

    // Initialize OpenDAL with our custom glog-style logger.
    opendal_init_glog_logging(my_glog_logger);

    printf("glog-style logger initialized. OpenDAL operations will now use it.\n");

    // Create an operator for the "memory" scheme
    opendal_operator_options *options = opendal_operator_options_new();
    opendal_result_operator_new result_op_new = opendal_operator_new("memory", options);
    opendal_operator_options_free(options);

    if (result_op_new.error) {
        // This error will also go through our logger if it occurs after initialization,
        // but opendal_operator_new itself might log internally before full op setup.
        // For critical init errors, it's also good to check directly.
        fprintf(stderr, "Failed to create operator: %s\n", (const char*)result_op_new.error->message.data);
        my_glog_logger(3, __FILE__, __LINE__, (const char*)result_op_new.error->message.data); // Manually log critical error
        opendal_error_free(result_op_new.error);
        return 1;
    }

    opendal_operator *op = result_op_new.op;
    printf("Memory operator created successfully.\n");

    // Perform an operation that should generate logs
    const char *path = "test_glog_logging.txt";
    printf("Checking if path '%s' exists... (This should be logged by glog adapter)\n", path);
    // Use the deprecated one first to see if it still logs, then the new one
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    opendal_result_is_exist old_result_is_exist = opendal_operator_is_exist(op, path);
    #pragma GCC diagnostic pop

    if (old_result_is_exist.error) {
        // This will be logged via glog adapter
        opendal_error_free(old_result_is_exist.error);
    } else {
        // This info message might also appear in logs if level is appropriate
    }

    opendal_result_exists result_exists = opendal_operator_exists(op, path);
    if (result_exists.error) {
        // This will be logged via glog adapter
        opendal_error_free(result_exists.error);
    }

    // Example of writing a small file to see more logs
    const char* content = "Hello from OpenDAL C glog example!";
    opendal_bytes data_to_write = {
        .data = (uint8_t*)content,
        .len = strlen(content)
    };
    printf("Writing to path '%s' (This should be logged by glog adapter)\n", path);
    opendal_error* write_err = opendal_operator_write(op, path, &data_to_write);
    if (write_err) {
        // This error will be logged via glog adapter
        opendal_error_free(write_err);
    } else {
        printf("Successfully wrote to '%s'\n", path);
    }

    // Free the operator
    opendal_operator_free(op);
    printf("Operator freed.\n");

    printf("glog example finished. Check console for logs via my_glog_logger.\n");

    return 0;
} 