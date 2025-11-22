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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "opendal.h"

int main(int argc, char* argv[])
{
    printf("Starting OpenDAL async C example...\n");

    // Create a new async operator for the "memory" service
    // No options needed for memory backend
    printf("Creating async operator for 'memory' backend...\n");
    opendal_result_operator_new result_op = opendal_async_operator_new("memory", NULL);
    if (result_op.error != NULL) {
        // Use %.*s to print the message safely as it's not null-terminated
        printf("Error creating operator: %.*s (Code: %d)\n",
            (int)result_op.error->message.len,
            (char*)result_op.error->message.data,
            result_op.error->code);
        opendal_error_free(result_op.error);
        return 1;
    }

    // IMPORTANT: Cast the operator pointer from the result struct
    // The opendal_result_operator_new struct is reused, but for async,
    // the `op` field points to an opendal_async_operator.
    const opendal_async_operator* op = (const opendal_async_operator*)result_op.op;
    assert(op != NULL);
    printf("Async operator created successfully.\n");

    // --- Async await-style API ---
    const char* path = "non_existent_file.txt";
    printf("Calling async stat (future) for path: %s\n", path);

    opendal_result_future_stat future_result = opendal_async_operator_stat(op, path);
    if (future_result.error != NULL) {
        printf("Error creating future: %.*s (Code: %d)\n",
            (int)future_result.error->message.len,
            (char*)future_result.error->message.data,
            future_result.error->code);
        opendal_error_free(future_result.error);
        printf("Cleaning up resources...\n");
        opendal_async_operator_free(op);
        printf("OpenDAL async C example finished with errors.\n");
        return 1;
    }

    opendal_result_stat stat_result = opendal_future_stat_await(future_result.future);
    if (stat_result.error != NULL) {
        printf("Await failed as expected for non-existent file (future API).\n");
        printf("Error: %.*s (Code: %d)\n",
            (int)stat_result.error->message.len,
            (char*)stat_result.error->message.data,
            stat_result.error->code);
        assert(stat_result.error->code == OPENDAL_NOT_FOUND);
        opendal_error_free(stat_result.error);
    } else if (stat_result.meta != NULL) {
        // Should not happen in this example
        opendal_metadata_free(stat_result.meta);
    }

    // --- Async write/read/delete demo ---
    const char* write_path = "greeting.txt";
    const char* message = "hi from async write";
    opendal_bytes data = {
        .data = (uint8_t*)message,
        .len = strlen(message),
        .capacity = strlen(message),
    };

    printf("Writing '%s' to %s asynchronously...\n", message, write_path);
    opendal_result_future_write write_future = opendal_async_operator_write(op, write_path, &data);
    if (write_future.error != NULL) {
        printf("Write future creation failed: %.*s\n", (int)write_future.error->message.len, (char*)write_future.error->message.data);
        opendal_error_free(write_future.error);
    } else {
        opendal_error* write_err = opendal_future_write_await(write_future.future);
        if (write_err != NULL) {
            printf("Write failed: %.*s\n", (int)write_err->message.len, (char*)write_err->message.data);
            opendal_error_free(write_err);
        } else {
            printf("Write completed. Reading it back asynchronously...\n");
            opendal_result_future_read read_future = opendal_async_operator_read(op, write_path);
            if (read_future.error != NULL) {
                printf("Read future creation failed: %.*s\n", (int)read_future.error->message.len, (char*)read_future.error->message.data);
                opendal_error_free(read_future.error);
            } else {
                opendal_result_read read_result = opendal_future_read_await(read_future.future);
                if (read_result.error != NULL) {
                    printf("Read failed: %.*s\n", (int)read_result.error->message.len, (char*)read_result.error->message.data);
                    opendal_error_free(read_result.error);
                } else {
                    printf("Read back %zu bytes: %.*s\n", read_result.data.len, (int)read_result.data.len, read_result.data.data);
                    opendal_bytes_free(&read_result.data);
                }
            }

            printf("Deleting %s asynchronously...\n", write_path);
            opendal_result_future_delete delete_future = opendal_async_operator_delete(op, write_path);
            if (delete_future.error != NULL) {
                printf("Delete future creation failed: %.*s\n", (int)delete_future.error->message.len, (char*)delete_future.error->message.data);
                opendal_error_free(delete_future.error);
            } else {
                opendal_error* delete_err = opendal_future_delete_await(delete_future.future);
                if (delete_err != NULL) {
                    printf("Delete failed: %.*s\n", (int)delete_err->message.len, (char*)delete_err->message.data);
                    opendal_error_free(delete_err);
                } else {
                    printf("Delete completed.\n");
                }
            }
        }
    }

    // --- Cleanup ---
    printf("Cleaning up resources...\n");

    // Free the operator
    opendal_async_operator_free(op);

    printf("OpenDAL async C example finished.\n");
    return 0;
}
