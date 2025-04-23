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
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // For sleep

#include "opendal.h"

// Structure to hold callback results and synchronization primitives
typedef struct {
    pthread_mutex_t mtx;
    pthread_cond_t cv;
    bool completed;
    opendal_result_stat result; // Store the result here
} callback_args_t;

// C-style callback function conforming to opendal_stat_callback
void stat_callback(opendal_result_stat result, void* user_data)
{
    callback_args_t* args = (callback_args_t*)user_data;

    pthread_mutex_lock(&args->mtx);

    // Store the result (shallow copy is okay, ownership is transferred)
    args->result = result;
    args->completed = true;

    // Notify the waiting thread
    pthread_cond_signal(&args->cv);
    pthread_mutex_unlock(&args->mtx);

    printf("Callback: Stat operation completed.\n");
}

int main(int argc, char* argv[])
{
    printf("Starting OpenDAL async C example...\n");

    // Initialize callback arguments and synchronization primitives
    callback_args_t cb_args;
    cb_args.completed = false;
    cb_args.result.meta = NULL; // Initialize result fields
    cb_args.result.error = NULL;

    if (pthread_mutex_init(&cb_args.mtx, NULL) != 0) {
        perror("Mutex initialization failed");
        return 1;
    }
    if (pthread_cond_init(&cb_args.cv, NULL) != 0) {
        perror("Condition variable initialization failed");
        pthread_mutex_destroy(&cb_args.mtx);
        return 1;
    }

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
        pthread_mutex_destroy(&cb_args.mtx);
        pthread_cond_destroy(&cb_args.cv);
        return 1;
    }

    // IMPORTANT: Cast the operator pointer from the result struct
    // The opendal_result_operator_new struct is reused, but for async,
    // the `op` field points to an opendal_async_operator.
    const opendal_async_operator* op = (const opendal_async_operator*)result_op.op;
    assert(op != NULL);
    printf("Async operator created successfully.\n");

    // --- Example: Stat a non-existent file ---
    const char* path = "non_existent_file.txt";
    printf("Calling async stat for path: %s\n", path);

    // Call the asynchronous stat function with the callback
    opendal_async_operator_stat_with_callback(op, path, stat_callback, &cb_args);

    printf("Waiting for callback to complete...\n");
    // Wait for the callback to signal completion
    pthread_mutex_lock(&cb_args.mtx);
    while (!cb_args.completed) {
        pthread_cond_wait(&cb_args.cv, &cb_args.mtx);
    }
    pthread_mutex_unlock(&cb_args.mtx);
    printf("Callback completed. Processing result...\n");

    // Process the result received by the callback
    if (cb_args.result.error != NULL) {
        printf("Async stat failed as expected for non-existent file.\n");
        // Use %.*s to print the message safely as it's not null-terminated
        printf("Error: %.*s (Code: %d)\n",
            (int)cb_args.result.error->message.len,
            (char*)cb_args.result.error->message.data,
            cb_args.result.error->code);
        assert(cb_args.result.error->code == OPENDAL_NOT_FOUND);

        // Free the error structure
        opendal_error_free(cb_args.result.error);
        cb_args.result.error = NULL; // Avoid double free later
    } else {
        printf("Async stat succeeded unexpectedly!\n");
        // If it succeeded (it shouldn't here), free the metadata
        if (cb_args.result.meta != NULL) {
            opendal_metadata_free(cb_args.result.meta);
            cb_args.result.meta = NULL;
        }
    }

    // --- Cleanup ---
    printf("Cleaning up resources...\n");

    // Free the operator
    opendal_async_operator_free(op);

    // Destroy mutex and condition variable
    pthread_mutex_destroy(&cb_args.mtx);
    pthread_cond_destroy(&cb_args.cv);

    printf("OpenDAL async C example finished.\n");
    return 0;
}
