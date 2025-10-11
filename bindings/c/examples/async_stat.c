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
        goto cleanup;
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

    // --- Cleanup ---
    printf("Cleaning up resources...\n");

    // Free the operator
    opendal_async_operator_free(op);

    printf("OpenDAL async C example finished.\n");
    return 0;
}
