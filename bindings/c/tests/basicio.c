/**
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

#include "assert.h"
#include "opendal.h"
#include "stdio.h"

void test_operator_rw(opendal_operator_ptr ptr) {
    // have to be valid ptr
    assert(ptr);

    // write some contents by the operator, must be successful
    char path[] = "test";
    char content[] = "Hello World";
    const opendal_bytes data = {
        .len = sizeof(content) - 1,
        .data = (uint8_t *)content,
    };
    opendal_code code = opendal_operator_blocking_write(ptr, path, data);
    assert(code == OPENDAL_OK);

    // reads the data out from the bytes, must be successful
    struct opendal_result_read r = opendal_operator_blocking_read(ptr, path);
    assert(r.code == OPENDAL_OK);
    assert(r.data->len == (sizeof(content) - 1));

    for (int i = 0; i < r.data->len; i++) {
        printf("%c", (char)(r.data->data[i]));
    }
    printf("\n");

    // free the bytes's heap memory
    opendal_bytes_free(r.data);
}

int main(int argc, char *argv[]) {
    // test memory operator
    char scheme_memory[] = "memory";
    opendal_operator_ptr op_mem = opendal_operator_new(scheme_memory, NULL);

    assert(op_mem);
    test_operator_rw(op_mem);

    // test fs operator
    char scheme_fs[] = "fs";
    char root[] = "/tmp";
    opendal_operator_ptr op_fs = opendal_operator_new(scheme_fs, root);
    assert(op_fs);
    test_operator_rw(op_fs);

    return 0;
}
