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

// Tests the basic IO operations work as expected
//
// Asserts:
// * A valid ptr is given
// * The blocking write operation is successful
// * The blocking read operation is successful and works as expected
void test_operator_rw(opendal_operator_ptr ptr)
{
    // have to be valid ptr
    assert(ptr);

    // write some contents by the operator, must be successful
    char path[] = "test";
    char content[] = "Hello World";
    const opendal_bytes data = {
        .len = sizeof(content) - 1,
        .data = (uint8_t*)content,
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

    // free the bytes's heap memory
    opendal_bytes_free(r.data);
}

int main(int argc, char* argv[])
{
    // construct the memory operator
    char scheme1[] = "memory";
    opendal_operator_ptr p1 = opendal_operator_new(scheme1);
    assert(p1);

    test_operator_rw(p1);

    // free the operator
    opendal_operator_free(p1);

    return 0;
}
