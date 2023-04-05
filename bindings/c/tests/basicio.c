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
#include "stdio.h"
#include "opendal.h"

int main(int argc, char *argv[]) {
    // creates a memory operator
    char scheme[] = "memory";
    opendal_operator_ptr ptr = opendal_new_operator(scheme);
    if (!opendal_is_ptr_valid(&ptr)) {
        return -1;
    }

    // write some contents by the operator
    char path[] = "test";
    char content[] = "Hello World";
    const opendal_bytes data = {
        .len = sizeof(content) - 1,
        .data = (uint8_t*)content,
    };
    if (!opendal_operator_blocking_write(ptr, path, data)) {
        return -2;
    }

    // reads the data out from the bytes
    opendal_bytes* v = opendal_operator_blocking_read(ptr, path);
    for (int i = 0; i < v->len; i++) {
        printf("%c", (char)(v->data[i]));
    }

    // free the bytes's heap memory
    opendal_free_bytes(v);

    return 0;
}
