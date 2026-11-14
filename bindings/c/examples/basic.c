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

#include "assert.h"
#include "opendal.h"
#include "stdio.h"

int main()
{
    /* Initialize a operator for "memory" backend, with no options */
    opendal_operator* op = NULL;
    opendal_error* error = opendal_operator_new("memory", 0, &op);
    assert(op != NULL);
    assert(error == NULL);

    /* Prepare some data to be written */
    opendal_bytes data = {
        .data = (uint8_t*)"this_string_length_is_24",
        .len = 24,
    };

    /* Write this into path "/testpath" */
    error = opendal_operator_write(op, "/testpath", &data);
    assert(error == NULL);

    /* We can read it out, make sure the data is the same */
    opendal_bytes read_bytes = { 0 };
    error = opendal_operator_read(op, "/testpath", &read_bytes);
    assert(error == NULL);
    assert(read_bytes.len == 24);

    /* Lets print it out */
    for (int i = 0; i < 24; ++i) {
        printf("%c", read_bytes.data[i]);
    }
    printf("\n");

    /* the opendal_bytes read is heap allocated, please free it */
    opendal_bytes_free(&read_bytes);

    /* the operator_ptr is also heap allocated */
    opendal_operator_free(op);
}
