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

// The Getting Started example for the C binding, embedded into the website
// guide. It runs against the in-memory service with no credentials, so CI can
// execute it directly. The region between the ANCHOR markers is what the docs
// show — keep it copy-pasteable.

// ANCHOR: quickstart
#include <assert.h>
#include <stdio.h>
#include "opendal.h"

int main(void)
{
    /* Build an operator for the "memory" service with no options. */
    opendal_operator *op = NULL;
    opendal_error *err = opendal_operator_new("memory", NULL, &op);
    assert(err == NULL);
    assert(op != NULL);

    /* Write bytes to a path. */
    const char *msg = "Hello, OpenDAL!";
    opendal_bytes data = {
        .data = (uint8_t *)msg,
        .len  = 15,
    };
    err = opendal_operator_write(op, "/hello.txt", &data);
    assert(err == NULL);

    /* Read the bytes back. */
    opendal_bytes read_bytes = { 0 };
    err = opendal_operator_read(op, "/hello.txt", &read_bytes);
    assert(err == NULL);
    assert(read_bytes.len == 15);
    printf("%.*s\n", (int)read_bytes.len, read_bytes.data); /* Hello, OpenDAL! */
    opendal_bytes_free(&read_bytes);

    /* Stat the path to get metadata. */
    opendal_metadata *meta = NULL;
    err = opendal_operator_stat(op, "/hello.txt", &meta);
    assert(err == NULL);
    printf("size = %llu\n", (unsigned long long)opendal_metadata_content_length(meta));
    opendal_metadata_free(meta);

    /* Delete the file. */
    opendal_error *del_err = opendal_operator_delete(op, "/hello.txt");
    assert(del_err == NULL);

    opendal_operator_free(op);
    return 0;
}
// ANCHOR_END: quickstart
