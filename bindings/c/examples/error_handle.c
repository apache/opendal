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

// this example shows how to get error message from opendal_error
int main()
{
    /* Initialize a operator for "memory" backend, with no options */
    opendal_result_operator_new result = opendal_operator_new("memory", 0);
    assert(result.op != NULL);
    assert(result.error == NULL);

    opendal_operator* op = result.op;

    /* The read is supposed to fail */
    opendal_result_read r = opendal_operator_read(op, "/testpath");
    assert(r.error != NULL);
    assert(r.error->code == OPENDAL_NOT_FOUND);

    /* Lets print the error message out */
    struct opendal_bytes* error_msg = &r.error->message;
    for (int i = 0; i < error_msg->len; ++i) {
        printf("%c", error_msg->data[i]);
    }

    /* free the error since the error is not NULL */
    opendal_error_free(r.error);

    /* the operator_ptr is also heap allocated */
    opendal_operator_free(op);
}
