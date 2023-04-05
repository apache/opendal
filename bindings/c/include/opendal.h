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


#ifndef _OPENDAL_H
#define _OPENDAL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/*
 The [`OperatorPtr`] owns a pointer to a [`BlockingOperator`].
 It is also the key struct that OpenDAL's APIs access the real
 operator's memory. The use of OperatorPtr is zero cost, it
 only returns a reference of the underlying Operator.
 */
typedef struct opendal_operator_ptr {
  const void *ptr;
} opendal_operator_ptr;

/*
 The [`Bytes`] type is a C-compatible substitute for [`Bytes`]
 in Rust, it will not be deallocated automatically like what
 has been done in Rust. Instead, you have to call [`free_bytes`]
 to free the heap memory to avoid memory leak.
 The field `data` should not be modified since it might causes
 the reallocation of the Vector.
 */
typedef struct opendal_bytes {
  const uint8_t *data;
  uintptr_t len;
} opendal_bytes;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/*
 Constructs a new [`OperatorPtr`] which contains a underlying [`BlockingOperator`]
 If the scheme is invalid, or the operator constructions failed, a nullptr is returned

 # Safety

 It is [safe] under two cases below
 * The memory pointed to by `scheme` must contain a valid nul terminator at the end of
   the string.
 * The `scheme` points to NULL, this function simply returns you a null opendal_operator_ptr
 */
struct opendal_operator_ptr opendal_new_operator(const char *scheme);

/*
 Write the data into the path blockingly by operator, returns whether the write succeeds

 # Safety

 It is [safe] under two cases below
 * The memory pointed to by `path` must contain a valid nul terminator at the end of
   the string.
 * The `path` points to NULL, this function simply returns you false
 */
bool opendal_operator_blocking_write(struct opendal_operator_ptr op_ptr,
                                     const char *path,
                                     struct opendal_bytes bytes);

/*
 Read the data out from path into a [`Bytes`] blockingly by operator, returns
 a pointer to the [`Bytes`] if succeeds, nullptr otherwise

 # Safety

 It is [safe] under two cases below
 * The memory pointed to by `path` must contain a valid nul terminator at the end of
   the string.
 * The `path` points to NULL, this function simply returns you a nullptr
 */
struct opendal_bytes *opendal_operator_blocking_read(struct opendal_operator_ptr op_ptr,
                                                     const char *path);

/*
 Hello, OpenDAL!
 */
void hello_opendal(void);

/*
 Returns whether the [`OperatorPtr`] is valid, i.e. whether
 there exists a underlying [`BlockingOperator`]
 */
bool opendal_is_ptr_valid(const struct opendal_operator_ptr *self);

/*
 Frees the heap memory used by the [`Bytes`]
 */
void opendal_free_bytes(const struct opendal_bytes *vec);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* _OPENDAL_H */
