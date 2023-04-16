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
 The error code for opendal APIs in C binding
 */
typedef enum opendal_code {
  /*
   All is well
   */
  OPENDAL_OK,
  /*
   General error
   */
  OPENDAL_ERROR,
  /*
   returning it back. For example, s3 returns an internal service error.
   */
  OPENDAL_UNEXPECTED,
  /*
   Underlying service doesn't support this operation.
   */
  OPENDAL_UNSUPPORTED,
  /*
   The config for backend is invalid.
   */
  OPENDAL_CONFIG_INVALID,
  /*
   The given path is not found.
   */
  OPENDAL_NOT_FOUND,
  /*
   The given path doesn't have enough permission for this operation
   */
  OPENDAL_PERMISSION_DENIED,
  /*
   The given path is a directory.
   */
  OPENDAL_IS_A_DIRECTORY,
  /*
   The given path is not a directory.
   */
  OPENDAL_NOT_A_DIRECTORY,
  /*
   The given path already exists thus we failed to the specified operation on it.
   */
  OPENDAL_ALREADY_EXISTS,
  /*
   Requests that sent to this path is over the limit, please slow down.
   */
  OPENDAL_RATE_LIMITED,
  /*
   The given file paths are same.
   */
  OPENDAL_IS_SAME_FILE,
} opendal_code;

/*
 BlockingOperator is the entry for all public blocking APIs.

 Read [`concepts`][docs::concepts] for know more about [`Operator`].

 # Examples

 Read more backend init examples in [`services`]

 ```
 # use anyhow::Result;
 use opendal::services::Fs;
 use opendal::BlockingOperator;
 use opendal::Operator;
 #[tokio::main]
 async fn main() -> Result<()> {
     // Create fs backend builder.
     let mut builder = Fs::default();
     // Set the root for fs, all operations will happen under this root.
     //
     // NOTE: the root must be absolute path.
     builder.root("/tmp");

     // Build an `BlockingOperator` to start operating the storage.
     let _: BlockingOperator = Operator::new(builder)?.finish().blocking();

     Ok(())
 }
 ```
 */
typedef struct BlockingOperator BlockingOperator;

/*
 The [`opendal_operator_ptr`] owns a pointer to a [`od::BlockingOperator`].
 It is also the key struct that OpenDAL's APIs access the real
 operator's memory. The use of OperatorPtr is zero cost, it
 only returns a reference of the underlying Operator.

 The [`opendal_operator_ptr`] also has a transparent layout, allowing you
 to check its validity by native boolean operator.
 e.g. you could check by (!ptr) on a [`opendal_operator_ptr`]
 */
typedef const struct BlockingOperator *opendal_operator_ptr;

/*
 The [`opendal_bytes`] type is a C-compatible substitute for [`Vec`]
 in Rust, it will not be deallocated automatically like what
 has been done in Rust. Instead, you have to call [`opendal_free_bytes`]
 to free the heap memory to avoid memory leak.
 */
typedef struct opendal_bytes {
  const uint8_t *data;
  uintptr_t len;
} opendal_bytes;

/*
 The Rust-like Result type of opendal C binding, it contains
 the data that the read operation returns and a error code
 If the read operation failed, the `data` fields should be a nullptr
 and the error code is NOT OPENDAL_OK.
 */
typedef struct opendal_result_read {
  struct opendal_bytes *data;
  enum opendal_code code;
} opendal_result_read;

/*
 The result type for [`opendal_operator_is_exist()`], the field `is_exist`
 contains whether the path exists, and the field `code` contains the
 corresponding error code.
 */
typedef struct opendal_result_is_exist {
  bool is_exist;
  enum opendal_code code;
} opendal_result_is_exist;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/*
 Returns a result type [`opendal_result_op`], with operator_ptr. If the construction succeeds
 the error is nullptr, otherwise it contains the error information.

 # Safety

 It is [safe] under two cases below
 * The memory pointed to by `scheme` must contain a valid nul terminator at the end of
   the string.
 * The `scheme` points to NULL, this function simply returns you a null opendal_operator_ptr
 */
opendal_operator_ptr opendal_operator_new(const char *scheme);

/*
 Free the allocated operator pointed by [`opendal_operator_ptr`]
 */
void opendal_operator_free(opendal_operator_ptr op_ptr);

/*
 Write the data into the path blockingly by operator, returns the error code OPENDAL_OK
 if succeeds, others otherwise

 # Safety

 It is [safe] under two cases below
 * The memory pointed to by `path` must contain a valid nul terminator at the end of
   the string.

 # Panic

 * If the `path` points to NULL, this function panics
 */
enum opendal_code opendal_operator_blocking_write(opendal_operator_ptr op_ptr,
                                                  const char *path,
                                                  struct opendal_bytes bytes);

/*
 Read the data out from path into a [`Bytes`] blockingly by operator, returns
 a result with error code. If the error code is not OPENDAL_OK, the `data` field
 of the result points to NULL.

 # Safety

 It is [safe] under two cases below
 * The memory pointed to by `path` must contain a valid nul terminator at the end of
   the string.

 # Panic

 * If the `path` points to NULL, this function panics
 */
struct opendal_result_read opendal_operator_blocking_read(opendal_operator_ptr op_ptr,
                                                          const char *path);

/*
 Check whether the path exists.

 If the operation succeeds, no matter the path exists or not,
 the error code should be opendal_code::OPENDAL_OK. Otherwise,
 the field `is_exist` is filled with false, and the error code
 is set correspondingly.

 # Safety

 It is [safe] under two cases below
 * The memory pointed to by `path` must contain a valid nul terminator at the end of
   the string.

 # Panic

 * If the `path` points to NULL, this function panics
 */
struct opendal_result_is_exist opendal_operator_is_exist(opendal_operator_ptr op_ptr,
                                                         const char *path);

/*
 Frees the heap memory used by the [`opendal_bytes`]
 */
void opendal_bytes_free(const struct opendal_bytes *self);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* _OPENDAL_H */
