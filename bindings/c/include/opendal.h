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


#ifndef _OPENDAL_H
#define _OPENDAL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/**
 * \brief The error code for all opendal APIs in C binding.
 * \todo The error handling is not complete, the error with error message will be
 * added in the future.
 */
typedef enum opendal_code {
  /**
   * returning it back. For example, s3 returns an internal service error.
   */
  OPENDAL_UNEXPECTED,
  /**
   * Underlying service doesn't support this operation.
   */
  OPENDAL_UNSUPPORTED,
  /**
   * The config for backend is invalid.
   */
  OPENDAL_CONFIG_INVALID,
  /**
   * The given path is not found.
   */
  OPENDAL_NOT_FOUND,
  /**
   * The given path doesn't have enough permission for this operation
   */
  OPENDAL_PERMISSION_DENIED,
  /**
   * The given path is a directory.
   */
  OPENDAL_IS_A_DIRECTORY,
  /**
   * The given path is not a directory.
   */
  OPENDAL_NOT_A_DIRECTORY,
  /**
   * The given path already exists thus we failed to the specified operation on it.
   */
  OPENDAL_ALREADY_EXISTS,
  /**
   * Requests that sent to this path is over the limit, please slow down.
   */
  OPENDAL_RATE_LIMITED,
  /**
   * The given file paths are same.
   */
  OPENDAL_IS_SAME_FILE,
} opendal_code;

/**
 * BlockingLister is designed to list entries at given path in a blocking
 * manner.
 *
 * Users can construct Lister by `blocking_lister`.
 */
typedef struct BlockingLister BlockingLister;

/**
 * BlockingOperator is the entry for all public blocking APIs.
 *
 * Read [`concepts`][docs::concepts] for know more about [`Operator`].
 *
 * # Examples
 *
 * ## Init backends
 *
 * Read more backend init examples in [`services`]
 *
 * ```
 * # use anyhow::Result;
 * use opendal::services::Fs;
 * use opendal::BlockingOperator;
 * use opendal::Operator;
 *
 * fn main() -> Result<()> {
 *     // Create fs backend builder.
 *     let mut builder = Fs::default();
 *     // Set the root for fs, all operations will happen under this root.
 *     //
 *     // NOTE: the root must be absolute path.
 *     builder.root("/tmp");
 *
 *     // Build an `BlockingOperator` to start operating the storage.
 *     let _: BlockingOperator = Operator::new(builder)?.finish().blocking();
 *
 *     Ok(())
 * }
 * ```
 *
 * ## Init backends with blocking layer
 *
 * Some services like s3, gcs doesn't have native blocking supports, we can use [`layers::BlockingLayer`]
 * to wrap the async operator to make it blocking.
 *
 * ```rust
 * # use anyhow::Result;
 * use opendal::layers::BlockingLayer;
 * use opendal::services::S3;
 * use opendal::BlockingOperator;
 * use opendal::Operator;
 *
 * #[tokio::main]
 * async fn main() -> Result<()> {
 *     // Create fs backend builder.
 *     let mut builder = S3::default();
 *     builder.bucket("test");
 *     builder.region("us-east-1");
 *
 *     // Build an `BlockingOperator` with blocking layer to start operating the storage.
 *     let _: BlockingOperator = Operator::new(builder)?
 *         .layer(BlockingLayer::create()?)
 *         .finish()
 *         .blocking();
 *
 *     Ok(())
 * }
 * ```
 */
typedef struct BlockingOperator BlockingOperator;

/**
 * BlockingReader is designed to read data from given path in an blocking
 * manner.
 */
typedef struct BlockingReader BlockingReader;

/**
 * Entry returned by [`Lister`] or [`BlockingLister`] to represent a path and it's relative metadata.
 *
 * # Notes
 *
 * Entry returned by [`Lister`] or [`BlockingLister`] may carry some already known metadata.
 * Lister by default only make sure that `Mode` is fetched. To make sure the entry contains
 * metadata you want, please use `list_with` or `lister_with` and `metakey`.
 *
 * For example:
 *
 * ```no_run
 * # use anyhow::Result;
 * use opendal::EntryMode;
 * use opendal::Metakey;
 * use opendal::Operator;
 * # #[tokio::main]
 * # async fn test(op: Operator) -> Result<()> {
 * let mut entries = op
 *     .list_with("dir/")
 *     .metakey(Metakey::ContentLength | Metakey::LastModified)
 *     .await?;
 * for entry in entries {
 *     let meta = entry.metadata();
 *     match meta.mode() {
 *         EntryMode::FILE => {
 *             println!(
 *                 "Handling file {} with size {}",
 *                 entry.path(),
 *                 meta.content_length()
 *             )
 *         }
 *         EntryMode::DIR => {
 *             println!("Handling dir {}", entry.path())
 *         }
 *         EntryMode::Unknown => continue,
 *     }
 * }
 * # Ok(())
 * # }
 * ```
 */
typedef struct Entry Entry;

typedef struct HashMap_String__String HashMap_String__String;

/**
 * Metadata carries all metadata associated with a path.
 *
 * # Notes
 *
 * mode and content_length are required metadata that all services
 * should provide during `stat` operation. But in `list` operation,
 * a.k.a., `Entry`'s content length could be `None`.
 */
typedef struct Metadata Metadata;

/**
 * \brief opendal_bytes carries raw-bytes with its length
 *
 * The opendal_bytes type is a C-compatible substitute for Vec type
 * in Rust, it has to be manually freed. You have to call opendal_bytes_free()
 * to free the heap memory to avoid memory leak.
 *
 * @see opendal_bytes_free
 */
typedef struct opendal_bytes {
  /**
   * Pointing to the byte array on heap
   */
  const uint8_t *data;
  /**
   * The length of the byte array
   */
  uintptr_t len;
} opendal_bytes;

/**
 * \brief The opendal error type for C binding, containing an error code and corresponding error
 * message.
 *
 * The normal operations returns a pointer to the opendal_error, and the **nullptr normally
 * represents no error has taken placed**. If any error has taken place, the caller should check
 * the error code and print the error message.
 *
 * The error code is represented in opendal_code, which is a enum on different type of errors.
 * The error messages is represented in opendal_bytes, which is a non-null terminated byte array.
 *
 * \note 1. The error message is on heap, so the error needs to be freed by the caller, by calling
 *       opendal_error_free. 2. The error message is not null terminated, so the caller should
 *       never use "%s" to print the error message.
 *
 * @see opendal_code
 * @see opendal_bytes
 * @see opendal_error_free
 */
typedef struct opendal_error {
  enum opendal_code code;
  struct opendal_bytes message;
} opendal_error;

/**
 * \brief opendal_list_entry is the entry under a path, which is listed from the opendal_lister
 *
 * For examples, please see the comment section of opendal_operator_list()
 * @see opendal_operator_list()
 * @see opendal_list_entry_path()
 * @see opendal_list_entry_name()
 */
typedef struct opendal_entry {
  struct Entry *inner;
} opendal_entry;

/**
 * \brief The result type returned by opendal_lister_next().
 * The list entry is the list result of the list operation, the error field is the error code and error message.
 * If the operation succeeds, the error should be NULL.
 *
 * \note Please notice if the lister reaches the end, both the list_entry and error will be NULL.
 */
typedef struct opendal_result_lister_next {
  /**
   * The next object name
   */
  struct opendal_entry *entry;
  /**
   * The error, if ok, it is null
   */
  struct opendal_error *error;
} opendal_result_lister_next;

/**
 * \brief BlockingLister is designed to list entries at given path in a blocking
 * manner.
 *
 * Users can construct Lister by `blocking_list` or `blocking_scan`(currently not supported in C binding)
 *
 * For examples, please see the comment section of opendal_operator_list()
 * @see opendal_operator_list()
 */
typedef struct opendal_lister {
  struct BlockingLister *inner;
} opendal_lister;

/**
 * \brief Carries all metadata associated with a path.
 *
 * The metadata of the "thing" under a path. Please **only** use the opendal_metadata
 * with our provided API, e.g. opendal_metadata_content_length().
 *
 * \note The metadata is also heap-allocated, please call opendal_metadata_free() on this
 * to free the heap memory.
 *
 * @see opendal_metadata_free
 */
typedef struct opendal_metadata {
  /**
   * The pointer to the opendal::Metadata in the Rust code.
   * Only touch this on judging whether it is NULL.
   */
  struct Metadata *inner;
} opendal_metadata;

/**
 * \brief Used to access almost all OpenDAL APIs. It represents a
 * operator that provides the unified interfaces provided by OpenDAL.
 *
 * @see opendal_operator_new This function construct the operator
 * @see opendal_operator_free This function frees the heap memory of the operator
 *
 * \note The opendal_operator actually owns a pointer to
 * a opendal::BlockingOperator, which is inside the Rust core code.
 *
 * \remark You may use the field `ptr` to check whether this is a NULL
 * operator.
 */
typedef struct opendal_operator {
  /**
   * The pointer to the opendal::BlockingOperator in the Rust code.
   * Only touch this on judging whether it is NULL.
   */
  const struct BlockingOperator *ptr;
} opendal_operator;

/**
 * \brief The result type returned by opendal_operator_new() operation.
 *
 * If the init logic is successful, the `op` field will be set to a valid
 * pointer, and the `error` field will be set to null. If the init logic fails, the
 * `op` field will be set to null, and the `error` field will be set to a
 * valid pointer with error code and error message.
 *
 * @see opendal_operator_new()
 * @see opendal_operator
 * @see opendal_error
 */
typedef struct opendal_result_operator_new {
  /**
   * The pointer for operator.
   */
  struct opendal_operator *op;
  /**
   * The error pointer for error.
   */
  struct opendal_error *error;
} opendal_result_operator_new;

/**
 * \brief The configuration for the initialization of opendal_operator.
 *
 * \note This is also a heap-allocated struct, please free it after you use it
 *
 * @see opendal_operator_new has an example of using opendal_operator_options
 * @see opendal_operator_options_new This function construct the operator
 * @see opendal_operator_options_free This function frees the heap memory of the operator
 * @see opendal_operator_options_set This function allow you to set the options
 */
typedef struct opendal_operator_options {
  /**
   * The pointer to the Rust HashMap<String, String>
   * Only touch this on judging whether it is NULL.
   */
  struct HashMap_String__String *inner;
} opendal_operator_options;

/**
 * \brief The result type returned by opendal's read operation.
 *
 * The result type of read operation in opendal C binding, it contains
 * the data that the read operation returns and an NULL error.
 * If the read operation failed, the `data` fields should be a nullptr
 * and the error is not NULL.
 */
typedef struct opendal_result_read {
  /**
   * The byte array with length returned by read operations
   */
  struct opendal_bytes *data;
  /**
   * The error, if ok, it is null
   */
  struct opendal_error *error;
} opendal_result_read;

/**
 * \brief The result type returned by opendal's reader operation.
 *
 * \note The opendal_reader actually owns a pointer to
 * a opendal::BlockingReader, which is inside the Rust core code.
 */
typedef struct opendal_reader {
  struct BlockingReader *inner;
} opendal_reader;

/**
 * \brief The result type returned by opendal_operator_reader().
 * The result type for opendal_operator_reader(), the field `reader` contains the reader
 * of the path, which is an iterator of the objects under the path. the field `code` represents
 * whether the stat operation is successful.
 */
typedef struct opendal_result_operator_reader {
  /**
   * The pointer for opendal_reader
   */
  struct opendal_reader *reader;
  /**
   * The error, if ok, it is null
   */
  struct opendal_error *error;
} opendal_result_operator_reader;

/**
 * \brief The result type returned by opendal_operator_is_exist().
 *
 * The result type for opendal_operator_is_exist(), the field `is_exist`
 * contains whether the path exists, and the field `error` contains the
 * corresponding error. If successful, the `error` field is null.
 *
 * \note If the opendal_operator_is_exist() fails, the `is_exist` field
 * will be set to false.
 */
typedef struct opendal_result_is_exist {
  /**
   * Whether the path exists
   */
  bool is_exist;
  /**
   * The error, if ok, it is null
   */
  struct opendal_error *error;
} opendal_result_is_exist;

/**
 * \brief The result type returned by opendal_operator_stat().
 *
 * The result type for opendal_operator_stat(), the field `meta` contains the metadata
 * of the path, the field `error` represents whether the stat operation is successful.
 * If successful, the `error` field is null.
 */
typedef struct opendal_result_stat {
  /**
   * The metadata output of the stat
   */
  struct opendal_metadata *meta;
  /**
   * The error, if ok, it is null
   */
  struct opendal_error *error;
} opendal_result_stat;

/**
 * \brief The result type returned by opendal_operator_list().
 *
 * The result type for opendal_operator_list(), the field `lister` contains the lister
 * of the path, which is an iterator of the objects under the path. the field `error` represents
 * whether the stat operation is successful. If successful, the `error` field is null.
 */
typedef struct opendal_result_list {
  /**
   * The lister, used for further listing operations
   */
  struct opendal_lister *lister;
  /**
   * The error, if ok, it is null
   */
  struct opendal_error *error;
} opendal_result_list;

/**
 * \brief The is the result type returned by opendal_reader_read().
 * The result type contains a size field, which is the size of the data read,
 * which is zero on error. The error field is the error code and error message.
 */
typedef struct opendal_result_reader_read {
  /**
   * The read size if succeed.
   */
  uintptr_t size;
  /**
   * The error, if ok, it is null
   */
  struct opendal_error *error;
} opendal_result_reader_read;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * \brief Frees the opendal_error, ok to call on NULL
 */
void opendal_error_free(struct opendal_error *ptr);

/**
 * \brief Return the next object to be listed
 *
 * Lister is an iterator of the objects under its path, this method is the same as
 * calling next() on the iterator
 *
 * For examples, please see the comment section of opendal_operator_list()
 * @see opendal_operator_list()
 */
struct opendal_result_lister_next opendal_lister_next(const struct opendal_lister *self);

/**
 * \brief Free the heap-allocated metadata used by opendal_lister
 */
void opendal_lister_free(const struct opendal_lister *p);

/**
 * \brief Free the heap-allocated metadata used by opendal_metadata
 */
void opendal_metadata_free(struct opendal_metadata *ptr);

/**
 * \brief Return the content_length of the metadata
 *
 * # Example
 * ```C
 * // ... previously you wrote "Hello, World!" to path "/testpath"
 * opendal_result_stat s = opendal_operator_stat(ptr, "/testpath");
 * assert(s.error == NULL);
 *
 * opendal_metadata *meta = s.meta;
 * assert(opendal_metadata_content_length(meta) == 13);
 * ```
 */
uint64_t opendal_metadata_content_length(const struct opendal_metadata *self);

/**
 * \brief Return whether the path represents a file
 *
 * # Example
 * ```C
 * // ... previously you wrote "Hello, World!" to path "/testpath"
 * opendal_result_stat s = opendal_operator_stat(ptr, "/testpath");
 * assert(s.error == NULL);
 *
 * opendal_metadata *meta = s.meta;
 * assert(opendal_metadata_is_file(meta));
 * ```
 */
bool opendal_metadata_is_file(const struct opendal_metadata *self);

/**
 * \brief Return whether the path represents a directory
 *
 * # Example
 * ```C
 * // ... previously you wrote "Hello, World!" to path "/testpath"
 * opendal_result_stat s = opendal_operator_stat(ptr, "/testpath");
 * assert(s.error == NULL);
 *
 * opendal_metadata *meta = s.meta;
 *
 * // this is not a directory
 * assert(!opendal_metadata_is_dir(meta));
 * ```
 *
 * \todo This is not a very clear example. A clearer example will be added
 * after we support opendal_operator_mkdir()
 */
bool opendal_metadata_is_dir(const struct opendal_metadata *self);

/**
 * \brief Free the heap-allocated operator pointed by opendal_operator.
 *
 * Please only use this for a pointer pointing at a valid opendal_operator.
 * Calling this function on NULL does nothing, but calling this function on pointers
 * of other type will lead to segfault.
 *
 * # Example
 *
 * ```C
 * opendal_operator *ptr = opendal_operator_new("fs", NULL);
 * // ... use this ptr, maybe some reads and writes
 *
 * // free this operator
 * opendal_operator_free(ptr);
 * ```
 */
void opendal_operator_free(const struct opendal_operator *op);

/**
 * \brief Construct an operator based on `scheme` and `options`
 *
 * Uses an array of key-value pairs to initialize the operator based on provided `scheme`
 * and `options`. For each scheme, i.e. Backend, different options could be set, you may
 * reference the [documentation](https://opendal.apache.org/docs/category/services/) for
 * each service, especially for the **Configuration Part**.
 *
 * @param scheme the service scheme you want to specify, e.g. "fs", "s3", "supabase"
 * @param options the pointer to the options for this operators, it could be NULL, which means no
 * option is set
 * @see opendal_operator_options
 * @return A valid opendal_result_operator_new setup with the `scheme` and `options` is the construction
 * succeeds. On success the operator field is a valid pointer to a newly allocated opendal_operator,
 * and the error field is NULL. Otherwise, the operator field is a NULL pointer and the error field.
 *
 * # Example
 *
 * Following is an example.
 * ```C
 * // Allocate a new options
 * opendal_operator_options *options = opendal_operator_options_new();
 * // Set the options you need
 * opendal_operator_options_set(options, "root", "/myroot");
 *
 * // Construct the operator based on the options and scheme
 * opendal_result_operator_new result = opendal_operator_new("memory", options);
 * opendal_operator* op = result.operator;
 *
 * // you could free the options right away since the options is not used afterwards
 * opendal_operator_options_free(options);
 *
 * // ... your operations
 * ```
 *
 * # Safety
 *
 * The only unsafe case is passing a invalid c string pointer to the `scheme` argument.
 */
struct opendal_result_operator_new opendal_operator_new(const char *scheme,
                                                        const struct opendal_operator_options *options);

/**
 * \brief Blockingly write raw bytes to `path`.
 *
 * Write the `bytes` into the `path` blockingly by `op_ptr`.
 * Error is NULL if successful, otherwise it contains the error code and error message.
 *
 * \note It is important to notice that the `bytes` that is passes in will be consumed by this
 *       function. Therefore, you should not use the `bytes` after this function returns.
 *
 * @param ptr The opendal_operator created previously
 * @param path The designated path you want to write your bytes in
 * @param bytes The opendal_byte typed bytes to be written
 * @see opendal_operator
 * @see opendal_bytes
 * @see opendal_error
 * @return NULL if succeeds, otherwise it contains the error code and error message.
 *
 * # Example
 *
 * Following is an example
 * ```C
 * //...prepare your opendal_operator, named ptr for example
 *
 * // prepare your data
 * char* data = "Hello, World!";
 * opendal_bytes bytes = opendal_bytes { .data = (uint8_t*)data, .len = 13 };
 *
 * // now you can write!
 * opendal_error *err = opendal_operator_write(ptr, "/testpath", bytes);
 *
 * // Assert that this succeeds
 * assert(err == NULL);
 * ```
 *
 * # Safety
 *
 * It is **safe** under the cases below
 * * The memory pointed to by `path` must contain a valid nul terminator at the end of
 *   the string.
 * * The `bytes` provided has valid byte in the `data` field and the `len` field is set
 *   correctly.
 *
 * # Panic
 *
 * * If the `path` points to NULL, this function panics, i.e. exits with information
 */
struct opendal_error *opendal_operator_write(const struct opendal_operator *op,
                                             const char *path,
                                             struct opendal_bytes bytes);

/**
 * \brief Blockingly read the data from `path`.
 *
 * Read the data out from `path` blockingly by operator.
 *
 * @param ptr The opendal_operator created previously
 * @param path The path you want to read the data out
 * @see opendal_operator
 * @see opendal_result_read
 * @see opendal_error
 * @return Returns opendal_result_read, the `data` field is a pointer to a newly allocated
 * opendal_bytes, the `error` field contains the error. If the `error` is not NULL, then
 * the operation failed and the `data` field is a nullptr.
 *
 * \note If the read operation succeeds, the returned opendal_bytes is newly allocated on heap.
 * After your usage of that, please call opendal_bytes_free() to free the space.
 *
 * # Example
 *
 * Following is an example
 * ```C
 * // ... you have write "Hello, World!" to path "/testpath"
 *
 * opendal_result_read r = opendal_operator_read(ptr, "testpath");
 * assert(r.error == NULL);
 *
 * opendal_bytes *bytes = r.data;
 * assert(bytes->len == 13);
 * ```
 *
 * # Safety
 *
 * It is **safe** under the cases below
 * * The memory pointed to by `path` must contain a valid nul terminator at the end of
 *   the string.
 *
 * # Panic
 *
 * * If the `path` points to NULL, this function panics, i.e. exits with information
 */
struct opendal_result_read opendal_operator_read(const struct opendal_operator *op,
                                                 const char *path);

/**
 * \brief Blockingly read the data from `path`.
 *
 * Read the data out from `path` blockingly by operator, returns
 * an opendal_result_read with error code.
 *
 * @param ptr The opendal_operator created previously
 * @param path The path you want to read the data out
 * @param buffer The buffer you want to read the data into
 * @param buffer_len The length of the buffer
 * @see opendal_operator
 * @see opendal_result_read
 * @see opendal_code
 * @return Returns opendal_code
 *
 * \note If the read operation succeeds, the returned opendal_bytes is newly allocated on heap.
 * After your usage of that, please call opendal_bytes_free() to free the space.
 *
 * # Example
 *
 * Following is an example
 * ```C
 * // ... you have created an operator named op
 *
 * opendal_result_operator_reader result = opendal_operator_reader(op, "/testpath");
 * assert(result.error == NULL);
 * // The reader is in result.reader
 * opendal_reader *reader = result.reader;
 * ```
 *
 * # Safety
 *
 * It is **safe** under the cases below
 * * The memory pointed to by `path` must contain a valid nul terminator at the end of
 *   the string.
 *
 * # Panic
 *
 * * If the `path` points to NULL, this function panics, i.e. exits with information
 */
struct opendal_result_operator_reader opendal_operator_reader(const struct opendal_operator *op,
                                                              const char *path);

/**
 * \brief Blockingly delete the object in `path`.
 *
 * Delete the object in `path` blockingly by `op_ptr`.
 * Error is NULL if successful, otherwise it contains the error code and error message.
 *
 * @param ptr The opendal_operator created previously
 * @param path The designated path you want to delete
 * @see opendal_operator
 * @see opendal_error
 * @return NULL if succeeds, otherwise it contains the error code and error message.
 *
 * # Example
 *
 * Following is an example
 * ```C
 * //...prepare your opendal_operator, named ptr for example
 *
 * // prepare your data
 * char* data = "Hello, World!";
 * opendal_bytes bytes = opendal_bytes { .data = (uint8_t*)data, .len = 13 };
 * opendal_error *error = opendal_operator_write(ptr, "/testpath", bytes);
 *
 * assert(error == NULL);
 *
 * // now you can delete!
 * opendal_error *error = opendal_operator_delete(ptr, "/testpath");
 *
 * // Assert that this succeeds
 * assert(error == NULL);
 * ```
 *
 * # Safety
 *
 * It is **safe** under the cases below
 * * The memory pointed to by `path` must contain a valid nul terminator at the end of
 *   the string.
 *
 * # Panic
 *
 * * If the `path` points to NULL, this function panics, i.e. exits with information
 */
struct opendal_error *opendal_operator_delete(const struct opendal_operator *op, const char *path);

/**
 * \brief Check whether the path exists.
 *
 * If the operation succeeds, no matter the path exists or not,
 * the error should be a nullptr. Otherwise, the field `is_exist`
 * is filled with false, and the error is set
 *
 * @param ptr The opendal_operator created previously
 * @param path The path you want to check existence
 * @see opendal_operator
 * @see opendal_result_is_exist
 * @see opendal_error
 * @return Returns opendal_result_is_exist, the `is_exist` field contains whether the path exists.
 * However, it the operation fails, the `is_exist` will contains false and the error will be set.
 *
 * # Example
 *
 * ```C
 * // .. you previously wrote some data to path "/mytest/obj"
 * opendal_result_is_exist e = opendal_operator_is_exist(ptr, "/mytest/obj");
 * assert(e.error == NULL);
 * assert(e.is_exist);
 *
 * // but you previously did **not** write any data to path "/yourtest/obj"
 * opendal_result_is_exist e = opendal_operator_is_exist(ptr, "/yourtest/obj");
 * assert(e.error == NULL);
 * assert(!e.is_exist);
 * ```
 *
 * # Safety
 *
 * It is **safe** under the cases below
 * * The memory pointed to by `path` must contain a valid nul terminator at the end of
 *   the string.
 *
 * # Panic
 *
 * * If the `path` points to NULL, this function panics, i.e. exits with information
 */
struct opendal_result_is_exist opendal_operator_is_exist(const struct opendal_operator *op,
                                                         const char *path);

/**
 * \brief Stat the path, return its metadata.
 *
 * Error is NULL if successful, otherwise it contains the error code and error message.
 *
 * @param ptr The opendal_operator created previously
 * @param path The path you want to stat
 * @see opendal_operator
 * @see opendal_result_stat
 * @see opendal_metadata
 * @return Returns opendal_result_stat, containing a metadata and an opendal_error.
 * If the operation succeeds, the `meta` field would holds a valid metadata and
 * the `error` field should hold nullptr. Otherwise the metadata will contain a
 * NULL pointer, i.e. invalid, and the `error` will be set correspondingly.
 *
 * # Example
 *
 * ```C
 * // ... previously you wrote "Hello, World!" to path "/testpath"
 * opendal_result_stat s = opendal_operator_stat(ptr, "/testpath");
 * assert(s.error == NULL);
 *
 * const opendal_metadata *meta = s.meta;
 *
 * // ... you could now use your metadata, notice that please only access metadata
 * // using the APIs provided by OpenDAL
 * ```
 *
 * # Safety
 *
 * It is **safe** under the cases below
 * * The memory pointed to by `path` must contain a valid nul terminator at the end of
 *   the string.
 *
 * # Panic
 *
 * * If the `path` points to NULL, this function panics, i.e. exits with information
 */
struct opendal_result_stat opendal_operator_stat(const struct opendal_operator *op,
                                                 const char *path);

/**
 * \brief Blockingly list the objects in `path`.
 *
 * List the object in `path` blockingly by `op_ptr`, return a result with a
 * opendal_lister. Users should call opendal_lister_next() on the
 * lister.
 *
 * @param ptr The opendal_operator created previously
 * @param path The designated path you want to delete
 * @see opendal_lister
 * @return Returns opendal_result_list, containing a lister and an opendal_error.
 * If the operation succeeds, the `lister` field would holds a valid lister and
 * the `error` field should hold nullptr. Otherwise the `lister`` will contain a
 * NULL pointer, i.e. invalid, and the `error` will be set correspondingly.
 *
 * # Example
 *
 * Following is an example
 * ```C
 * // You have written some data into some files path "root/dir1"
 * // Your opendal_operator was called ptr
 * opendal_result_list l = opendal_operator_list(ptr, "root/dir1");
 * assert(l.error == ERROR);
 *
 * opendal_lister *lister = l.lister;
 * opendal_list_entry *entry;
 *
 * while ((entry = opendal_lister_next(lister)) != NULL) {
 *     const char* de_path = opendal_list_entry_path(entry);
 *     const char* de_name = opendal_list_entry_name(entry);
 *     // ...... your operations
 *
 *     // remember to free the entry after you are done using it
 *     opendal_list_entry_free(entry);
 * }
 *
 * // and remember to free the lister
 * opendal_lister_free(lister);
 * ```
 *
 * # Safety
 *
 * It is **safe** under the cases below
 * * The memory pointed to by `path` must contain a valid nul terminator at the end of
 *   the string.
 *
 * # Panic
 *
 * * If the `path` points to NULL, this function panics, i.e. exits with information
 */
struct opendal_result_list opendal_operator_list(const struct opendal_operator *op,
                                                 const char *path);

/**
 * \brief Frees the heap memory used by the opendal_bytes
 */
void opendal_bytes_free(struct opendal_bytes *bs);

/**
 * \brief Construct a heap-allocated opendal_operator_options
 *
 * @return An empty opendal_operator_option, which could be set by
 * opendal_operator_option_set().
 *
 * @see opendal_operator_option_set
 */
struct opendal_operator_options *opendal_operator_options_new(void);

/**
 * \brief Set a Key-Value pair inside opendal_operator_options
 *
 * # Safety
 *
 * This function is unsafe because it dereferences and casts the raw pointers
 * Make sure the pointer of `key` and `value` point to a valid string.
 *
 * # Example
 *
 * ```C
 * opendal_operator_options *options = opendal_operator_options_new();
 * opendal_operator_options_set(options, "root", "/myroot");
 *
 * // .. use your opendal_operator_options
 *
 * opendal_operator_options_free(options);
 * ```
 */
void opendal_operator_options_set(struct opendal_operator_options *self,
                                  const char *key,
                                  const char *value);

/**
 * \brief Free the allocated memory used by [`opendal_operator_options`]
 */
void opendal_operator_options_free(const struct opendal_operator_options *options);

/**
 * \brief Path of entry.
 *
 * Path is relative to operator's root. Only valid in current operator.
 *
 * \note To free the string, you can directly call free()
 */
char *opendal_entry_path(const struct opendal_entry *self);

/**
 * \brief Name of entry.
 *
 * Name is the last segment of path.
 * If this entry is a dir, `Name` MUST endswith `/`
 * Otherwise, `Name` MUST NOT endswith `/`.
 *
 * \note To free the string, you can directly call free()
 */
char *opendal_entry_name(const struct opendal_entry *self);

/**
 * \brief Frees the heap memory used by the opendal_list_entry
 */
void opendal_entry_free(struct opendal_entry *ptr);

/**
 * \brief Read data from the reader.
 */
struct opendal_result_reader_read opendal_reader_read(const struct opendal_reader *reader,
                                                      uint8_t *buf,
                                                      uintptr_t len);

/**
 * \brief Frees the heap memory used by the opendal_reader.
 */
void opendal_reader_free(struct opendal_reader *ptr);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* _OPENDAL_H */
