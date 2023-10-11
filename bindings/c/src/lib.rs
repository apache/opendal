// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![warn(missing_docs)]
#![allow(non_camel_case_types)]

//! The OpenDAL C binding.
//!
//! The OpenDAL C binding allows users to utilize the OpenDAL's amazing storage accessing capability
//! in the C programming language.
//!
//! For examples, you may see the examples subdirectory

mod error;
mod result;
mod types;

use core::slice;
use std::collections::HashMap;
use std::io::Read;
use std::os::raw::c_char;
use std::str::FromStr;

use ::opendal as od;
use error::opendal_error;
use result::opendal_result_list;
use result::opendal_result_operator_new;
use types::opendal_blocking_lister;

use crate::result::opendal_result_is_exist;
use crate::result::opendal_result_read;
use crate::result::opendal_result_stat;
use crate::types::opendal_bytes;
use crate::types::opendal_metadata;
use crate::types::opendal_operator_options;
use crate::types::opendal_operator_ptr;

/// \brief Construct an operator based on `scheme` and `options`
///
/// Uses an array of key-value pairs to initialize the operator based on provided `scheme`
/// and `options`. For each scheme, i.e. Backend, different options could be set, you may
/// reference the [documentation](https://opendal.apache.org/docs/category/services/) for
/// each service, especially for the **Configuration Part**.
///
/// @param scheme the service scheme you want to specify, e.g. "fs", "s3", "supabase"
/// @param options the pointer to the options for this operators, it could be NULL, which means no
/// option is set
/// @see opendal_operator_options
/// @return A valid opendal_result_operator_new setup with the `scheme` and `options` is the construction
/// succeeds. On success the operator_ptr field is a valid pointer to a newly allocated opendal_operator_ptr,
/// and the error field is NULL. Otherwise, the operator_ptr field is a NULL pointer and the error field.
///
/// # Example
///
/// Following is an example.
/// ```C
/// // Allocate a new options
/// opendal_operator_options *options = opendal_operator_options_new();
/// // Set the options you need
/// opendal_operator_options_set(options, "root", "/myroot");
///
/// // Construct the operator based on the options and scheme
/// opendal_result_operator_new result = opendal_operator_new("memory", options);
/// opendal_operator_ptr* op = result.operator_ptr;
///
/// // you could free the options right away since the options is not used afterwards
/// opendal_operator_options_free(options);
///
/// // ... your operations
/// ```
///
/// # Safety
///
/// The only unsafe case is passing a invalid c string pointer to the `scheme` argument.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_new(
    scheme: *const c_char,
    options: *const opendal_operator_options,
) -> opendal_result_operator_new {
    if scheme.is_null() {
        let error = opendal_error::manual_error(
            error::opendal_code::OPENDAL_CONFIG_INVALID,
            "The scheme given is pointing at NULL".into(),
        );
        let result = opendal_result_operator_new {
            operator_ptr: std::ptr::null_mut(),
            error: Box::into_raw(Box::new(error)),
        };
        return result;
    }

    let scheme_str = unsafe { std::ffi::CStr::from_ptr(scheme).to_str().unwrap() };
    let scheme = match od::Scheme::from_str(scheme_str) {
        Ok(s) => s,
        Err(e) => {
            let e = opendal_error::from_opendal_error(e);
            let result = opendal_result_operator_new {
                operator_ptr: std::ptr::null_mut(),
                error: Box::into_raw(Box::new(e)),
            };
            return result;
        }
    };

    let mut map = HashMap::default();
    if !options.is_null() {
        for (k, v) in (*options).as_ref() {
            map.insert(k.to_string(), v.to_string());
        }
    }

    let op = match od::Operator::via_map(scheme, map) {
        Ok(o) => o.blocking(),
        Err(e) => {
            let e = opendal_error::from_opendal_error(e);
            let result = opendal_result_operator_new {
                operator_ptr: std::ptr::null_mut(),
                error: Box::into_raw(Box::new(e)),
            };
            return result;
        }
    };

    // this prevents the operator memory from being dropped by the Box
    let op = opendal_operator_ptr::from(Box::into_raw(Box::new(op)));
    opendal_result_operator_new {
        operator_ptr: Box::into_raw(Box::new(op)),
        error: std::ptr::null_mut(),
    }
}

/// \brief Blockingly write raw bytes to `path`.
///
/// Write the `bytes` into the `path` blockingly by `op_ptr`.
/// Error is NULL if successful, otherwise it contains the error code and error message.
///
/// \note It is important to notice that the `bytes` that is passes in will be consumed by this
///       function. Therefore, you should not use the `bytes` after this function returns.
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The designated path you want to write your bytes in
/// @param bytes The opendal_byte typed bytes to be written
/// @see opendal_operator_ptr
/// @see opendal_bytes
/// @see opendal_error
/// @return NULL if succeeds, otherwise it contains the error code and error message.
///
/// # Example
///
/// Following is an example
/// ```C
/// //...prepare your opendal_operator_ptr, named ptr for example
///
/// // prepare your data
/// char* data = "Hello, World!";
/// opendal_bytes bytes = opendal_bytes { .data = (uint8_t*)data, .len = 13 };
///
/// // now you can write!
/// opendal_error *err = opendal_operator_blocking_write(ptr, "/testpath", bytes);
///
/// // Assert that this succeeds
/// assert(err == NULL);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * The `bytes` provided has valid byte in the `data` field and the `len` field is set
///   correctly.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_write(
    ptr: *const opendal_operator_ptr,
    path: *const c_char,
    bytes: opendal_bytes,
) -> *mut opendal_error {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = (*ptr).as_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    match op.write(path, bytes) {
        Ok(_) => std::ptr::null_mut(),
        Err(e) => {
            let e = Box::new(opendal_error::from_opendal_error(e));
            Box::into_raw(e)
        }
    }
}

/// \brief Blockingly read the data from `path`.
///
/// Read the data out from `path` blockingly by operator.
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The path you want to read the data out
/// @see opendal_operator_ptr
/// @see opendal_result_read
/// @see opendal_error
/// @return Returns opendal_result_read, the `data` field is a pointer to a newly allocated
/// opendal_bytes, the `error` field contains the error. If the `error` is not NULL, then
/// the operation failed and the `data` field is a nullptr.
///
/// \note If the read operation succeeds, the returned opendal_bytes is newly allocated on heap.
/// After your usage of that, please call opendal_bytes_free() to free the space.
///
/// # Example
///
/// Following is an example
/// ```C
/// // ... you have write "Hello, World!" to path "/testpath"
///
/// opendal_result_read r = opendal_operator_blocking_read(ptr, "testpath");
/// assert(r.error == NULL);
///
/// opendal_bytes *bytes = r.data;
/// assert(bytes->len == 13);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_read(
    ptr: *const opendal_operator_ptr,
    path: *const c_char,
) -> opendal_result_read {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = (*ptr).as_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    let data = op.read(path);
    match data {
        Ok(d) => {
            let v = Box::new(opendal_bytes::new(d));
            opendal_result_read {
                data: Box::into_raw(v),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => {
            let e = Box::new(opendal_error::from_opendal_error(e));
            opendal_result_read {
                data: std::ptr::null_mut(),
                error: Box::into_raw(e),
            }
        }
    }
}

/// \brief Blockingly read the data from `path`.
///
/// Read the data out from `path` blockingly by operator, returns
/// an opendal_result_read with error code.
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The path you want to read the data out
/// @param buffer The buffer you want to read the data into
/// @param buffer_len The length of the buffer
/// @see opendal_operator_ptr
/// @see opendal_result_read
/// @see opendal_code
/// @return Returns opendal_code
///
/// \note If the read operation succeeds, the returned opendal_bytes is newly allocated on heap.
/// After your usage of that, please call opendal_bytes_free() to free the space.
///
/// # Example
///
/// Following is an example
/// ```C
/// // ... you have write "Hello, World!" to path "/testpath"
///
/// int length = 13;
/// unsigned char buffer[length];
/// opendal_code r = opendal_operator_blocking_read_with_buffer(ptr, "testpath", buffer, length);
/// assert(r == OPENDAL_OK);
/// // assert buffer == "Hello, World!"
///
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_read_with_buffer(
    ptr: *const opendal_operator_ptr,
    path: *const c_char,
    buffer: *mut u8,
    buffer_len: usize,
) -> opendal_code {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }
    if buffer.is_null() {
        panic!("The buffer given is pointing at NULL");
    }
    let op = (*ptr).as_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    match op.reader(path) {
        Ok(mut reader) => {
            let mut buf = unsafe { slice::from_raw_parts_mut(buffer, buffer_len) };
            match reader.read(&mut buf) {
                Ok(_) => opendal_code::OPENDAL_OK,
                Err(_) => opendal_code::OPENDAL_UNEXPECTED,
            }
        }
        Err(e) => opendal_code::from_opendal_error(e),
    }
}

/// \brief Blockingly delete the object in `path`.
///
/// Delete the object in `path` blockingly by `op_ptr`.
/// Error is NULL if successful, otherwise it contains the error code and error message.
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The designated path you want to delete
/// @see opendal_operator_ptr
/// @see opendal_error
/// @return NULL if succeeds, otherwise it contains the error code and error message.
///
/// # Example
///
/// Following is an example
/// ```C
/// //...prepare your opendal_operator_ptr, named ptr for example
///
/// // prepare your data
/// char* data = "Hello, World!";
/// opendal_bytes bytes = opendal_bytes { .data = (uint8_t*)data, .len = 13 };
/// opendal_error *error = opendal_operator_blocking_write(ptr, "/testpath", bytes);
///
/// assert(error == NULL);
///
/// // now you can delete!
/// opendal_error *error = opendal_operator_blocking_delete(ptr, "/testpath");
///
/// // Assert that this succeeds
/// assert(error == NULL);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_delete(
    ptr: *const opendal_operator_ptr,
    path: *const c_char,
) -> *mut opendal_error {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = (*ptr).as_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    match op.delete(path) {
        Ok(_) => std::ptr::null_mut(),
        Err(e) => {
            let e = Box::new(opendal_error::from_opendal_error(e));
            Box::into_raw(e)
        }
    }
}

/// \brief Check whether the path exists.
///
/// If the operation succeeds, no matter the path exists or not,
/// the error should be a nullptr. Otherwise, the field `is_exist`
/// is filled with false, and the error is set
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The path you want to check existence
/// @see opendal_operator_ptr
/// @see opendal_result_is_exist
/// @see opendal_error
/// @return Returns opendal_result_is_exist, the `is_exist` field contains whether the path exists.
/// However, it the operation fails, the `is_exist` will contains false and the error will be set.
///
/// # Example
///
/// ```C
/// // .. you previously wrote some data to path "/mytest/obj"
/// opendal_result_is_exist e = opendal_operator_is_exist(ptr, "/mytest/obj");
/// assert(e.error == NULL);
/// assert(e.is_exist);
///
/// // but you previously did **not** write any data to path "/yourtest/obj"
/// opendal_result_is_exist e = opendal_operator_is_exist(ptr, "/yourtest/obj");
/// assert(e.error == NULL);
/// assert(!e.is_exist);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_is_exist(
    ptr: *const opendal_operator_ptr,
    path: *const c_char,
) -> opendal_result_is_exist {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = (*ptr).as_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    match op.is_exist(path) {
        Ok(e) => opendal_result_is_exist {
            is_exist: e,
            error: std::ptr::null_mut(),
        },
        Err(e) => {
            let e = Box::new(opendal_error::from_opendal_error(e));
            opendal_result_is_exist {
                is_exist: false,
                error: Box::into_raw(e),
            }
        }
    }
}

/// \brief Stat the path, return its metadata.
///
/// Error is NULL if successful, otherwise it contains the error code and error message.
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The path you want to stat
/// @see opendal_operator_ptr
/// @see opendal_result_stat
/// @see opendal_metadata
/// @return Returns opendal_result_stat, containing a metadata and an opendal_error.
/// If the operation succeeds, the `meta` field would holds a valid metadata and
/// the `error` field should hold nullptr. Otherwise the metadata will contain a
/// NULL pointer, i.e. invalid, and the `error` will be set correspondingly.
///
/// # Example
///
/// ```C
/// // ... previously you wrote "Hello, World!" to path "/testpath"
/// opendal_result_stat s = opendal_operator_stat(ptr, "/testpath");
/// assert(s.error == NULL);
///
/// const opendal_metadata *meta = s.meta;
///
/// // ... you could now use your metadata, notice that please only access metadata
/// // using the APIs provided by OpenDAL
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_stat(
    ptr: *const opendal_operator_ptr,
    path: *const c_char,
) -> opendal_result_stat {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = (*ptr).as_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    match op.stat(path) {
        Ok(m) => opendal_result_stat {
            meta: Box::into_raw(Box::new(opendal_metadata::new(m))),
            error: std::ptr::null_mut(),
        },
        Err(e) => {
            let e = Box::new(opendal_error::from_opendal_error(e));
            opendal_result_stat {
                meta: std::ptr::null_mut(),
                error: Box::into_raw(e),
            }
        }
    }
}

/// \brief Blockingly list the objects in `path`.
///
/// List the object in `path` blockingly by `op_ptr`, return a result with a
/// opendal_blocking_lister. Users should call opendal_lister_next() on the
/// lister.
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The designated path you want to delete
/// @see opendal_blocking_lister
/// @return Returns opendal_result_list, containing a lister and an opendal_error.
/// If the operation succeeds, the `lister` field would holds a valid lister and
/// the `error` field should hold nullptr. Otherwise the `lister`` will contain a
/// NULL pointer, i.e. invalid, and the `error` will be set correspondingly.
///
/// # Example
///
/// Following is an example
/// ```C
/// // You have written some data into some files path "root/dir1"
/// // Your opendal_operator_ptr was called ptr
/// opendal_result_list l = opendal_operator_blocking_list(ptr, "root/dir1");
/// assert(l.error == ERROR);
///
/// opendal_blocking_lister *lister = l.lister;
/// opendal_list_entry *entry;
///
/// while ((entry = opendal_lister_next(lister)) != NULL) {
///     const char* de_path = opendal_list_entry_path(entry);
///     const char* de_name = opendal_list_entry_name(entry);
///     // ...... your operations
///
///     // remember to free the entry after you are done using it
///     opendal_list_entry_free(entry);
/// }
///
/// // and remember to free the lister
/// opendal_lister_free(lister);
/// ```
///
/// # Safety
///
/// It is **safe** under the cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics, i.e. exits with information
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_list(
    ptr: *const opendal_operator_ptr,
    path: *const c_char,
) -> opendal_result_list {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = (*ptr).as_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    match op.lister(path) {
        Ok(lister) => opendal_result_list {
            lister: Box::into_raw(Box::new(opendal_blocking_lister::new(lister))),
            error: std::ptr::null_mut(),
        },

        Err(e) => {
            let e = Box::new(opendal_error::from_opendal_error(e));
            opendal_result_list {
                lister: std::ptr::null_mut(),
                error: Box::into_raw(e),
            }
        }
    }
}
