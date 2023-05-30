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

#![allow(non_camel_case_types)]

mod error;
mod macros;
mod result;
mod types;

use std::collections::HashMap;
use std::os::raw::c_char;
use std::str::FromStr;

use ::opendal as od;
use error::opendal_code;
use result::opendal_result_is_exist;
use result::opendal_result_read;
use result::opendal_result_stat;
use types::opendal_metadata;
use types::opendal_operator_options;

use crate::types::opendal_bytes;
use crate::types::opendal_operator_ptr;

/// Uses an array of key-value pairs to initialize the operator based on provided `scheme`
/// and `options`. For each scheme, i.e. Backend, different options could be set, you may
/// reference the [documentation](https://opendal.apache.org/docs/category/services/) for
/// each service, especially for the **Configuration Part**.
///
/// @param scheme the service scheme you want to specify, e.g. "fs", "s3", "supabase"
/// @param options the options for this operators
/// @see opendal_operator_options
/// @return A valid opendal_operator_ptr setup with the `scheme` and `options` is the construction
/// succeeds. A null opendal_operator_ptr if any error happens.
///
/// # Example
///
/// Following is an example.
/// ```C
/// // Allocate a new options
/// opendal_operator_options options = opendal_operator_options_new();
/// // Set the options you need
/// opendal_operator_options_set(&options, "root", "/myroot");
///
/// // Construct the operator based on the options and scheme
/// opendal_operator_ptr ptr = opendal_operator_new("memory", options);
///
/// // you could free the options right away since the options is not used afterwards
/// opendal_operator_options_free(&options);
///
/// // ... your operations
/// ```
///
/// # Safety
///
/// It is **safe** under two cases below
/// * The memory pointed to by `scheme` contain a valid nul terminator at the end of
///   the string.
/// * The `scheme` points to NULL, this function simply returns you a null opendal_operator_ptr.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_new(
    scheme: *const c_char,
    options: opendal_operator_options,
) -> opendal_operator_ptr {
    if scheme.is_null() || options.is_null() {
        return opendal_operator_ptr::null();
    }

    let scheme_str = unsafe { std::ffi::CStr::from_ptr(scheme).to_str().unwrap() };
    let scheme = match od::Scheme::from_str(scheme_str) {
        Ok(s) => s,
        Err(_) => {
            return opendal_operator_ptr::null();
        }
    };

    let mut map = HashMap::default();
    for (k, v) in options.as_ref() {
        map.insert(k.to_string(), v.to_string());
    }

    let op = match scheme {
        od::Scheme::Memory => generate_operator!(od::services::Memory, map),
        _ => {
            return opendal_operator_ptr::null();
        }
    }
    .blocking();

    // this prevents the operator memory from being dropped by the Box
    let op = Box::leak(Box::new(op));

    opendal_operator_ptr::from(op)
}

/// Write the `bytes` into the `path` blockingly by `op_ptr`, returns the opendal_code OPENDAL_OK
/// if succeeds, others otherwise
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The designated path you want to write your bytes in
/// @param bytes The opendal_byte typed bytes to be written
/// @see opendal_operator_ptr
/// @see opendal_bytes
/// @see opendal_code
/// @return OPENDAL_OK if succeeds others otherwise
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
/// opendal_code code = opendal_operator_blocking_write(ptr, "/testpath", bytes);
///
/// // Assert that this succeeds
/// assert(code == OPENDAL_OK)
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
    ptr: opendal_operator_ptr,
    path: *const c_char,
    bytes: opendal_bytes,
) -> opendal_code {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = ptr.get_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    match op.write(path, bytes) {
        Ok(_) => opendal_code::OPENDAL_OK,
        Err(e) => opendal_code::from_opendal_error(e),
    }
}

/// Read the data out from `path` blockingly by operator, returns
/// an opendal_result_read with error code.
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The path you want to read the data out
/// @see opendal_operator_ptr
/// @see opendal_result_read
/// @see opendal_code
/// @return Returns opendal_result_read, the `data` field is a pointer to a newly allocated
/// opendal_bytes, the `code` field contains the error code. If the `code` is not OPENDAL_OK,
/// the `data` field points to NULL.
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
/// assert(r.code == OPENDAL_OK);
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
    ptr: opendal_operator_ptr,
    path: *const c_char,
) -> opendal_result_read {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = ptr.get_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    let data = op.read(path);
    match data {
        Ok(d) => {
            let v = Box::new(opendal_bytes::from_vec(d));
            opendal_result_read {
                data: Box::into_raw(v),
                code: opendal_code::OPENDAL_OK,
            }
        }
        Err(e) => opendal_result_read {
            data: std::ptr::null_mut(),
            code: opendal_code::from_opendal_error(e),
        },
    }
}

/// \brief Check whether the path exists.
///
/// If the operation succeeds, no matter the path exists or not,
/// the error code should be opendal_code::OPENDAL_OK. Otherwise,
/// the field `is_exist` is filled with false, and the error code
/// is set correspondingly.
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The path you want to check existence
/// @see opendal_operator_ptr
/// @see opendal_result_is_exist
/// @see opendal_code
/// @return Returns opendal_result_is_exist, the `is_exist` field contains whether the path exists.
/// However, it the operation fails, the `is_exist` will contains false and the error code will be
/// stored in the `code` field.
///
/// # Example
///
/// ```C
/// // .. you previously wrote some data to path "/mytest/obj"
/// opendal_result_is_exist e = opendal_operator_is_exist(ptr, "/mytest/obj");
/// assert(e.code == OPENDAL_OK);
/// assert(e.is_exist);
///
/// // but you previously did **not** write any data to path "/yourtest/obj"
/// opendal_result_is_exist e = opendal_operator_is_exist(ptr, "yourtest/obj");
/// assert(e.code == OPENDAL_OK);
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
    ptr: opendal_operator_ptr,
    path: *const c_char,
) -> opendal_result_is_exist {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = ptr.get_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    match op.is_exist(path) {
        Ok(e) => opendal_result_is_exist {
            is_exist: e,
            code: opendal_code::OPENDAL_OK,
        },
        Err(err) => opendal_result_is_exist {
            is_exist: false,
            code: opendal_code::from_opendal_error(err),
        },
    }
}

/// \brief Stat the path, return its metadata.
///
/// If the operation succeeds, the error code should be
/// OPENDAL_OK. Otherwise, the field `meta` is filled with
/// a NULL pointer, and the error code is set correspondingly.
///
/// @param ptr The opendal_operator_ptr created previously
/// @param path The path you want to stat
/// @see opendal_operator_ptr
/// @see opendal_result_stat
/// @see opendal_metadata
/// @return Returns opendal_result_stat, containing a metadata and a opendal_code.
/// If the operation succeeds, the `meta` field would holds a valid metadata and
/// the `code` field should hold OPENDAL_OK. Otherwise the metadata will contain a
/// NULL pointer, i.e. invalid, and the `code` will be set correspondingly.
///
/// # Example
///
/// ```C
/// // ... previously you wrote "Hello, World!" to path "/testpath"
/// opendal_result_stat s = opendal_operator_stat(ptr, "/testpath");
/// assert(s.code == OPENDAL_OK);
///
/// opendal_metadata meta = s.meta;
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
    ptr: opendal_operator_ptr,
    path: *const c_char,
) -> opendal_result_stat {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = ptr.get_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    match op.stat(path) {
        Ok(m) => opendal_result_stat {
            meta: opendal_metadata::from_metadata(m),
            code: opendal_code::OPENDAL_OK,
        },
        Err(err) => opendal_result_stat {
            meta: opendal_metadata::null(),
            code: opendal_code::from_opendal_error(err),
        },
    }
}
