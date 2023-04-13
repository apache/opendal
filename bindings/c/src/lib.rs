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

mod builders;
mod error;
mod macros;
mod result;
mod types;

use std::os::raw::{c_char, c_void};
use std::str::FromStr;

use crate::builders::{opendal_builder_memory, CastBuilder};
use crate::types::{opendal_bytes, opendal_operator_ptr};

use ::opendal as od;
use error::opendal_code;
use result::{opendal_result_is_exist, opendal_result_read};

/// Returns a pointer [`opendal_operator_ptr`], with operator_ptr. If the construction failed,
/// the pointer will be pointing at NULL.
///
/// # Safety
///
/// It is [safe] under if followings are satisfied
/// * The memory pointed to by `scheme` must contain a valid nul terminator at the end of
///   the string. Or the `scheme` points to NULL, this function simply returns you a
///   null [`opendal_operator_ptr`].
/// * The `builder` is pointing at a valid opendal_builder, e.g. [`opendal_builder_memory`]
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_new(
    scheme: *const c_char,
    builder: *mut c_void,
) -> opendal_operator_ptr {
    if scheme.is_null() {
        return opendal_operator_ptr::null();
    }

    let scheme_str = unsafe { std::ffi::CStr::from_ptr(scheme).to_str().unwrap() };
    let scheme = match od::Scheme::from_str(scheme_str) {
        Ok(s) => s,
        Err(_) => {
            return opendal_operator_ptr::null();
        }
    };

    let op = match scheme {
        od::Scheme::Memory => generate_operator!(opendal_builder_memory, builder),
        _ => {
            return opendal_operator_ptr::null();
        }
    }
    .blocking();

    // this prevents the operator memory from being dropped by the Box
    let op = Box::leak(Box::new(op));

    opendal_operator_ptr::from(op)
}

/// Free the allocated operator pointed by [`opendal_operator_ptr`]
#[no_mangle]
pub extern "C" fn opendal_operator_free(op_ptr: opendal_operator_ptr) {
    if op_ptr.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(op_ptr.get_ref_mut()) };
    // dropped
}

/// Write the data into the path blockingly by operator, returns the error code OPENDAL_OK
/// if succeeds, others otherwise
///
/// # Safety
///
/// It is [safe] under two cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_write(
    op_ptr: opendal_operator_ptr,
    path: *const c_char,
    bytes: opendal_bytes,
) -> opendal_code {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = op_ptr.get_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    match op.write(path, bytes) {
        Ok(_) => opendal_code::OPENDAL_OK,
        Err(e) => opendal_code::from_opendal_error(e),
    }
}

/// Read the data out from path into a [`Bytes`] blockingly by operator, returns
/// a result with error code. If the error code is not OPENDAL_OK, the `data` field
/// of the result points to NULL.
///
/// # Safety
///
/// It is [safe] under two cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_read(
    op_ptr: opendal_operator_ptr,
    path: *const c_char,
) -> opendal_result_read {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = op_ptr.get_ref();
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

/// Check whether the path exists.
///
/// If the operation succeeds, no matter the path exists or not,
/// the error code should be opendal_code::OPENDAL_OK. Otherwise,
/// the field `is_exist` is filled with false, and the error code
/// is set correspondingly.
///
/// # Safety
///
/// It is [safe] under two cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
///
/// # Panic
///
/// * If the `path` points to NULL, this function panics
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_is_exist(
    op_ptr: opendal_operator_ptr,
    path: *const c_char,
) -> opendal_result_is_exist {
    if path.is_null() {
        panic!("The path given is pointing at NULL");
    }

    let op = op_ptr.get_ref();
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
