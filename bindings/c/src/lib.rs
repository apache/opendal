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

use crate::types::{opendal_bytes, opendal_operator_ptr};

use ::opendal as od;
use error::opendal_code;
use result::opendal_result_read;

/// Returns a result type [`opendal_result_op`], with operator_ptr. If the construction succeeds
/// the error is nullptr, otherwise it contains the error information.
///
/// # Safety
///
/// It is [safe] under two cases below
/// * The memory pointed to by `scheme` must contain a valid nul terminator at the end of
///   the string.
/// * The `scheme` points to NULL, this function simply returns you a null opendal_operator_ptr
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_new(
    scheme: *const c_char,
    root: *const c_char,
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

    let root = if root.is_null() {
        "/".to_string()
    } else {
        unsafe { std::ffi::CStr::from_ptr(root).to_str().unwrap() }.to_string()
    };

    // todo: api for map construction
    let options = HashMap::from([("root".to_string(), root)]);

    let op = match build_operator(scheme, options) {
        Ok(op) => op.blocking(),
        Err(_) => {
            return opendal_operator_ptr::null();
        }
    };

    // this prevents the operator memory from being dropped by the Box
    let op = Box::leak(Box::new(op));

    opendal_operator_ptr::from(op)
}

/// Write the data into the path blockingly by operator, returns the error code OPENDAL_OK
/// if succeeds, others otherwise
///
/// # Safety
///
/// It is [safe] under two cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * The `path` points to NULL, this function simply returns you false
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_write(
    op_ptr: opendal_operator_ptr,
    path: *const c_char,
    bytes: opendal_bytes,
) -> opendal_code {
    if path.is_null() {
        return opendal_code::OPENDAL_ERROR;
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
/// * The `path` points to NULL, this function simply returns you a nullptr
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_read(
    op_ptr: opendal_operator_ptr,
    path: *const c_char,
) -> opendal_result_read {
    if path.is_null() {
        return opendal_result_read {
            data: std::ptr::null_mut(),
            code: opendal_code::OPENDAL_ERROR,
        };
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

fn build_operator(
    scheme: opendal::Scheme,
    map: HashMap<String, String>,
) -> Result<opendal::Operator, opendal::Error> {
    use opendal::services::*;

    let op = match scheme {
        opendal::Scheme::Azblob => opendal::Operator::from_map::<Azblob>(map).unwrap().finish(),
        opendal::Scheme::Azdfs => opendal::Operator::from_map::<Azdfs>(map).unwrap().finish(),
        opendal::Scheme::Fs => opendal::Operator::from_map::<Fs>(map).unwrap().finish(),
        opendal::Scheme::Gcs => opendal::Operator::from_map::<Gcs>(map).unwrap().finish(),
        opendal::Scheme::Ghac => opendal::Operator::from_map::<Ghac>(map).unwrap().finish(),
        opendal::Scheme::Http => opendal::Operator::from_map::<Http>(map).unwrap().finish(),
        opendal::Scheme::Ipmfs => opendal::Operator::from_map::<Ipmfs>(map).unwrap().finish(),
        opendal::Scheme::Memory => opendal::Operator::from_map::<Memory>(map).unwrap().finish(),
        opendal::Scheme::Obs => opendal::Operator::from_map::<Obs>(map).unwrap().finish(),
        opendal::Scheme::Oss => opendal::Operator::from_map::<Oss>(map).unwrap().finish(),
        opendal::Scheme::S3 => opendal::Operator::from_map::<S3>(map).unwrap().finish(),
        opendal::Scheme::Webdav => opendal::Operator::from_map::<Webdav>(map).unwrap().finish(),
        opendal::Scheme::Webhdfs => opendal::Operator::from_map::<Webhdfs>(map)
            .unwrap()
            .finish(),

        _ => {
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "Scheme not supported",
            ));
        }
    };

    Ok(op)
}
