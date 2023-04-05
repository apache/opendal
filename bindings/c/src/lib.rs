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

mod types;

use std::collections::HashMap;
use std::os::raw::c_char;
use std::str::FromStr;

use crate::types::{Bytes, OperatorPtr};
use ::opendal as od;

/// Constructs a new [`OperatorPtr`] which contains a underlying [`BlockingOperator`]
/// If the scheme is invalid, or the operator constructions failed, a nullptr is returned
///
/// # Safety
///
/// It is [safe] under two cases below
/// * The memory pointed to by `scheme` must contain a valid nul terminator at the end of
///   the string.
/// * The `scheme` points to NULL, this function simply returns you a null opendal_operator_ptr
#[no_mangle]
pub unsafe extern "C" fn opendal_new_operator(scheme: *const c_char) -> OperatorPtr {
    use od::services::*;

    if scheme.is_null() {
        return OperatorPtr::null();
    }

    let scheme_str = unsafe { std::ffi::CStr::from_ptr(scheme).to_str().unwrap() };
    let scheme = match od::Scheme::from_str(scheme_str) {
        Ok(s) => s,
        Err(_) => return OperatorPtr::null(),
    };

    // todo: api for map construction
    let map = HashMap::default();

    let op = match scheme {
        od::Scheme::Memory => {
            let b = od::Operator::from_map::<Memory>(map);
            match b {
                Ok(b) => b.finish(),
                Err(_) => return OperatorPtr::null(),
            }
        }
        od::Scheme::Fs => {
            let b = od::Operator::from_map::<Fs>(map);
            match b {
                Ok(b) => b.finish(),
                Err(_) => return OperatorPtr::null(),
            }
        }
        _ => return OperatorPtr::null(),
    }
    .blocking();

    // this prevents the operator memory from being dropped by the Box
    let op = Box::leak(Box::new(op));

    OperatorPtr::from(op)
}

/// Write the data into the path blockingly by operator, returns whether the write succeeds
///
/// # Safety
///
/// It is [safe] under two cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * The `path` points to NULL, this function simply returns you false
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_write(
    op_ptr: OperatorPtr,
    path: *const c_char,
    bytes: Bytes,
) -> bool {
    if path.is_null() {
        return false;
    }

    let op = op_ptr.get_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    op.write(path, bytes).is_ok()
}

/// Read the data out from path into a [`Bytes`] blockingly by operator, returns
/// a pointer to the [`Bytes`] if succeeds, nullptr otherwise
///
/// # Safety
///
/// It is [safe] under two cases below
/// * The memory pointed to by `path` must contain a valid nul terminator at the end of
///   the string.
/// * The `path` points to NULL, this function simply returns you a nullptr
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_blocking_read(
    op_ptr: OperatorPtr,
    path: *const c_char,
) -> *mut Bytes {
    if path.is_null() {
        return std::ptr::null_mut();
    }

    let op = op_ptr.get_ref();
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_str().unwrap() };
    let data = op.read(path);
    match data {
        Ok(d) => {
            let mut v = std::mem::ManuallyDrop::new(Bytes::from_vec(d));
            &mut *v
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Hello, OpenDAL!
#[no_mangle]
pub extern "C" fn hello_opendal() {
    let op = od::Operator::new(od::services::Memory::default())
        .unwrap()
        .finish();
    println!("{op:?}")
}
