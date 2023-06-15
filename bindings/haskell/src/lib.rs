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

mod result;

use ::opendal as od;
use result::FFIResult;
use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;
use std::str::FromStr;

// FFI wrapper for via_map function
#[no_mangle]
pub unsafe extern "C" fn via_map_ffi(
    scheme: *const c_char,
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
    result: *mut FFIResult<od::BlockingOperator>,
) {
    let scheme_str = match CStr::from_ptr(scheme).to_str() {
        Ok(s) => s,
        Err(_) => {
            *result = FFIResult::err("Failed to convert scheme to string");
            return;
        }
    };

    let scheme = match od::Scheme::from_str(scheme_str) {
        Ok(s) => s,
        Err(_) => {
            *result = FFIResult::err("Failed to parse scheme");
            return;
        }
    };

    let keys_vec = std::slice::from_raw_parts(keys, len);
    let values_vec = std::slice::from_raw_parts(values, len);

    let map = keys_vec
        .iter()
        .zip(values_vec.iter())
        .map(|(&k, &v)| {
            (
                CStr::from_ptr(k).to_string_lossy().into_owned(),
                CStr::from_ptr(v).to_string_lossy().into_owned(),
            )
        })
        .collect::<HashMap<String, String>>();

    let res = match od::Operator::via_map(scheme, map) {
        Ok(operator) => FFIResult::ok(Box::into_raw(Box::new(operator.blocking()))),
        Err(_) => FFIResult::err("Failed to create Operator via map"),
    };

    *result = res;
}

#[no_mangle]
pub unsafe extern "C" fn free_operator(operator: *mut od::BlockingOperator) {
    if !operator.is_null() {
        drop(Box::from_raw(operator));
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct ByteSlice {
    data: *mut c_char,
    len: usize,
}

impl ByteSlice {
    fn from_vec(vec: Vec<u8>) -> Self {
        let data = vec.as_ptr() as *mut c_char;
        let len = vec.len();

        // Leak the memory to pass the ownership to Haskell
        std::mem::forget(vec);

        ByteSlice { data, len }
    }

    #[no_mangle]
    pub unsafe extern "C" fn free_byteslice(ptr: *mut c_char, len: usize) {
        if !ptr.is_null() {
            drop(Vec::from_raw_parts(ptr, len, len));
        }
    }
}

impl Into<Vec<u8>> for &mut ByteSlice {
    fn into(self) -> Vec<u8> {
        unsafe { Vec::from_raw_parts(self.data as *mut u8, self.len, self.len) }
    }
}

#[no_mangle]
pub unsafe extern "C" fn blocking_read(
    op: *mut od::BlockingOperator,
    path: *const c_char,
    result: *mut FFIResult<ByteSlice>,
) {
    let op = if op.is_null() {
        *result = FFIResult::err("Operator is null");
        return;
    } else {
        &mut *op
    };

    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => {
            *result = FFIResult::err("Failed to convert scheme to string");
            return;
        }
    };

    let res = match op.read(path_str) {
        Ok(bytes) => FFIResult::ok(Box::into_raw(Box::new(ByteSlice::from_vec(bytes)))),
        Err(_) => FFIResult::err("Failed to read from Operator"),
    };

    *result = res;
}

#[no_mangle]
pub unsafe extern "C" fn blocking_write(
    op: *mut od::BlockingOperator,
    path: *const c_char,
    bytes: *const c_char,
    len: usize,
    result: *mut FFIResult<()>,
) {
    let op = if op.is_null() {
        *result = FFIResult::err("Operator is null");
        return;
    } else {
        &mut *op
    };

    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => {
            *result = FFIResult::err("Failed to convert scheme to string");
            return;
        }
    };

    let bytes = Vec::from_raw_parts(bytes as *mut u8, len, len);

    let res = match op.write(path_str, bytes) {
        Ok(()) => FFIResult::ok(ptr::null_mut()),
        Err(_) => FFIResult::err("Failed to read from Operator"),
    };

    *result = res;
}
