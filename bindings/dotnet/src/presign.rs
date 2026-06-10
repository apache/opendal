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

use std::ffi::{c_char, c_void};

use crate::error::OpenDALError;
use crate::utils::into_string_ptr;

#[repr(C)]
pub struct OpendalPresignedRequest {
    pub method: *mut c_char,
    pub uri: *mut c_char,
    pub headers_keys: *mut *mut c_char,
    pub headers_values: *mut *mut c_char,
    pub headers_len: usize,
}

pub fn into_presigned_request_ptr(
    request: opendal::raw::PresignedRequest,
) -> Result<*mut c_void, OpenDALError> {
    let method = into_string_ptr(request.method().as_str().to_string());
    let uri = into_string_ptr(request.uri().to_string());

    let mut keys = Vec::new();
    let mut values = Vec::new();

    for (key, value) in request.header() {
        let value = value
            .to_str()
            .map_err(|err| {
                OpenDALError::from_opendal_error(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    err.to_string(),
                ))
            })?
            .to_string();

        keys.push(into_string_ptr(key.to_string()));
        values.push(into_string_ptr(value));
    }

    let len = keys.len();
    let (headers_keys, headers_values, headers_len) = if len == 0 {
        (std::ptr::null_mut(), std::ptr::null_mut(), 0)
    } else {
        let mut keys = keys;
        let mut values = values;
        let keys_ptr = keys.as_mut_ptr();
        let values_ptr = values.as_mut_ptr();
        std::mem::forget(keys);
        std::mem::forget(values);

        (keys_ptr, values_ptr, len)
    };

    let request = OpendalPresignedRequest {
        method,
        uri,
        headers_keys,
        headers_values,
        headers_len,
    };

    Ok(Box::into_raw(Box::new(request)) as *mut c_void)
}

/// # Safety
///
/// - `request` must be null or a pointer produced by `into_presigned_request_ptr`.
/// - This function must be called at most once per non-null pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn presigned_request_free(request: *mut OpendalPresignedRequest) {
    if request.is_null() {
        return;
    }

    unsafe {
        let request = Box::from_raw(request);

        if !request.method.is_null() {
            drop(std::ffi::CString::from_raw(request.method));
        }
        if !request.uri.is_null() {
            drop(std::ffi::CString::from_raw(request.uri));
        }

        if request.headers_len > 0 {
            if !request.headers_keys.is_null() {
                let keys = Vec::from_raw_parts(
                    request.headers_keys,
                    request.headers_len,
                    request.headers_len,
                );
                for key in keys {
                    if !key.is_null() {
                        drop(std::ffi::CString::from_raw(key));
                    }
                }
            }

            if !request.headers_values.is_null() {
                let values = Vec::from_raw_parts(
                    request.headers_values,
                    request.headers_len,
                    request.headers_len,
                );
                for value in values {
                    if !value.is_null() {
                        drop(std::ffi::CString::from_raw(value));
                    }
                }
            }
        }
    }
}
