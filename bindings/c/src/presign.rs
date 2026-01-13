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

use std::ffi::{c_char, CStr, CString};
use std::time::Duration;

use opendal::raw::PresignedRequest as ocorePresignedRequest;

use crate::error::opendal_error;
use crate::operator::opendal_operator;

/// \brief The key-value pair for the headers of the presigned request.
#[repr(C)]
#[derive(Debug)]
pub struct opendal_http_header_pair {
    /// The key of the header.
    pub key: *const c_char,
    /// The value of the header.
    pub value: *const c_char,
}

// The internal Rust-only struct that holds the owned data.
#[allow(dead_code)]
#[derive(Debug)]
struct opendal_presigned_request_inner {
    method: CString,
    uri: CString,
    headers: Vec<opendal_http_header_pair>,
    // These vecs own the CString data for the headers
    header_keys: Vec<CString>,
    header_values: Vec<CString>,
}

impl opendal_presigned_request_inner {
    fn new(req: ocorePresignedRequest) -> Self {
        let method = CString::new(req.method().as_str()).unwrap();
        let uri = CString::new(req.uri().to_string()).unwrap();

        let mut header_keys = Vec::with_capacity(req.header().len());
        let mut header_values = Vec::with_capacity(req.header().len());
        for (k, v) in req.header().iter() {
            header_keys.push(CString::new(k.as_str()).unwrap());
            header_values.push(CString::new(v.to_str().unwrap()).unwrap());
        }

        let mut headers: Vec<opendal_http_header_pair> = Vec::with_capacity(header_keys.len());
        for i in 0..header_keys.len() {
            headers.push(opendal_http_header_pair {
                key: header_keys[i].as_ptr(),
                value: header_values[i].as_ptr(),
            });
        }

        Self {
            method,
            uri,
            headers,
            header_keys,
            header_values,
        }
    }
}

/// \brief The underlying presigned request, which contains the HTTP method, URI, and headers.
/// This is an opaque struct, please use the accessor functions to get the fields.
#[repr(C)]
pub struct opendal_presigned_request {
    inner: *mut opendal_presigned_request_inner,
}

/// @brief The result of a presign operation.
#[repr(C)]
pub struct opendal_result_presign {
    /// The presigned request.
    pub req: *mut opendal_presigned_request,
    /// The error.
    pub error: *mut opendal_error,
}

/// \brief Presign a read operation.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_read(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
) -> opendal_result_presign {
    if path.is_null() {
        return opendal_result_presign {
            req: std::ptr::null_mut(),
            error: opendal_error::new(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "path is null",
            )),
        };
    }

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().unwrap();
    let duration = Duration::from_secs(expire_secs);

    match op.presign_read(path, duration) {
        Ok(req) => {
            let inner = Box::new(opendal_presigned_request_inner::new(req));
            let presigned_req = Box::new(opendal_presigned_request {
                inner: Box::into_raw(inner),
            });
            opendal_result_presign {
                req: Box::into_raw(presigned_req),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => opendal_result_presign {
            req: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
    }
}

/// \brief Presign a write operation.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_write(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
) -> opendal_result_presign {
    if path.is_null() {
        return opendal_result_presign {
            req: std::ptr::null_mut(),
            error: opendal_error::new(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "path is null",
            )),
        };
    }

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().unwrap();
    let duration = Duration::from_secs(expire_secs);

    match op.presign_write(path, duration) {
        Ok(req) => {
            let inner = Box::new(opendal_presigned_request_inner::new(req));
            let presigned_req = Box::new(opendal_presigned_request {
                inner: Box::into_raw(inner),
            });
            opendal_result_presign {
                req: Box::into_raw(presigned_req),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => opendal_result_presign {
            req: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
    }
}

/// \brief Presign a delete operation.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_delete(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
) -> opendal_result_presign {
    if path.is_null() {
        return opendal_result_presign {
            req: std::ptr::null_mut(),
            error: opendal_error::new(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "path is null",
            )),
        };
    }
    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().unwrap();
    let duration = Duration::from_secs(expire_secs);
    match op.presign_delete(path, duration) {
        Ok(req) => {
            let inner = Box::new(opendal_presigned_request_inner::new(req));
            let presigned_req = Box::new(opendal_presigned_request {
                inner: Box::into_raw(inner),
            });
            opendal_result_presign {
                req: Box::into_raw(presigned_req),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => opendal_result_presign {
            req: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
    }
}

/// \brief Presign a stat operation.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_stat(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
) -> opendal_result_presign {
    if path.is_null() {
        return opendal_result_presign {
            req: std::ptr::null_mut(),
            error: opendal_error::new(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "path is null",
            )),
        };
    }

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().unwrap();
    let duration = Duration::from_secs(expire_secs);

    match op.presign_stat(path, duration) {
        Ok(req) => {
            let inner = Box::new(opendal_presigned_request_inner::new(req));
            let presigned_req = Box::new(opendal_presigned_request {
                inner: Box::into_raw(inner),
            });
            opendal_result_presign {
                req: Box::into_raw(presigned_req),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => opendal_result_presign {
            req: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
    }
}

/// Get the method of the presigned request.
#[no_mangle]
pub unsafe extern "C" fn opendal_presigned_request_method(
    req: *const opendal_presigned_request,
) -> *const c_char {
    (*(*req).inner).method.as_ptr()
}

/// Get the URI of the presigned request.
#[no_mangle]
pub unsafe extern "C" fn opendal_presigned_request_uri(
    req: *const opendal_presigned_request,
) -> *const c_char {
    (*(*req).inner).uri.as_ptr()
}

/// Get the headers of the presigned request.
#[no_mangle]
pub unsafe extern "C" fn opendal_presigned_request_headers(
    req: *const opendal_presigned_request,
) -> *const opendal_http_header_pair {
    (*(*req).inner).headers.as_ptr()
}

/// Get the length of the headers of the presigned request.
#[no_mangle]
pub unsafe extern "C" fn opendal_presigned_request_headers_len(
    req: *const opendal_presigned_request,
) -> usize {
    (*(*req).inner).headers.len()
}

/// \brief Free the presigned request.
#[no_mangle]
pub unsafe extern "C" fn opendal_presigned_request_free(req: *mut opendal_presigned_request) {
    if !req.is_null() {
        // Drop the inner struct
        drop(Box::from_raw((*req).inner));
        // Drop the outer struct
        drop(Box::from_raw(req));
    }
}
