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

use ::opendal as core;
use opendal::raw::PresignedRequest as ocorePresignedRequest;

use crate::error::opendal_error;
use crate::operator::opendal_operator;
use crate::types::{
    opendal_delete_options, opendal_read_options, opendal_stat_options, opendal_write_options,
};

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
#[derive(Debug)]
struct opendal_presigned_request_inner {
    method: CString,
    uri: CString,
    headers: Vec<opendal_http_header_pair>,
    // These vecs own the CString data for the headers
    #[allow(dead_code)]
    header_keys: Vec<CString>,
    #[allow(dead_code)]
    header_values: Vec<CString>,
}

impl opendal_presigned_request_inner {
    fn new(req: ocorePresignedRequest) -> Self {
        let method = CString::new(req.method().as_str()).unwrap();
        let uri = CString::new(req.uri().to_string()).unwrap();

        let header_keys: Vec<CString> = req
            .header()
            .iter()
            .map(|(k, _)| CString::new(k.as_str()).unwrap())
            .collect();
        let header_values: Vec<CString> = req
            .header()
            .iter()
            .map(|(_, v)| CString::new(v.to_str().unwrap()).unwrap())
            .collect();

        let headers: Vec<opendal_http_header_pair> = header_keys
            .iter()
            .zip(header_values.iter())
            .map(|(k, v)| opendal_http_header_pair {
                key: k.as_ptr(),
                value: v.as_ptr(),
            })
            .collect();

        Self {
            method,
            uri,
            headers,
            header_keys,
            header_values,
        }
    }
}

unsafe fn make_presign_result(
    result: core::Result<ocorePresignedRequest>,
    req_out: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!req_out.is_null());
    match result {
        Ok(req) => {
            let inner = Box::new(opendal_presigned_request_inner::new(req));
            let presigned_req = Box::new(opendal_presigned_request {
                inner: Box::into_raw(inner),
            });
            std::ptr::write(req_out, Box::into_raw(presigned_req));
            std::ptr::null_mut()
        }
        Err(e) => {
            std::ptr::write(req_out, std::ptr::null_mut());
            opendal_error::new(e)
        }
    }
}

/// \brief The underlying presigned request, which contains the HTTP method, URI, and headers.
/// This is an opaque struct, please use the accessor functions to get the fields.
#[repr(C)]
pub struct opendal_presigned_request {
    inner: *mut opendal_presigned_request_inner,
}

/// \brief Presign a read operation.
///
/// `req` must point to writable storage for a request pointer. On success,
/// writes the request to `*req` and returns NULL; free the request with
/// `opendal_presigned_request_free()`. On failure, writes NULL to `*req` and
/// returns an error that must be freed with `opendal_error_free()`.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_read(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);

    make_presign_result(op.presign_read(path, duration), req)
}

/// \brief Presign a read operation with options.
///
/// `req` must point to writable storage for a request pointer. On success,
/// writes the request to `*req` and returns NULL; free the request with
/// `opendal_presigned_request_free()`. On failure, writes NULL to `*req` and
/// returns an error that must be freed with `opendal_error_free()`.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_read_with(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    opts: *const opendal_read_options,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);
    let opts = if opts.is_null() {
        core::options::ReadOptions::default()
    } else {
        (&*opts).into()
    };

    make_presign_result(op.presign_read_options(path, duration, opts), req)
}

/// \brief Presign a write operation.
///
/// `req` must point to writable storage for a request pointer. On success,
/// writes the request to `*req` and returns NULL; free the request with
/// `opendal_presigned_request_free()`. On failure, writes NULL to `*req` and
/// returns an error that must be freed with `opendal_error_free()`.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_write(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);

    make_presign_result(op.presign_write(path, duration), req)
}

/// \brief Presign a write operation with options.
///
/// `req` must point to writable storage for a request pointer. On success,
/// writes the request to `*req` and returns NULL; free the request with
/// `opendal_presigned_request_free()`. On failure, writes NULL to `*req` and
/// returns an error that must be freed with `opendal_error_free()`.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_write_with(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    opts: *const opendal_write_options,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);
    let opts = if opts.is_null() {
        core::options::WriteOptions::default()
    } else {
        (&*opts).into()
    };

    make_presign_result(op.presign_write_options(path, duration, opts), req)
}

/// \brief Presign a delete operation.
///
/// `req` must point to writable storage for a request pointer. On success,
/// writes the request to `*req` and returns NULL; free the request with
/// `opendal_presigned_request_free()`. On failure, writes NULL to `*req` and
/// returns an error that must be freed with `opendal_error_free()`.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_delete(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);
    make_presign_result(op.presign_delete(path, duration), req)
}

/// \brief Presign a delete operation with options.
///
/// `req` must point to writable storage for a request pointer. On success,
/// writes the request to `*req` and returns NULL; free the request with
/// `opendal_presigned_request_free()`. On failure, writes NULL to `*req` and
/// returns an error that must be freed with `opendal_error_free()`.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_delete_with(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    opts: *const opendal_delete_options,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);
    let opts = if opts.is_null() {
        core::options::DeleteOptions::default()
    } else {
        let opts = &*opts;
        let version = if opts.version.is_null() {
            None
        } else {
            Some(
                CStr::from_ptr(opts.version)
                    .to_str()
                    .expect("malformed version")
                    .to_owned(),
            )
        };
        core::options::DeleteOptions {
            version,
            recursive: opts.recursive,
            if_match: None,
            ..Default::default()
        }
    };

    make_presign_result(op.presign_delete_options(path, duration, opts), req)
}

/// \brief Presign a stat operation.
///
/// `req` must point to writable storage for a request pointer. On success,
/// writes the request to `*req` and returns NULL; free the request with
/// `opendal_presigned_request_free()`. On failure, writes NULL to `*req` and
/// returns an error that must be freed with `opendal_error_free()`.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_stat(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);

    make_presign_result(op.presign_stat(path, duration), req)
}

/// \brief Presign a stat operation with options.
///
/// `req` must point to writable storage for a request pointer. On success,
/// writes the request to `*req` and returns NULL; free the request with
/// `opendal_presigned_request_free()`. On failure, writes NULL to `*req` and
/// returns an error that must be freed with `opendal_error_free()`.
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_stat_with(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    opts: *const opendal_stat_options,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());

    let op = op.deref();
    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);
    let opts = if opts.is_null() {
        core::options::StatOptions::default()
    } else {
        (&*opts).into()
    };

    make_presign_result(op.presign_stat_options(path, duration, opts), req)
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
        drop(Box::from_raw((*req).inner));
        drop(Box::from_raw(req));
    }
}
