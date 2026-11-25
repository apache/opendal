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

use crate::cancel::{block_on_cancellable, opendal_cancel_token, CancellableError};
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
    fn new(req: ocorePresignedRequest) -> core::Result<Self> {
        let invalid_request = |source| {
            core::Error::new(
                core::ErrorKind::Unexpected,
                "presigned request contains interior nul byte",
            )
            .set_source(source)
        };
        let method = CString::new(req.method().as_str()).map_err(invalid_request)?;
        let uri = CString::new(req.uri().to_string()).map_err(invalid_request)?;

        let mut header_keys = Vec::with_capacity(req.header().len());
        let mut header_values = Vec::with_capacity(req.header().len());
        for (k, v) in req.header().iter() {
            header_keys.push(CString::new(k.as_str()).map_err(invalid_request)?);
            header_values.push(CString::new(v.as_bytes()).map_err(|e| {
                core::Error::new(
                    core::ErrorKind::Unexpected,
                    "presigned request header value contains interior nul byte",
                )
                .set_source(e)
            })?);
        }

        let headers: Vec<opendal_http_header_pair> = header_keys
            .iter()
            .zip(header_values.iter())
            .map(|(k, v)| opendal_http_header_pair {
                key: k.as_ptr(),
                value: v.as_ptr(),
            })
            .collect();

        Ok(Self {
            method,
            uri,
            headers,
            header_keys,
            header_values,
        })
    }
}

unsafe fn make_presign_result(
    result: Result<ocorePresignedRequest, CancellableError<core::Error>>,
    req_out: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!req_out.is_null());
    match result {
        Ok(req) => match opendal_presigned_request_inner::new(req) {
            Ok(inner) => {
                let presigned_req = Box::new(opendal_presigned_request {
                    inner: Box::into_raw(Box::new(inner)),
                });
                std::ptr::write(req_out, Box::into_raw(presigned_req));
                std::ptr::null_mut()
            }
            Err(e) => {
                std::ptr::write(req_out, std::ptr::null_mut());
                opendal_error::new(e)
            }
        },
        Err(e) => {
            std::ptr::write(req_out, std::ptr::null_mut());
            opendal_error::from_cancellable(e)
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
    opendal_operator_presign_read_with_cancel(
        op,
        path,
        expire_secs,
        std::ptr::null(),
        std::ptr::null(),
        req,
    )
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
    opendal_operator_presign_read_with_cancel(op, path, expire_secs, opts, std::ptr::null(), req)
}

/// \brief Presign a read operation with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
/// On cancellation or failure, writes NULL to `*req` and returns an `opendal_error *`
/// (`OPENDAL_CANCELLED` on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_read_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    opts: *const opendal_read_options,
    cancel: *const opendal_cancel_token,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());
    std::ptr::write(req, std::ptr::null_mut());

    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);
    let opts = if opts.is_null() {
        core::options::ReadOptions::default()
    } else {
        match core::options::ReadOptions::try_from(&*opts) {
            Ok(opts) => opts,
            Err(e) => return opendal_error::new(e),
        }
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);

    let res = block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().presign_read_options(path, duration, opts),
    );
    make_presign_result(res, req)
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
    opendal_operator_presign_write_with_cancel(
        op,
        path,
        expire_secs,
        std::ptr::null(),
        std::ptr::null(),
        req,
    )
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
    opendal_operator_presign_write_with_cancel(op, path, expire_secs, opts, std::ptr::null(), req)
}

/// \brief Presign a write operation with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
/// On cancellation or failure, writes NULL to `*req` and returns an `opendal_error *`
/// (`OPENDAL_CANCELLED` on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_write_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    opts: *const opendal_write_options,
    cancel: *const opendal_cancel_token,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());

    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);
    let opts = if opts.is_null() {
        core::options::WriteOptions::default()
    } else {
        (&*opts).into()
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);

    let res = block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().presign_write_options(path, duration, opts),
    );
    make_presign_result(res, req)
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
    opendal_operator_presign_delete_with_cancel(
        op,
        path,
        expire_secs,
        std::ptr::null(),
        std::ptr::null(),
        req,
    )
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
    opendal_operator_presign_delete_with_cancel(op, path, expire_secs, opts, std::ptr::null(), req)
}

/// \brief Presign a delete operation with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
/// On cancellation or failure, writes NULL to `*req` and returns an `opendal_error *`
/// (`OPENDAL_CANCELLED` on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_delete_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    opts: *const opendal_delete_options,
    cancel: *const opendal_cancel_token,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());

    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);
    let opts = if opts.is_null() {
        core::options::DeleteOptions::default()
    } else {
        (&*opts).into()
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);

    let res = block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().presign_delete_options(path, duration, opts),
    );
    make_presign_result(res, req)
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
    opendal_operator_presign_stat_with_cancel(
        op,
        path,
        expire_secs,
        std::ptr::null(),
        std::ptr::null(),
        req,
    )
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
    opendal_operator_presign_stat_with_cancel(op, path, expire_secs, opts, std::ptr::null(), req)
}

/// \brief Presign a stat operation with options and optional cancellation.
///
/// Pass NULL for `opts` to use defaults, and NULL for `cancel` to run without cancellation.
/// On cancellation or failure, writes NULL to `*req` and returns an `opendal_error *`
/// (`OPENDAL_CANCELLED` on cancellation).
#[no_mangle]
pub unsafe extern "C" fn opendal_operator_presign_stat_with_cancel(
    op: &opendal_operator,
    path: *const c_char,
    expire_secs: u64,
    opts: *const opendal_stat_options,
    cancel: *const opendal_cancel_token,
    req: *mut *mut opendal_presigned_request,
) -> *mut opendal_error {
    assert!(!path.is_null());
    assert!(!req.is_null());
    std::ptr::write(req, std::ptr::null_mut());

    let path = CStr::from_ptr(path).to_str().expect("malformed path");
    let duration = Duration::from_secs(expire_secs);
    let opts = if opts.is_null() {
        core::options::StatOptions::default()
    } else {
        match core::options::StatOptions::try_from(&*opts) {
            Ok(opts) => opts,
            Err(e) => return opendal_error::new(e),
        }
    };
    let cancel_token = opendal_cancel_token::as_inner(cancel);

    let res = block_on_cancellable(
        op.handle(),
        cancel_token,
        op.deref().presign_stat_options(path, duration, opts),
    );
    make_presign_result(res, req)
}

/// Get the method of the presigned request.
#[no_mangle]
pub unsafe extern "C" fn opendal_presigned_request_method(
    req: *const opendal_presigned_request,
) -> *const c_char {
    if req.is_null() || (*req).inner.is_null() {
        return std::ptr::null();
    }
    (*(*req).inner).method.as_ptr()
}

/// Get the URI of the presigned request.
#[no_mangle]
pub unsafe extern "C" fn opendal_presigned_request_uri(
    req: *const opendal_presigned_request,
) -> *const c_char {
    if req.is_null() || (*req).inner.is_null() {
        return std::ptr::null();
    }
    (*(*req).inner).uri.as_ptr()
}

/// Get the headers of the presigned request.
#[no_mangle]
pub unsafe extern "C" fn opendal_presigned_request_headers(
    req: *const opendal_presigned_request,
) -> *const opendal_http_header_pair {
    if req.is_null() || (*req).inner.is_null() {
        return std::ptr::null();
    }
    (*(*req).inner).headers.as_ptr()
}

/// Get the length of the headers of the presigned request.
#[no_mangle]
pub unsafe extern "C" fn opendal_presigned_request_headers_len(
    req: *const opendal_presigned_request,
) -> usize {
    if req.is_null() || (*req).inner.is_null() {
        return 0;
    }
    (*(*req).inner).headers.len()
}

/// \brief Free the presigned request.
#[no_mangle]
pub unsafe extern "C" fn opendal_presigned_request_free(req: *mut opendal_presigned_request) {
    if !req.is_null() {
        if !(*req).inner.is_null() {
            drop(Box::from_raw((*req).inner));
        }
        drop(Box::from_raw(req));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::opendal_code;
    use ::opendal::raw::{self, oio};
    use std::sync::Arc;

    #[derive(Debug)]
    struct NonAsciiPresignService {
        inner: raw::Servicer,
    }

    #[derive(Debug)]
    struct NonAsciiPresignLayer;

    impl raw::Layer for NonAsciiPresignLayer {
        fn apply_service(&self, inner: raw::Servicer) -> raw::Servicer {
            Arc::new(NonAsciiPresignService { inner })
        }
    }

    impl raw::Service for NonAsciiPresignService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;
        type Composer = oio::Composer;

        fn info(&self) -> raw::ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> core::Capability {
            let mut cap = self.inner.capability();
            cap.presign = true;
            cap.presign_write = true;
            cap.presign_read = true;
            cap.presign_stat = true;
            cap
        }

        async fn create_dir(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpCreateDir,
        ) -> core::Result<raw::RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }

        async fn stat(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpStat,
        ) -> core::Result<raw::RpStat> {
            self.inner.stat(ctx, path, args).await
        }

        fn read(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpRead,
        ) -> core::Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }

        fn write(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpWrite,
        ) -> core::Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }

        fn delete(&self, ctx: &core::OperationContext) -> core::Result<Self::Deleter> {
            self.inner.delete(ctx)
        }

        fn list(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpList,
        ) -> core::Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }

        fn copy(
            &self,
            ctx: &core::OperationContext,
            from: &str,
            to: &str,
            args: raw::OpCopy,
        ) -> core::Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args)
        }

        async fn rename(
            &self,
            ctx: &core::OperationContext,
            from: &str,
            to: &str,
            args: raw::OpRename,
        ) -> core::Result<raw::RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }

        fn compose(
            &self,
            ctx: &core::OperationContext,
            path: &str,
            args: raw::OpCompose,
        ) -> core::Result<Self::Composer> {
            self.inner.compose(ctx, path, args)
        }

        async fn presign(
            &self,
            _ctx: &core::OperationContext,
            _path: &str,
            _args: raw::OpPresign,
        ) -> core::Result<raw::RpPresign> {
            let mut headers = http::HeaderMap::new();
            headers.insert(
                http::header::CONTENT_DISPOSITION,
                http::HeaderValue::from_bytes("attachment; filename=\"测试.txt\"".as_bytes())
                    .unwrap(),
            );
            let req = ocorePresignedRequest::new(
                http::Method::PUT,
                "https://example.com/object".parse().unwrap(),
                headers,
            );
            Ok(raw::RpPresign::new(req))
        }
    }

    #[test]
    fn test_presign_non_ascii_header_value_does_not_panic() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let op = core::Operator::new(core::services::Memory::default())
            .unwrap()
            .layer(NonAsciiPresignLayer);
        let c_op = opendal_operator::from_operator(runtime.handle().clone(), op);
        let path = CString::new("test.txt").unwrap();

        unsafe {
            let mut req: *mut opendal_presigned_request = std::ptr::null_mut();
            let err = opendal_operator_presign_write(&*c_op, path.as_ptr(), 60, &mut req);
            assert!(err.is_null());
            assert!(!req.is_null());

            assert_eq!(
                CStr::from_ptr(opendal_presigned_request_method(req))
                    .to_str()
                    .unwrap(),
                "PUT"
            );
            assert_eq!(
                CStr::from_ptr(opendal_presigned_request_uri(req))
                    .to_str()
                    .unwrap(),
                "https://example.com/object"
            );
            assert_eq!(opendal_presigned_request_headers_len(req), 1);

            let headers = opendal_presigned_request_headers(req);
            assert!(!headers.is_null());
            let pair = &*headers;
            assert_eq!(
                CStr::from_ptr(pair.key).to_str().unwrap(),
                "content-disposition"
            );
            assert_eq!(
                CStr::from_ptr(pair.value).to_bytes(),
                "attachment; filename=\"测试.txt\"".as_bytes()
            );

            opendal_presigned_request_free(req);
            opendal_operator::opendal_operator_free(c_op);
        }
    }

    #[test]
    fn test_presigned_request_null_guards() {
        unsafe {
            assert!(opendal_presigned_request_method(std::ptr::null()).is_null());
            assert!(opendal_presigned_request_uri(std::ptr::null()).is_null());
            assert!(opendal_presigned_request_headers(std::ptr::null()).is_null());
            assert_eq!(opendal_presigned_request_headers_len(std::ptr::null()), 0);
            opendal_presigned_request_free(std::ptr::null_mut());
        }
    }

    #[test]
    fn test_presign_out_of_range_timestamp_returns_config_invalid() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let op = core::Operator::new(core::services::Memory::default())
            .unwrap()
            .layer(NonAsciiPresignLayer);
        let c_op = opendal_operator::from_operator(runtime.handle().clone(), op);
        let path = CString::new("test.txt").unwrap();

        unsafe {
            let bad_read = opendal_read_options {
                has_if_modified_since: true,
                if_modified_since: i64::MAX,
                ..Default::default()
            };
            let mut req: *mut opendal_presigned_request = std::ptr::dangling_mut();
            let err =
                opendal_operator_presign_read_with(&*c_op, path.as_ptr(), 60, &bad_read, &mut req);
            assert!(!err.is_null());
            assert_eq!((*err).code(), opendal_code::OPENDAL_CONFIG_INVALID);
            assert!(req.is_null());
            opendal_error::opendal_error_free(err);

            let bad_stat = opendal_stat_options {
                has_if_unmodified_since: true,
                if_unmodified_since: i64::MIN,
                ..Default::default()
            };
            let mut req: *mut opendal_presigned_request = std::ptr::dangling_mut();
            let err =
                opendal_operator_presign_stat_with(&*c_op, path.as_ptr(), 60, &bad_stat, &mut req);
            assert!(!err.is_null());
            assert_eq!((*err).code(), opendal_code::OPENDAL_CONFIG_INVALID);
            assert!(req.is_null());
            opendal_error::opendal_error_free(err);

            opendal_operator::opendal_operator_free(c_op);
        }
    }
}
