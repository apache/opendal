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

use std::collections::HashMap;
use std::os::raw::c_char;
use std::str::FromStr;
use tokio::task;

use ::opendal as core;

use super::*;
use crate::error::opendal_error;
use crate::metadata::opendal_metadata; // Keep this
use crate::result::opendal_result_stat; // Keep this
use crate::types::opendal_operator_options; // Keep this

/// Future handle for asynchronous stat operations.
#[repr(C)]
pub struct opendal_future_stat {
    /// Pointer to an owned JoinHandle wrapped in Option for safe extraction.
    inner: *mut Option<task::JoinHandle<core::Result<core::Metadata>>>,
}

unsafe impl Send for opendal_future_stat {}

/// Result type for creating an asynchronous stat future.
#[repr(C)]
pub struct opendal_result_future_stat {
    /// The future handle. Null when creation fails.
    pub future: *mut opendal_future_stat,
    /// The error information. Null on success.
    pub error: *mut opendal_error,
}

unsafe impl Send for opendal_result_future_stat {}

/// \brief Represents an asynchronous OpenDAL Operator.
///
/// This operator interacts with storage services using non-blocking APIs.
/// Use `opendal_async_operator_new` to construct and `opendal_async_operator_free` to release.
#[repr(C)]
pub struct opendal_async_operator {
    /// Internal pointer to the Rust async Operator.
    inner: *mut core::Operator,
}

impl opendal_async_operator {
    /// Returns a reference to the inner asynchronous `core::Operator`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the `opendal_async_operator` pointer is valid
    /// and that the lifetime of the returned reference does not exceed the lifetime
    /// of the `opendal_async_operator`.
    pub(crate) unsafe fn as_ref(&self) -> &core::Operator {
        &*self.inner
    }
}

/// \brief Constructs a new asynchronous OpenDAL Operator.
///
/// @param scheme The storage service scheme (e.g., "s3", "fs").
/// @param options Configuration options for the service. Can be NULL.
/// @return Result containing the new operator or an error.
///
/// \see opendal_operator_options
/// \see opendal_result_operator_new (reused for simplicity, but contains async op)
///
/// # Safety
///
/// `scheme` must be a valid, null-terminated C string.
/// `options` must be a valid pointer or NULL.
#[no_mangle]
pub unsafe extern "C" fn opendal_async_operator_new(
    scheme: *const c_char,
    options: *const opendal_operator_options,
) -> opendal_result_operator_new {
    assert!(!scheme.is_null());
    let scheme_str = match std::ffi::CStr::from_ptr(scheme).to_str() {
        Ok(s) => s,
        Err(e) => {
            let err = core::Error::new(core::ErrorKind::Unexpected, "invalid scheme string")
                .set_source(e);
            return opendal_result_operator_new {
                op: std::ptr::null_mut(), // Represents async operator here
                error: opendal_error::new(err),
            };
        }
    };

    let scheme = match core::Scheme::from_str(scheme_str) {
        Ok(s) => s,
        Err(e) => {
            return opendal_result_operator_new {
                op: std::ptr::null_mut(),
                error: opendal_error::new(e),
            };
        }
    };

    let map: HashMap<_, _> = if options.is_null() {
        HashMap::default()
    } else {
        (*options)
            .deref()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };

    match core::Operator::via_iter(scheme, map) {
        Ok(mut op) => {
            // Apply common layers like retry
            op = op.layer(core::layers::RetryLayer::new());

            let async_op = Box::into_raw(Box::new(opendal_async_operator {
                inner: Box::into_raw(Box::new(op)),
            }));

            // We reuse opendal_result_operator_new, but the `op` field now points
            // to an opendal_async_operator. The C code needs to cast appropriately.
            opendal_result_operator_new {
                op: async_op as *mut opendal_operator, // Cast needed for return type
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => opendal_result_operator_new {
            op: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
    }
}

/// \brief Frees an asynchronous OpenDAL Operator.
///
/// # Safety
///
/// `op` must be a valid pointer previously returned by `opendal_async_operator_new`.
/// Calling with NULL does nothing.
#[no_mangle]
pub unsafe extern "C" fn opendal_async_operator_free(op: *const opendal_async_operator) {
    if !op.is_null() {
        // Drop the inner Operator
        drop(Box::from_raw((*op).inner));
        // Drop the container struct itself
        drop(Box::from_raw(op as *mut opendal_async_operator));
    }
}

// --- Async Stat Operation ---

/// \brief Asynchronously gets metadata of a path using a callback.
///
/// @param op A valid pointer to `opendal_async_operator`.
/// @param path The path to the object or directory.
/// @param callback The function to call when the operation completes.
/// @param user_data An opaque pointer passed directly to the callback function.
///
/// # Safety
/// `op` must be a valid `opendal_async_operator`.
/// `path` must be a valid, null-terminated C string.
/// `callback` must be a valid function pointer.
/// The `user_data` pointer's validity is the caller's responsibility.
/// The callback function will be invoked exactly once, either upon successful
/// \brief Asynchronously gets metadata of a path, returning a future handle.
///
/// The returned future can be awaited via `opendal_future_stat_await` to obtain the
/// resulting metadata or error, mirroring Rust's `async/await` ergonomics.
#[no_mangle]
pub unsafe extern "C" fn opendal_async_operator_stat(
    op: *const opendal_async_operator,
    path: *const c_char,
) -> opendal_result_future_stat {
    if op.is_null() {
        return opendal_result_future_stat {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "opendal_async_operator is null",
            )),
        };
    }
    if path.is_null() {
        return opendal_result_future_stat {
            future: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "path is null",
            )),
        };
    }

    let operator = (*op).as_ref();
    let path_str = match std::ffi::CStr::from_ptr(path).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => {
            return opendal_result_future_stat {
                future: std::ptr::null_mut(),
                error: opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "invalid path string")
                        .set_source(e),
                ),
            };
        }
    };

    let operator_clone = operator.clone();

    let handle =
        crate::operator::RUNTIME.spawn(async move { operator_clone.stat(&path_str).await });
    let future = Box::into_raw(Box::new(opendal_future_stat {
        inner: Box::into_raw(Box::new(Some(handle))),
    }));

    opendal_result_future_stat {
        future,
        error: std::ptr::null_mut(),
    }
}

/// \brief Await an asynchronous stat future and return the resulting metadata.
///
/// This function consumes the provided future pointer. After calling this function,
/// the future pointer must not be reused.
#[no_mangle]
pub unsafe extern "C" fn opendal_future_stat_await(
    future: *mut opendal_future_stat,
) -> opendal_result_stat {
    if future.is_null() {
        return opendal_result_stat {
            meta: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "opendal_future_stat is null",
            )),
        };
    }

    let mut future = Box::from_raw(future);
    if future.inner.is_null() {
        return opendal_result_stat {
            meta: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "opendal_future_stat inner handle is null",
            )),
        };
    }

    let mut handle_box = Box::from_raw(future.inner);
    future.inner = std::ptr::null_mut();
    let handle = match (*handle_box).take() {
        Some(handle) => handle,
        None => {
            return opendal_result_stat {
                meta: std::ptr::null_mut(),
                error: opendal_error::new(core::Error::new(
                    core::ErrorKind::Unexpected,
                    "opendal_future_stat already awaited",
                )),
            };
        }
    };

    let join_result = crate::operator::RUNTIME.block_on(async move { handle.await });

    match join_result {
        Ok(Ok(metadata)) => opendal_result_stat {
            meta: Box::into_raw(Box::new(opendal_metadata::new(metadata))),
            error: std::ptr::null_mut(),
        },
        Ok(Err(e)) => opendal_result_stat {
            meta: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
        Err(join_err) => opendal_result_stat {
            meta: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                format!("join error: {}", join_err),
            )),
        },
    }
}

/// \brief Cancel and free a stat future without awaiting it.
#[no_mangle]
pub unsafe extern "C" fn opendal_future_stat_free(future: *mut opendal_future_stat) {
    if future.is_null() {
        return;
    }

    let mut future = Box::from_raw(future);
    if !future.inner.is_null() {
        let mut handle_box = Box::from_raw(future.inner);
        future.inner = std::ptr::null_mut();
        if let Some(handle) = (*handle_box).take() {
            handle.abort();
        }
    }
}
