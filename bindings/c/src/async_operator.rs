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
use std::ffi::c_void;
use std::os::raw::c_char;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::runtime::Handle;
use tokio::sync::oneshot;

use ::opendal as core;

use super::*;
use crate::async_types::*; // We will create this file next

/// \brief Represents an asynchronous OpenDAL Operator.
///
/// This operator interacts with storage services using non-blocking APIs.
/// Use `opendal_async_operator_new` to construct and `opendal_async_operator_free` to release.
#[repr(C)]
pub struct opendal_async_operator {
    /// Internal pointer to the Rust async Operator.
    inner: *mut core::Operator,
    /// Tokio runtime handle.
    rt: *mut c_void,
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

    /// Returns a reference to the Tokio runtime handle.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the `opendal_async_operator` pointer is valid.
    pub(crate) unsafe fn runtime_handle(&self) -> &Handle {
        &*(self.rt as *const Handle)
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

    // Use the shared runtime defined in operator.rs
    // We need access to the RUNTIME static variable.
    // Let's make RUNTIME public or provide an accessor function.
    // For now, assume we can get the handle.
    // NOTE: This requires modification in operator.rs or a shared runtime module.
    // Assuming a function `get_runtime_handle()` exists for now.
    let runtime_handle = crate::operator::RUNTIME.handle().clone();

    match core::Operator::via_iter(scheme, map) {
        Ok(mut op) => {
            // Apply common layers like retry
            op = op.layer(core::layers::RetryLayer::new());

            let async_op = Box::into_raw(Box::new(opendal_async_operator {
                inner: Box::into_raw(Box::new(op)),
                rt: Box::into_raw(Box::new(runtime_handle)) as *mut c_void,
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
        // Drop the inner Operator and the Handle
        drop(Box::from_raw((*op).inner));
        drop(Box::from_raw((*op).rt as *mut Handle));
        // Drop the container struct itself
        drop(Box::from_raw(op as *mut opendal_async_operator));
    }
}

// --- Async Stat Operation ---

/// \brief Asynchronously gets metadata of a path.
///
/// @param op A valid pointer to `opendal_async_operator`.
/// @param path The path to the object or directory.
/// @return A pointer to `opendal_future_stat` representing the ongoing operation.
///         The caller is responsible for polling and freeing this future.
///         Returns NULL if path is invalid.
///
/// # Safety
/// `op` must be a valid `opendal_async_operator`.
/// `path` must be a valid, null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn opendal_async_operator_stat(
    op: *const opendal_async_operator,
    path: *const c_char,
) -> *mut opendal_future_stat {
    if op.is_null() || path.is_null() {
        return std::ptr::null_mut();
    }

    let operator = (*op).as_ref();
    let runtime_handle = (*op).runtime_handle();
    let path_str = match std::ffi::CStr::from_ptr(path).to_str() {
        Ok(s) => s.to_string(),                // Clone path string to own it
        Err(_) => return std::ptr::null_mut(), // Invalid UTF-8 path
    };

    // Channel to send the result back from the async task
    let (tx, rx) = oneshot::channel::<core::Result<core::Metadata>>();

    // Clone operator for the async task
    let operator_clone = operator.clone();

    // Spawn the async operation onto the Tokio runtime
    runtime_handle.spawn(async move {
        let result = operator_clone.stat(&path_str).await;
        // Ignore error if receiver is dropped (future was freed)
        let _ = tx.send(result);
    });

    // Create the future state
    let future_state = Box::new(opendal_future_stat_inner {
        result_receiver: Arc::new(Mutex::new(Some(rx))),
        cached_result: Arc::new(Mutex::new(None)),
    });

    // Return the opaque future pointer to C
    Box::into_raw(Box::new(opendal_future_stat {
        inner: Box::into_raw(future_state) as *mut c_void,
    }))
}
