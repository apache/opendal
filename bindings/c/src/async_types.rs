// filepath: /home/w/gitproject/opendal/bindings/c/src/async_types.rs
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

use std::ffi::c_void;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::oneshot;

use ::opendal as core;

use super::*;

/// \brief Represents the status of an asynchronous operation future.
#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub enum opendal_future_poll_status {
    /// The asynchronous operation is still in progress.
    PENDING,
    /// The asynchronous operation has completed.
    READY,
}

// --- Future Stat ---

/// Internal state for the stat future.
pub(crate) struct opendal_future_stat_inner {
    /// Receiver for the result from the spawned Tokio task.
    pub(crate) result_receiver: Arc<Mutex<Option<oneshot::Receiver<core::Result<core::Metadata>>>>>,
    /// Cached result once the future is ready.
    pub(crate) cached_result: Arc<Mutex<Option<core::Result<core::Metadata>>>>,
}

/// \brief Opaque handle representing an ongoing asynchronous `stat` operation.
///
/// Use `opendal_future_stat_poll` to check readiness and `opendal_future_stat_get`
/// to retrieve the result. Must be freed with `opendal_future_stat_free`.
#[repr(C)]
pub struct opendal_future_stat {
    /// Pointer to the internal state `opendal_future_stat_inner`.
    pub(crate) inner: *mut c_void,
}

impl opendal_future_stat {
    /// # Safety
    /// Inner pointer must be valid and point to `opendal_future_stat_inner`.
    unsafe fn get_inner_mut(&mut self) -> &mut opendal_future_stat_inner {
        &mut *(self.inner as *mut opendal_future_stat_inner)
    }
}

/// \brief Polls the status of an asynchronous stat operation.
///
/// This function checks if the result of the `stat` operation is available without blocking.
///
/// @param future A valid pointer to `opendal_future_stat`.
/// @return `READY` if the operation is complete, `PENDING` otherwise.
///
/// # Safety
/// `future` must be a valid pointer returned by `opendal_async_operator_stat`.
#[no_mangle]
pub unsafe extern "C" fn opendal_future_stat_poll(
    future: *mut opendal_future_stat,
) -> opendal_future_poll_status {
    if future.is_null() {
        // Or handle error appropriately
        return opendal_future_poll_status::PENDING;
    }

    let inner = (*future).get_inner_mut();

    // Check cache first
    if inner.cached_result.lock().unwrap().is_some() {
        return opendal_future_poll_status::READY;
    }

    // If no cached result, check the receiver
    let mut receiver_opt = inner.result_receiver.lock().unwrap();
    if let Some(ref mut receiver) = *receiver_opt {
        // Use a dummy waker as we are polling synchronously from C
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        match Pin::new(receiver).poll(&mut cx) {
            Poll::Ready(result) => {
                // Operation completed, cache the result
                let final_result = result.unwrap_or_else(|_| {
                    // Sender dropped, likely means task panicked or was cancelled.
                    Err(core::Error::new(
                        core::ErrorKind::Unexpected,
                        "async stat operation cancelled or panicked",
                    ))
                });
                *inner.cached_result.lock().unwrap() = Some(final_result);
                // Drop the receiver as it's no longer needed
                *receiver_opt = None;
                opendal_future_poll_status::READY
            }
            Poll::Pending => opendal_future_poll_status::PENDING,
        }
    } else {
        // Receiver already consumed or never existed (error state?)
        // Should be READY if cached_result is None and receiver is None only after poll succeeded.
        if inner.cached_result.lock().unwrap().is_some() {
            opendal_future_poll_status::READY
        } else {
            // This state should ideally not be reached if logic is correct
            // Consider logging an error or returning a specific error status
            opendal_future_poll_status::PENDING // Or indicate an error
        }
    }
}

/// \brief Gets the result of a completed asynchronous stat operation.
///
/// This function should only be called after `opendal_future_stat_poll` returns `READY`.
/// It retrieves the metadata or the error that occurred.
///
/// @param future A valid pointer to `opendal_future_stat`.
/// @return An `opendal_result_stat` containing the metadata or an error.
///         If called before the future is ready, the result is undefined (likely error).
///
/// # Safety
/// `future` must be a valid pointer returned by `opendal_async_operator_stat`,
/// and `opendal_future_stat_poll` must have returned `READY`.
#[no_mangle]
pub unsafe extern "C" fn opendal_future_stat_get(
    future: *mut opendal_future_stat,
) -> opendal_result_stat {
    if future.is_null() {
        return opendal_result_stat {
            meta: std::ptr::null_mut(),
            error: opendal_error::new(core::Error::new(
                core::ErrorKind::Unexpected,
                "null future passed to opendal_future_stat_get",
            )),
        };
    }

    let inner = (*future).get_inner_mut();
    let mut cached_result_guard = inner.cached_result.lock().unwrap();

    // Take the result from the cache. This ensures it's consumed only once.
    match cached_result_guard.take() {
        Some(Ok(metadata)) => opendal_result_stat {
            meta: Box::into_raw(Box::new(opendal_metadata::new(metadata))),
            error: std::ptr::null_mut(),
        },
        Some(Err(e)) => opendal_result_stat {
            meta: std::ptr::null_mut(),
            error: opendal_error::new(e),
        },
        None => {
            // Called before ready or called multiple times
            opendal_result_stat {
                meta: std::ptr::null_mut(),
                error: opendal_error::new(core::Error::new(
                    core::ErrorKind::Unexpected,
                    "future result already taken or not ready",
                )),
            }
        }
    }
}

/// \brief Frees the resources associated with an asynchronous stat future.
///
/// # Safety
/// `future` must be a valid pointer returned by `opendal_async_operator_stat`
/// or NULL. If non-NULL, it must not be used after calling this function.
#[no_mangle]
pub unsafe extern "C" fn opendal_future_stat_free(future: *mut opendal_future_stat) {
    if !future.is_null() {
        // Drop the inner state pointed to by the future's inner pointer
        drop(Box::from_raw(
            (*future).inner as *mut opendal_future_stat_inner,
        ));
        // Drop the future struct itself
        drop(Box::from_raw(future));
    }
}
