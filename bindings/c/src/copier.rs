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

use ::opendal as core;

use super::*;
use crate::cancel::{block_on_cancellable, CancelTokenInner};

struct CopierInner {
    handle: tokio::runtime::Handle,
    copier: Option<core::Copier>,
}

impl Drop for CopierInner {
    fn drop(&mut self) {
        if let Some(v) = self.copier.take() {
            self.handle.block_on(async move { drop(v) });
        }
    }
}

/// \brief opendal_copier completes a long-running copy operation to
/// completion in a blocking manner. opendal_copier repeatedly calls a
/// copy operation until completion.
///
/// Internally, each copy step performs a unit of work and reports progress.
/// When copy completes, `has_next` returns false.
///
/// A "step" is one backend-defined unit of work. For backends that copy in
/// multiple requests (e.g. multipart copy), one step typically copies one chunk;
/// for backends that only support single-request copy, the entire copy happens
/// in a single step. The reported byte count is best-effort: a step may report
/// 0 bytes when the backend advances its state without a reliable byte delta.
///
/// Users can construct a copier by `opendal_operator_copier`,
/// `opendal_operator_copier_with`, or `opendal_operator_copier_with_cancel`.
///
/// @see opendal_operator_copier()
/// @see opendal_copier_next()
#[repr(C)]
pub struct opendal_copier {
    /// The pointer to the internal Rust copier state.
    /// Only used to check whether the copier is NULL.
    inner: *mut c_void,
}

impl opendal_copier {
    fn deref_mut(&mut self) -> &mut CopierInner {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut CopierInner) }
    }
}

impl opendal_copier {
    pub(crate) fn new(handle: tokio::runtime::Handle, copier: core::Copier) -> Self {
        Self {
            inner: Box::into_raw(Box::new(CopierInner {
                handle,
                copier: Some(copier),
            })) as _,
        }
    }

    /// \brief Perform one step of the copy operation.
    ///
    /// One step performs one backend-defined unit of work: typically one chunk for
    /// backends that copy in multiple requests, or the entire copy for backends that
    /// only support single-request copy.
    ///
    /// Writes the number of bytes copied in this step to `*size` (best-effort; may be 0
    /// when the backend advances without a reliable byte delta) and whether more steps
    /// remain to `*has_next`. When `*has_next` is true the caller should call this
    /// function again to continue the copy. When `*has_next` is false and the returned
    /// error is NULL the copy has completed. On failure, writes `0` to `*size` and
    /// `false` to `*has_next`, and returns an `opendal_error *`.
    ///
    /// @see opendal_operator_copier()
    #[no_mangle]
    pub unsafe extern "C" fn opendal_copier_next(
        &mut self,
        size: *mut usize,
        has_next: *mut bool,
    ) -> *mut opendal_error {
        self.opendal_copier_next_with_cancel(std::ptr::null(), size, has_next)
    }

    /// \brief Perform one step of the copy operation, supporting cancellation via `cancel`.
    ///
    /// Pass NULL for `cancel` to run without cancellation. On cancellation, writes `0`
    /// to `*size` and `false` to `*has_next`, returns `OPENDAL_CANCELLED`, and preserves
    /// the copier so the caller can resume, abort, or free it cleanly.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_copier_next_with_cancel(
        &mut self,
        cancel: *const opendal_cancel_token,
        size: *mut usize,
        has_next: *mut bool,
    ) -> *mut opendal_error {
        assert!(!size.is_null());
        assert!(!has_next.is_null());
        std::ptr::write(size, 0);
        std::ptr::write(has_next, false);

        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = self.deref_mut();
        let Some(copier) = inner.copier.as_mut() else {
            if cancel_token.is_some_and(CancelTokenInner::is_cancelled) {
                return opendal_error::cancelled();
            }
            return std::ptr::null_mut();
        };

        match block_on_cancellable(&inner.handle, cancel_token, copier.next()) {
            Ok(Some(n)) => {
                std::ptr::write(size, n);
                std::ptr::write(has_next, true);
                std::ptr::null_mut()
            }
            Ok(None) => {
                if let Some(c) = inner.copier.take() {
                    inner.handle.block_on(async move { drop(c) });
                }
                std::ptr::null_mut()
            }
            Err(e) => opendal_error::from_cancellable(e),
        }
    }

    /// \brief Abort the pending copy operation.
    ///
    /// Returns NULL if the abort succeeds, otherwise it contains the error code and
    /// error message.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_copier_abort(&mut self) -> *mut opendal_error {
        self.opendal_copier_abort_with_cancel(std::ptr::null())
    }

    /// \brief Abort the pending copy operation, supporting cancellation via `cancel`.
    ///
    /// Pass NULL for `cancel` to run without cancellation.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_copier_abort_with_cancel(
        &mut self,
        cancel: *const opendal_cancel_token,
    ) -> *mut opendal_error {
        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = self.deref_mut();
        let Some(copier) = inner.copier.as_mut() else {
            if cancel_token.is_some_and(CancelTokenInner::is_cancelled) {
                return opendal_error::cancelled();
            }
            return std::ptr::null_mut();
        };

        match block_on_cancellable(&inner.handle, cancel_token, copier.abort()) {
            Ok(()) => std::ptr::null_mut(),
            Err(e) => opendal_error::from_cancellable(e),
        }
    }

    /// \brief Free the heap memory used by the opendal_copier.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_copier_free(ptr: *mut opendal_copier) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut CopierInner));
                // A use-after-free crashes on NULL instead of reading stale memory.
                (*ptr).inner = std::ptr::null_mut();
                drop(Box::from_raw(ptr));
            }
        }
    }
}
