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
use futures::StreamExt;

use super::*;
use crate::cancel::block_on_cancellable;

struct ListerInner {
    handle: tokio::runtime::Handle,
    lister: Option<core::Lister>,
}

impl Drop for ListerInner {
    fn drop(&mut self) {
        if let Some(v) = self.lister.take() {
            self.handle.block_on(async move { drop(v) });
        }
    }
}

/// \brief Lister is designed to list entries at a given path in a blocking
/// manner.
///
/// Users can construct a lister with `opendal_operator_list`,
/// `opendal_operator_list_with`, or `opendal_operator_list_with_cancel`.
///
/// For examples, please see the comment section of opendal_operator_list()
/// @see opendal_operator_list()
#[repr(C)]
pub struct opendal_lister {
    /// The pointer to the internal Rust lister state.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_lister {
    fn deref_mut(&mut self) -> &mut ListerInner {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut ListerInner) }
    }
}

impl opendal_lister {
    pub(crate) fn new(handle: tokio::runtime::Handle, lister: core::Lister) -> Self {
        Self {
            inner: Box::into_raw(Box::new(ListerInner {
                handle,
                lister: Some(lister),
            })) as _,
        }
    }

    /// \brief Return the next object to be listed.
    ///
    /// Lister is an iterator of the objects under its path, this method is the same as
    /// calling next() on the iterator.
    ///
    /// On success, writes the next entry pointer (or NULL when listing is exhausted)
    /// to `*entry` and returns NULL. On failure, writes NULL to `*entry` and returns
    /// a newly allocated error that must be freed with `opendal_error_free()`.
    ///
    /// For examples, please see the comment section of opendal_operator_list()
    /// @see opendal_operator_list()
    #[no_mangle]
    pub unsafe extern "C" fn opendal_lister_next(
        &mut self,
        entry: *mut *mut opendal_entry,
    ) -> *mut opendal_error {
        self.opendal_lister_next_with_cancel(std::ptr::null(), entry)
    }

    /// \brief Return the next object to be listed, supporting cancellation via `cancel`.
    ///
    /// Pass NULL for `cancel` to run without cancellation. On success, writes the next
    /// entry pointer (or NULL when listing is exhausted) to `*entry` and returns NULL.
    /// On cancellation or failure, writes NULL to `*entry` and returns an `opendal_error *`
    /// (`OPENDAL_CANCELLED` on cancellation).
    #[no_mangle]
    pub unsafe extern "C" fn opendal_lister_next_with_cancel(
        &mut self,
        cancel: *const opendal_cancel_token,
        entry: *mut *mut opendal_entry,
    ) -> *mut opendal_error {
        assert!(!entry.is_null());
        std::ptr::write(entry, std::ptr::null_mut());
        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = self.deref_mut();
        let lister = inner.lister.as_mut().expect("lister has been dropped");
        match block_on_cancellable(&inner.handle, cancel_token, async {
            lister.next().await.transpose()
        }) {
            Ok(None) => std::ptr::null_mut(),
            Ok(Some(e)) => {
                let ent = Box::into_raw(Box::new(opendal_entry::new(e)));
                std::ptr::write(entry, ent);
                std::ptr::null_mut()
            }
            Err(e) => opendal_error::from_cancellable(e),
        }
    }

    /// \brief Free the heap-allocated metadata used by opendal_lister
    #[no_mangle]
    pub unsafe extern "C" fn opendal_lister_free(ptr: *mut opendal_lister) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut ListerInner));
                drop(Box::from_raw(ptr));
            }
        }
    }
}
