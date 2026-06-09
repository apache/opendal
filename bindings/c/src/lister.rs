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

use ::opendal as core;
use futures_util::StreamExt;
use std::ffi::c_void;

use super::*;

/// \brief BlockingLister is designed to list entries at given path in a blocking
/// manner.
///
/// Users can construct Lister by `blocking_list` or `blocking_scan`(currently not supported in C binding)
///
/// For examples, please see the comment section of opendal_operator_list()
/// @see opendal_operator_list()
#[repr(C)]
pub struct opendal_lister {
    /// The pointer to the opendal::BlockingLister in the Rust code.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_lister {
    fn deref_mut(&mut self) -> &mut core::Lister {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut core::Lister) }
    }
}

impl opendal_lister {
    pub(crate) fn new_async(lister: core::Lister) -> Self {
        Self {
            inner: Box::into_raw(Box::new(lister)) as _,
        }
    }

    fn result_next(result: core::Result<Option<core::Entry>>) -> opendal_result_lister_next {
        match result {
            Ok(Some(e)) => opendal_result_lister_next {
                entry: Box::into_raw(Box::new(opendal_entry::new(e))),
                error: std::ptr::null_mut(),
            },
            Ok(None) => opendal_result_lister_next {
                entry: std::ptr::null_mut(),
                error: std::ptr::null_mut(),
            },
            Err(e) => opendal_result_lister_next {
                entry: std::ptr::null_mut(),
                error: opendal_error::new(e),
            },
        }
    }

    /// \brief Like `opendal_lister_next` with cooperative cancellation.
    ///
    /// Pass NULL for `token` to block until completion.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_lister_next_with_cancel(
        &mut self,
        token: *const opendal_cancel_token,
    ) -> opendal_result_lister_next {
        let lister = self.deref_mut();
        Self::result_next(cancel::block_on_cancelable(token, async move {
            lister.next().await.transpose()
        }))
    }

    /// \brief Return the next object to be listed
    ///
    /// Lister is an iterator of the objects under its path, this method is the same as
    /// calling next() on the iterator
    ///
    /// For examples, please see the comment section of opendal_operator_list()
    /// @see opendal_operator_list()
    #[no_mangle]
    pub unsafe extern "C" fn opendal_lister_next(&mut self) -> opendal_result_lister_next {
        unsafe { Self::opendal_lister_next_with_cancel(self, std::ptr::null()) }
    }

    /// \brief Free the heap-allocated metadata used by opendal_lister
    #[no_mangle]
    pub unsafe extern "C" fn opendal_lister_free(ptr: *mut opendal_lister) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut core::Lister));
                drop(Box::from_raw(ptr));
            }
        }
    }
}
