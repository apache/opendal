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
use std::ffi::c_void;

use super::*;

/// \brief BlockingCopier is designed to drive a long-running copy operation in a
/// blocking manner.
///
/// Users can construct a copier by `opendal_operator_copier` or
/// `opendal_operator_copier_with`. Each `opendal_copier_next` call drives the copy
/// forward by one step and reports the number of bytes copied; the copy is complete
/// once `has_next` is false.
///
/// @see opendal_operator_copier()
#[repr(C)]
pub struct opendal_copier {
    /// The pointer to the opendal::blocking::Copier in the Rust code.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_copier {
    fn deref_mut(&mut self) -> &mut core::blocking::Copier {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut core::blocking::Copier) }
    }
}

impl opendal_copier {
    pub(crate) fn new(copier: core::blocking::Copier) -> Self {
        Self {
            inner: Box::into_raw(Box::new(copier)) as _,
        }
    }

    /// \brief Drive the copy operation forward by one step.
    ///
    /// Returns the number of bytes copied in this step. When `has_next` is true the
    /// caller should keep calling this function to drive the copy. When `has_next` is
    /// false and `error` is null the copy has completed.
    ///
    /// @see opendal_operator_copier()
    #[no_mangle]
    pub unsafe extern "C" fn opendal_copier_next(&mut self) -> opendal_result_copier_next {
        match self.deref_mut().next() {
            Some(Ok(n)) => opendal_result_copier_next {
                size: n,
                has_next: true,
                error: std::ptr::null_mut(),
            },
            None => opendal_result_copier_next {
                size: 0,
                has_next: false,
                error: std::ptr::null_mut(),
            },
            Some(Err(e)) => opendal_result_copier_next {
                size: 0,
                has_next: false,
                error: opendal_error::new(e),
            },
        }
    }

    /// \brief Abort the pending copy operation.
    ///
    /// Returns NULL if the abort succeeds, otherwise it contains the error code and
    /// error message.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_copier_abort(&mut self) -> *mut opendal_error {
        if let Err(e) = self.deref_mut().abort() {
            opendal_error::new(e)
        } else {
            std::ptr::null_mut()
        }
    }

    /// \brief Free the heap memory used by the opendal_copier.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_copier_free(ptr: *mut opendal_copier) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut core::blocking::Copier));
                drop(Box::from_raw(ptr));
            }
        }
    }
}
