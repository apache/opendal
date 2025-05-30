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

/// \brief The result type returned by opendal's writer operation.
/// \note The opendal_writer actually owns a pointer to
/// an opendal::blocking::Writer, which is inside the Rust core code.
#[repr(C)]
pub struct opendal_writer {
    /// The pointer to the opendal::blocking::Writer in the Rust code.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_writer {
    fn deref_mut(&mut self) -> &mut core::blocking::Writer {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut core::blocking::Writer) }
    }
}

impl opendal_writer {
    pub(crate) fn new(writer: core::blocking::Writer) -> Self {
        Self {
            inner: Box::into_raw(Box::new(writer)) as _,
        }
    }

    /// \brief Write data to the writer.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_write(
        &mut self,
        bytes: &opendal_bytes,
    ) -> opendal_result_writer_write {
        let size = bytes.len;
        match self.deref_mut().write(bytes) {
            Ok(()) => opendal_result_writer_write {
                size,
                error: std::ptr::null_mut(),
            },
            Err(e) => opendal_result_writer_write {
                size: 0,
                error: opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "write failed from writer")
                        .set_source(e),
                ),
            },
        }
    }

    /// \brief Close the writer and make sure all data have been stored.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_close(ptr: *mut opendal_writer) -> *mut opendal_error {
        if !ptr.is_null() {
            if let Err(e) = (*ptr).deref_mut().close() {
                return opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "close writer failed")
                        .set_source(e),
                );
            }
        }
        std::ptr::null_mut()
    }

    /// \brief Frees the heap memory used by the opendal_writer.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_free(ptr: *mut opendal_writer) {
        if !ptr.is_null() {
            drop(Box::from_raw((*ptr).inner as *mut core::blocking::Writer));
            drop(Box::from_raw(ptr));
        }
    }
}
