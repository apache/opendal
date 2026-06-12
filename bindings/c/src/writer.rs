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
    fn deref_mut(&mut self) -> &mut core::Writer {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut core::Writer) }
    }
}

impl opendal_writer {
    pub(crate) fn new_async(writer: core::Writer) -> Self {
        Self {
            inner: Box::into_raw(Box::new(writer)) as _,
        }
    }

    fn result_write(result: core::Result<usize>) -> opendal_result_writer_write {
        match result {
            Ok(size) => opendal_result_writer_write {
                size,
                error: std::ptr::null_mut(),
            },
            Err(e) => opendal_result_writer_write {
                size: 0,
                error: opendal_error::new(e),
            },
        }
    }

    /// \brief Like `opendal_writer_write` with cooperative cancellation.
    ///
    /// Pass NULL for `token` to block until completion.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_write_with_cancel(
        &mut self,
        bytes: &opendal_bytes,
        token: *const opendal_cancel_token,
    ) -> opendal_result_writer_write {
        let size = bytes.len;
        let bytes = core::Buffer::from(bytes);
        let writer = self.deref_mut();
        Self::result_write(cancel::block_on_cancelable(token, async move {
            writer.write(bytes).await?;
            Ok(size)
        }))
    }

    /// \brief Write data to the writer.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_write(
        &mut self,
        bytes: &opendal_bytes,
    ) -> opendal_result_writer_write {
        unsafe { Self::opendal_writer_write_with_cancel(self, bytes, std::ptr::null()) }
    }

    /// \brief Like `opendal_writer_close` with cooperative cancellation.
    ///
    /// Pass NULL for `token` to block until completion.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_close_with_cancel(
        ptr: *mut opendal_writer,
        token: *const opendal_cancel_token,
    ) -> *mut opendal_error {
        unsafe {
            if !ptr.is_null() {
                let writer = (*ptr).deref_mut();
                if let Err(e) =
                    cancel::block_on_cancelable(token, async move { writer.close().await })
                {
                    return opendal_error::new(e);
                }
            }
            std::ptr::null_mut()
        }
    }

    /// \brief Close the writer and make sure all data have been stored.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_close(ptr: *mut opendal_writer) -> *mut opendal_error {
        unsafe { Self::opendal_writer_close_with_cancel(ptr, std::ptr::null()) }
    }

    /// \brief Frees the heap memory used by the opendal_writer.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_free(ptr: *mut opendal_writer) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut core::Writer));
                drop(Box::from_raw(ptr));
            }
        }
    }
}
