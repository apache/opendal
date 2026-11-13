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

    /// \brief Write data to the writer and write the number of bytes written to `*size`.
    ///
    /// On failure, writes `0` to `*size` and returns an `opendal_error *`.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_write(
        &mut self,
        bytes: &opendal_bytes,
        size: *mut usize,
    ) -> *mut opendal_error {
        assert!(!size.is_null());
        let len = bytes.len;
        let bytes = match bytes.to_buffer() {
            Ok(bytes) => bytes,
            Err(e) => {
                std::ptr::write(size, 0);
                return opendal_error::new(e);
            }
        };
        match self.deref_mut().write(bytes) {
            Ok(()) => {
                std::ptr::write(size, len);
                std::ptr::null_mut()
            }
            Err(e) => {
                std::ptr::write(size, 0);
                opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "write failed from writer")
                        .set_source(e),
                )
            }
        }
    }

    /// \brief Close the writer and make sure all data have been stored.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_close(ptr: *mut opendal_writer) -> *mut opendal_error {
        unsafe {
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
    }

    /// \brief Close the writer and write the written object's metadata to `*meta`.
    ///
    /// Like `opendal_writer_close`, but on success writes the metadata of the
    /// written object (e.g. etag, version, last modified) to `*meta` instead of discarding it.
    ///
    /// @param ptr The opendal_writer to close
    /// @param meta Output pointer receiving the allocated `opendal_metadata *` on success, or NULL on failure
    /// @return NULL on success, or an `opendal_error *` on failure.
    ///
    /// \note The returned metadata must be freed with opendal_metadata_free().
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_close_with_metadata(
        ptr: *mut opendal_writer,
        meta: *mut *mut opendal_metadata,
    ) -> *mut opendal_error {
        unsafe {
            assert!(!meta.is_null());
            if ptr.is_null() {
                std::ptr::write(meta, std::ptr::null_mut());
                return std::ptr::null_mut();
            }
            match (*ptr).deref_mut().close() {
                Ok(m) => {
                    std::ptr::write(meta, Box::into_raw(Box::new(opendal_metadata::new(m))));
                    std::ptr::null_mut()
                }
                Err(e) => {
                    std::ptr::write(meta, std::ptr::null_mut());
                    opendal_error::new(
                        core::Error::new(core::ErrorKind::Unexpected, "close writer failed")
                            .set_source(e),
                    )
                }
            }
        }
    }

    /// \brief Frees the heap memory used by the opendal_writer.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_free(ptr: *mut opendal_writer) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut core::blocking::Writer));
                drop(Box::from_raw(ptr));
            }
        }
    }
}
