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
use crate::cancel::{block_on_cancellable, CancellableError};

struct WriterInner {
    handle: tokio::runtime::Handle,
    writer: Option<core::Writer>,
}

impl Drop for WriterInner {
    fn drop(&mut self) {
        if let Some(v) = self.writer.take() {
            self.handle.block_on(async move { drop(v) });
        }
    }
}

/// \brief The result type returned by opendal's writer operation.
/// \note The opendal_writer owns an async `opendal::Writer` driven on the
/// operator's Tokio runtime handle.
#[repr(C)]
pub struct opendal_writer {
    /// The pointer to the internal Rust writer state.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_writer {
    fn deref_mut(&mut self) -> &mut WriterInner {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut WriterInner) }
    }
}

impl opendal_writer {
    pub(crate) fn new(handle: tokio::runtime::Handle, writer: core::Writer) -> Self {
        Self {
            inner: Box::into_raw(Box::new(WriterInner {
                handle,
                writer: Some(writer),
            })) as _,
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
        self.opendal_writer_write_with_cancel(bytes, std::ptr::null(), size)
    }

    /// \brief Write data to the writer, supporting cancellation via `cancel`.
    ///
    /// Pass NULL for `cancel` to run without cancellation. On cancellation or failure,
    /// writes `0` to `*size` and returns an `opendal_error *` (`OPENDAL_CANCELLED` on
    /// cancellation).
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_write_with_cancel(
        &mut self,
        bytes: &opendal_bytes,
        cancel: *const opendal_cancel_token,
        size: *mut usize,
    ) -> *mut opendal_error {
        assert!(!size.is_null());
        std::ptr::write(size, 0);
        let len = bytes.len;
        let bytes = match bytes.to_buffer() {
            Ok(bytes) => bytes,
            Err(e) => return opendal_error::new(e),
        };
        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = self.deref_mut();
        let writer = inner.writer.as_mut().expect("writer has been dropped");
        match block_on_cancellable(&inner.handle, cancel_token, writer.write(bytes)) {
            Ok(()) => {
                std::ptr::write(size, len);
                std::ptr::null_mut()
            }
            Err(CancellableError::Cancelled) => opendal_error::cancelled(),
            Err(CancellableError::Inner(e)) => opendal_error::new(
                core::Error::new(core::ErrorKind::Unexpected, "write failed from writer")
                    .set_source(e),
            ),
        }
    }

    /// \brief Close the writer and make sure all data have been stored.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_close(ptr: *mut opendal_writer) -> *mut opendal_error {
        Self::opendal_writer_close_with_cancel(ptr, std::ptr::null(), std::ptr::null_mut())
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
        assert!(!meta.is_null());
        Self::opendal_writer_close_with_cancel(ptr, std::ptr::null(), meta)
    }

    /// \brief Close the writer, supporting cancellation via `cancel` and optional metadata output.
    ///
    /// Pass NULL for `cancel` to run without cancellation. Pass NULL for `meta` to
    /// discard the written object's metadata; when non-null, `*meta` receives the
    /// allocated `opendal_metadata *` on success or NULL on cancellation or failure.
    ///
    /// @param ptr The opendal_writer to close (NULL is a no-op returning NULL)
    /// @param cancel Optional cancel token, or NULL
    /// @param meta Optional output pointer for the written object's metadata, or NULL
    /// @return NULL on success, or an `opendal_error *` on cancellation or failure.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_close_with_cancel(
        ptr: *mut opendal_writer,
        cancel: *const opendal_cancel_token,
        meta: *mut *mut opendal_metadata,
    ) -> *mut opendal_error {
        if !meta.is_null() {
            std::ptr::write(meta, std::ptr::null_mut());
        }
        if ptr.is_null() {
            return std::ptr::null_mut();
        }
        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = (*ptr).deref_mut();
        let writer = inner.writer.as_mut().expect("writer has been dropped");
        match block_on_cancellable(&inner.handle, cancel_token, writer.close()) {
            Ok(m) => {
                if !meta.is_null() {
                    std::ptr::write(meta, Box::into_raw(Box::new(opendal_metadata::new(m))));
                }
                std::ptr::null_mut()
            }
            Err(CancellableError::Cancelled) => opendal_error::cancelled(),
            Err(CancellableError::Inner(e)) => opendal_error::new(
                core::Error::new(core::ErrorKind::Unexpected, "close writer failed").set_source(e),
            ),
        }
    }

    /// \brief Frees the heap memory used by the opendal_writer.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_free(ptr: *mut opendal_writer) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut WriterInner));
                drop(Box::from_raw(ptr));
            }
        }
    }
}
