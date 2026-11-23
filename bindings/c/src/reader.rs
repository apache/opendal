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
use std::io::SeekFrom;

use ::opendal as core;
use futures::{AsyncReadExt, AsyncSeekExt};

use super::*;
use crate::cancel::{block_on_cancellable, CancellableError};

/// Seek from the start of the stream.
pub const OPENDAL_SEEK_SET: i32 = 0;
/// Seek relative to the current stream position.
pub const OPENDAL_SEEK_CUR: i32 = 1;
/// Seek relative to the end of the stream.
pub const OPENDAL_SEEK_END: i32 = 2;

struct ReaderInner {
    handle: tokio::runtime::Handle,
    reader: Option<core::FuturesAsyncReader>,
}

impl Drop for ReaderInner {
    fn drop(&mut self) {
        if let Some(v) = self.reader.take() {
            self.handle.block_on(async move { drop(v) });
        }
    }
}

/// \brief The result type returned by opendal's reader operation.
///
/// \note The opendal_reader owns an async OpenDAL reader driven on the
/// operator's Tokio runtime handle.
#[repr(C)]
pub struct opendal_reader {
    /// The pointer to the internal Rust reader state.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_reader {
    fn deref_mut(&mut self) -> &mut ReaderInner {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut ReaderInner) }
    }
}

impl opendal_reader {
    pub(crate) fn new(handle: tokio::runtime::Handle, reader: core::FuturesAsyncReader) -> Self {
        Self {
            inner: Box::into_raw(Box::new(ReaderInner {
                handle,
                reader: Some(reader),
            })) as _,
        }
    }

    /// \brief Read data from the reader into `buf` and write the number of bytes read to `*size`.
    ///
    /// At EOF, writes `0` to `*size` and returns NULL. On failure, writes `0` to `*size`
    /// and returns an `opendal_error *`.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_read(
        &mut self,
        buf: *mut u8,
        len: usize,
        size: *mut usize,
    ) -> *mut opendal_error {
        self.opendal_reader_read_with_cancel(buf, len, std::ptr::null(), size)
    }

    /// \brief Read data from the reader into `buf`, supporting cancellation via `cancel`.
    ///
    /// Pass NULL for `cancel` to run without cancellation. At EOF, writes `0` to
    /// `*size` and returns NULL. On cancellation or failure, writes `0` to `*size`
    /// and returns an `opendal_error *` (`OPENDAL_CANCELLED` on cancellation).
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_read_with_cancel(
        &mut self,
        buf: *mut u8,
        len: usize,
        cancel: *const opendal_cancel_token,
        size: *mut usize,
    ) -> *mut opendal_error {
        assert!(!buf.is_null());
        assert!(!size.is_null());
        std::ptr::write(size, 0);
        let buf = std::slice::from_raw_parts_mut(buf, len);
        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = self.deref_mut();
        let reader = inner.reader.as_mut().expect("reader has been dropped");
        match block_on_cancellable(&inner.handle, cancel_token, reader.read(buf)) {
            Ok(n) => {
                std::ptr::write(size, n);
                std::ptr::null_mut()
            }
            Err(CancellableError::Cancelled) => opendal_error::cancelled(),
            Err(CancellableError::Inner(e)) => opendal_error::new(
                core::Error::new(core::ErrorKind::Unexpected, "read failed from reader")
                    .set_source(e),
            ),
        }
    }

    /// \brief Seek to an offset, in bytes, in a stream, and write the new position to `*pos`.
    ///
    /// On failure, writes `0` to `*pos` and returns an `opendal_error *`.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_seek(
        &mut self,
        offset: i64,
        whence: i32,
        pos: *mut u64,
    ) -> *mut opendal_error {
        self.opendal_reader_seek_with_cancel(offset, whence, std::ptr::null(), pos)
    }

    /// \brief Seek to an offset, in bytes, in a stream, supporting cancellation via `cancel`.
    ///
    /// Pass NULL for `cancel` to run without cancellation. On cancellation or failure,
    /// writes `0` to `*pos` and returns an `opendal_error *` (`OPENDAL_CANCELLED` on
    /// cancellation).
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_seek_with_cancel(
        &mut self,
        offset: i64,
        whence: i32,
        cancel: *const opendal_cancel_token,
        pos: *mut u64,
    ) -> *mut opendal_error {
        assert!(!pos.is_null());
        std::ptr::write(pos, 0);
        let seek_from = match whence {
            OPENDAL_SEEK_SET => match u64::try_from(offset) {
                Ok(offset) => SeekFrom::Start(offset),
                Err(_) => {
                    return opendal_error::new(core::Error::new(
                        core::ErrorKind::Unexpected,
                        "negative seek offset is invalid with OPENDAL_SEEK_SET",
                    ));
                }
            },
            OPENDAL_SEEK_CUR => SeekFrom::Current(offset),
            OPENDAL_SEEK_END => SeekFrom::End(offset),
            _ => {
                return opendal_error::new(core::Error::new(
                    core::ErrorKind::Unexpected,
                    "undefined whence",
                ));
            }
        };

        let cancel_token = opendal_cancel_token::as_inner(cancel);
        let inner = self.deref_mut();
        let reader = inner.reader.as_mut().expect("reader has been dropped");
        match block_on_cancellable(&inner.handle, cancel_token, reader.seek(seek_from)) {
            Ok(p) => {
                std::ptr::write(pos, p);
                std::ptr::null_mut()
            }
            Err(CancellableError::Cancelled) => opendal_error::cancelled(),
            Err(CancellableError::Inner(e)) => opendal_error::new(
                core::Error::new(core::ErrorKind::Unexpected, "seek failed from reader")
                    .set_source(e),
            ),
        }
    }

    /// \brief Frees the heap memory used by the opendal_reader.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_free(ptr: *mut opendal_reader) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut ReaderInner));
                drop(Box::from_raw(ptr));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seek_set_rejects_negative_offset() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let op = core::Operator::new(core::services::Memory::default()).unwrap();
        let reader = runtime.block_on(async {
            op.write("test", "hello").await.unwrap();
            op.reader("test")
                .await
                .unwrap()
                .into_futures_async_read(core::BytesRange::default())
                .await
                .unwrap()
        });
        let reader = Box::into_raw(Box::new(opendal_reader::new(
            runtime.handle().clone(),
            reader,
        )));

        let mut pos = 42;
        let err = unsafe {
            opendal_reader::opendal_reader_seek(&mut *reader, -1, OPENDAL_SEEK_SET, &mut pos)
        };
        assert!(!err.is_null());
        assert_eq!(pos, 0);
        unsafe {
            opendal_error::opendal_error_free(err);
            opendal_reader::opendal_reader_free(reader);
        }
    }
}
