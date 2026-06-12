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
use futures_util::AsyncReadExt;
use futures_util::AsyncSeekExt;

use crate::result::opendal_result_reader_seek;

use super::*;

pub const OPENDAL_SEEK_SET: i32 = 0;
pub const OPENDAL_SEEK_CUR: i32 = 1;
pub const OPENDAL_SEEK_END: i32 = 2;

/// \brief The result type returned by opendal's reader operation.
///
/// \note The opendal_reader actually owns a pointer to
/// a opendal::FuturesAsyncReader, which is inside the Rust core code.
#[repr(C)]
pub struct opendal_reader {
    /// The pointer to the opendal::FuturesAsyncReader in the Rust code.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

pub(crate) struct AsyncReader {
    reader: core::FuturesAsyncReader,
}

impl opendal_reader {
    fn deref_mut(&mut self) -> &mut AsyncReader {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut AsyncReader) }
    }
}

impl opendal_reader {
    pub(crate) async fn create_async(reader: core::Reader) -> core::Result<AsyncReader> {
        let reader = reader.into_futures_async_read(..).await?;
        Ok(AsyncReader { reader })
    }

    pub(crate) fn from_async(reader: AsyncReader) -> Self {
        Self {
            inner: Box::into_raw(Box::new(reader)) as _,
        }
    }

    async fn read_async_inner(reader: &mut AsyncReader, buf: &mut [u8]) -> core::Result<usize> {
        reader
            .reader
            .read(buf)
            .await
            .map_err(|err| core::Error::new(core::ErrorKind::Unexpected, err.to_string()))
    }

    fn result_read(result: core::Result<usize>) -> opendal_result_reader_read {
        match result {
            Ok(n) => opendal_result_reader_read {
                size: n,
                error: std::ptr::null_mut(),
            },
            Err(e) => opendal_result_reader_read {
                size: 0,
                error: opendal_error::new(e),
            },
        }
    }

    /// \brief Like `opendal_reader_read` with cooperative cancellation.
    ///
    /// Pass NULL for `token` to block until completion.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_read_with_cancel(
        &mut self,
        buf: *mut u8,
        len: usize,
        token: *const opendal_cancel_token,
    ) -> opendal_result_reader_read {
        assert!(!buf.is_null());
        let buf = std::slice::from_raw_parts_mut(buf, len);
        let reader = self.deref_mut();
        Self::result_read(cancel::block_on_cancelable(token, async move {
            Self::read_async_inner(reader, buf).await
        }))
    }

    /// \brief Read data from the reader.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_read(
        &mut self,
        buf: *mut u8,
        len: usize,
    ) -> opendal_result_reader_read {
        unsafe { Self::opendal_reader_read_with_cancel(self, buf, len, std::ptr::null()) }
    }

    /// \brief Like `opendal_reader_seek` with cooperative cancellation.
    ///
    /// Pass NULL for `token` to block until completion.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_seek_with_cancel(
        &mut self,
        offset: i64,
        whence: i32,
        token: *const opendal_cancel_token,
    ) -> opendal_result_reader_seek {
        let reader = self.deref_mut();
        match cancel::block_on_cancelable(token, async move {
            seek_async_reader(reader, offset, whence).await
        }) {
            Ok(pos) => opendal_result_reader_seek {
                pos,
                error: std::ptr::null_mut(),
            },
            Err(e) => opendal_result_reader_seek {
                pos: 0,
                error: opendal_error::new(e),
            },
        }
    }

    /// \brief Seek to an offset, in bytes, in a stream.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_seek(
        &mut self,
        offset: i64,
        whence: i32,
    ) -> opendal_result_reader_seek {
        unsafe { Self::opendal_reader_seek_with_cancel(self, offset, whence, std::ptr::null()) }
    }

    /// \brief Frees the heap memory used by the opendal_reader.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_free(ptr: *mut opendal_reader) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut AsyncReader));
                drop(Box::from_raw(ptr));
            }
        }
    }
}

async fn seek_async_reader(
    reader: &mut AsyncReader,
    offset: i64,
    whence: i32,
) -> core::Result<u64> {
    let pos = match whence {
        _x @ OPENDAL_SEEK_SET => SeekFrom::Start(offset.try_into().map_err(|_| {
            core::Error::new(
                core::ErrorKind::Unexpected,
                "reader seek position is negative",
            )
        })?),
        _x @ OPENDAL_SEEK_CUR => SeekFrom::Current(offset),
        _x @ OPENDAL_SEEK_END => SeekFrom::End(offset),
        _ => {
            return Err(core::Error::new(
                core::ErrorKind::Unexpected,
                "undefined whence",
            ));
        }
    };

    reader
        .reader
        .seek(pos)
        .await
        .map_err(|err| core::Error::new(core::ErrorKind::Unexpected, err.to_string()))
}
