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
use std::io::{Read, Seek, SeekFrom};

use ::opendal as core;

use super::*;

pub const OPENDAL_SEEK_SET: i32 = 0;
pub const OPENDAL_SEEK_CUR: i32 = 1;
pub const OPENDAL_SEEK_END: i32 = 2;

/// \brief The result type returned by opendal's reader operation.
///
/// \note The opendal_reader actually owns a pointer to
/// a opendal::BlockingReader, which is inside the Rust core code.
#[repr(C)]
pub struct opendal_reader {
    /// The pointer to the opendal::StdReader in the Rust code.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_reader {
    fn deref_mut(&mut self) -> &mut core::blocking::StdReader {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut core::blocking::StdReader) }
    }
}

impl opendal_reader {
    pub(crate) fn new(reader: core::blocking::StdReader) -> Self {
        Self {
            inner: Box::into_raw(Box::new(reader)) as _,
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
        assert!(!buf.is_null());
        assert!(!size.is_null());
        let buf = std::slice::from_raw_parts_mut(buf, len);
        match self.deref_mut().read(buf) {
            Ok(n) => {
                std::ptr::write(size, n);
                std::ptr::null_mut()
            }
            Err(e) => {
                std::ptr::write(size, 0);
                opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "read failed from reader")
                        .set_source(e),
                )
            }
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
        assert!(!pos.is_null());
        let seek_from = match whence {
            OPENDAL_SEEK_SET => SeekFrom::Start(offset as u64),
            OPENDAL_SEEK_CUR => SeekFrom::Current(offset),
            OPENDAL_SEEK_END => SeekFrom::End(offset),
            _ => {
                std::ptr::write(pos, 0);
                return opendal_error::new(core::Error::new(
                    core::ErrorKind::Unexpected,
                    "undefined whence",
                ));
            }
        };

        match self.deref_mut().seek(seek_from) {
            Ok(p) => {
                std::ptr::write(pos, p);
                std::ptr::null_mut()
            }
            Err(e) => {
                std::ptr::write(pos, 0);
                opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "seek failed from reader")
                        .set_source(e),
                )
            }
        }
    }

    /// \brief Frees the heap memory used by the opendal_reader.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_free(ptr: *mut opendal_reader) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw(
                    (*ptr).inner as *mut core::blocking::StdReader,
                ));
                drop(Box::from_raw(ptr));
            }
        }
    }
}
