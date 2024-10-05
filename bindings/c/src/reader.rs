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
use std::io::Read;

use ::opendal as core;

use super::*;

/// \brief The result type returned by opendal's reader operation.
///
/// \note The opendal_reader actually owns a pointer to
/// a opendal::BlockingReader, which is inside the Rust core code.
#[repr(C)]
pub struct opendal_reader {
    inner: *mut c_void,
}

impl opendal_reader {
    fn deref_mut(&self) -> &mut core::StdReader {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut core::StdReader) }
    }
}

impl opendal_reader {
    pub(crate) fn new(reader: core::StdReader) -> Self {
        Self {
            inner: Box::into_raw(Box::new(reader)) as _,
        }
    }

    /// \brief Read data from the reader.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_read(
        &self,
        buf: *mut u8,
        len: usize,
    ) -> opendal_result_reader_read {
        if buf.is_null() {
            panic!("buf is NULL");
        }

        let buf = unsafe { std::slice::from_raw_parts_mut(buf, len) };
        match self.deref_mut().read(buf) {
            Ok(n) => opendal_result_reader_read {
                size: n,
                error: std::ptr::null_mut(),
            },
            Err(e) => opendal_result_reader_read {
                size: 0,
                error: opendal_error::new(
                    core::Error::new(core::ErrorKind::Unexpected, "read failed from reader")
                        .set_source(e),
                ),
            },
        }
    }

    /// \brief Frees the heap memory used by the opendal_reader.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_reader_free(ptr: *mut opendal_reader) {
        if !ptr.is_null() {
            let _ = Box::from_raw((*ptr).inner as *mut core::StdReader);
            let _ = Box::from_raw(ptr);
        }
    }
}
