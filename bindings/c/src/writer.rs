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

use std::io::Write;

use ::opendal as core;
use opendal::Buffer;

use super::*;

/// \brief The result type returned by opendal's writer operation.
/// \note The opendal_writer actually owns a pointer to
/// a opendal::BlockingWriter, which is inside the Rust core code.
#[repr(C)]
pub struct opendal_writer {
    inner: *mut core::StdWriter,
}

impl opendal_writer {
    pub(crate) fn new(writer: core::StdWriter) -> Self {
        Self {
            inner: Box::into_raw(Box::new(writer)),
        }
    }

    /// \brief Write data to the writer.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_write(
        writer: *const Self,
        bytes: opendal_bytes,
    ) -> opendal_result_writer_write {
        let inner = unsafe { &mut *(*writer).inner };
        let buf: Buffer = bytes.into();
        let n = inner.write(&buf.to_vec());
        match n {
            Ok(n) => opendal_result_writer_write {
                size: n,
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

    /// \brief Frees the heap memory used by the opendal_writer.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_writer_free(ptr: *mut opendal_writer) {
        if !ptr.is_null() {
            let _ = unsafe { Box::from_raw((*ptr).inner) };
            let _ = unsafe { Box::from_raw(ptr) };
        }
    }
}
