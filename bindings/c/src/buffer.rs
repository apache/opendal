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

use std::ffi::CString;
use std::os::raw::c_char;

use opendal::{Buffer, Error, ErrorKind};

/// \brief Frees a heap-allocated string returned by OpenDAL C APIs.
///
/// \note Only pass pointers returned from OpenDAL APIs that transfer string ownership.
#[no_mangle]
pub unsafe extern "C" fn opendal_string_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(unsafe { CString::from_raw(ptr) });
    }
}

/// \brief opendal_bytes carries raw-bytes with its length
///
/// The opendal_bytes type is a C-compatible substitute for Vec type in Rust.
/// For buffers returned by OpenDAL C APIs, call opendal_bytes_free() to free
/// the heap memory and avoid memory leaks. If you reuse an `opendal_bytes`
/// output variable across multiple read calls, call `opendal_bytes_free()`
/// on the previous result before passing it again as an output parameter.
/// For caller-owned input buffers passed to OpenDAL C APIs, the caller keeps
/// ownership and must not call opendal_bytes_free() on them.
///
/// @see opendal_bytes_free
#[repr(C)]
pub struct opendal_bytes {
    /// Pointing to the byte array on heap
    pub data: *mut u8,
    /// The length of the byte array
    pub len: usize,
    /// The capacity of the byte array
    pub capacity: usize,
}

impl opendal_bytes {
    pub(crate) fn empty() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        }
    }

    /// Construct a [`opendal_bytes`] from the Rust [`Vec`] of bytes
    pub(crate) fn new(b: Buffer) -> Self {
        if b.is_empty() {
            return Self::empty();
        }

        let mut b = std::mem::ManuallyDrop::new(b.to_vec());
        Self {
            data: b.as_mut_ptr(),
            len: b.len(),
            capacity: b.capacity(),
        }
    }

    /// \brief Frees the heap memory used by the opendal_bytes
    #[no_mangle]
    pub unsafe extern "C" fn opendal_bytes_free(ptr: *mut opendal_bytes) {
        unsafe {
            if !ptr.is_null() {
                let bs = &mut *ptr;
                if !bs.data.is_null() {
                    drop(Vec::from_raw_parts(bs.data, bs.len, bs.capacity));
                }
                bs.data = std::ptr::null_mut();
                bs.len = 0;
                bs.capacity = 0;
            }
        }
    }

    pub(crate) fn to_buffer(&self) -> opendal::Result<Buffer> {
        if self.len == 0 {
            if self.data.is_null() {
                return Ok(Buffer::new());
            }

            return Err(Error::new(
                ErrorKind::Unexpected,
                "empty opendal_bytes must have null data",
            ));
        }

        if self.data.is_null() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "non-empty opendal_bytes has null data",
            ));
        }

        let slice = unsafe { std::slice::from_raw_parts(self.data, self.len) };
        Ok(Buffer::from(bytes::Bytes::copy_from_slice(slice)))
    }
}

impl Drop for opendal_bytes {
    fn drop(&mut self) {
        unsafe {
            // Safety: the pointer is always valid
            Self::opendal_bytes_free(self);
        }
    }
}

#[cfg(test)]
mod bytes_tests {
    use super::*;

    #[test]
    fn test_empty_buffer_has_null_data() {
        let mut bytes = opendal_bytes::new(Buffer::new());

        assert!(bytes.data.is_null());
        assert_eq!(bytes.len, 0);
        assert_eq!(bytes.capacity, 0);

        let buffer = bytes.to_buffer().expect("empty bytes should convert");
        assert!(buffer.is_empty());

        unsafe { opendal_bytes::opendal_bytes_free(&mut bytes) };
        assert!(bytes.data.is_null());
        assert_eq!(bytes.len, 0);
        assert_eq!(bytes.capacity, 0);
    }

    #[test]
    fn test_non_empty_buffer_round_trips() {
        let mut bytes = opendal_bytes::new(Buffer::from("hello"));

        assert!(!bytes.data.is_null());
        assert_eq!(bytes.len, 5);
        assert_eq!(
            unsafe { std::slice::from_raw_parts(bytes.data, bytes.len) },
            b"hello"
        );
        assert_eq!(bytes.to_buffer().unwrap().to_vec(), b"hello");

        unsafe { opendal_bytes::opendal_bytes_free(&mut bytes) };
        assert!(bytes.data.is_null());
        assert_eq!(bytes.len, 0);
        assert_eq!(bytes.capacity, 0);
    }
}
