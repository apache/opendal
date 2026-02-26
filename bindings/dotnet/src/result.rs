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
use std::os::raw::c_char;

use crate::byte_buffer::ByteBuffer;
use crate::error::ErrorCode;

#[repr(C)]
/// Error payload returned by exported FFI functions.
pub struct OpenDALError {
    /// `1` when an error is present, otherwise `0`.
    pub has_error: u8,
    /// Numeric error code mapped from `ErrorCode`.
    pub code: i32,
    /// Heap-allocated UTF-8 message created by Rust.
    ///
    /// The receiver must call `message_free` to release it when non-null.
    pub message: *mut c_char,
}

#[repr(C)]
/// Result for operations that only report success or failure.
pub struct OpendalResult {
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning a native pointer.
pub struct OpendalIntPtrResult {
    /// Pointer payload on success, null on error.
    pub ptr: *mut c_void,
    /// Error information for the operation.
    pub error: OpenDALError,
}

#[repr(C)]
/// Result for operations returning a byte buffer.
pub struct OpendalByteBufferResult {
    /// Buffer payload on success.
    pub buffer: ByteBuffer,
    /// Error information for the operation.
    pub error: OpenDALError,
}

/// Build an empty error payload that represents success.
pub fn ok_error() -> OpenDALError {
    OpenDALError {
        has_error: 0,
        code: 0,
        message: std::ptr::null_mut(),
    }
}

fn error_message_ptr(message: impl Into<String>) -> *mut c_char {
    let message = message.into().replace('\0', " ");
    let message = match std::ffi::CString::new(message) {
        Ok(message) => message,
        Err(_) => std::ffi::CString::new("invalid error message")
            .expect("hardcoded error message should be valid"),
    };

    message.into_raw()
}

pub fn make_error(code: ErrorCode, message: impl Into<String>) -> OpenDALError {
    OpenDALError {
        has_error: 1,
        code: code as i32,
        message: error_message_ptr(message),
    }
}

/// Build a success result without payload.
pub fn ok_result() -> OpendalResult {
    OpendalResult { error: ok_error() }
}

/// Build a success result carrying a raw pointer payload.
pub fn ok_ptr_result(ptr: *mut c_void) -> OpendalIntPtrResult {
    OpendalIntPtrResult {
        ptr,
        error: ok_error(),
    }
}

/// Build a success result carrying a byte buffer payload.
pub fn ok_buffer_result(buffer: ByteBuffer) -> OpendalByteBufferResult {
    OpendalByteBufferResult {
        buffer,
        error: ok_error(),
    }
}

impl OpendalResult {
    /// Build an operation result that contains an error.
    pub fn from_error(error: OpenDALError) -> Self {
        Self { error }
    }
}

impl OpendalIntPtrResult {
    /// Build a pointer result from an error.
    ///
    /// The pointer field is always null.
    pub fn from_error(error: OpenDALError) -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            error,
        }
    }
}

impl OpendalByteBufferResult {
    /// Build a byte-buffer result from an error.
    ///
    /// The buffer field is always empty.
    pub fn from_error(error: OpenDALError) -> Self {
        Self {
            buffer: ByteBuffer::empty(),
            error,
        }
    }
}
