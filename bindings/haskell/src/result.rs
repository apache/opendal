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

use std::ffi::c_char;
use std::ptr;

use ::opendal as od;

use crate::types::into_raw_c_string;

/// Result value returned by every exported FFI function through a caller
/// provided out pointer.
///
/// Ownership contract for `data_ptr` on success (`code == Ok`):
///
/// - For handle results the pointer itself is the owned resource and is
///   released by its dedicated finalizer (`free_operator`, `free_lister`,
///   `free_writer`).
/// - For string results the pointer is a `CString` released by `free_string`.
/// - For inline value results (`ByteSlice`, `Metadata`, `bool`) the pointer
///   is a `Box<T>` released together with the payload by the matching
///   `free_byteslice`, `free_metadata` or `free_bool` function.
///
/// `error_message` is a `CString` on failure and must be released by
/// `free_string`. Every allocation owned by a `FFIResult` is allocated by Rust
/// and must be freed by the matching Rust function, never by C `free()`.
#[repr(C)]
#[derive(Debug)]
pub struct FFIResult<T> {
    pub(crate) code: FFIErrorCode,
    pub(crate) data_ptr: *mut T,
    pub(crate) error_message: *mut c_char,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub enum FFIErrorCode {
    Ok = 0,
    FFIError = 1,
    Unexpected = 2,
    Unsupported = 3,
    ConfigInvalid = 4,
    NotFound = 5,
    PermissionDenied = 6,
    IsADirectory = 7,
    NotADirectory = 8,
    AlreadyExists = 9,
    RateLimited = 10,
    IsSameFile = 11,
    Conflict = 12,
    ConditionNotMatch = 13,
    RangeNotSatisfied = 14,
    ChecksumMismatch = 15,
}

impl<T> Default for FFIResult<T> {
    fn default() -> Self {
        FFIResult {
            code: FFIErrorCode::FFIError,
            data_ptr: ptr::null_mut(),
            error_message: ptr::null_mut(),
        }
    }
}

impl<T> FFIResult<T> {
    /// Box `data` and transfer the box to the caller through `data_ptr`.
    ///
    /// For results where `T` is a handle or `c_char`, prefer `ok_with_ptr`
    /// so `data_ptr` is the resource itself instead of a leaked
    /// `Box<*mut T>` wrapper.
    pub fn ok(data: T) -> Self {
        FFIResult {
            code: FFIErrorCode::Ok,
            data_ptr: Box::into_raw(Box::new(data)),
            error_message: ptr::null_mut(),
        }
    }

    /// Report success while handing `data_ptr` itself to the caller.
    ///
    /// Used when the payload is already the owned resource: `CString`
    /// results from `lister_next` and `operator_info`. Opaque handles are
    /// passed through `ok`, which boxes them into `data_ptr` directly.
    pub fn ok_with_ptr(data_ptr: *mut T) -> Self {
        FFIResult {
            code: FFIErrorCode::Ok,
            data_ptr,
            error_message: ptr::null_mut(),
        }
    }

    /// Report success without a payload, e.g. the end of a `Lister`.
    pub fn none() -> Self {
        FFIResult {
            code: FFIErrorCode::Ok,
            data_ptr: ptr::null_mut(),
            error_message: ptr::null_mut(),
        }
    }

    pub fn err(error_message: &str) -> Self {
        FFIResult {
            code: FFIErrorCode::FFIError,
            data_ptr: ptr::null_mut(),
            error_message: into_raw_c_string(error_message),
        }
    }

    pub fn err_with_source(error_message: &str, source: od::Error) -> Self {
        let msg = format!("{error_message}, source error: {source}");
        FFIResult {
            code: source.kind().into(),
            data_ptr: ptr::null_mut(),
            error_message: into_raw_c_string(&msg),
        }
    }
}

impl From<od::ErrorKind> for FFIErrorCode {
    fn from(kind: od::ErrorKind) -> Self {
        match kind {
            od::ErrorKind::Unexpected => FFIErrorCode::Unexpected,
            od::ErrorKind::Unsupported => FFIErrorCode::Unsupported,
            od::ErrorKind::ConfigInvalid => FFIErrorCode::ConfigInvalid,
            od::ErrorKind::NotFound => FFIErrorCode::NotFound,
            od::ErrorKind::PermissionDenied => FFIErrorCode::PermissionDenied,
            od::ErrorKind::IsADirectory => FFIErrorCode::IsADirectory,
            od::ErrorKind::NotADirectory => FFIErrorCode::NotADirectory,
            od::ErrorKind::AlreadyExists => FFIErrorCode::AlreadyExists,
            od::ErrorKind::RateLimited => FFIErrorCode::RateLimited,
            od::ErrorKind::IsSameFile => FFIErrorCode::IsSameFile,
            od::ErrorKind::Conflict => FFIErrorCode::Conflict,
            od::ErrorKind::ConditionNotMatch => FFIErrorCode::ConditionNotMatch,
            od::ErrorKind::RangeNotSatisfied => FFIErrorCode::RangeNotSatisfied,
            od::ErrorKind::ChecksumMismatch => FFIErrorCode::ChecksumMismatch,
            _ => FFIErrorCode::Unexpected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::free_string;
    use std::ffi::CStr;

    #[test]
    fn error_kind_conversion_preserves_public_error_kinds() {
        assert!(matches!(
            FFIErrorCode::from(od::ErrorKind::ConditionNotMatch),
            FFIErrorCode::ConditionNotMatch
        ));
        assert!(matches!(
            FFIErrorCode::from(od::ErrorKind::RangeNotSatisfied),
            FFIErrorCode::RangeNotSatisfied
        ));
        assert!(matches!(
            FFIErrorCode::from(od::ErrorKind::Conflict),
            FFIErrorCode::Conflict
        ));
        assert_eq!(
            FFIErrorCode::from(od::ErrorKind::ChecksumMismatch) as u32,
            15
        );
    }

    #[test]
    fn err_result_carries_message_owned_by_free_string() {
        let result: FFIResult<()> = FFIResult::err("something went wrong");
        assert_eq!(result.code, FFIErrorCode::FFIError);
        assert!(result.data_ptr.is_null());
        assert!(!result.error_message.is_null());
        unsafe {
            let msg = CStr::from_ptr(result.error_message).to_string_lossy();
            assert!(msg.contains("something went wrong"));
            free_string(result.error_message);
        }
    }

    #[test]
    fn err_result_sanitizes_interior_nul_bytes() {
        // An error message containing NUL bytes used to abort the process
        // through CString::new(..).unwrap(). It must now be escaped instead.
        let result: FFIResult<()> = FFIResult::err("a\0b");
        assert!(!result.error_message.is_null());
        unsafe {
            let msg = CStr::from_ptr(result.error_message);
            assert_eq!(msg.to_bytes(), b"a\\0b");
            free_string(result.error_message);
        }
    }

    #[test]
    fn err_with_source_maps_error_kind() {
        let source = od::Error::new(od::ErrorKind::NotFound, "no such key");
        let result: FFIResult<()> = FFIResult::err_with_source("Failed to read", source);
        assert_eq!(result.code, FFIErrorCode::NotFound);
        unsafe {
            let msg = CStr::from_ptr(result.error_message).to_string_lossy();
            assert!(msg.contains("Failed to read"));
            assert!(msg.contains("no such key"));
            free_string(result.error_message);
        }
    }

    #[test]
    fn ok_with_ptr_passes_the_pointer_through() {
        let s = into_raw_c_string("hello");
        let result = FFIResult::<c_char>::ok_with_ptr(s);
        assert_eq!(result.code, FFIErrorCode::Ok);
        assert_eq!(result.data_ptr, s);
        unsafe { free_string(result.data_ptr) };
    }

    #[test]
    fn none_result_has_no_payload() {
        let result = FFIResult::<c_char>::none();
        assert_eq!(result.code, FFIErrorCode::Ok);
        assert!(result.data_ptr.is_null());
        assert!(result.error_message.is_null());
    }
}
