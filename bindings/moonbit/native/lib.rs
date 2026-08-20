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

//! Private native bridge for the experimental MoonBit binding.
//!
//! All exported pointers are borrowed for the duration of a call unless the
//! function name ends in `_free`. Non-empty byte inputs must be readable for
//! their declared length. Owned outputs must be released exactly once by the
//! matching `_free` function. Fallible operations translate Rust panics into
//! an `Unexpected` error.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::slice;
use std::str;
use std::sync::{LazyLock, Mutex};

use opendal::blocking;
use opendal::services::Memory;

const ERROR_UNEXPECTED: i32 = 0;
const ERROR_UNSUPPORTED: i32 = 1;
const ERROR_CONFIG_INVALID: i32 = 2;
const ERROR_NOT_FOUND: i32 = 3;
const ERROR_PERMISSION_DENIED: i32 = 4;
const ERROR_IS_A_DIRECTORY: i32 = 5;
const ERROR_NOT_A_DIRECTORY: i32 = 6;
const ERROR_ALREADY_EXISTS: i32 = 7;
const ERROR_RATE_LIMITED: i32 = 8;
const ERROR_IS_SAME_FILE: i32 = 9;
const ERROR_CONDITION_NOT_MATCH: i32 = 10;
const ERROR_RANGE_NOT_SATISFIED: i32 = 11;
const ERROR_NONE: i32 = -1;
const ERROR_RESOURCE_CLOSED: i32 = 0x1001;
const ERROR_BUFFER_TOO_LARGE: i32 = 0x1002;
const ERROR_INVALID_ARGUMENT: i32 = 0x1003;
const ERROR_UNKNOWN: i32 = 0x10ff;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .build()
        .expect("the MoonBit binding runtime must start")
});

struct BridgeError {
    kind: i32,
    message: String,
}

impl BridgeError {
    fn new(kind: i32, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ERROR_INVALID_ARGUMENT, message)
    }

    fn resource_closed() -> Self {
        Self::new(ERROR_RESOURCE_CLOSED, "operator is closed")
    }
}

impl From<opendal::Error> for BridgeError {
    fn from(error: opendal::Error) -> Self {
        let kind = match error.kind() {
            opendal::ErrorKind::Unexpected => ERROR_UNEXPECTED,
            opendal::ErrorKind::Unsupported => ERROR_UNSUPPORTED,
            opendal::ErrorKind::ConfigInvalid => ERROR_CONFIG_INVALID,
            opendal::ErrorKind::NotFound => ERROR_NOT_FOUND,
            opendal::ErrorKind::PermissionDenied => ERROR_PERMISSION_DENIED,
            opendal::ErrorKind::IsADirectory => ERROR_IS_A_DIRECTORY,
            opendal::ErrorKind::NotADirectory => ERROR_NOT_A_DIRECTORY,
            opendal::ErrorKind::AlreadyExists => ERROR_ALREADY_EXISTS,
            opendal::ErrorKind::RateLimited => ERROR_RATE_LIMITED,
            opendal::ErrorKind::IsSameFile => ERROR_IS_SAME_FILE,
            opendal::ErrorKind::ConditionNotMatch => ERROR_CONDITION_NOT_MATCH,
            opendal::ErrorKind::RangeNotSatisfied => ERROR_RANGE_NOT_SATISFIED,
            _ => ERROR_UNKNOWN,
        };
        Self::new(kind, error.to_string())
    }
}

type BridgeResult<T> = Result<T, BridgeError>;

#[derive(Default)]
#[repr(C)]
pub struct NativeBytes {
    pub data: *mut u8,
    pub len: usize,
}

impl NativeBytes {
    fn new(data: Vec<u8>) -> Self {
        let data = data.into_boxed_slice();
        let len = data.len();
        Self {
            data: Box::into_raw(data).cast(),
            len,
        }
    }

    unsafe fn release(&mut self) {
        if self.data.is_null() {
            return;
        }
        let data = std::mem::take(self);
        let data = ptr::slice_from_raw_parts_mut(data.data, data.len);
        // SAFETY: the pointer and length came from the same boxed slice in `new`.
        drop(unsafe { Box::from_raw(data) });
    }
}

#[repr(C)]
pub struct NativeError {
    pub kind: i32,
    pub message: NativeBytes,
}

impl NativeError {
    fn ok() -> Self {
        Self {
            kind: ERROR_NONE,
            message: NativeBytes::default(),
        }
    }

    fn from_error(error: BridgeError) -> Self {
        Self {
            kind: error.kind,
            message: NativeBytes::new(error.message.into_bytes()),
        }
    }
}

#[repr(C)]
pub struct NativeResult<T> {
    pub error: NativeError,
    pub value: T,
}

pub type NativeOperatorResult = NativeResult<*mut NativeOperator>;
pub type NativeReadResult = NativeResult<NativeBytes>;

pub struct NativeOperator(Mutex<Option<blocking::Operator>>);

impl NativeOperator {
    fn new(operator: blocking::Operator) -> Self {
        Self(Mutex::new(Some(operator)))
    }

    fn operator(&self) -> BridgeResult<blocking::Operator> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .ok_or_else(BridgeError::resource_closed)
    }

    fn close(&self) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }
}

fn catch_bridge<T>(operation: impl FnOnce() -> BridgeResult<T>) -> BridgeResult<T> {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(result) => result,
        Err(_) => Err(BridgeError::new(
            ERROR_UNEXPECTED,
            "native binding panicked",
        )),
    }
}

fn ignore_panic(operation: impl FnOnce()) {
    let _ = catch_unwind(AssertUnwindSafe(operation));
}

unsafe fn input_slice<'a>(data: *const u8, len: u32, label: &str) -> BridgeResult<&'a [u8]> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(BridgeError::invalid_argument(format!(
            "{label} pointer is null"
        )));
    }
    // SAFETY: the caller guarantees that `data` points to `len` readable bytes.
    Ok(unsafe { slice::from_raw_parts(data, len as usize) })
}

unsafe fn copy_text(data: *const u8, len: u32, label: &str) -> BridgeResult<String> {
    // SAFETY: this function forwards the same pointer and length contract.
    let bytes = unsafe { input_slice(data, len, label)? };
    str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| BridgeError::invalid_argument(format!("{label} must be UTF-8")))
}

fn build_operator(scheme: &str) -> BridgeResult<NativeOperator> {
    if scheme != "memory" {
        return Err(BridgeError::new(
            ERROR_UNSUPPORTED,
            "only the memory service is enabled",
        ));
    }
    let _guard = RUNTIME.enter();
    let operator = opendal::Operator::new(Memory::default()).map_err(BridgeError::from)?;
    let operator = blocking::Operator::new(operator).map_err(BridgeError::from)?;
    Ok(NativeOperator::new(operator))
}

/// # Safety
/// `scheme` must point to `scheme_len` readable bytes when non-empty.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_operator_new(
    scheme: *const u8,
    scheme_len: u32,
) -> NativeOperatorResult {
    let result = catch_bridge(|| {
        // SAFETY: the caller upholds this function's pointer contract.
        let scheme = unsafe { copy_text(scheme, scheme_len, "service scheme")? };
        build_operator(&scheme)
    });
    match result {
        Ok(operator) => NativeOperatorResult {
            error: NativeError::ok(),
            value: Box::into_raw(Box::new(operator)),
        },
        Err(error) => NativeOperatorResult {
            error: NativeError::from_error(error),
            value: ptr::null_mut(),
        },
    }
}

/// # Safety
/// `operator` must be live and `path` readable for `path_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_operator_read(
    operator: *mut NativeOperator,
    path: *const u8,
    path_len: u32,
) -> NativeReadResult {
    let result = catch_bridge(|| {
        // SAFETY: the caller upholds this function's pointer contracts.
        let operator = unsafe { operator.as_ref() }.ok_or_else(BridgeError::resource_closed)?;
        // SAFETY: the caller upholds this function's pointer contracts.
        let path = unsafe { copy_text(path, path_len, "path")? };
        let data = operator
            .operator()?
            .read(&path)
            .map_err(BridgeError::from)?;
        if data.len() > i32::MAX as usize {
            return Err(BridgeError::new(
                ERROR_BUFFER_TOO_LARGE,
                "read result exceeds MoonBit Bytes capacity",
            ));
        }
        Ok(data.to_vec())
    });
    match result {
        Ok(data) => NativeReadResult {
            error: NativeError::ok(),
            value: NativeBytes::new(data),
        },
        Err(error) => NativeReadResult {
            error: NativeError::from_error(error),
            value: NativeBytes::default(),
        },
    }
}

/// # Safety
/// The handle must be live and both byte pointers readable for their lengths.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_operator_write(
    operator: *mut NativeOperator,
    path: *const u8,
    path_len: u32,
    data: *const u8,
    data_len: u32,
) -> NativeError {
    let result = catch_bridge(|| {
        // SAFETY: the caller upholds this function's pointer contracts.
        let operator = unsafe { operator.as_ref() }.ok_or_else(BridgeError::resource_closed)?;
        // SAFETY: the caller upholds this function's pointer contracts.
        let path = unsafe { copy_text(path, path_len, "path")? };
        // SAFETY: the caller upholds this function's pointer contracts.
        let data = unsafe { input_slice(data, data_len, "data")? }.to_vec();
        operator
            .operator()?
            .write(&path, data)
            .map(|_| ())
            .map_err(BridgeError::from)
    });
    match result {
        Ok(()) => NativeError::ok(),
        Err(error) => NativeError::from_error(error),
    }
}

/// # Safety
/// `operator` must be null or a live handle returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_operator_close(operator: *mut NativeOperator) {
    ignore_panic(|| {
        // SAFETY: the caller provides either null or a live handle.
        if let Some(operator) = unsafe { operator.as_ref() } {
            operator.close();
        }
    });
}

/// # Safety
/// `operator` must be null or an unfreed handle returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_operator_free(operator: *mut NativeOperator) {
    ignore_panic(|| {
        if !operator.is_null() {
            // SAFETY: the caller transfers this allocation exactly once.
            drop(unsafe { Box::from_raw(operator) });
        }
    });
}

/// # Safety
/// `bytes` must be null or an owned descriptor not previously released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_bytes_free(bytes: *mut NativeBytes) {
    // SAFETY: the caller provides either null or an owned byte descriptor.
    if let Some(bytes) = unsafe { bytes.as_mut() } {
        // SAFETY: the caller transfers the byte allocation exactly once.
        unsafe { bytes.release() };
    }
}
