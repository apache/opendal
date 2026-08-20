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

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::slice;
use std::str;
use std::sync::{LazyLock, Mutex, MutexGuard};

use opendal::blocking;
use opendal::services::Memory;

const STATUS_OK: i32 = 0;
const STATUS_ERROR: i32 = 1;

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
const ERROR_RESOURCE_CLOSED: i32 = 0x1001;
const ERROR_BUFFER_TOO_LARGE: i32 = 0x1002;
const ERROR_INVALID_ARGUMENT: i32 = 0x1003;
const ERROR_UNKNOWN: i32 = 0x10ff;

static RUNTIME: LazyLock<Result<tokio::runtime::Runtime, String>> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .build()
        .map_err(|error| error.to_string())
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

    fn unexpected(message: impl Into<String>) -> Self {
        Self::new(ERROR_UNEXPECTED, message)
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

fn runtime() -> BridgeResult<&'static tokio::runtime::Runtime> {
    match &*RUNTIME {
        Ok(runtime) => Ok(runtime),
        Err(message) => Err(BridgeError::unexpected(format!(
            "unable to create the native runtime: {message}"
        ))),
    }
}

fn lock_operator(
    inner: &Mutex<Option<blocking::Operator>>,
) -> MutexGuard<'_, Option<blocking::Operator>> {
    match inner.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub struct NativeOperator {
    inner: Mutex<Option<blocking::Operator>>,
}

impl NativeOperator {
    fn new(operator: blocking::Operator) -> Self {
        Self {
            inner: Mutex::new(Some(operator)),
        }
    }

    fn with_operator<T>(
        &self,
        operation: impl FnOnce(&blocking::Operator) -> BridgeResult<T>,
    ) -> BridgeResult<T> {
        let guard = lock_operator(&self.inner);
        let operator = guard.as_ref().ok_or_else(BridgeError::resource_closed)?;
        operation(operator)
    }

    fn close(&self) {
        let operator = lock_operator(&self.inner).take();
        drop(operator);
    }
}

#[repr(C)]
pub struct NativeBytes {
    pub data: *const u8,
    pub len: usize,
}

impl NativeBytes {
    fn new(data: Vec<u8>) -> Self {
        let data = data.into_boxed_slice();
        let len = data.len();
        let data = Box::into_raw(data) as *mut u8;
        Self { data, len }
    }

    fn empty() -> Self {
        Self::new(Vec::new())
    }
}

impl Drop for NativeBytes {
    fn drop(&mut self) {
        if self.data.is_null() {
            return;
        }
        let data = ptr::slice_from_raw_parts_mut(self.data.cast_mut(), self.len);
        // SAFETY: `data` and `len` came from the same boxed slice in `new`.
        drop(unsafe { Box::from_raw(data) });
    }
}

#[repr(C)]
pub struct NativeResult {
    pub status: i32,
    pub error_kind: i32,
    pub message: NativeBytes,
    pub operator: *mut NativeOperator,
    pub data: NativeBytes,
    pub has_data: u8,
}

impl NativeResult {
    fn error(error: BridgeError) -> Self {
        Self {
            status: STATUS_ERROR,
            error_kind: error.kind,
            message: NativeBytes::new(error.message.into_bytes()),
            operator: ptr::null_mut(),
            data: NativeBytes::empty(),
            has_data: 0,
        }
    }

    fn operator(operator: NativeOperator) -> Self {
        Self {
            status: STATUS_OK,
            error_kind: ERROR_UNEXPECTED,
            message: NativeBytes::empty(),
            operator: Box::into_raw(Box::new(operator)),
            data: NativeBytes::empty(),
            has_data: 0,
        }
    }

    fn data(data: Vec<u8>) -> Self {
        Self {
            status: STATUS_OK,
            error_kind: ERROR_UNEXPECTED,
            message: NativeBytes::empty(),
            operator: ptr::null_mut(),
            data: NativeBytes::new(data),
            has_data: 1,
        }
    }

    fn unit() -> Self {
        Self {
            status: STATUS_OK,
            error_kind: ERROR_UNEXPECTED,
            message: NativeBytes::empty(),
            operator: ptr::null_mut(),
            data: NativeBytes::empty(),
            has_data: 0,
        }
    }
}

impl Drop for NativeResult {
    fn drop(&mut self) {
        if self.operator.is_null() {
            return;
        }
        let operator = self.operator;
        self.operator = ptr::null_mut();
        // SAFETY: the pointer came from `Box::into_raw` and is still owned here.
        drop(unsafe { Box::from_raw(operator) });
    }
}

fn catch_result(operation: impl FnOnce() -> BridgeResult<NativeResult>) -> *mut NativeResult {
    let result = match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => NativeResult::error(error),
        Err(_) => NativeResult::error(BridgeError::unexpected("native binding panicked")),
    };
    Box::into_raw(Box::new(result))
}

unsafe fn copy_input(data: *const u8, len: usize, label: &str) -> BridgeResult<Vec<u8>> {
    if len > isize::MAX as usize {
        return Err(BridgeError::invalid_argument(format!(
            "{label} is too large"
        )));
    }
    if len == 0 {
        return Ok(Vec::new());
    }
    if data.is_null() {
        return Err(BridgeError::invalid_argument(format!(
            "{label} pointer is null"
        )));
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(len)
        .map_err(|_| BridgeError::unexpected(format!("unable to allocate {label}")))?;
    // SAFETY: the caller guarantees that `data` points to `len` readable bytes.
    let input = unsafe { slice::from_raw_parts(data, len) };
    output.extend_from_slice(input);
    Ok(output)
}

unsafe fn copy_text(data: *const u8, len: usize, label: &str) -> BridgeResult<String> {
    // SAFETY: this function forwards the same pointer and length contract.
    let bytes = unsafe { copy_input(data, len, label)? };
    if bytes.contains(&0) {
        return Err(BridgeError::invalid_argument(format!(
            "{label} contains an embedded NUL byte"
        )));
    }
    str::from_utf8(&bytes)
        .map(str::to_owned)
        .map_err(|_| BridgeError::invalid_argument(format!("{label} must be valid UTF-8")))
}

fn build_operator(scheme: &str) -> BridgeResult<NativeOperator> {
    if scheme != "memory" {
        return Err(BridgeError::new(
            ERROR_UNSUPPORTED,
            "only the memory service is enabled in this experimental phase",
        ));
    }
    let runtime = runtime()?;
    let _guard = runtime.enter();
    let operator = opendal::Operator::new(Memory::default()).map_err(BridgeError::from)?;
    let operator = blocking::Operator::new(operator).map_err(BridgeError::from)?;
    Ok(NativeOperator::new(operator))
}

/// Constructs the memory operator used by the first MoonBit binding phase.
///
/// # Safety
///
/// `scheme` must point to `scheme_len` readable bytes when `scheme_len` is nonzero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_operator_new(
    scheme: *const u8,
    scheme_len: usize,
) -> *mut NativeResult {
    catch_result(|| {
        // SAFETY: the caller upholds this function's pointer contract.
        let scheme = unsafe { copy_text(scheme, scheme_len, "service scheme")? };
        Ok(NativeResult::operator(build_operator(&scheme)?))
    })
}

/// Closes an operator without releasing its handle allocation.
///
/// # Safety
///
/// `operator` must be null or point to a live handle returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_operator_close(operator: *mut NativeOperator) {
    if operator.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller upholds this function's pointer contract.
        unsafe { &*operator }.close();
    }));
}

/// Releases an operator handle and closes it if necessary.
///
/// # Safety
///
/// `operator` must be null or an unfreed pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_operator_free(operator: *mut NativeOperator) {
    if operator.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller transfers ownership of this allocation.
        drop(unsafe { Box::from_raw(operator) });
    }));
}

/// Reads a whole object.
///
/// # Safety
///
/// `operator` must point to a live handle. `path` must point to `path_len`
/// readable bytes when `path_len` is nonzero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_operator_read(
    operator: *mut NativeOperator,
    path: *const u8,
    path_len: usize,
) -> *mut NativeResult {
    catch_result(|| {
        // SAFETY: the caller upholds this function's path pointer contract.
        let path = unsafe { copy_text(path, path_len, "path")? };
        // SAFETY: the caller upholds this function's operator pointer contract.
        let operator = unsafe { operator.as_ref() }.ok_or_else(BridgeError::resource_closed)?;
        let data = operator.with_operator(|operator| {
            let buffer = operator.read(&path).map_err(BridgeError::from)?;
            if buffer.len() > i32::MAX as usize {
                return Err(BridgeError::new(
                    ERROR_BUFFER_TOO_LARGE,
                    "read result exceeds MoonBit Bytes capacity",
                ));
            }
            Ok(buffer.to_vec())
        })?;
        Ok(NativeResult::data(data))
    })
}

/// Writes a whole object.
///
/// # Safety
///
/// `operator` must point to a live handle. The path and data pointers must each
/// point to their declared readable lengths when those lengths are nonzero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_operator_write(
    operator: *mut NativeOperator,
    path: *const u8,
    path_len: usize,
    data: *const u8,
    data_len: usize,
) -> *mut NativeResult {
    catch_result(|| {
        // SAFETY: the caller upholds this function's input pointer contracts.
        let path = unsafe { copy_text(path, path_len, "path")? };
        // SAFETY: the caller upholds this function's input pointer contracts.
        let data = unsafe { copy_input(data, data_len, "data")? };
        // SAFETY: the caller upholds this function's operator pointer contract.
        let operator = unsafe { operator.as_ref() }.ok_or_else(BridgeError::resource_closed)?;
        operator.with_operator(|operator| {
            operator
                .write(&path, data)
                .map(|_| ())
                .map_err(BridgeError::from)
        })?;
        Ok(NativeResult::unit())
    })
}

/// Releases a result and any payload that has not been transferred.
///
/// # Safety
///
/// `result` must be null or an unfreed pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_moonbit_result_free(result: *mut NativeResult) {
    if result.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller transfers ownership of this allocation.
        drop(unsafe { Box::from_raw(result) });
    }));
}
