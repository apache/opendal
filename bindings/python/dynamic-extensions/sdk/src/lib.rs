// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::c_void;
use std::slice;
use std::str;

pub const RUNTIME_PROTOCOL: u32 = 1;
pub const STATUS_OK: i32 = 0;
pub const STATUS_INVALID_ARGUMENT: i32 = 1;
pub const STATUS_INCOMPATIBLE: i32 = 2;
pub const STATUS_CONFLICT: i32 = 3;
pub const STATUS_LOAD_FAILED: i32 = 4;
pub const STATUS_OPERATION_FAILED: i32 = 5;
pub const STATUS_BUFFER_TOO_SMALL: i32 = 6;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ByteSlice {
    pub data: *const u8,
    pub len: usize,
}

// Protocol byte slices point to immutable memory whose owner must retain it
// for the documented call or table lifetime.
unsafe impl Send for ByteSlice {}
unsafe impl Sync for ByteSlice {}

impl ByteSlice {
    pub const fn from_static(value: &'static str) -> Self {
        Self {
            data: value.as_ptr(),
            len: value.len(),
        }
    }

    /// # Safety
    ///
    /// The caller must keep `data` valid for `len` bytes for the returned
    /// slice's lifetime.
    pub unsafe fn as_bytes<'a>(self) -> Result<&'a [u8], &'static str> {
        if self.len == 0 {
            return Ok(&[]);
        }
        if self.data.is_null() {
            return Err("byte slice has a null pointer");
        }
        Ok(unsafe { slice::from_raw_parts(self.data, self.len) })
    }

    /// # Safety
    ///
    /// The caller must satisfy [`ByteSlice::as_bytes`].
    pub unsafe fn as_str<'a>(self) -> Result<&'a str, &'static str> {
        str::from_utf8(unsafe { self.as_bytes()? }).map_err(|_| "byte slice is not valid UTF-8")
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct KeyValue {
    pub key: ByteSlice,
    pub value: ByteSlice,
}

#[repr(C)]
pub struct OutputBuffer {
    pub data: *mut u8,
    pub capacity: usize,
    pub len: usize,
}

/// Copy bytes into a caller-owned output buffer.
///
/// The function always records the required length. It returns
/// [`STATUS_BUFFER_TOO_SMALL`] without copying when the supplied capacity is
/// insufficient.
///
/// # Safety
///
/// `output` must be null or point to a valid [`OutputBuffer`]. Its `data` must
/// be valid for `capacity` writable bytes when the capacity is non-zero.
pub unsafe fn write_output(output: *mut OutputBuffer, value: &[u8]) -> i32 {
    if output.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let output = unsafe { &mut *output };
    output.len = value.len();
    if output.capacity < value.len() {
        return STATUS_BUFFER_TOO_SMALL;
    }
    if value.is_empty() {
        return STATUS_OK;
    }
    if output.data.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    unsafe { std::ptr::copy(value.as_ptr(), output.data, value.len()) };
    STATUS_OK
}

/// Creates an extension-owned operator handle.
///
/// Input slices remain caller-owned and are valid only for the call. On
/// success, `operator` receives a non-null handle that the caller must pass
/// only to callbacks from the same extension table and destroy exactly once.
pub type CreateOperatorFn = unsafe extern "C" fn(
    options: *const KeyValue,
    options_len: usize,
    operator: *mut *mut c_void,
    error: *mut OutputBuffer,
) -> i32;
/// Destroys a handle returned by [`CreateOperatorFn`].
pub type DestroyOperatorFn = unsafe extern "C" fn(operator: *mut c_void);

/// Describes one extension for the lifetime of its loaded native library.
///
/// The table and all byte slices in it point to immutable package-owned
/// storage. Callers must serialize operations and destruction for each handle;
/// this POC does not define concurrent callback behavior. Operation callbacks
/// borrow their input buffers for the call and report text errors through
/// caller-owned [`OutputBuffer`] values.
#[repr(C)]
pub struct ExtensionApiV1 {
    pub struct_size: usize,
    pub required_runtime_protocol: u32,
    pub opendal_version: ByteSlice,
    pub package_id: ByteSlice,
    pub component_id: ByteSlice,
    pub entry_symbol: ByteSlice,
    pub create_operator: CreateOperatorFn,
    pub destroy_operator: DestroyOperatorFn,
    pub operator_info: OperatorInfoFn,
    pub operator_write: OperatorWriteFn,
    pub operator_read: OperatorReadFn,
}

/// Returns an immutable extension table owned by the loaded native library.
pub type ExtensionBootstrapV1 = unsafe extern "C" fn(extension: *mut *const ExtensionApiV1) -> i32;

/// Provides the logical manifest fields and package-resolved artifact path.
///
/// Every byte slice is caller-owned and remains valid only for the registration
/// call. The runtime copies fields that must outlive the call and retains the
/// loaded library while any operator created from it remains alive.
#[repr(C)]
pub struct ServiceRegistrationV1 {
    pub struct_size: usize,
    pub required_runtime_protocol: u32,
    pub package_id: ByteSlice,
    pub component_id: ByteSlice,
    pub entry_symbol: ByteSlice,
    pub library_path: ByteSlice,
}

/// Registers one service and writes a diagnostic into `error` on failure.
pub type RegisterServiceFn = unsafe extern "C" fn(
    registration: *const ServiceRegistrationV1,
    error: *mut OutputBuffer,
) -> i32;
/// Creates a runtime-owned wrapper around an extension operator handle.
pub type CreateRuntimeOperatorFn = unsafe extern "C" fn(
    component_id: ByteSlice,
    options: *const KeyValue,
    options_len: usize,
    operator: *mut *mut c_void,
    error: *mut OutputBuffer,
) -> i32;
/// Writes JSON operator information into caller-owned output storage.
pub type OperatorInfoFn = unsafe extern "C" fn(
    operator: *mut c_void,
    output: *mut OutputBuffer,
    error: *mut OutputBuffer,
) -> i32;
/// Writes bytes through an operator without taking ownership of input slices.
pub type OperatorWriteFn = unsafe extern "C" fn(
    operator: *mut c_void,
    path: ByteSlice,
    data: ByteSlice,
    error: *mut OutputBuffer,
) -> i32;
/// Reads bytes into caller-owned output storage.
pub type OperatorReadFn = unsafe extern "C" fn(
    operator: *mut c_void,
    path: ByteSlice,
    output: *mut OutputBuffer,
    error: *mut OutputBuffer,
) -> i32;
/// Destroys a runtime operator wrapper exactly once.
pub type OperatorDestroyFn = unsafe extern "C" fn(operator: *mut c_void);

/// Runtime callbacks available for the lifetime of the runtime library.
#[repr(C)]
pub struct RuntimeApiV1 {
    pub struct_size: usize,
    pub register_service: RegisterServiceFn,
    pub create_operator: CreateRuntimeOperatorFn,
    pub operator_info: OperatorInfoFn,
    pub operator_write: OperatorWriteFn,
    pub operator_read: OperatorReadFn,
    pub operator_destroy: OperatorDestroyFn,
}

/// Reports the protocol interval supported by the loaded runtime.
#[repr(C)]
pub struct RuntimeProtocolInfoV1 {
    pub struct_size: usize,
    pub minimum_runtime_protocol: u32,
    pub runtime_protocol: u32,
}
