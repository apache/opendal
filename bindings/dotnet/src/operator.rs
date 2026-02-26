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

use crate::{
    byte_buffer::ByteBuffer,
    error::ErrorCode,
    result::{
        OpenDALError, OpendalByteBufferResult, OpendalIntPtrResult, OpendalResult,
        make_error, ok_buffer_result, ok_ptr_result, ok_result,
    },
    utils::cstr_to_str,
};

use std::collections::HashMap;
use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::LazyLock;

type WriteCallback = unsafe extern "C" fn(context: *mut c_void, result: OpendalResult);
type ReadCallback = unsafe extern "C" fn(context: *mut c_void, result: OpendalByteBufferResult);

fn config_invalid_error(message: impl Into<String>) -> OpenDALError {
    make_error(ErrorCode::ConfigInvalid, message.into())
}

fn from_opendal_error(error: opendal::Error) -> OpenDALError {
    make_error(ErrorCode::from_error_kind(error.kind()), error.to_string())
}

fn invalid_utf8_message(field: &str) -> String {
    format!("{field} is null or invalid UTF-8")
}

fn invalid_utf8_message_at(field: &str, index: usize) -> String {
    format!("{field} at index {index} is null or invalid UTF-8")
}

fn require_cstr<'a>(value: *const c_char, field: &str) -> Result<&'a str, OpenDALError> {
    cstr_to_str(value).ok_or_else(|| config_invalid_error(invalid_utf8_message(field)))
}

fn require_cstr_at<'a>(
    value: *const c_char,
    field: &str,
    index: usize,
) -> Result<&'a str, OpenDALError> {
    cstr_to_str(value)
        .ok_or_else(|| config_invalid_error(invalid_utf8_message_at(field, index)))
}

fn require_operator<'a>(op: *const opendal::Operator) -> Result<&'a opendal::Operator, OpenDALError> {
    if op.is_null() {
        return Err(config_invalid_error("operator pointer is null"));
    }

    Ok(unsafe { &*op })
}

fn require_data_ptr(data: *const u8, len: usize) -> Result<(), OpenDALError> {
    if len > 0 && data.is_null() {
        return Err(config_invalid_error("data pointer is null while len > 0"));
    }

    Ok(())
}

fn require_callback<T>(callback: Option<T>) -> Result<T, OpenDALError> {
    callback.ok_or_else(|| config_invalid_error("callback pointer is null"))
}

unsafe fn collect_options(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> Result<HashMap<String, String>, OpenDALError> {
    if len > 0 && (keys.is_null() || values.is_null()) {
        return Err(config_invalid_error(
            "keys or values pointer is null while len > 0",
        ));
    }

    let mut options = HashMap::<String, String>::with_capacity(len);
    for index in 0..len {
        let key_ptr = unsafe { *keys.add(index) };
        let value_ptr = unsafe { *values.add(index) };

        let key = require_cstr_at(key_ptr, "key", index)?;
        let value = require_cstr_at(value_ptr, "value", index)?;
        options.insert(key.to_string(), value.to_string());
    }

    Ok(options)
}

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

/// Construct an OpenDAL operator instance from a scheme and key/value options.
///
/// Returns a pointer that must be released with `operator_free`.
/// # Safety
///
/// - `scheme` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `keys` and `values` must be non-null and point to arrays
///   of at least `len` entries.
/// - Every key/value entry in those arrays must be a valid null-terminated
///   UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_construct(
    scheme: *const c_char,
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalIntPtrResult {
    match unsafe { operator_construct_inner(scheme, keys, values, len) } {
        Ok(op) => ok_ptr_result(op),
        Err(error) => OpendalIntPtrResult::from_error(error),
    }
}

unsafe fn operator_construct_inner(
    scheme: *const c_char,
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> Result<*mut c_void, OpenDALError> {
    let scheme = require_cstr(scheme, "scheme")?;
    let options = unsafe { collect_options(keys, values, len) }?;
    let op = opendal::Operator::via_iter(scheme, options).map_err(from_opendal_error)?;
    Ok(Box::into_raw(Box::new(op)) as *mut c_void)
}

/// Release an operator created by `operator_construct`.
/// # Safety
///
/// - `op` must be either null or a pointer returned by `operator_construct`.
/// - The pointer must not be used after this call.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_free(op: *mut opendal::Operator) {
    if op.is_null() {
        return;
    }

    unsafe {
        drop(Box::from_raw(op));
    }
}

/// Write bytes to `path` synchronously.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write(
    op: *const opendal::Operator,
    path: *const c_char,
    data: *const u8,
    len: usize,
) -> OpendalResult {
    match unsafe { operator_write_inner(op, path, data, len) } {
        Ok(()) => ok_result(),
        Err(error) => OpendalResult::from_error(error),
    }
}

unsafe fn operator_write_inner(
    op: *const opendal::Operator,
    path: *const c_char,
    data: *const u8,
    len: usize,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let path = require_cstr(path, "path")?;
    require_data_ptr(data, len)?;

    let payload = if len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }
    };

    RUNTIME
        .block_on(op.write(path, payload))
        .map(|_| ())
        .map_err(from_opendal_error)
}

/// Read all bytes from `path` synchronously.
///
/// On success, the returned buffer must be released with `buffer_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_read(
    op: *const opendal::Operator,
    path: *const c_char,
) -> OpendalByteBufferResult {
    match unsafe { operator_read_inner(op, path) } {
        Ok(value) => ok_buffer_result(value),
        Err(error) => OpendalByteBufferResult::from_error(error),
    }
}

unsafe fn operator_read_inner(
    op: *const opendal::Operator,
    path: *const c_char,
) -> Result<ByteBuffer, OpenDALError> {
    let op = require_operator(op)?;
    let path = require_cstr(path, "path")?;
    let value = RUNTIME
        .block_on(op.read(path))
        .map(|v| v.to_vec())
        .map_err(from_opendal_error)?;

    Ok(ByteBuffer::from_vec(value))
}

/// Write bytes to `path` asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
/// - `callback` must be a valid function pointer and remain callable until it
///   is invoked.
/// - `context` is passed through as-is to `callback` and must remain valid for
///   the callback's usage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write_async(
    op: *const opendal::Operator,
    path: *const c_char,
    data: *const u8,
    len: usize,
    callback: Option<WriteCallback>,
    context: *mut c_void,
) -> OpendalResult {
    match unsafe { operator_write_async_inner(op, path, data, len, callback, context) } {
        Ok(()) => ok_result(),
        Err(error) => OpendalResult::from_error(error),
    }
}

unsafe fn operator_write_async_inner(
    op: *const opendal::Operator,
    path: *const c_char,
    data: *const u8,
    len: usize,
    callback: Option<WriteCallback>,
    context: *mut c_void,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let path = require_cstr(path, "path")?.to_string();
    require_data_ptr(data, len)?;
    let callback = require_callback(callback)?;

    let payload = if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
    };

    let op = op.clone();
    let context = context as usize;
    RUNTIME.spawn(async move {
        let result = op
            .write(&path, payload)
            .await
            .map(|_| ())
            .map_err(from_opendal_error);

        unsafe {
            callback(
                context as *mut c_void,
                match result {
                    Ok(()) => ok_result(),
                    Err(error) => OpendalResult::from_error(error),
                },
            );
        }
    });

    Ok(())
}

/// Read bytes from `path` asynchronously.
///
/// The callback is invoked exactly once. On successful reads, the returned
/// buffer in callback result must be released with `buffer_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until it
///   is invoked.
/// - `context` is passed through as-is to `callback` and must remain valid for
///   the callback's usage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_read_async(
    op: *const opendal::Operator,
    path: *const c_char,
    callback: Option<ReadCallback>,
    context: *mut c_void,
) -> OpendalResult {
    match unsafe { operator_read_async_inner(op, path, callback, context) } {
        Ok(()) => ok_result(),
        Err(error) => OpendalResult::from_error(error),
    }
}

unsafe fn operator_read_async_inner(
    op: *const opendal::Operator,
    path: *const c_char,
    callback: Option<ReadCallback>,
    context: *mut c_void,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;

    let op = op.clone();
    let context = context as usize;
    RUNTIME.spawn(async move {
        let result = op
            .read(&path)
            .await
            .map(|v| ByteBuffer::from_vec(v.to_vec()))
            .map_err(from_opendal_error);

        unsafe {
            callback(
                context as *mut c_void,
                match result {
                    Ok(value) => ok_buffer_result(value),
                    Err(error) => OpendalByteBufferResult::from_error(error),
                },
            );
        }
    });

    Ok(())
}
