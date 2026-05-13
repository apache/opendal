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
    entry::into_entry_list_ptr,
    error::OpenDALError,
    executor::executor_or_default,
    metadata::OpendalMetadata,
    operator_info::{OpendalOperatorInfo, into_operator_info},
    options::{parse_list_options, parse_read_options, parse_stat_options, parse_write_options},
    presign::into_presigned_request_ptr,
    result::{
        OpendalEntryListResult, OpendalMetadataResult, OpendalOperatorInfoResult,
        OpendalOperatorResult, OpendalOptionsResult, OpendalPresignedRequestResult,
        OpendalReadResult, OpendalResult,
    },
    utils::{collect_options, require_callback, require_cstr, require_data_ptr, require_operator},
    validators::prelude::{
        validate_concurrent_limit_options, validate_retry_options, validate_timeout_options,
    },
};

use std::collections::HashMap;
use std::ffi::c_void;
use std::os::raw::c_char;
use std::time::Duration;

/// Callback signature for async write completion.
///
/// The callback is provided by the .NET side and must remain valid until
/// invoked by Rust.
type WriteCallback = extern "C" fn(context: i64, result: OpendalResult);
type ReadCallback = extern "C" fn(context: i64, result: OpendalReadResult);
type StatCallback = extern "C" fn(context: i64, result: OpendalMetadataResult);
type ListCallback = extern "C" fn(context: i64, result: OpendalEntryListResult);
type PresignCallback = extern "C" fn(context: i64, result: OpendalPresignedRequestResult);

/// Build constructor options from raw C string key/value arrays.
///
/// On success, the returned pointer must be released by
/// `constructor_option_free`.
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each key/value entry must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn constructor_option_build(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalOptionsResult {
    match unsafe { collect_options(keys, values, len) } {
        Ok(options) => OpendalOptionsResult::ok(Box::into_raw(Box::new(options)) as *mut c_void),
        Err(error) => OpendalOptionsResult::from_error(error),
    }
}

/// # Safety
///
/// - `options` must be null or a pointer returned by
///   `constructor_option_build`.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub extern "C" fn constructor_option_free(options: *mut HashMap<String, String>) {
    if options.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(options));
    }
}

/// Build read options from raw C string key/value arrays.
///
/// On success, the returned pointer must be released by `read_option_free`.
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each key/value entry must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_option_build(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalOptionsResult {
    match unsafe { collect_options(keys, values, len) }
        .and_then(|values| parse_read_options(&values))
    {
        Ok(options) => OpendalOptionsResult::ok(Box::into_raw(Box::new(options)) as *mut c_void),
        Err(error) => OpendalOptionsResult::from_error(error),
    }
}

/// # Safety
///
/// - `options` must be null or a pointer returned by `read_option_build`.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read_option_free(options: *mut opendal::options::ReadOptions) {
    if options.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(options));
    }
}

/// Build write options from raw C string key/value arrays.
///
/// On success, the returned pointer must be released by `write_option_free`.
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each key/value entry must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn write_option_build(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalOptionsResult {
    match unsafe { collect_options(keys, values, len) }
        .and_then(|values| parse_write_options(&values))
    {
        Ok(options) => OpendalOptionsResult::ok(Box::into_raw(Box::new(options)) as *mut c_void),
        Err(error) => OpendalOptionsResult::from_error(error),
    }
}

/// # Safety
///
/// - `options` must be null or a pointer returned by `write_option_build`.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn write_option_free(options: *mut opendal::options::WriteOptions) {
    if options.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(options));
    }
}

/// Build stat options from raw C string key/value arrays.
///
/// On success, the returned pointer must be released by `stat_option_free`.
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each key/value entry must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stat_option_build(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalOptionsResult {
    match unsafe { collect_options(keys, values, len) }
        .and_then(|values| parse_stat_options(&values))
    {
        Ok(options) => OpendalOptionsResult::ok(Box::into_raw(Box::new(options)) as *mut c_void),
        Err(error) => OpendalOptionsResult::from_error(error),
    }
}

/// # Safety
///
/// - `options` must be null or a pointer returned by `stat_option_build`.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stat_option_free(options: *mut opendal::options::StatOptions) {
    if options.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(options));
    }
}

/// Build list options from raw C string key/value arrays.
///
/// On success, the returned pointer must be released by `list_option_free`.
/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each key/value entry must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn list_option_build(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> OpendalOptionsResult {
    match unsafe { collect_options(keys, values, len) }
        .and_then(|values| parse_list_options(&values))
    {
        Ok(options) => OpendalOptionsResult::ok(Box::into_raw(Box::new(options)) as *mut c_void),
        Err(error) => OpendalOptionsResult::from_error(error),
    }
}

/// # Safety
///
/// - `options` must be null or a pointer returned by `list_option_build`.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn list_option_free(options: *mut opendal::options::ListOptions) {
    if options.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(options));
    }
}

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
    options: *const HashMap<String, String>,
) -> OpendalOperatorResult {
    match operator_construct_inner(scheme, options) {
        Ok(op) => OpendalOperatorResult::ok(op),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_construct_inner(
    scheme: *const c_char,
    options: *const HashMap<String, String>,
) -> Result<*mut c_void, OpenDALError> {
    let scheme = require_cstr(scheme, "scheme")?;
    let options = if options.is_null() {
        HashMap::default()
    } else {
        unsafe { (&*options).clone() }
    };
    let op =
        opendal::Operator::via_iter(scheme, options).map_err(OpenDALError::from_opendal_error)?;
    Ok(Box::into_raw(Box::new(op)) as *mut c_void)
}

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

/// Get operator info payload.
///
/// On success, payload must be released by `opendal_operator_info_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_info_get(
    op: *const opendal::Operator,
) -> OpendalOperatorInfoResult {
    match operator_info_get_inner(op) {
        Ok(value) => OpendalOperatorInfoResult::ok(value),
        Err(error) => OpendalOperatorInfoResult::from_error(error),
    }
}

fn operator_info_get_inner(op: *const opendal::Operator) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    let info = into_operator_info(op.info());
    Ok(Box::into_raw(Box::new(info)) as *mut c_void)
}

/// # Safety
///
/// - `info` must be either null or a pointer returned by `operator_info_get`.
/// - The pointer must not be used after this call.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_info_free(info: *mut OpendalOperatorInfo) {
    if info.is_null() {
        return;
    }

    unsafe {
        let info = Box::from_raw(info);
        if !info.scheme.is_null() {
            drop(std::ffi::CString::from_raw(info.scheme));
        }
        if !info.root.is_null() {
            drop(std::ffi::CString::from_raw(info.root));
        }
        if !info.name.is_null() {
            drop(std::ffi::CString::from_raw(info.name));
        }
    }
}

/// Create a new operator layered with retry behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_layer_retry(
    op: *const opendal::Operator,
    jitter: bool,
    factor: f32,
    min_delay_nanos: u64,
    max_delay_nanos: u64,
    max_times: usize,
) -> OpendalOperatorResult {
    match operator_layer_retry_inner(
        op,
        jitter,
        factor,
        min_delay_nanos,
        max_delay_nanos,
        max_times,
    ) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_retry_inner(
    op: *const opendal::Operator,
    jitter: bool,
    factor: f32,
    min_delay_nanos: u64,
    max_delay_nanos: u64,
    max_times: usize,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    validate_retry_options(factor, min_delay_nanos, max_delay_nanos)?;

    let mut retry = opendal::layers::RetryLayer::new();
    retry = retry.with_factor(factor);
    retry = retry.with_min_delay(Duration::from_nanos(min_delay_nanos));
    retry = retry.with_max_delay(Duration::from_nanos(max_delay_nanos));
    retry = retry.with_max_times(max_times);
    if jitter {
        retry = retry.with_jitter();
    }

    Ok(Box::into_raw(Box::new(op.clone().layer(retry))) as *mut c_void)
}

/// Create a new operator layered with concurrent-limit behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_layer_concurrent_limit(
    op: *const opendal::Operator,
    permits: usize,
) -> OpendalOperatorResult {
    match operator_layer_concurrent_limit_inner(op, permits) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_concurrent_limit_inner(
    op: *const opendal::Operator,
    permits: usize,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    validate_concurrent_limit_options(permits)?;

    let concurrent_limit = opendal::layers::ConcurrentLimitLayer::new(permits);
    Ok(Box::into_raw(Box::new(op.clone().layer(concurrent_limit))) as *mut c_void)
}

/// Create a new operator layered with timeout behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_layer_timeout(
    op: *const opendal::Operator,
    timeout_nanos: u64,
    io_timeout_nanos: u64,
) -> OpendalOperatorResult {
    match operator_layer_timeout_inner(op, timeout_nanos, io_timeout_nanos) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_timeout_inner(
    op: *const opendal::Operator,
    timeout_nanos: u64,
    io_timeout_nanos: u64,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    validate_timeout_options(timeout_nanos, io_timeout_nanos)?;

    let timeout = opendal::layers::TimeoutLayer::new()
        .with_timeout(Duration::from_nanos(timeout_nanos))
        .with_io_timeout(Duration::from_nanos(io_timeout_nanos));

    Ok(Box::into_raw(Box::new(op.clone().layer(timeout))) as *mut c_void)
}

/// Duplicate an operator instance.
///
/// Returned pointer must be released with `operator_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_duplicate(op: *const opendal::Operator) -> OpendalOperatorResult {
    match operator_duplicate_inner(op) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_duplicate_inner(op: *const opendal::Operator) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    Ok(Box::into_raw(Box::new(op.clone())) as *mut c_void)
}

/// Delete `path` synchronously.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn operator_delete(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
) -> OpendalResult {
    match operator_delete_inner(op, executor, path) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_delete_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?;

    executor
        .block_on(op.delete(path))
        .map_err(OpenDALError::from_opendal_error)
}

/// Delete `path` asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_delete_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    callback: Option<WriteCallback>,
    context: i64,
) -> OpendalResult {
    match operator_delete_async_inner(op, executor, path, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_delete_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    callback: Option<WriteCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .delete(&path)
            .await
            .map_err(OpenDALError::from_opendal_error);

        callback(
            context,
            match result {
                Ok(()) => OpendalResult::ok(),
                Err(error) => OpendalResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Create directory at `path` synchronously.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn operator_create_dir(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
) -> OpendalResult {
    match operator_create_dir_inner(op, executor, path) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_create_dir_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?;

    executor
        .block_on(op.create_dir(path))
        .map_err(OpenDALError::from_opendal_error)
}

/// Create directory at `path` asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_create_dir_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    callback: Option<WriteCallback>,
    context: i64,
) -> OpendalResult {
    match operator_create_dir_async_inner(op, executor, path, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_create_dir_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    callback: Option<WriteCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .create_dir(&path)
            .await
            .map_err(OpenDALError::from_opendal_error);

        callback(
            context,
            match result {
                Ok(()) => OpendalResult::ok(),
                Err(error) => OpendalResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Copy from `source_path` to `target_path` synchronously.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `source_path` and `target_path` must be valid null-terminated UTF-8 strings.
#[unsafe(no_mangle)]
pub extern "C" fn operator_copy(
    op: *const opendal::Operator,
    executor: *const c_void,
    source_path: *const c_char,
    target_path: *const c_char,
) -> OpendalResult {
    match operator_copy_inner(op, executor, source_path, target_path) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_copy_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    source_path: *const c_char,
    target_path: *const c_char,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let source_path = require_cstr(source_path, "source_path")?;
    let target_path = require_cstr(target_path, "target_path")?;

    executor
        .block_on(op.copy(source_path, target_path))
        .map_err(OpenDALError::from_opendal_error)
}

/// Copy from `source_path` to `target_path` asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `source_path` and `target_path` must be valid null-terminated UTF-8 strings.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_copy_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    source_path: *const c_char,
    target_path: *const c_char,
    callback: Option<WriteCallback>,
    context: i64,
) -> OpendalResult {
    match operator_copy_async_inner(op, executor, source_path, target_path, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_copy_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    source_path: *const c_char,
    target_path: *const c_char,
    callback: Option<WriteCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let source_path = require_cstr(source_path, "source_path")?.to_string();
    let target_path = require_cstr(target_path, "target_path")?.to_string();
    let callback = require_callback(callback)?;

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .copy(&source_path, &target_path)
            .await
            .map_err(OpenDALError::from_opendal_error);

        callback(
            context,
            match result {
                Ok(()) => OpendalResult::ok(),
                Err(error) => OpendalResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Rename from `source_path` to `target_path` synchronously.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `source_path` and `target_path` must be valid null-terminated UTF-8 strings.
#[unsafe(no_mangle)]
pub extern "C" fn operator_rename(
    op: *const opendal::Operator,
    executor: *const c_void,
    source_path: *const c_char,
    target_path: *const c_char,
) -> OpendalResult {
    match operator_rename_inner(op, executor, source_path, target_path) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_rename_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    source_path: *const c_char,
    target_path: *const c_char,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let source_path = require_cstr(source_path, "source_path")?;
    let target_path = require_cstr(target_path, "target_path")?;

    executor
        .block_on(op.rename(source_path, target_path))
        .map_err(OpenDALError::from_opendal_error)
}

/// Rename from `source_path` to `target_path` asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `source_path` and `target_path` must be valid null-terminated UTF-8 strings.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_rename_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    source_path: *const c_char,
    target_path: *const c_char,
    callback: Option<WriteCallback>,
    context: i64,
) -> OpendalResult {
    match operator_rename_async_inner(op, executor, source_path, target_path, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_rename_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    source_path: *const c_char,
    target_path: *const c_char,
    callback: Option<WriteCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let source_path = require_cstr(source_path, "source_path")?.to_string();
    let target_path = require_cstr(target_path, "target_path")?.to_string();
    let callback = require_callback(callback)?;

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .rename(&source_path, &target_path)
            .await
            .map_err(OpenDALError::from_opendal_error);

        callback(
            context,
            match result {
                Ok(()) => OpendalResult::ok(),
                Err(error) => OpendalResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Remove `path` recursively synchronously.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn operator_remove_all(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
) -> OpendalResult {
    match operator_remove_all_inner(op, executor, path) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_remove_all_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?;

    let options = opendal::options::DeleteOptions {
        recursive: true,
        ..Default::default()
    };

    executor
        .block_on(op.delete_options(path, options))
        .map_err(OpenDALError::from_opendal_error)
}

/// Remove `path` recursively asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_remove_all_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    callback: Option<WriteCallback>,
    context: i64,
) -> OpendalResult {
    match operator_remove_all_async_inner(op, executor, path, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_remove_all_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    callback: Option<WriteCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = opendal::options::DeleteOptions {
        recursive: true,
        ..Default::default()
    };

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .delete_options(&path, options)
            .await
            .map_err(OpenDALError::from_opendal_error);

        callback(
            context,
            match result {
                Ok(()) => OpendalResult::ok(),
                Err(error) => OpendalResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Presign read asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_presign_read_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> OpendalResult {
    match operator_presign_read_async_inner(op, executor, path, expire_nanos, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_presign_read_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let expire = Duration::from_nanos(expire_nanos);

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .presign_read(&path, expire)
            .await
            .map_err(OpenDALError::from_opendal_error)
            .and_then(into_presigned_request_ptr);

        callback(
            context,
            match result {
                Ok(value) => OpendalPresignedRequestResult::ok(value),
                Err(error) => OpendalPresignedRequestResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Presign write asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_presign_write_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> OpendalResult {
    match operator_presign_write_async_inner(op, executor, path, expire_nanos, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_presign_write_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let expire = Duration::from_nanos(expire_nanos);

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .presign_write(&path, expire)
            .await
            .map_err(OpenDALError::from_opendal_error)
            .and_then(into_presigned_request_ptr);

        callback(
            context,
            match result {
                Ok(value) => OpendalPresignedRequestResult::ok(value),
                Err(error) => OpendalPresignedRequestResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Presign stat asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_presign_stat_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> OpendalResult {
    match operator_presign_stat_async_inner(op, executor, path, expire_nanos, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_presign_stat_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let expire = Duration::from_nanos(expire_nanos);

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .presign_stat(&path, expire)
            .await
            .map_err(OpenDALError::from_opendal_error)
            .and_then(into_presigned_request_ptr);

        callback(
            context,
            match result {
                Ok(value) => OpendalPresignedRequestResult::ok(value),
                Err(error) => OpendalPresignedRequestResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Presign delete asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_presign_delete_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> OpendalResult {
    match operator_presign_delete_async_inner(op, executor, path, expire_nanos, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_presign_delete_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let expire = Duration::from_nanos(expire_nanos);

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .presign_delete(&path, expire)
            .await
            .map_err(OpenDALError::from_opendal_error)
            .and_then(into_presigned_request_ptr);

        callback(
            context,
            match result {
                Ok(value) => OpendalPresignedRequestResult::ok(value),
                Err(error) => OpendalPresignedRequestResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Create an input stream for `path` with read options.
///
/// Returned pointer must be released by `operator_input_stream_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn operator_input_stream_create(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ReadOptions,
) -> OpendalOperatorResult {
    match operator_input_stream_create_inner(op, executor, path, options) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_input_stream_create_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ReadOptions,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let options = if options.is_null() {
        opendal::options::ReadOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let _guard = executor.enter();

    let blocking_op =
        opendal::blocking::Operator::new(op.clone()).map_err(OpenDALError::from_opendal_error)?;
    let reader = blocking_op
        .reader_options(&path, opendal::options::ReaderOptions::default())
        .map_err(OpenDALError::from_opendal_error)?;
    let stream = reader
        .into_bytes_iterator(options.range.to_range())
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(Box::into_raw(Box::new(stream)) as *mut c_void)
}

/// Read next bytes chunk from input stream.
///
/// Returns an empty buffer when EOF is reached.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_input_stream_create`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_input_stream_read_next(
    stream: *mut opendal::blocking::StdBytesIterator,
) -> OpendalReadResult {
    match operator_input_stream_read_next_inner(stream) {
        Ok(buffer) => OpendalReadResult::ok(buffer),
        Err(error) => OpendalReadResult::from_error(error),
    }
}

fn operator_input_stream_read_next_inner(
    stream: *mut opendal::blocking::StdBytesIterator,
) -> Result<ByteBuffer, OpenDALError> {
    if stream.is_null() {
        return Err(crate::utils::config_invalid_error(
            "input stream pointer is null",
        ));
    }

    let stream = unsafe { &mut *stream };

    let value = stream
        .next()
        .transpose()
        .map_err(|err| {
            OpenDALError::from_opendal_error(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                err.to_string(),
            ))
        })?
        .map(|v| ByteBuffer::from_vec(v.to_vec()))
        .unwrap_or_else(ByteBuffer::empty);

    Ok(value)
}

/// # Safety
///
/// - `stream` must be null or a pointer returned by `operator_input_stream_create`.
/// - Must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_input_stream_free(
    stream: *mut opendal::blocking::StdBytesIterator,
) {
    if stream.is_null() {
        return;
    }

    unsafe {
        drop(Box::from_raw(stream));
    }
}

/// Create an output stream for `path` with write options.
///
/// Returned pointer must be released by `operator_output_stream_free`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn operator_output_stream_create(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::WriteOptions,
) -> OpendalOperatorResult {
    match operator_output_stream_create_inner(op, executor, path, options) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_output_stream_create_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::WriteOptions,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let options = if options.is_null() {
        opendal::options::WriteOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let _guard = executor.enter();

    let blocking_op =
        opendal::blocking::Operator::new(op.clone()).map_err(OpenDALError::from_opendal_error)?;
    let stream = blocking_op
        .writer_options(&path, options)
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(Box::into_raw(Box::new(stream)) as *mut c_void)
}

/// Write bytes to output stream.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_output_stream_create`.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
#[unsafe(no_mangle)]
pub extern "C" fn operator_output_stream_write(
    stream: *mut opendal::blocking::Writer,
    data: *const u8,
    len: usize,
) -> OpendalResult {
    match operator_output_stream_write_inner(stream, data, len) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_output_stream_write_inner(
    stream: *mut opendal::blocking::Writer,
    data: *const u8,
    len: usize,
) -> Result<(), OpenDALError> {
    if stream.is_null() {
        return Err(crate::utils::config_invalid_error(
            "output stream pointer is null",
        ));
    }
    require_data_ptr(data, len)?;

    let stream = unsafe { &mut *stream };
    let payload = if len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }
    };

    stream
        .write(payload)
        .map(|_| ())
        .map_err(OpenDALError::from_opendal_error)
}

/// Flush output stream.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_output_stream_create`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_output_stream_flush(
    stream: *mut opendal::blocking::Writer,
) -> OpendalResult {
    match operator_output_stream_flush_inner(stream) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_output_stream_flush_inner(
    stream: *mut opendal::blocking::Writer,
) -> Result<(), OpenDALError> {
    if stream.is_null() {
        return Err(crate::utils::config_invalid_error(
            "output stream pointer is null",
        ));
    }

    Ok(())
}

/// Close output stream.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_output_stream_create`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_output_stream_close(
    stream: *mut opendal::blocking::Writer,
) -> OpendalResult {
    match operator_output_stream_close_inner(stream) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_output_stream_close_inner(
    stream: *mut opendal::blocking::Writer,
) -> Result<(), OpenDALError> {
    if stream.is_null() {
        return Err(crate::utils::config_invalid_error(
            "output stream pointer is null",
        ));
    }

    let stream = unsafe { &mut *stream };
    stream
        .close()
        .map(|_| ())
        .map_err(OpenDALError::from_opendal_error)
}

/// # Safety
///
/// - `stream` must be null or a pointer returned by `operator_output_stream_create`.
/// - Must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_output_stream_free(stream: *mut opendal::blocking::Writer) {
    if stream.is_null() {
        return;
    }

    unsafe {
        drop(Box::from_raw(stream));
    }
}

/// Write bytes to `path` synchronously with options.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
#[unsafe(no_mangle)]
pub extern "C" fn operator_write_with_options(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
    options: *const opendal::options::WriteOptions,
) -> OpendalResult {
    match operator_write_with_options_inner(op, executor, path, data, len, options) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_write_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: *const u8,
    len: usize,
    options: *const opendal::options::WriteOptions,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?;
    require_data_ptr(data, len)?;
    let options = if options.is_null() {
        opendal::options::WriteOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let payload = if len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }
    };

    executor
        .block_on(op.write_options(path, payload, options))
        .map(|_| ())
        .map_err(OpenDALError::from_opendal_error)
}

/// Write bytes to `path` asynchronously with options.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `data.data` must be non-null and readable for `data.len` bytes when `data.len > 0`.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write_with_options_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: ByteBuffer,
    options: *const opendal::options::WriteOptions,
    callback: Option<WriteCallback>,
    context: i64,
) -> OpendalResult {
    match operator_write_with_options_async_inner(
        op, executor, path, data, options, callback, context,
    ) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_write_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    data: ByteBuffer,
    options: *const opendal::options::WriteOptions,
    callback: Option<WriteCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    require_data_ptr(data.data.cast_const(), data.len)?;
    let callback = require_callback(callback)?;
    let options = if options.is_null() {
        opendal::options::WriteOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let payload = if data.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data.data.cast_const(), data.len) }.to_vec()
    };

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .write_options(&path, payload, options)
            .await
            .map(|_| ())
            .map_err(OpenDALError::from_opendal_error);

        callback(
            context,
            match result {
                Ok(()) => OpendalResult::ok(),
                Err(error) => OpendalResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Read bytes from `path` synchronously with options.
///
/// On success, the returned buffer must be released with `opendal_read_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_read_with_options(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ReadOptions,
) -> OpendalReadResult {
    match operator_read_with_options_inner(op, executor, path, options) {
        Ok(value) => OpendalReadResult::ok(value),
        Err(error) => OpendalReadResult::from_error(error),
    }
}

fn operator_read_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ReadOptions,
) -> Result<ByteBuffer, OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?;
    let options = if options.is_null() {
        opendal::options::ReadOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let value = executor
        .block_on(op.read_options(path, options))
        .map(|v| v.to_vec())
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(ByteBuffer::from_vec(value))
}

/// Read bytes from `path` asynchronously with options.
///
/// The callback is invoked exactly once. On successful reads, the callback
/// result must be released with `opendal_read_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_read_with_options_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ReadOptions,
    callback: Option<ReadCallback>,
    context: i64,
) -> OpendalResult {
    match operator_read_with_options_async_inner(op, executor, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_read_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ReadOptions,
    callback: Option<ReadCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = if options.is_null() {
        opendal::options::ReadOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .read_options(&path, options)
            .await
            .map(|v| ByteBuffer::from_vec(v.to_vec()))
            .map_err(OpenDALError::from_opendal_error);

        callback(
            context,
            match result {
                Ok(value) => OpendalReadResult::ok(value),
                Err(error) => OpendalReadResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Stat `path` synchronously with options.
///
/// On success, returned payload must be released with `opendal_metadata_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
#[unsafe(no_mangle)]
pub extern "C" fn operator_stat_with_options(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::StatOptions,
) -> OpendalMetadataResult {
    match operator_stat_with_options_inner(op, executor, path, options) {
        Ok(value) => OpendalMetadataResult::ok(value as *mut c_void),
        Err(error) => OpendalMetadataResult::from_error(error),
    }
}

fn operator_stat_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::StatOptions,
) -> Result<*mut OpendalMetadata, OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?;
    let options = if options.is_null() {
        opendal::options::StatOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let metadata = executor
        .block_on(op.stat_options(path, options))
        .map_err(OpenDALError::from_opendal_error)?;
    Ok(Box::into_raw(Box::new(OpendalMetadata::from_metadata(
        metadata,
    ))))
}

/// Stat `path` asynchronously with options.
///
/// The callback is invoked exactly once. On success, callback result must be
/// released with `opendal_metadata_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_stat_with_options_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::StatOptions,
    callback: Option<StatCallback>,
    context: i64,
) -> OpendalResult {
    match operator_stat_with_options_async_inner(op, executor, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_stat_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::StatOptions,
    callback: Option<StatCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = if options.is_null() {
        opendal::options::StatOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .stat_options(&path, options)
            .await
            .map(OpendalMetadata::from_metadata)
            .map(|v| Box::into_raw(Box::new(v)))
            .map_err(OpenDALError::from_opendal_error);

        callback(
            context,
            match result {
                Ok(value) => OpendalMetadataResult::ok(value as *mut c_void),
                Err(error) => OpendalMetadataResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// List entries from `path` synchronously with options.
///
/// On success, returned payload must be released with `opendal_entry_list_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
#[unsafe(no_mangle)]
pub extern "C" fn operator_list_with_options(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ListOptions,
) -> OpendalEntryListResult {
    match operator_list_with_options_inner(op, executor, path, options) {
        Ok(value) => OpendalEntryListResult::ok(value),
        Err(error) => OpendalEntryListResult::from_error(error),
    }
}

fn operator_list_with_options_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ListOptions,
) -> Result<*mut c_void, OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?;
    let options = if options.is_null() {
        opendal::options::ListOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let entries = executor
        .block_on(op.list_options(path, options))
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(into_entry_list_ptr(entries))
}

/// List entries from `path` asynchronously with options.
///
/// The callback is invoked exactly once. On success, callback result must be
/// released with `opendal_entry_list_result_release`.
/// # Safety
///
/// - `op` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid arrays.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_list_with_options_async(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ListOptions,
    callback: Option<ListCallback>,
    context: i64,
) -> OpendalResult {
    match operator_list_with_options_async_inner(op, executor, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_list_with_options_async_inner(
    op: *const opendal::Operator,
    executor: *const c_void,
    path: *const c_char,
    options: *const opendal::options::ListOptions,
    callback: Option<ListCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let op = require_operator(op)?;
    let executor = executor_or_default(executor)?;
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = if options.is_null() {
        opendal::options::ListOptions::default()
    } else {
        unsafe { (&*options).clone() }
    };

    let op = op.clone();
    executor.spawn(async move {
        let result = op
            .list_options(&path, options)
            .await
            .map(into_entry_list_ptr)
            .map_err(OpenDALError::from_opendal_error);

        callback(
            context,
            match result {
                Ok(value) => OpendalEntryListResult::ok(value),
                Err(error) => OpendalEntryListResult::from_error(error),
            },
        );
    });

    Ok(())
}
