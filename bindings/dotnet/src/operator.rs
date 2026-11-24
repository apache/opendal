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
    buffer::{OpendalReadBuffer, WriteBufferSlot, take_payload},
    entry::into_entry_list_ptr,
    error::{ErrorCode, OpenDALError},
    executor::executor_or_default,
    metadata::OpendalMetadata,
    operator_info::{OpendalOperatorInfo, into_operator_info},
    options::{
        OpendalComposeOptions, OpendalComposeSourceOptions, OpendalCopyOptions,
        OpendalDeleteOptions, OpendalListOptions, OpendalPathOptions, OpendalReadOptions,
        OpendalRenameOptions, OpendalRestoreOptions, OpendalStatOptions, OpendalWriteOptions,
        read_options, read_path_options,
    },
    presign::into_presigned_request_ptr,
    result::{
        OpendalBoolResult, OpendalEntryListResult, OpendalMetadataResult,
        OpendalOperatorInfoResult, OpendalOperatorResult, OpendalPresignedRequestResult,
        OpendalReadResult, OpendalResult,
    },
    utils::{
        borrowed_string_map, release_c_string, require_callback, require_cstr, require_data_ptr,
        require_op_handle,
    },
    validators::prelude::{
        validate_concurrent_limit_options, validate_retry_options, validate_throttle_options,
        validate_timeout_options,
    },
};

use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::Arc;
use std::time::Duration;

use asyncband::mutex::Mutex;
use futures::StreamExt;

use crate::executor::Executor;

/// Box metadata into the payload of an `OpendalMetadataResult`.
///
/// The pointer is released by `opendal_metadata_result_release`.
fn into_metadata_ptr(metadata: opendal::Metadata) -> *mut OpendalMetadata {
    Box::into_raw(Box::new(OpendalMetadata::from_metadata(metadata)))
}

/// The operator handle handed to .NET: the opendal operator plus the executor
/// it was bound to at construction. Every operation runs on that executor, and
/// the owned `Arc` keeps the runtime alive for as long as the operator lives.
pub(crate) struct OperatorHandle {
    op: opendal::Operator,
    executor: Arc<Executor>,
}

impl OperatorHandle {
    /// Build a handle around a derived operator (layered or duplicated), keeping the same executor.
    fn with_operator(&self, op: opendal::Operator) -> OperatorHandle {
        OperatorHandle {
            op,
            executor: self.executor.clone(),
        }
    }

    /// Clone the inner opendal operator into an owned value, e.g. to move it into a spawned task.
    fn operator(&self) -> opendal::Operator {
        self.op.clone()
    }
}

impl std::ops::Deref for OperatorHandle {
    type Target = opendal::Operator;

    fn deref(&self) -> &opendal::Operator {
        &self.op
    }
}

/// Callback signatures for async completion.
///
/// The callbacks are provided by the .NET side and must remain valid until
/// invoked by Rust. `VoidCallback` reports success or failure only, while
/// `MetadataCallback` carries the metadata that write, copy, and stat return.
type VoidCallback = extern "C" fn(context: i64, result: OpendalResult);
type BoolCallback = extern "C" fn(context: i64, result: OpendalBoolResult);
type ReadCallback = extern "C" fn(context: i64, result: OpendalReadResult);
type MetadataCallback = extern "C" fn(context: i64, result: OpendalMetadataResult);
type ListCallback = extern "C" fn(context: i64, result: OpendalEntryListResult);
type PresignCallback = extern "C" fn(context: i64, result: OpendalPresignedRequestResult);

/// Construct an OpenDAL operator instance from a scheme and key/value options.
///
/// Returns a pointer that must be released with `operator_free`.
/// # Safety
///
/// - `scheme` must be a valid null-terminated UTF-8 string.
/// - When `option_len > 0`, `option_keys` and `option_values` must be valid
///   arrays of `option_len` null-terminated UTF-8 strings; a null value is
///   reported by key.
/// - `executor` must be either null or a live pointer returned by
///   `executor_create`. The operator binds it for its whole lifetime.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_construct(
    scheme: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
    executor: *const c_void,
) -> OpendalOperatorResult {
    match operator_construct_inner(scheme, option_keys, option_values, option_len, executor) {
        Ok(op) => OpendalOperatorResult::ok(op),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_construct_inner(
    scheme: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    option_len: usize,
    executor: *const c_void,
) -> Result<*mut c_void, OpenDALError> {
    let scheme = require_cstr(scheme, "scheme")?;
    let options = borrowed_string_map(option_keys, option_values, option_len, "option")?;
    // SAFETY: the caller keeps the executor handle alive across this call per
    // `operator_construct`'s contract; the clone owns the runtime afterwards.
    let executor = unsafe { executor_or_default(executor)? };
    let op =
        opendal::Operator::via_iter(scheme, options).map_err(OpenDALError::from_opendal_error)?;
    Ok(Box::into_raw(Box::new(OperatorHandle { op, executor })) as *mut c_void)
}

/// # Safety
///
/// - `op_handle` must be either null or a pointer returned by `operator_construct`.
/// - The pointer must not be used after this call.
/// - This function must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_free(op_handle: *mut OperatorHandle) {
    if op_handle.is_null() {
        return;
    }

    unsafe {
        drop(Box::from_raw(op_handle));
    }
}

/// Get operator info payload.
///
/// On success, payload must be released by `opendal_operator_info_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_info_get(
    op_handle: *const OperatorHandle,
) -> OpendalOperatorInfoResult {
    match operator_info_get_inner(op_handle) {
        Ok(value) => OpendalOperatorInfoResult::ok(value),
        Err(error) => OpendalOperatorInfoResult::from_error(error),
    }
}

fn operator_info_get_inner(op_handle: *const OperatorHandle) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let info = into_operator_info(handle.info());
    Ok(Box::into_raw(Box::new(info)) as *mut c_void)
}

/// # Safety
///
/// - `info` must be either null or a pointer returned by `operator_info_get`.
/// - The pointer must not be used after this call.
/// - This function must be called at most once for the same pointer.
pub(crate) unsafe fn operator_info_free(info: *mut OpendalOperatorInfo) {
    if info.is_null() {
        return;
    }

    unsafe {
        let mut info = Box::from_raw(info);
        release_c_string(&mut info.scheme);
        release_c_string(&mut info.root);
        release_c_string(&mut info.name);
    }
}

/// Create a new operator layered with retry behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_layer_retry(
    op_handle: *const OperatorHandle,
    jitter: bool,
    factor: f32,
    min_delay_nanos: u64,
    max_delay_nanos: u64,
    max_times: usize,
) -> OpendalOperatorResult {
    match operator_layer_retry_inner(
        op_handle,
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
    op_handle: *const OperatorHandle,
    jitter: bool,
    factor: f32,
    min_delay_nanos: u64,
    max_delay_nanos: u64,
    max_times: usize,
) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    validate_retry_options(factor, min_delay_nanos, max_delay_nanos)?;

    let mut retry = opendal::layers::RetryLayer::new();
    retry = retry.with_factor(factor);
    retry = retry.with_min_delay(Duration::from_nanos(min_delay_nanos));
    retry = retry.with_max_delay(Duration::from_nanos(max_delay_nanos));
    retry = retry.with_max_times(max_times);
    if jitter {
        retry = retry.with_jitter();
    }

    Ok(Box::into_raw(Box::new(
        handle.with_operator(handle.operator().layer(retry)),
    )) as *mut c_void)
}

/// Create a new operator layered with concurrent-limit behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
///
/// `http_permits` limits concurrent HTTP requests independently of `permits`,
/// which limits concurrent operations. It is read only when `has_http_permits`
/// is true; otherwise the HTTP limit is left unset.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_layer_concurrent_limit(
    op_handle: *const OperatorHandle,
    permits: usize,
    http_permits: usize,
    has_http_permits: bool,
) -> OpendalOperatorResult {
    let http_permits = has_http_permits.then_some(http_permits);
    match operator_layer_concurrent_limit_inner(op_handle, permits, http_permits) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_concurrent_limit_inner(
    op_handle: *const OperatorHandle,
    permits: usize,
    http_permits: Option<usize>,
) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    validate_concurrent_limit_options(permits, http_permits)?;

    let mut concurrent_limit = opendal::layers::ConcurrentLimitLayer::new(permits);
    if let Some(http_permits) = http_permits {
        concurrent_limit = concurrent_limit.with_http_concurrent_limit(http_permits);
    }

    Ok(Box::into_raw(Box::new(
        handle.with_operator(handle.operator().layer(concurrent_limit)),
    )) as *mut c_void)
}

/// Create a new operator layered with capability override behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `overrides` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn operator_layer_capability_override(
    op_handle: *const OperatorHandle,
    overrides: *const c_char,
) -> OpendalOperatorResult {
    match operator_layer_capability_override_inner(op_handle, overrides) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_capability_override_inner(
    op_handle: *const OperatorHandle,
    overrides: *const c_char,
) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let overrides = require_cstr(overrides, "capability overrides")?;
    let layer = opendal::layers::CapabilityOverrideLayer::from_overrides(overrides)
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(Box::into_raw(Box::new(
        handle.with_operator(handle.operator().layer(layer)),
    )) as *mut c_void)
}

/// Create a new operator layered with timeout behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_layer_timeout(
    op_handle: *const OperatorHandle,
    timeout_nanos: u64,
    io_timeout_nanos: u64,
) -> OpendalOperatorResult {
    match operator_layer_timeout_inner(op_handle, timeout_nanos, io_timeout_nanos) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_timeout_inner(
    op_handle: *const OperatorHandle,
    timeout_nanos: u64,
    io_timeout_nanos: u64,
) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    validate_timeout_options(timeout_nanos, io_timeout_nanos)?;

    let timeout = opendal::layers::TimeoutLayer::new()
        .with_timeout(Duration::from_nanos(timeout_nanos))
        .with_io_timeout(Duration::from_nanos(io_timeout_nanos));

    Ok(Box::into_raw(Box::new(
        handle.with_operator(handle.operator().layer(timeout)),
    )) as *mut c_void)
}

/// Create a new operator layered with throttle behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
///
/// `bandwidth` is the maximum number of bytes allowed through per second, and
/// `burst` is the maximum number of bytes allowed through at once. `burst` must
/// exceed the largest possible operation size, otherwise that operation can
/// never acquire enough quota to proceed.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_layer_throttle(
    op_handle: *const OperatorHandle,
    bandwidth: u32,
    burst: u32,
) -> OpendalOperatorResult {
    match operator_layer_throttle_inner(op_handle, bandwidth, burst) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_throttle_inner(
    op_handle: *const OperatorHandle,
    bandwidth: u32,
    burst: u32,
) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    validate_throttle_options(bandwidth, burst)?;

    let throttle = opendal::layers::ThrottleLayer::new(bandwidth, burst);
    Ok(Box::into_raw(Box::new(
        handle.with_operator(handle.operator().layer(throttle)),
    )) as *mut c_void)
}

/// Create a new operator layered with MIME-guess behavior.
///
/// The current operator is not modified. Returned pointer must be released with
/// `operator_free`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_layer_mime_guess(
    op_handle: *const OperatorHandle,
) -> OpendalOperatorResult {
    match operator_layer_mime_guess_inner(op_handle) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_layer_mime_guess_inner(
    op_handle: *const OperatorHandle,
) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;

    let mime_guess = opendal::layers::MimeGuessLayer::default();
    Ok(Box::into_raw(Box::new(
        handle.with_operator(handle.operator().layer(mime_guess)),
    )) as *mut c_void)
}

/// Duplicate an operator instance.
///
/// Returned pointer must be released with `operator_free`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_duplicate(op_handle: *const OperatorHandle) -> OpendalOperatorResult {
    match operator_duplicate_inner(op_handle) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

fn operator_duplicate_inner(op_handle: *const OperatorHandle) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    Ok(Box::into_raw(Box::new(handle.with_operator(handle.operator()))) as *mut c_void)
}

/// Delete `path` synchronously with options.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalDeleteOptions` whose
///   pointers stay valid for the duration of the call.
#[unsafe(no_mangle)]
pub extern "C" fn operator_delete_with_options(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalDeleteOptions,
) -> OpendalResult {
    match operator_delete_with_options_inner(op_handle, path, options) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_delete_with_options_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalDeleteOptions,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?;

    let options = read_options(options)?;

    executor
        .block_on(handle.delete_options(path, options))
        .map_err(OpenDALError::from_opendal_error)
}

/// Delete `path` asynchronously with options.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalDeleteOptions` whose
///   pointers stay valid for the duration of the call.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_delete_with_options_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalDeleteOptions,
    callback: Option<VoidCallback>,
    context: i64,
) -> OpendalResult {
    match operator_delete_with_options_async_inner(op_handle, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_delete_with_options_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalDeleteOptions,
    callback: Option<VoidCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;

    let options = read_options(options)?;

    let op = handle.operator();
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

/// Create directory at `path` synchronously.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn operator_create_dir(
    op_handle: *const OperatorHandle,
    path: *const c_char,
) -> OpendalResult {
    match operator_create_dir_inner(op_handle, path) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_create_dir_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?;

    executor
        .block_on(handle.create_dir(path))
        .map_err(OpenDALError::from_opendal_error)
}

/// Create directory at `path` asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_create_dir_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    callback: Option<VoidCallback>,
    context: i64,
) -> OpendalResult {
    match operator_create_dir_async_inner(op_handle, path, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_create_dir_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    callback: Option<VoidCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;

    let op = handle.operator();
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

/// Copy from `source_path` to `target_path` synchronously with options.
///
/// On success, the returned payload must be released with
/// `opendal_metadata_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `source_path` and `target_path` must be valid null-terminated UTF-8 strings.
/// - `options` must be null or point to a valid `OpendalCopyOptions` whose
///   pointers stay valid for the duration of the call.
#[unsafe(no_mangle)]
pub extern "C" fn operator_copy_with_options(
    op_handle: *const OperatorHandle,
    source_path: *const c_char,
    target_path: *const c_char,
    options: *const OpendalCopyOptions,
) -> OpendalMetadataResult {
    match operator_copy_with_options_inner(op_handle, source_path, target_path, options) {
        Ok(value) => OpendalMetadataResult::ok(value as *mut c_void),
        Err(error) => OpendalMetadataResult::from_error(error),
    }
}

fn operator_copy_with_options_inner(
    op_handle: *const OperatorHandle,
    source_path: *const c_char,
    target_path: *const c_char,
    options: *const OpendalCopyOptions,
) -> Result<*mut OpendalMetadata, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let source_path = require_cstr(source_path, "source_path")?;
    let target_path = require_cstr(target_path, "target_path")?;
    let options = read_options(options)?;

    let metadata = executor
        .block_on(handle.copy_options(source_path, target_path, options))
        .map_err(OpenDALError::from_opendal_error)?;
    Ok(into_metadata_ptr(metadata))
}

/// Copy from `source_path` to `target_path` asynchronously with options.
///
/// The callback is invoked exactly once. On success, the callback result must
/// be released with `opendal_metadata_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `source_path` and `target_path` must be valid null-terminated UTF-8 strings.
/// - `options` must be null or point to a valid `OpendalCopyOptions` whose
///   pointers stay valid for the duration of the call.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_copy_with_options_async(
    op_handle: *const OperatorHandle,
    source_path: *const c_char,
    target_path: *const c_char,
    options: *const OpendalCopyOptions,
    callback: Option<MetadataCallback>,
    context: i64,
) -> OpendalResult {
    match operator_copy_with_options_async_inner(
        op_handle,
        source_path,
        target_path,
        options,
        callback,
        context,
    ) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_copy_with_options_async_inner(
    op_handle: *const OperatorHandle,
    source_path: *const c_char,
    target_path: *const c_char,
    options: *const OpendalCopyOptions,
    callback: Option<MetadataCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let source_path = require_cstr(source_path, "source_path")?.to_string();
    let target_path = require_cstr(target_path, "target_path")?.to_string();
    let callback = require_callback(callback)?;
    let options = read_options(options)?;

    let op = handle.operator();
    executor.spawn(async move {
        let result = op
            .copy_options(&source_path, &target_path, options)
            .await
            .map(into_metadata_ptr)
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

/// Rename from `source_path` to `target_path` synchronously with options.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `source_path` and `target_path` must be valid null-terminated UTF-8 strings.
/// - `options` must be null or point to a valid `OpendalRenameOptions` whose
///   pointers stay valid for the duration of the call.
#[unsafe(no_mangle)]
pub extern "C" fn operator_rename_with_options(
    op_handle: *const OperatorHandle,
    source_path: *const c_char,
    target_path: *const c_char,
    options: *const OpendalRenameOptions,
) -> OpendalResult {
    match operator_rename_with_options_inner(op_handle, source_path, target_path, options) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_rename_with_options_inner(
    op_handle: *const OperatorHandle,
    source_path: *const c_char,
    target_path: *const c_char,
    options: *const OpendalRenameOptions,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let source_path = require_cstr(source_path, "source_path")?;
    let target_path = require_cstr(target_path, "target_path")?;
    let options = read_options(options)?;

    executor
        .block_on(handle.rename_options(source_path, target_path, options))
        .map_err(OpenDALError::from_opendal_error)
}

/// Rename from `source_path` to `target_path` asynchronously with options.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `source_path` and `target_path` must be valid null-terminated UTF-8 strings.
/// - `options` must be null or point to a valid `OpendalRenameOptions` whose
///   pointers stay valid for the duration of the call.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_rename_with_options_async(
    op_handle: *const OperatorHandle,
    source_path: *const c_char,
    target_path: *const c_char,
    options: *const OpendalRenameOptions,
    callback: Option<VoidCallback>,
    context: i64,
) -> OpendalResult {
    match operator_rename_with_options_async_inner(
        op_handle,
        source_path,
        target_path,
        options,
        callback,
        context,
    ) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_rename_with_options_async_inner(
    op_handle: *const OperatorHandle,
    source_path: *const c_char,
    target_path: *const c_char,
    options: *const OpendalRenameOptions,
    callback: Option<VoidCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let source_path = require_cstr(source_path, "source_path")?.to_string();
    let target_path = require_cstr(target_path, "target_path")?.to_string();
    let callback = require_callback(callback)?;
    let options = read_options(options)?;

    let op = handle.operator();
    executor.spawn(async move {
        let result = op
            .rename_options(&source_path, &target_path, options)
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

/// Restore `path` synchronously with options.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalRestoreOptions` whose
///   pointers stay valid for the duration of the call.
#[unsafe(no_mangle)]
pub extern "C" fn operator_restore_with_options(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalRestoreOptions,
) -> OpendalResult {
    match operator_restore_with_options_inner(op_handle, path, options) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_restore_with_options_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalRestoreOptions,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?;
    let options = read_options(options)?;

    executor
        .block_on(handle.restore_options(path, options))
        .map_err(OpenDALError::from_opendal_error)
}

/// Restore `path` asynchronously with options.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalRestoreOptions` whose
///   pointers stay valid for the duration of the call.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_restore_with_options_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalRestoreOptions,
    callback: Option<VoidCallback>,
    context: i64,
) -> OpendalResult {
    match operator_restore_with_options_async_inner(op_handle, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_restore_with_options_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalRestoreOptions,
    callback: Option<VoidCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = read_options(options)?;

    let op = handle.operator();
    executor.spawn(async move {
        let result = op
            .restore_options(&path, options)
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

/// Compose `sources` into `target_path` synchronously with options.
///
/// On success, the returned payload must be released with
/// `opendal_metadata_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - When `sources_len > 0`, `sources` must point to `sources_len` valid
///   items whose pointers stay valid for the duration of the call.
/// - `target_path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalComposeOptions` whose
///   pointers stay valid for the duration of the call.
#[unsafe(no_mangle)]
pub extern "C" fn operator_compose_with_options(
    op_handle: *const OperatorHandle,
    sources: *const OpendalPathOptions<OpendalComposeSourceOptions>,
    sources_len: usize,
    target_path: *const c_char,
    options: *const OpendalComposeOptions,
) -> OpendalMetadataResult {
    match operator_compose_with_options_inner(op_handle, sources, sources_len, target_path, options)
    {
        Ok(value) => OpendalMetadataResult::ok(value as *mut c_void),
        Err(error) => OpendalMetadataResult::from_error(error),
    }
}

fn operator_compose_with_options_inner(
    op_handle: *const OperatorHandle,
    sources: *const OpendalPathOptions<OpendalComposeSourceOptions>,
    sources_len: usize,
    target_path: *const c_char,
    options: *const OpendalComposeOptions,
) -> Result<*mut OpendalMetadata, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let sources = read_path_options(sources, sources_len, "sources")?;
    let target_path = require_cstr(target_path, "target_path")?;
    let options = read_options(options)?;

    let metadata = executor
        .block_on(handle.compose_options(sources, target_path, options))
        .map_err(OpenDALError::from_opendal_error)?;
    Ok(into_metadata_ptr(metadata))
}

/// Compose `sources` into `target_path` asynchronously with options.
///
/// The callback is invoked exactly once. On success, the callback result must
/// be released with `opendal_metadata_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - When `sources_len > 0`, `sources` must point to `sources_len` valid
///   items whose pointers stay valid for the duration of the call.
/// - `target_path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalComposeOptions` whose
///   pointers stay valid for the duration of the call.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_compose_with_options_async(
    op_handle: *const OperatorHandle,
    sources: *const OpendalPathOptions<OpendalComposeSourceOptions>,
    sources_len: usize,
    target_path: *const c_char,
    options: *const OpendalComposeOptions,
    callback: Option<MetadataCallback>,
    context: i64,
) -> OpendalResult {
    match operator_compose_with_options_async_inner(
        op_handle,
        sources,
        sources_len,
        target_path,
        options,
        callback,
        context,
    ) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_compose_with_options_async_inner(
    op_handle: *const OperatorHandle,
    sources: *const OpendalPathOptions<OpendalComposeSourceOptions>,
    sources_len: usize,
    target_path: *const c_char,
    options: *const OpendalComposeOptions,
    callback: Option<MetadataCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let sources = read_path_options(sources, sources_len, "sources")?;
    let target_path = require_cstr(target_path, "target_path")?.to_string();
    let callback = require_callback(callback)?;
    let options = read_options(options)?;

    let op = handle.operator();
    executor.spawn(async move {
        let result = op
            .compose_options(sources, &target_path, options)
            .await
            .map(into_metadata_ptr)
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

/// Presign read asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_presign_read_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> OpendalResult {
    match operator_presign_read_async_inner(op_handle, path, expire_nanos, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_presign_read_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let expire = Duration::from_nanos(expire_nanos);

    let op = handle.operator();
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
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_presign_write_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> OpendalResult {
    match operator_presign_write_async_inner(op_handle, path, expire_nanos, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_presign_write_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let expire = Duration::from_nanos(expire_nanos);

    let op = handle.operator();
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
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_presign_stat_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> OpendalResult {
    match operator_presign_stat_async_inner(op_handle, path, expire_nanos, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_presign_stat_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let expire = Duration::from_nanos(expire_nanos);

    let op = handle.operator();
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
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_presign_delete_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> OpendalResult {
    match operator_presign_delete_async_inner(op_handle, path, expire_nanos, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_presign_delete_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    expire_nanos: u64,
    callback: Option<PresignCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let expire = Duration::from_nanos(expire_nanos);

    let op = handle.operator();
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
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn operator_input_stream_create(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalReadOptions,
) -> OpendalOperatorResult {
    match operator_input_stream_create_inner(op_handle, path, options) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

/// Streaming reader backing `OperatorInputStream`.
///
/// The underlying byte stream is async; the sync FFI path blocks on it
/// through the owning executor while the async path spawns onto it. The
/// mutex is defensive — the C# stream contract already forbids concurrent
/// reads — and the `Arc` lets in-flight tasks outlive an early free safely.
struct InputStream {
    // Declare `inner` before `executor` so the stream drops before the last
    // `Arc<Executor>` shuts down its Tokio runtime.
    inner: Arc<Mutex<opendal::FuturesBytesStream>>,
    executor: Arc<Executor>,
}

fn require_input_stream<'a>(stream: *mut c_void) -> Result<&'a InputStream, OpenDALError> {
    if stream.is_null() {
        return Err(crate::utils::config_invalid_error(
            "input stream pointer is null",
        ));
    }

    Ok(unsafe { &*(stream as *const InputStream) })
}

/// Convert one polled chunk into an FFI read payload.
///
/// EOF becomes an empty buffer, matching the one-shot read contract.
fn next_chunk_to_buffer(
    value: Option<std::io::Result<bytes::Bytes>>,
) -> Result<OpendalReadBuffer, OpenDALError> {
    value
        .transpose()
        .map_err(|err| match err.downcast::<opendal::Error>() {
            Ok(error) => OpenDALError::from_opendal_error(error),
            Err(err) => OpenDALError::from_opendal_error(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                err.to_string(),
            )),
        })
        .map(|chunk| {
            chunk
                .map(|bytes| OpendalReadBuffer::from_buffer(bytes.into()))
                .unwrap_or_else(OpendalReadBuffer::empty)
        })
}

fn operator_input_stream_create_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalReadOptions,
) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let options = read_options(options)?;

    if options.limit.is_some() {
        return Err(OpenDALError::from_error(
            ErrorCode::ConfigInvalid,
            "read limit is only supported by Read and ReadAsync",
        ));
    }
    let range = options.range;
    let reader_options = opendal::options::ReaderOptions {
        version: options.version,
        if_match: options.if_match,
        if_none_match: options.if_none_match,
        if_version_match: options.if_version_match,
        if_version_not_match: options.if_version_not_match,
        if_modified_since: options.if_modified_since,
        if_unmodified_since: options.if_unmodified_since,
        content_length_hint: options.content_length_hint,
        concurrent: options.concurrent,
        chunk: options.chunk,
        gap: options.gap,
        ..Default::default()
    };

    let stream_op = handle.operator();
    let stream = executor
        .block_on(async move {
            let reader = stream_op.reader_options(&path, reader_options).await?;
            reader.into_bytes_stream(range).await
        })
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(Box::into_raw(Box::new(InputStream {
        inner: Arc::new(Mutex::new(stream)),
        executor,
    })) as *mut c_void)
}

/// Read next bytes chunk from input stream.
///
/// Returns an empty buffer when EOF is reached.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_input_stream_create`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_input_stream_read_next(stream: *mut c_void) -> OpendalReadResult {
    match operator_input_stream_read_next_inner(stream) {
        Ok(buffer) => OpendalReadResult::ok(buffer),
        Err(error) => OpendalReadResult::from_error(error),
    }
}

fn operator_input_stream_read_next_inner(
    stream: *mut c_void,
) -> Result<OpendalReadBuffer, OpenDALError> {
    let stream = require_input_stream(stream)?;
    let inner = stream.inner.clone();
    let value = stream
        .executor
        .block_on(async move { inner.lock().await.next().await });

    next_chunk_to_buffer(value)
}

/// Read next bytes chunk from input stream asynchronously.
///
/// The callback is invoked exactly once; EOF is reported as an empty buffer.
/// On successful reads, the callback result must be released with
/// `opendal_read_result_release`. The stream must not be read again, on
/// either path, until the callback fires.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_input_stream_create`.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_input_stream_read_next_async(
    stream: *mut c_void,
    callback: Option<ReadCallback>,
    context: i64,
) -> OpendalResult {
    match operator_input_stream_read_next_async_inner(stream, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_input_stream_read_next_async_inner(
    stream: *mut c_void,
    callback: Option<ReadCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let stream = require_input_stream(stream)?;
    let callback = require_callback(callback)?;

    let inner = stream.inner.clone();
    stream.executor.spawn(async move {
        let value = inner.lock().await.next().await;

        callback(
            context,
            match next_chunk_to_buffer(value) {
                Ok(buffer) => OpendalReadResult::ok(buffer),
                Err(error) => OpendalReadResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// # Safety
///
/// - `stream` must be null or a pointer returned by `operator_input_stream_create`.
/// - Must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_input_stream_free(stream: *mut c_void) {
    if stream.is_null() {
        return;
    }

    unsafe {
        drop(Box::from_raw(stream as *mut InputStream));
    }
}

/// Delete `targets` synchronously, batching where the service supports it.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - When `targets_len > 0`, `targets` must point to `targets_len` valid
///   items whose pointers stay valid for the duration of the call.
#[unsafe(no_mangle)]
pub extern "C" fn operator_delete_many(
    op_handle: *const OperatorHandle,
    targets: *const OpendalPathOptions<OpendalDeleteOptions>,
    targets_len: usize,
) -> OpendalResult {
    match operator_delete_many_inner(op_handle, targets, targets_len) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_delete_many_inner(
    op_handle: *const OperatorHandle,
    targets: *const OpendalPathOptions<OpendalDeleteOptions>,
    targets_len: usize,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let targets = read_path_options(targets, targets_len, "targets")?;

    executor
        .block_on(handle.delete_iter(targets))
        .map_err(OpenDALError::from_opendal_error)
}

/// Delete `targets` asynchronously, batching where the service supports it.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - When `targets_len > 0`, `targets` must point to `targets_len` valid
///   items whose pointers stay valid for the duration of the call.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_delete_many_async(
    op_handle: *const OperatorHandle,
    targets: *const OpendalPathOptions<OpendalDeleteOptions>,
    targets_len: usize,
    callback: Option<VoidCallback>,
    context: i64,
) -> OpendalResult {
    match operator_delete_many_async_inner(op_handle, targets, targets_len, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_delete_many_async_inner(
    op_handle: *const OperatorHandle,
    targets: *const OpendalPathOptions<OpendalDeleteOptions>,
    targets_len: usize,
    callback: Option<VoidCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let targets = read_path_options(targets, targets_len, "targets")?;
    let callback = require_callback(callback)?;

    let op = handle.operator();
    executor.spawn(async move {
        let result = op
            .delete_iter(targets)
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

/// Create an output stream for `path` with write options.
///
/// Returned pointer must be released by `operator_output_stream_free`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn operator_output_stream_create(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalWriteOptions,
) -> OpendalOperatorResult {
    match operator_output_stream_create_inner(op_handle, path, options) {
        Ok(value) => OpendalOperatorResult::ok(value),
        Err(error) => OpendalOperatorResult::from_error(error),
    }
}

/// Streaming writer backing `OperatorOutputStream`.
///
/// Same split as [`InputStream`]: async writer underneath, sync FFI blocks on
/// it, async FFI spawns onto it. The C# stream contract forbids concurrent
/// writes; the mutex is defensive and the `Arc` keeps in-flight tasks safe
/// against an early free.
struct OutputStream {
    // Declare `inner` before `executor` so the writer drops before the last
    // `Arc<Executor>` shuts down its Tokio runtime.
    inner: Arc<Mutex<opendal::Writer>>,
    executor: Arc<Executor>,
}

fn require_output_stream<'a>(stream: *mut c_void) -> Result<&'a OutputStream, OpenDALError> {
    if stream.is_null() {
        return Err(crate::utils::config_invalid_error(
            "output stream pointer is null",
        ));
    }

    Ok(unsafe { &*(stream as *const OutputStream) })
}

fn operator_output_stream_create_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalWriteOptions,
) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let options = read_options(options)?;

    let writer_op = handle.operator();
    let writer = executor
        .block_on(async move { writer_op.writer_options(&path, options).await })
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(Box::into_raw(Box::new(OutputStream {
        inner: Arc::new(Mutex::new(writer)),
        executor,
    })) as *mut c_void)
}

/// Write bytes to output stream.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_output_stream_create`.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
#[unsafe(no_mangle)]
pub extern "C" fn operator_output_stream_write(
    stream: *mut c_void,
    data: *const u8,
    len: usize,
) -> OpendalResult {
    match operator_output_stream_write_inner(stream, data, len) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_output_stream_write_inner(
    stream: *mut c_void,
    data: *const u8,
    len: usize,
) -> Result<(), OpenDALError> {
    let stream = require_output_stream(stream)?;
    require_data_ptr(data, len)?;

    let payload = if len == 0 {
        bytes::Bytes::new()
    } else {
        bytes::Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len) })
    };

    let inner = stream.inner.clone();
    stream
        .executor
        .block_on(async move { inner.lock().await.write(payload).await })
        .map(|_| ())
        .map_err(OpenDALError::from_opendal_error)
}

/// Write bytes to output stream asynchronously.
///
/// The payload is copied before this function returns, so the caller only has
/// to keep `data` alive for the duration of this call. The callback is
/// invoked exactly once. The stream must not be written to again, on either
/// path, until the callback fires.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_output_stream_create`.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_output_stream_write_async(
    stream: *mut c_void,
    data: *const u8,
    len: usize,
    callback: Option<VoidCallback>,
    context: i64,
) -> OpendalResult {
    match operator_output_stream_write_async_inner(stream, data, len, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_output_stream_write_async_inner(
    stream: *mut c_void,
    data: *const u8,
    len: usize,
    callback: Option<VoidCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let stream = require_output_stream(stream)?;
    require_data_ptr(data, len)?;
    let callback = require_callback(callback)?;

    let payload = if len == 0 {
        bytes::Bytes::new()
    } else {
        bytes::Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len) })
    };

    let inner = stream.inner.clone();
    stream.executor.spawn(async move {
        let result = inner
            .lock()
            .await
            .write(payload)
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

/// Flush output stream.
///
/// The underlying writer only persists data on close, so this is a no-op that
/// exists to keep the FFI surface symmetric.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_output_stream_create`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_output_stream_flush(stream: *mut c_void) -> OpendalResult {
    match operator_output_stream_flush_inner(stream) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_output_stream_flush_inner(stream: *mut c_void) -> Result<(), OpenDALError> {
    require_output_stream(stream)?;
    Ok(())
}

/// Close output stream.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_output_stream_create`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_output_stream_close(stream: *mut c_void) -> OpendalResult {
    match operator_output_stream_close_inner(stream) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_output_stream_close_inner(stream: *mut c_void) -> Result<(), OpenDALError> {
    let stream = require_output_stream(stream)?;
    let inner = stream.inner.clone();
    stream
        .executor
        .block_on(async move { inner.lock().await.close().await })
        .map(|_| ())
        .map_err(OpenDALError::from_opendal_error)
}

/// Close output stream asynchronously.
///
/// The callback is invoked exactly once. After a successful close the handle
/// only needs `operator_output_stream_free`.
/// # Safety
///
/// - `stream` must be a valid pointer returned by `operator_output_stream_create`.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_output_stream_close_async(
    stream: *mut c_void,
    callback: Option<VoidCallback>,
    context: i64,
) -> OpendalResult {
    match operator_output_stream_close_async_inner(stream, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_output_stream_close_async_inner(
    stream: *mut c_void,
    callback: Option<VoidCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let stream = require_output_stream(stream)?;
    let callback = require_callback(callback)?;

    let inner = stream.inner.clone();
    stream.executor.spawn(async move {
        let result = inner
            .lock()
            .await
            .close()
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

/// # Safety
///
/// - `stream` must be null or a pointer returned by `operator_output_stream_create`.
/// - Must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_output_stream_free(stream: *mut c_void) {
    if stream.is_null() {
        return;
    }

    unsafe {
        drop(Box::from_raw(stream as *mut OutputStream));
    }
}

/// Write the committed bytes of a write buffer to `path` synchronously.
///
/// On a non-error return the contents belong to the write and the caller
/// must not write through the buffer's pointers again; the handle itself
/// still needs `write_buffer_free`. An error raised before the payload is
/// taken leaves the slot untouched, while a backend failure after it has
/// already consumed the contents.
///
/// On success, the returned payload must be released with
/// `opendal_metadata_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `buffer` must be a handle from `write_buffer_create` whose contents
///   have not been consumed, and must not have been freed.
/// - Calls taking the same buffer handle must not run concurrently.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write_with_options(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    buffer: *mut c_void,
    committed_in_current: usize,
    options: *const OpendalWriteOptions,
) -> OpendalMetadataResult {
    match operator_write_with_options_inner(op_handle, path, buffer, committed_in_current, options)
    {
        Ok(value) => OpendalMetadataResult::ok(value as *mut c_void),
        Err(error) => OpendalMetadataResult::from_error(error),
    }
}

fn operator_write_with_options_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    buffer: *mut c_void,
    committed_in_current: usize,
    options: *const OpendalWriteOptions,
) -> Result<*mut OpendalMetadata, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?;
    if buffer.is_null() {
        return Err(crate::utils::config_invalid_error(
            "write buffer handle is null",
        ));
    }
    let options = read_options(options)?;

    // Taken only after every fallible check above, so an error result means
    // ownership never transferred and the buffer stays usable.
    let slot = unsafe { &mut *(buffer as *mut WriteBufferSlot) };
    let payload = take_payload(slot, committed_in_current)?;

    let metadata = executor
        .block_on(handle.write_options(path, payload, options))
        .map_err(OpenDALError::from_opendal_error)?;
    Ok(into_metadata_ptr(metadata))
}

/// Write the committed bytes of a write buffer to `path` asynchronously.
///
/// The callback is invoked exactly once. Ownership behaves as in the sync
/// variant: consumed on a non-error return, untouched on error. On success,
/// the callback result must be released with
/// `opendal_metadata_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `buffer` must be a handle from `write_buffer_create` whose contents
///   have not been consumed, and must not have been freed.
/// - Calls taking the same buffer handle must not run concurrently.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write_with_options_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    buffer: *mut c_void,
    committed_in_current: usize,
    options: *const OpendalWriteOptions,
    callback: Option<MetadataCallback>,
    context: i64,
) -> OpendalResult {
    match operator_write_with_options_async_inner(
        op_handle,
        path,
        buffer,
        committed_in_current,
        options,
        callback,
        context,
    ) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

// Mirrors the `extern "C"` signature above, which clippy exempts because a C ABI
// is not a design choice. This helper exists only to give that function a body it
// can write with `?`.
#[allow(clippy::too_many_arguments)]
fn operator_write_with_options_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    buffer: *mut c_void,
    committed_in_current: usize,
    options: *const OpendalWriteOptions,
    callback: Option<MetadataCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    if buffer.is_null() {
        return Err(crate::utils::config_invalid_error(
            "write buffer handle is null",
        ));
    }
    let options = read_options(options)?;

    // Taken only after every fallible check above, so an error result means
    // ownership never transferred and the buffer stays usable.
    let slot = unsafe { &mut *(buffer as *mut WriteBufferSlot) };
    let payload = take_payload(slot, committed_in_current)?;

    let op = handle.operator();
    executor.spawn(async move {
        let result = op
            .write_options(&path, payload, options)
            .await
            .map(into_metadata_ptr)
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

/// Write a caller-owned byte range to `path` synchronously.
///
/// The bytes are copied before the write, so `data` only has to stay valid
/// for the duration of this call. On success, the returned payload must be
/// released with `opendal_metadata_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write_bytes_with_options(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    data: *const u8,
    len: usize,
    options: *const OpendalWriteOptions,
) -> OpendalMetadataResult {
    match operator_write_bytes_with_options_inner(op_handle, path, data, len, options) {
        Ok(value) => OpendalMetadataResult::ok(value as *mut c_void),
        Err(error) => OpendalMetadataResult::from_error(error),
    }
}

fn operator_write_bytes_with_options_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    data: *const u8,
    len: usize,
    options: *const OpendalWriteOptions,
) -> Result<*mut OpendalMetadata, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?;
    require_data_ptr(data, len)?;
    let options = read_options(options)?;

    // Copied explicitly: the only `Into<Buffer>` impl for slices is
    // `From<&'static [u8]>`, which is zero-copy, so passing the raw slice
    // would launder the caller's pointer into `'static` and let backends
    // retain memory that is only pinned for the duration of this call.
    let payload = if len == 0 {
        bytes::Bytes::new()
    } else {
        bytes::Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len) })
    };

    let metadata = executor
        .block_on(handle.write_options(path, payload, options))
        .map_err(OpenDALError::from_opendal_error)?;
    Ok(into_metadata_ptr(metadata))
}

/// Write a caller-owned byte range to `path` asynchronously.
///
/// The bytes are copied before the task is spawned, so `data` only has to
/// stay valid for the duration of this call. The callback is invoked exactly
/// once. On success, the callback result must be released with
/// `opendal_metadata_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - When `len > 0`, `data` must be non-null and readable for `len` bytes.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_write_bytes_with_options_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    data: *const u8,
    len: usize,
    options: *const OpendalWriteOptions,
    callback: Option<MetadataCallback>,
    context: i64,
) -> OpendalResult {
    match operator_write_bytes_with_options_async_inner(
        op_handle, path, data, len, options, callback, context,
    ) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

// Mirrors the `extern "C"` signature above, which clippy exempts because a C ABI
// is not a design choice. This helper exists only to give that function a body it
// can write with `?`.
#[allow(clippy::too_many_arguments)]
fn operator_write_bytes_with_options_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    data: *const u8,
    len: usize,
    options: *const OpendalWriteOptions,
    callback: Option<MetadataCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    require_data_ptr(data, len)?;
    let callback = require_callback(callback)?;
    let options = read_options(options)?;

    // Copied before the task is spawned, so the caller only has to keep its
    // array alive for the duration of this call.
    let payload = if len == 0 {
        bytes::Bytes::new()
    } else {
        bytes::Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len) })
    };

    let op = handle.operator();
    executor.spawn(async move {
        let result = op
            .write_options(&path, payload, options)
            .await
            .map(into_metadata_ptr)
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

/// Read bytes from `path` synchronously with options.
///
/// On success, the returned buffer must be released with `opendal_read_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalReadOptions` whose
///   pointers stay valid for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_read_with_options(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalReadOptions,
) -> OpendalReadResult {
    match operator_read_with_options_inner(op_handle, path, options) {
        Ok(value) => OpendalReadResult::ok(value),
        Err(error) => OpendalReadResult::from_error(error),
    }
}

fn operator_read_with_options_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalReadOptions,
) -> Result<OpendalReadBuffer, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?;
    let options = read_options(options)?;

    let value = executor
        .block_on(handle.read_options(path, options))
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(OpendalReadBuffer::from_buffer(value))
}

/// Read bytes from `path` asynchronously with options.
///
/// The callback is invoked exactly once. On successful reads, the callback
/// result must be released with `opendal_read_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalReadOptions` whose
///   pointers stay valid for the duration of the call.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_read_with_options_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalReadOptions,
    callback: Option<ReadCallback>,
    context: i64,
) -> OpendalResult {
    match operator_read_with_options_async_inner(op_handle, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_read_with_options_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalReadOptions,
    callback: Option<ReadCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = read_options(options)?;

    let op = handle.operator();
    executor.spawn(async move {
        let result = op
            .read_options(&path, options)
            .await
            .map(OpendalReadBuffer::from_buffer)
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
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalStatOptions` whose
///   pointers stay valid for the duration of the call.
#[unsafe(no_mangle)]
pub extern "C" fn operator_stat_with_options(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalStatOptions,
) -> OpendalMetadataResult {
    match operator_stat_with_options_inner(op_handle, path, options) {
        Ok(value) => OpendalMetadataResult::ok(value as *mut c_void),
        Err(error) => OpendalMetadataResult::from_error(error),
    }
}

fn operator_stat_with_options_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalStatOptions,
) -> Result<*mut OpendalMetadata, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?;
    let options = read_options(options)?;

    let metadata = executor
        .block_on(handle.stat_options(path, options))
        .map_err(OpenDALError::from_opendal_error)?;
    Ok(into_metadata_ptr(metadata))
}

/// Stat `path` asynchronously with options.
///
/// The callback is invoked exactly once. On success, callback result must be
/// released with `opendal_metadata_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalStatOptions` whose
///   pointers stay valid for the duration of the call.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_stat_with_options_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalStatOptions,
    callback: Option<MetadataCallback>,
    context: i64,
) -> OpendalResult {
    match operator_stat_with_options_async_inner(op_handle, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_stat_with_options_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalStatOptions,
    callback: Option<MetadataCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = read_options(options)?;

    let op = handle.operator();
    executor.spawn(async move {
        let result = op
            .stat_options(&path, options)
            .await
            .map(into_metadata_ptr)
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

/// Check whether `path` exists synchronously.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub extern "C" fn operator_exists(
    op_handle: *const OperatorHandle,
    path: *const c_char,
) -> OpendalBoolResult {
    match operator_exists_inner(op_handle, path) {
        Ok(value) => OpendalBoolResult::ok(value as u8),
        Err(error) => OpendalBoolResult::from_error(error),
    }
}

fn operator_exists_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
) -> Result<bool, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?;

    executor
        .block_on(handle.exists(path))
        .map_err(OpenDALError::from_opendal_error)
}

/// Check whether `path` exists asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_exists_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    callback: Option<BoolCallback>,
    context: i64,
) -> OpendalResult {
    match operator_exists_async_inner(op_handle, path, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_exists_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    callback: Option<BoolCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;

    let op = handle.operator();
    executor.spawn(async move {
        let result = op
            .exists(&path)
            .await
            .map_err(OpenDALError::from_opendal_error);

        callback(
            context,
            match result {
                Ok(value) => OpendalBoolResult::ok(value as u8),
                Err(error) => OpendalBoolResult::from_error(error),
            },
        );
    });

    Ok(())
}

/// Check whether the operator can reach its service.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
#[unsafe(no_mangle)]
pub extern "C" fn operator_check(op_handle: *const OperatorHandle) -> OpendalResult {
    match operator_check_inner(op_handle) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_check_inner(op_handle: *const OperatorHandle) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();

    executor
        .block_on(handle.check())
        .map_err(OpenDALError::from_opendal_error)
}

/// Check whether the operator can reach its service asynchronously.
///
/// The callback is invoked exactly once with the final result.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_check_async(
    op_handle: *const OperatorHandle,
    callback: Option<VoidCallback>,
    context: i64,
) -> OpendalResult {
    match operator_check_async_inner(op_handle, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_check_async_inner(
    op_handle: *const OperatorHandle,
    callback: Option<VoidCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let callback = require_callback(callback)?;

    let op = handle.operator();
    executor.spawn(async move {
        let result = op.check().await.map_err(OpenDALError::from_opendal_error);

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

/// List entries from `path` synchronously with options.
///
/// On success, returned payload must be released with `opendal_entry_list_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalListOptions` whose
///   pointers stay valid for the duration of the call.
#[unsafe(no_mangle)]
pub extern "C" fn operator_list_with_options(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalListOptions,
) -> OpendalEntryListResult {
    match operator_list_with_options_inner(op_handle, path, options) {
        Ok(value) => OpendalEntryListResult::ok(value),
        Err(error) => OpendalEntryListResult::from_error(error),
    }
}

fn operator_list_with_options_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalListOptions,
) -> Result<*mut c_void, OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?;
    let options = read_options(options)?;

    let entries = executor
        .block_on(handle.list_options(path, options))
        .map_err(OpenDALError::from_opendal_error)?;

    Ok(into_entry_list_ptr(entries))
}

/// List entries from `path` asynchronously with options.
///
/// The callback is invoked exactly once. On success, callback result must be
/// released with `opendal_entry_list_result_release`.
/// # Safety
///
/// - `op_handle` must be a valid operator pointer from `operator_construct`.
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `options` must be null or point to a valid `OpendalListOptions` whose
///   pointers stay valid for the duration of the call.
/// - `callback` must be a valid function pointer and remain callable until invoked.
#[unsafe(no_mangle)]
pub extern "C" fn operator_list_with_options_async(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalListOptions,
    callback: Option<ListCallback>,
    context: i64,
) -> OpendalResult {
    match operator_list_with_options_async_inner(op_handle, path, options, callback, context) {
        Ok(()) => OpendalResult::ok(),
        Err(error) => OpendalResult::from_error(error),
    }
}

fn operator_list_with_options_async_inner(
    op_handle: *const OperatorHandle,
    path: *const c_char,
    options: *const OpendalListOptions,
    callback: Option<ListCallback>,
    context: i64,
) -> Result<(), OpenDALError> {
    let handle = require_op_handle(op_handle)?;
    let executor = handle.executor.clone();
    let path = require_cstr(path, "path")?.to_string();
    let callback = require_callback(callback)?;
    let options = read_options(options)?;

    let op = handle.operator();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::ffi::CString;
    use std::ptr;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::{Mutex as StdMutex, OnceLock, mpsc};

    use crate::buffer::read_buffer_copy_to;
    use crate::executor::{executor_create, executor_free};
    use crate::result::{opendal_metadata_result_release, opendal_read_result_release};

    struct CallbackContext {
        tx: mpsc::Sender<Result<Vec<u8>, i32>>,
        op_to_free: Option<*mut OperatorHandle>,
        executor_to_free: Option<*mut c_void>,
    }

    // SAFETY: Raw handles are transferred once to the completion callback,
    // which frees each handle at most once.
    unsafe impl Send for CallbackContext {}

    fn callback_registry() -> &'static StdMutex<HashMap<i64, CallbackContext>> {
        static REGISTRY: OnceLock<StdMutex<HashMap<i64, CallbackContext>>> = OnceLock::new();
        REGISTRY.get_or_init(|| StdMutex::new(HashMap::new()))
    }

    fn register_callback(ctx: CallbackContext) -> i64 {
        static NEXT_ID: AtomicI64 = AtomicI64::new(1);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        callback_registry()
            .lock()
            .expect("lock callback registry")
            .insert(id, ctx);
        id
    }

    extern "C" fn test_read_callback(context: i64, result: OpendalReadResult) {
        let ctx = callback_registry()
            .lock()
            .expect("lock callback registry")
            .remove(&context)
            .expect("registered callback context");

        let payload = if result.error.has_error != 0 {
            Err(result.error.code)
        } else {
            let mut bytes = vec![0u8; result.buffer.len];
            if !bytes.is_empty() {
                let copied = unsafe {
                    read_buffer_copy_to(result.buffer.handle, 0, bytes.as_mut_ptr(), bytes.len())
                };
                bytes.truncate(copied);
            }
            Ok(bytes)
        };

        opendal_read_result_release(result);

        if let Some(op_ptr) = ctx.op_to_free {
            unsafe { operator_free(op_ptr) };
        }
        if let Some(executor_ptr) = ctx.executor_to_free {
            unsafe { executor_free(executor_ptr) };
        }

        let _ = ctx.tx.send(payload);
    }

    fn create_memory_operator(executor_ptr: *const c_void) -> *mut OperatorHandle {
        let scheme = CString::new("memory").expect("scheme cstr");
        let op_res = unsafe {
            operator_construct(scheme.as_ptr(), ptr::null(), ptr::null(), 0, executor_ptr)
        };
        assert_eq!(op_res.error.has_error, 0, "construct memory operator");
        assert!(!op_res.ptr.is_null());
        op_res.ptr as *mut OperatorHandle
    }

    fn write_test_object(op_ptr: *const OperatorHandle, path: &str, data: &[u8]) {
        let path_cstr = CString::new(path).expect("path cstr");
        let write_res = unsafe {
            operator_write_bytes_with_options(
                op_ptr,
                path_cstr.as_ptr(),
                data.as_ptr(),
                data.len(),
                ptr::null(),
            )
        };
        assert_eq!(write_res.error.has_error, 0, "write test object");
        opendal_metadata_result_release(write_res);
    }

    #[test]
    fn async_ffi_read_completes_after_handles_freed() {
        let exec_res = executor_create(1);
        assert_eq!(exec_res.error.has_error, 0);
        let executor_ptr = exec_res.ptr;

        let op_ptr = create_memory_operator(executor_ptr);
        write_test_object(op_ptr, "inflight.txt", b"hello-inflight");

        let path_cstr = CString::new("inflight.txt").expect("path cstr");
        let stream_res = operator_input_stream_create(op_ptr, path_cstr.as_ptr(), ptr::null());
        assert_eq!(stream_res.error.has_error, 0);
        let stream_ptr = stream_res.ptr;

        // Hold the stream's async mutex so the spawned read task deterministically
        // suspends at `.await` while we free every handle on the calling thread.
        let inner = unsafe { (&*(stream_ptr as *const InputStream)).inner.clone() };
        let guard = futures::executor::block_on(inner.lock());

        let (tx, rx) = mpsc::channel();
        let ctx_id = register_callback(CallbackContext {
            tx,
            op_to_free: None,
            executor_to_free: None,
        });

        let start_res = unsafe {
            operator_input_stream_read_next_async(stream_ptr, Some(test_read_callback), ctx_id)
        };
        assert_eq!(start_res.error.has_error, 0);

        // Free stream, operator, and executor while the async read is still in flight.
        unsafe {
            operator_input_stream_free(stream_ptr);
            operator_free(op_ptr);
            executor_free(executor_ptr);
        }

        // Release the mutex so the in-flight task can finish and invoke the callback.
        drop(guard);

        let payload = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("in-flight async FFI callback must still fire after handles are freed")
            .expect("async FFI read should succeed");
        assert_eq!(payload, b"hello-inflight");
    }

    #[test]
    fn freeing_operator_and_executor_inside_async_callback_does_not_panic() {
        let exec_res = executor_create(1);
        assert_eq!(exec_res.error.has_error, 0);
        let executor_ptr = exec_res.ptr;

        let op_ptr = create_memory_operator(executor_ptr);
        write_test_object(op_ptr, "callback-free.txt", b"hello-callback");

        let path_cstr = CString::new("callback-free.txt").expect("path cstr");
        let (tx, rx) = mpsc::channel();
        let ctx_id = register_callback(CallbackContext {
            tx,
            op_to_free: Some(op_ptr),
            executor_to_free: Some(executor_ptr),
        });

        let start_res = operator_read_with_options_async(
            op_ptr,
            path_cstr.as_ptr(),
            ptr::null(),
            Some(test_read_callback),
            ctx_id,
        );
        assert_eq!(start_res.error.has_error, 0);

        let payload = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("async FFI callback should complete when freeing handles on worker thread")
            .expect("async FFI read should succeed");
        assert_eq!(payload, b"hello-callback");
    }

    struct DropProbe {
        executor: std::sync::Weak<Executor>,
        executor_alive_on_drop: Arc<std::sync::atomic::AtomicBool>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.executor_alive_on_drop
                .store(self.executor.upgrade().is_some(), Ordering::SeqCst);
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct DropObserverLayer {
        executor: std::sync::Weak<Executor>,
        executor_alive_on_drop: Arc<std::sync::atomic::AtomicBool>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl opendal::raw::Layer for DropObserverLayer {
        fn apply_service(&self, inner: opendal::raw::Servicer) -> opendal::raw::Servicer {
            Arc::new(DropObserverService {
                inner,
                executor: self.executor.clone(),
                executor_alive_on_drop: self.executor_alive_on_drop.clone(),
                dropped: self.dropped.clone(),
            })
        }
    }

    #[derive(Debug)]
    struct DropObserverService {
        inner: opendal::raw::Servicer,
        executor: std::sync::Weak<Executor>,
        executor_alive_on_drop: Arc<std::sync::atomic::AtomicBool>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl DropObserverService {
        fn probe(&self) -> DropProbe {
            DropProbe {
                executor: self.executor.clone(),
                executor_alive_on_drop: self.executor_alive_on_drop.clone(),
                dropped: self.dropped.clone(),
            }
        }
    }

    struct DropObserverReader {
        inner: opendal::raw::oio::Reader,
        _probe: DropProbe,
    }

    impl opendal::raw::oio::Read for DropObserverReader {
        async fn open(
            &self,
            range: opendal::BytesRange,
        ) -> opendal::Result<(
            opendal::raw::RpRead,
            Box<dyn opendal::raw::oio::ReadStreamDyn>,
        )> {
            self.inner.open_dyn(range).await
        }

        async fn read(
            &self,
            range: opendal::BytesRange,
        ) -> opendal::Result<(opendal::raw::RpRead, opendal::Buffer)> {
            self.inner.read_dyn(range).await
        }
    }

    struct DropObserverWriter {
        inner: opendal::raw::oio::Writer,
        _probe: DropProbe,
    }

    impl opendal::raw::oio::Write for DropObserverWriter {
        async fn write(&mut self, bs: opendal::Buffer) -> opendal::Result<()> {
            self.inner.write_dyn(bs).await
        }

        async fn close(&mut self) -> opendal::Result<opendal::Metadata> {
            self.inner.close_dyn().await
        }

        async fn abort(&mut self) -> opendal::Result<()> {
            self.inner.abort_dyn().await
        }
    }

    impl opendal::raw::Service for DropObserverService {
        type Reader = DropObserverReader;
        type Writer = DropObserverWriter;
        type Lister = opendal::raw::oio::Lister;
        type Deleter = opendal::raw::oio::Deleter;
        type Copier = opendal::raw::oio::Copier;
        type Composer = opendal::raw::oio::Composer;

        fn info(&self) -> opendal::raw::ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> opendal::Capability {
            self.inner.capability()
        }

        async fn create_dir(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: opendal::raw::OpCreateDir,
        ) -> opendal::Result<opendal::raw::RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }

        async fn stat(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: opendal::raw::OpStat,
        ) -> opendal::Result<opendal::raw::RpStat> {
            self.inner.stat(ctx, path, args).await
        }

        fn read(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: opendal::raw::OpRead,
        ) -> opendal::Result<Self::Reader> {
            let inner = self.inner.read(ctx, path, args)?;
            Ok(DropObserverReader {
                inner,
                _probe: self.probe(),
            })
        }

        fn write(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: opendal::raw::OpWrite,
        ) -> opendal::Result<Self::Writer> {
            let inner = self.inner.write(ctx, path, args)?;
            Ok(DropObserverWriter {
                inner,
                _probe: self.probe(),
            })
        }

        fn copy(
            &self,
            ctx: &opendal::OperationContext,
            from: &str,
            to: &str,
            args: opendal::raw::OpCopy,
        ) -> opendal::Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args)
        }

        fn compose(
            &self,
            ctx: &opendal::OperationContext,
            to: &str,
            args: opendal::raw::OpCompose,
        ) -> opendal::Result<Self::Composer> {
            self.inner.compose(ctx, to, args)
        }

        async fn rename(
            &self,
            ctx: &opendal::OperationContext,
            from: &str,
            to: &str,
            args: opendal::raw::OpRename,
        ) -> opendal::Result<opendal::raw::RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }

        fn delete(&self, ctx: &opendal::OperationContext) -> opendal::Result<Self::Deleter> {
            self.inner.delete(ctx)
        }

        fn list(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: opendal::raw::OpList,
        ) -> opendal::Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }

        async fn restore(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: opendal::raw::OpRestore,
        ) -> opendal::Result<opendal::raw::RpRestore> {
            self.inner.restore(ctx, path, args).await
        }

        async fn presign(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: opendal::raw::OpPresign,
        ) -> opendal::Result<opendal::raw::RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    fn create_drop_observer_operator() -> (
        *mut OperatorHandle,
        *mut c_void,
        std::sync::Weak<Executor>,
        Arc<std::sync::atomic::AtomicBool>,
        Arc<std::sync::atomic::AtomicBool>,
    ) {
        let exec_res = executor_create(1);
        assert_eq!(exec_res.error.has_error, 0);
        let executor_ptr = exec_res.ptr;

        let executor = unsafe { crate::executor::executor_or_default(executor_ptr) }
            .expect("clone dedicated executor");
        let executor_weak = Arc::downgrade(&executor);
        let executor_alive_on_drop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let op = opendal::Operator::new(opendal::services::Memory::default())
            .expect("build memory operator")
            .layer(DropObserverLayer {
                executor: executor_weak.clone(),
                executor_alive_on_drop: executor_alive_on_drop.clone(),
                dropped: dropped.clone(),
            });

        let op_ptr = Box::into_raw(Box::new(OperatorHandle { op, executor }));
        (
            op_ptr,
            executor_ptr,
            executor_weak,
            executor_alive_on_drop,
            dropped,
        )
    }

    #[test]
    fn input_stream_drops_inner_before_last_executor() {
        let (op_ptr, executor_ptr, executor_weak, executor_alive_on_drop, dropped) =
            create_drop_observer_operator();
        write_test_object(op_ptr, "drop-order.txt", b"payload");
        // Reset flags recorded during the setup write.
        executor_alive_on_drop.store(false, Ordering::SeqCst);
        dropped.store(false, Ordering::SeqCst);

        let path_cstr = CString::new("drop-order.txt").expect("path cstr");
        let stream_res = operator_input_stream_create(op_ptr, path_cstr.as_ptr(), ptr::null());
        assert_eq!(stream_res.error.has_error, 0);
        let stream_ptr = stream_res.ptr;

        // Release operator and executor so the stream holds the last Arc<Executor>.
        unsafe {
            operator_free(op_ptr);
            executor_free(executor_ptr);
        }
        assert_eq!(executor_weak.strong_count(), 1);
        assert!(!dropped.load(Ordering::SeqCst));

        unsafe {
            operator_input_stream_free(stream_ptr);
        }

        assert!(dropped.load(Ordering::SeqCst));
        assert!(
            executor_alive_on_drop.load(Ordering::SeqCst),
            "InputStream must drop its inner stream before the last Arc<Executor>"
        );
        assert_eq!(executor_weak.strong_count(), 0);
    }

    #[test]
    fn output_stream_drops_inner_before_last_executor() {
        let (op_ptr, executor_ptr, executor_weak, executor_alive_on_drop, dropped) =
            create_drop_observer_operator();

        let path_cstr = CString::new("drop-order-out.txt").expect("path cstr");
        let stream_res = operator_output_stream_create(op_ptr, path_cstr.as_ptr(), ptr::null());
        assert_eq!(stream_res.error.has_error, 0);
        let stream_ptr = stream_res.ptr;

        // Release operator and executor so the stream holds the last Arc<Executor>.
        unsafe {
            operator_free(op_ptr);
            executor_free(executor_ptr);
        }
        assert_eq!(executor_weak.strong_count(), 1);
        assert!(!dropped.load(Ordering::SeqCst));

        unsafe {
            operator_output_stream_free(stream_ptr);
        }

        assert!(dropped.load(Ordering::SeqCst));
        assert!(
            executor_alive_on_drop.load(Ordering::SeqCst),
            "OutputStream must drop its inner writer before the last Arc<Executor>"
        );
        assert_eq!(executor_weak.strong_count(), 0);
    }
}
