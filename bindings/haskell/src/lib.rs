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

mod logger;
mod result;
mod types;

use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::LazyLock;

use ::opendal as od;
use logger::HsLogger;
use od::layers::LoggingLayer;
use od::layers::RetryLayer;
use od::options;
use opendal::blocking;
use result::FFIResult;
use types::ByteSlice;
use types::Metadata;
use types::into_raw_c_string;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

/// Convert a caller-provided, possibly null, C string argument into `&str`.
///
/// Returns `None` for a null pointer or non UTF-8 content so callers can
/// report an `FFIResult::err` instead of dereferencing an invalid pointer.
unsafe fn c_str_arg<'a>(ptr: *const c_char) -> Option<&'a str> {
    unsafe {
        if ptr.is_null() {
            return None;
        }
        CStr::from_ptr(ptr).to_str().ok()
    }
}

/// # Safety
///
/// * `scheme` points to a nul-terminated string, or is null.
/// * When `len > 0`, `keys` and `values` point to arrays of `len` valid
///   nul-terminated strings.
/// * `result` is a valid pointer with space for one `FFIResult`.
///
/// # Panics
///
/// * If `callback` is not a valid function pointer.
/// * If `result` is not a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn via_map_ffi(
    scheme: *const c_char,
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
    callback: Option<extern "C" fn(u32, *const c_char)>,
    result: *mut FFIResult<od::blocking::Operator>,
) {
    unsafe {
        let scheme = match c_str_arg(scheme) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert scheme to string");
                return;
            }
        };

        if len > 0 && (keys.is_null() || values.is_null()) {
            *result = FFIResult::err("Config keys/values are null");
            return;
        }

        let keys_vec = if len == 0 {
            &[][..]
        } else {
            std::slice::from_raw_parts(keys, len)
        };
        let values_vec = if len == 0 {
            &[][..]
        } else {
            std::slice::from_raw_parts(values, len)
        };

        let mut map = HashMap::with_capacity(len);
        for (&k, &v) in keys_vec.iter().zip(values_vec.iter()) {
            let key = match c_str_arg(k) {
                Some(s) => s.to_string(),
                None => {
                    *result = FFIResult::err("Config key is not a valid string");
                    return;
                }
            };
            let value = match c_str_arg(v) {
                Some(s) => s.to_string(),
                None => {
                    *result = FFIResult::err("Config value is not a valid string");
                    return;
                }
            };
            map.insert(key, value);
        }

        if let Some(callback) = callback
            && let Err(e) = log::set_boxed_logger(Box::new(HsLogger { callback }))
                .map(|()| log::set_max_level(log::LevelFilter::Debug))
        {
            *result = FFIResult::err_with_source(
                "Failed to register logger",
                od::Error::new(od::ErrorKind::Unexpected, e.to_string().as_str()),
            );
            return;
        }

        let res = match od::Operator::via_iter(scheme, map) {
            Ok(mut operator) => {
                // Keep the Haskell defaults outside any layers configured in the map.
                // A configured retry layer runs inside this default retry layer.
                // A log callback adds a logging layer outside the retry layers.
                operator = operator.layer(RetryLayer::new());
                if callback.is_some() {
                    operator = operator.layer(LoggingLayer::default());
                }

                let handle = RUNTIME.handle();
                let _enter = handle.enter();
                match od::blocking::Operator::new(operator) {
                    Ok(op) => FFIResult::ok(op),
                    Err(e) => FFIResult::err_with_source("Failed to create blocking Operator", e),
                }
            }
            Err(e) => FFIResult::err_with_source("Failed to create Operator", e),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `operator` is a valid pointer to a `blocking::Operator` created by
///   `via_map_ffi`, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_operator(operator: *mut od::blocking::Operator) {
    unsafe {
        if !operator.is_null() {
            drop(Box::from_raw(operator));
        }
    }
}

/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `result` is a valid pointer with space for one `FFIResult`. On success
///   `data_ptr` must be released with `free_byteslice`, on failure
///   `error_message` must be released with `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_read(
    op: *const od::blocking::Operator,
    path: *const c_char,
    result: *mut FFIResult<ByteSlice>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        let res = match op.read(path_str) {
            Ok(bytes) => FFIResult::ok(ByteSlice::from_vec(bytes.to_vec())),
            Err(e) => FFIResult::err_with_source("Failed to read", e),
        };

        *result = res;
    }
}

/// Copy a caller-provided byte buffer into an owned `Vec<u8>`.
///
/// The buffer is borrowed for the duration of the call only; returning the
/// slice itself across the FFI boundary would let Rust free memory it does
/// not own.
unsafe fn copy_bytes<'a>(bytes: *const c_char, len: usize) -> Option<&'a [u8]> {
    unsafe {
        if len == 0 {
            return Some(&[]);
        }
        if bytes.is_null() {
            return None;
        }
        Some(std::slice::from_raw_parts(bytes as *const u8, len))
    }
}

/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `bytes` points to a byte array of `len` bytes, or is null when `len == 0`.
/// * `result` is a valid pointer with space for one `FFIResult`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_write(
    op: *const od::blocking::Operator,
    path: *const c_char,
    bytes: *const c_char,
    len: usize,
    result: *mut FFIResult<()>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        let bytes = match copy_bytes(bytes, len) {
            Some(b) => b.to_vec(),
            None => {
                *result = FFIResult::err("Invalid bytes buffer");
                return;
            }
        };

        let res = match op.write(path_str, bytes) {
            Ok(_) => FFIResult::ok(()),
            Err(e) => FFIResult::err_with_source("Failed to write", e),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `result` is a valid pointer with space for one `FFIResult`. On success
///   `data_ptr` must be released with `free_bool`, on failure
///   `error_message` must be released with `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_is_exist(
    op: *const od::blocking::Operator,
    path: *const c_char,
    result: *mut FFIResult<bool>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        let res = match op.exists(path_str) {
            Ok(exist) => FFIResult::ok(exist),
            Err(e) => FFIResult::err_with_source("Failed to check if path exists", e),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `result` is a valid pointer with space for one `FFIResult`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_create_dir(
    op: *const od::blocking::Operator,
    path: *const c_char,
    result: *mut FFIResult<()>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        let res = match op.create_dir(path_str) {
            Ok(()) => FFIResult::ok(()),
            Err(e) => FFIResult::err_with_source("Failed to create directory", e),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path_from` and `path_to` point to nul-terminated strings, or are null.
/// * `result` is a valid pointer with space for one `FFIResult`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_copy(
    op: *const od::blocking::Operator,
    path_from: *const c_char,
    path_to: *const c_char,
    result: *mut FFIResult<()>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_from_str = match c_str_arg(path_from) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert source path to string");
                return;
            }
        };

        let path_to_str = match c_str_arg(path_to) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert destination path to string");
                return;
            }
        };

        let res = match op.copy(path_from_str, path_to_str) {
            Ok(_) => FFIResult::ok(()),
            Err(e) => FFIResult::err_with_source("Failed to copy", e),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path_from` and `path_to` point to nul-terminated strings, or are null.
/// * `result` is a valid pointer with space for one `FFIResult`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_rename(
    op: *const od::blocking::Operator,
    path_from: *const c_char,
    path_to: *const c_char,
    result: *mut FFIResult<()>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_from_str = match c_str_arg(path_from) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert source path to string");
                return;
            }
        };

        let path_to_str = match c_str_arg(path_to) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert destination path to string");
                return;
            }
        };

        let res = match op.rename(path_from_str, path_to_str) {
            Ok(()) => FFIResult::ok(()),
            Err(e) => FFIResult::err_with_source("Failed to rename", e),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `result` is a valid pointer with space for one `FFIResult`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_delete(
    op: *const od::blocking::Operator,
    path: *const c_char,
    result: *mut FFIResult<()>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        let res = match op.delete(path_str) {
            Ok(()) => FFIResult::ok(()),
            Err(e) => FFIResult::err_with_source("Failed to delete", e),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `result` is a valid pointer with space for one `FFIResult`. On success
///   `data_ptr` must be released with `free_metadata`, on failure
///   `error_message` must be released with `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_stat(
    op: *const od::blocking::Operator,
    path: *const c_char,
    result: *mut FFIResult<Metadata>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        let res = match op.stat(path_str) {
            Ok(meta) => FFIResult::ok(meta.into()),
            Err(e) => FFIResult::err_with_source("Failed to stat", e),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `result` is a valid pointer with space for one `FFIResult`. On success
///   `data_ptr` is a boxed `blocking::Lister` released by `free_lister`, on
///   failure `error_message` must be released with `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_list(
    op: *const od::blocking::Operator,
    path: *const c_char,
    result: *mut FFIResult<blocking::Lister>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        let res = match op.lister(path_str) {
            Ok(lister) => FFIResult::ok(lister),
            Err(e) => FFIResult::err_with_source("Failed to list", e),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `result` is a valid pointer with space for one `FFIResult`. On success
///   `data_ptr` is a boxed `blocking::Lister` released by `free_lister`, on
///   failure `error_message` must be released with `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_scan(
    op: *const od::blocking::Operator,
    path: *const c_char,
    result: *mut FFIResult<blocking::Lister>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        let res = match op.lister_options(
            path_str,
            options::ListOptions {
                recursive: true,
                ..Default::default()
            },
        ) {
            Ok(lister) => FFIResult::ok(lister),
            Err(e) => FFIResult::err_with_source("Failed to scan", e),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `lister` is a valid pointer to a `blocking::Lister`.
/// * `result` is a valid pointer with space for one `FFIResult`. On success
///   `data_ptr` is a `CString` released by `free_string`, or null when the
///   lister is exhausted; on failure `error_message` must be released with
///   `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lister_next(
    lister: *mut blocking::Lister,
    result: *mut FFIResult<c_char>,
) {
    unsafe {
        let lister = if lister.is_null() {
            *result = FFIResult::err("Lister is null");
            return;
        } else {
            &mut *lister
        };

        let res = match lister.next() {
            Some(Ok(item)) => FFIResult::ok_with_ptr(into_raw_c_string(item.path())),
            Some(Err(e)) => FFIResult::err_with_source("Failed to get next item", e),
            None => FFIResult::none(),
        };

        *result = res;
    }
}

/// # Safety
///
/// * `lister` is a valid pointer to a `blocking::Lister` created by
///   `blocking_list` or `blocking_scan`, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_lister(lister: *mut blocking::Lister) {
    unsafe {
        if !lister.is_null() {
            drop(Box::from_raw(lister));
        }
    }
}

/// Get operator info (scheme)
///
/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `result` is a valid pointer with space for one `FFIResult`. On success
///   `data_ptr` is a `CString` released by `free_string`, on failure
///   `error_message` must be released with `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn operator_info(
    op: *const od::blocking::Operator,
    result: *mut FFIResult<c_char>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let info = op.info();
        let scheme_str = info.scheme().to_string();

        *result = FFIResult::ok_with_ptr(into_raw_c_string(&scheme_str));
    }
}

/// Remove all files and directories recursively
///
/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `result` is a valid pointer with space for one `FFIResult`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_remove_all(
    op: *const od::blocking::Operator,
    path: *const c_char,
    result: *mut FFIResult<()>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        let res = {
            use od::options::DeleteOptions;
            match op.delete_options(
                path_str,
                DeleteOptions {
                    recursive: true,
                    ..Default::default()
                },
            ) {
                Ok(()) => FFIResult::ok(()),
                Err(e) => FFIResult::err_with_source("Failed to remove all", e),
            }
        };

        *result = res;
    }
}

/// Creates a blocking writer for the given path
///
/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `result` is a valid pointer with space for one `FFIResult`. On success
///   `data_ptr` is a boxed `blocking::Writer` released by `free_writer`, on
///   failure `error_message` must be released with `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_writer(
    op: *const od::blocking::Operator,
    path: *const c_char,
    result: *mut FFIResult<blocking::Writer>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        let res = match op.writer(path_str) {
            Ok(writer) => FFIResult::ok(writer),
            Err(e) => FFIResult::err_with_source("Failed to create writer", e),
        };

        *result = res;
    }
}

/// Creates a blocking writer for the given path with append mode
///
/// # Safety
///
/// * `op` is a valid pointer to a `blocking::Operator`.
/// * `path` points to a nul-terminated string, or is null.
/// * `result` is a valid pointer with space for one `FFIResult`. On success
///   `data_ptr` is a boxed `blocking::Writer` released by `free_writer`, on
///   failure `error_message` must be released with `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn blocking_writer_append(
    op: *const od::blocking::Operator,
    path: *const c_char,
    result: *mut FFIResult<blocking::Writer>,
) {
    unsafe {
        let op = if op.is_null() {
            *result = FFIResult::err("Operator is null");
            return;
        } else {
            &*op
        };

        let path_str = match c_str_arg(path) {
            Some(s) => s,
            None => {
                *result = FFIResult::err("Failed to convert path to string");
                return;
            }
        };

        // Create writer with append option
        let opts = od::options::WriteOptions {
            append: true,
            ..Default::default()
        };

        let res = match op.writer_options(path_str, opts) {
            Ok(writer) => FFIResult::ok(writer),
            Err(e) => FFIResult::err_with_source("Failed to create append writer", e),
        };

        *result = res;
    }
}

/// Write data using a blocking writer
///
/// # Safety
///
/// * `writer` is a valid pointer to a `blocking::Writer`.
/// * `bytes` points to a byte array of `len` bytes, or is null when `len == 0`.
/// * `result` is a valid pointer with space for one `FFIResult`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn writer_write(
    writer: *mut blocking::Writer,
    bytes: *const c_char,
    len: usize,
    result: *mut FFIResult<()>,
) {
    unsafe {
        let writer = if writer.is_null() {
            *result = FFIResult::err("Writer is null");
            return;
        } else {
            &mut *writer
        };

        let bytes = match copy_bytes(bytes, len) {
            Some(b) => b.to_vec(),
            None => {
                *result = FFIResult::err("Invalid bytes buffer");
                return;
            }
        };

        let res = match writer.write(bytes) {
            Ok(_) => FFIResult::ok(()),
            Err(e) => FFIResult::err_with_source("Failed to write with writer", e),
        };

        *result = res;
    }
}

/// Close a blocking writer
///
/// # Safety
///
/// * `writer` is a valid pointer to a `blocking::Writer`.
/// * `result` is a valid pointer with space for one `FFIResult`. On success
///   `data_ptr` must be released with `free_metadata`, on failure
///   `error_message` must be released with `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn writer_close(
    writer: *mut blocking::Writer,
    result: *mut FFIResult<Metadata>,
) {
    unsafe {
        let writer = if writer.is_null() {
            *result = FFIResult::err("Writer is null");
            return;
        } else {
            &mut *writer
        };

        let res = match writer.close() {
            Ok(meta) => FFIResult::ok(meta.into()),
            Err(e) => FFIResult::err_with_source("Failed to close writer", e),
        };

        *result = res;
    }
}

/// Free a blocking writer
///
/// # Safety
///
/// * `writer` is a valid pointer to a `blocking::Writer` created by
///   `blocking_writer` or `blocking_writer_append`, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_writer(writer: *mut blocking::Writer) {
    unsafe {
        if !writer.is_null() {
            drop(Box::from_raw(writer));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    use std::ptr;
    use types::{free_bool, free_byteslice, free_metadata, free_string};

    fn memory_operator() -> *mut od::blocking::Operator {
        let operator = od::Operator::via_iter("memory", HashMap::new()).unwrap();
        let handle = RUNTIME.handle();
        let _enter = handle.enter();
        let op = od::blocking::Operator::new(operator).unwrap();
        Box::into_raw(Box::new(op))
    }

    #[test]
    fn ffi_result_layout_matches_abi() {
        // code (padded to usize) + data_ptr + error_message.
        assert_eq!(
            std::mem::size_of::<FFIResult<()>>(),
            std::mem::size_of::<usize>() * 3
        );
    }

    #[test]
    fn via_map_ffi_rejects_null_config_entry() {
        unsafe {
            let scheme = c"memory";
            let keys: [*const c_char; 1] = [ptr::null()];
            let values: [*const c_char; 1] = [c"/".as_ptr()];

            let mut result = FFIResult::<od::blocking::Operator>::default();
            via_map_ffi(
                scheme.as_ptr(),
                keys.as_ptr(),
                values.as_ptr(),
                1,
                None,
                &mut result,
            );
            assert_eq!(result.code, result::FFIErrorCode::FFIError);
            assert!(result.data_ptr.is_null());
            assert!(!result.error_message.is_null());
            free_string(result.error_message);
        }
    }

    #[test]
    fn via_map_ffi_rejects_null_config_arrays() {
        unsafe {
            let scheme = c"memory";
            let mut result = FFIResult::<od::blocking::Operator>::default();
            via_map_ffi(
                scheme.as_ptr(),
                ptr::null(),
                ptr::null(),
                2,
                None,
                &mut result,
            );
            assert_eq!(result.code, result::FFIErrorCode::FFIError);
            free_string(result.error_message);
        }
    }

    #[test]
    fn via_map_ffi_builds_memory_operator() {
        unsafe {
            let scheme = c"memory";
            let mut result = FFIResult::<od::blocking::Operator>::default();
            via_map_ffi(
                scheme.as_ptr(),
                ptr::null(),
                ptr::null(),
                0,
                None,
                &mut result,
            );
            assert_eq!(result.code, result::FFIErrorCode::Ok);
            assert!(!result.data_ptr.is_null());
            free_operator(result.data_ptr);
        }
    }

    #[test]
    fn write_then_read_roundtrip_releases_all_buffers() {
        unsafe {
            let op = memory_operator();
            let path = c"ffi-test/file.bin";

            let payload: Vec<u8> = (0u8..=255).collect();
            let mut write_result = FFIResult::<()>::default();
            blocking_write(
                op,
                path.as_ptr(),
                payload.as_ptr() as *const c_char,
                payload.len(),
                &mut write_result,
            );
            assert_eq!(write_result.code, result::FFIErrorCode::Ok);

            let mut read_result = FFIResult::<ByteSlice>::default();
            blocking_read(op, path.as_ptr(), &mut read_result);
            assert_eq!(read_result.code, result::FFIErrorCode::Ok);
            assert!(!read_result.data_ptr.is_null());
            let slice = &*read_result.data_ptr;
            assert_eq!(slice.len, payload.len());
            let view = std::slice::from_raw_parts(slice.data as *const u8, slice.len);
            assert_eq!(view, payload.as_slice());
            free_byteslice(read_result.data_ptr);

            free_operator(op);
        }
    }

    #[test]
    fn read_empty_write_roundtrip() {
        unsafe {
            let op = memory_operator();
            let path = c"ffi-test/empty.bin";

            let mut write_result = FFIResult::<()>::default();
            blocking_write(op, path.as_ptr(), ptr::null(), 0, &mut write_result);
            assert_eq!(write_result.code, result::FFIErrorCode::Ok);

            let mut read_result = FFIResult::<ByteSlice>::default();
            blocking_read(op, path.as_ptr(), &mut read_result);
            assert_eq!(read_result.code, result::FFIErrorCode::Ok);
            assert_eq!((*read_result.data_ptr).len, 0);
            free_byteslice(read_result.data_ptr);

            free_operator(op);
        }
    }

    #[test]
    fn read_missing_key_returns_error_and_message_is_freed() {
        unsafe {
            let op = memory_operator();
            let path = c"ffi-test/missing";

            let mut read_result = FFIResult::<ByteSlice>::default();
            blocking_read(op, path.as_ptr(), &mut read_result);
            assert_eq!(read_result.code, result::FFIErrorCode::NotFound);
            assert!(read_result.data_ptr.is_null());
            assert!(!read_result.error_message.is_null());
            free_string(read_result.error_message);

            free_operator(op);
        }
    }

    #[test]
    fn null_operator_reports_error_instead_of_crashing() {
        unsafe {
            let path = c"file";
            let mut read_result = FFIResult::<ByteSlice>::default();
            blocking_read(ptr::null(), path.as_ptr(), &mut read_result);
            assert_eq!(read_result.code, result::FFIErrorCode::FFIError);
            free_string(read_result.error_message);
        }
    }

    #[test]
    fn is_exist_result_is_released_by_free_bool() {
        unsafe {
            let op = memory_operator();
            let path = c"ffi-test/exists";
            let mut write_result = FFIResult::<()>::default();
            blocking_write(op, path.as_ptr(), c"x".as_ptr(), 1, &mut write_result);
            assert_eq!(write_result.code, result::FFIErrorCode::Ok);

            let mut exists_result = FFIResult::<bool>::default();
            blocking_is_exist(op, path.as_ptr(), &mut exists_result);
            assert_eq!(exists_result.code, result::FFIErrorCode::Ok);
            assert!(*exists_result.data_ptr);
            free_bool(exists_result.data_ptr);

            free_operator(op);
        }
    }

    #[test]
    fn stat_result_is_released_by_free_metadata() {
        unsafe {
            let op = memory_operator();
            let path = c"ffi-test/meta";
            let mut write_result = FFIResult::<()>::default();
            blocking_write(op, path.as_ptr(), c"abc".as_ptr(), 3, &mut write_result);
            assert_eq!(write_result.code, result::FFIErrorCode::Ok);

            let mut stat_result = FFIResult::<Metadata>::default();
            blocking_stat(op, path.as_ptr(), &mut stat_result);
            assert_eq!(stat_result.code, result::FFIErrorCode::Ok);
            assert_eq!((*stat_result.data_ptr).content_length, 3);
            free_metadata(stat_result.data_ptr);

            free_operator(op);
        }
    }

    #[test]
    fn operator_info_returns_owned_scheme_string() {
        unsafe {
            let op = memory_operator();
            let mut info_result = FFIResult::<c_char>::default();
            operator_info(op, &mut info_result);
            assert_eq!(info_result.code, result::FFIErrorCode::Ok);
            assert_eq!(CStr::from_ptr(info_result.data_ptr).to_bytes(), b"memory");
            free_string(info_result.data_ptr);
            free_operator(op);
        }
    }

    #[test]
    fn list_and_next_release_every_allocation() {
        unsafe {
            let op = memory_operator();
            let path_a = c"ffi-list/a";
            let path_b = c"ffi-list/b";
            let dir = c"ffi-list/";
            for path in [path_a, path_b] {
                let mut write_result = FFIResult::<()>::default();
                blocking_write(op, path.as_ptr(), c"x".as_ptr(), 1, &mut write_result);
                assert_eq!(write_result.code, result::FFIErrorCode::Ok);
            }

            let mut list_result = FFIResult::<blocking::Lister>::default();
            blocking_list(op, dir.as_ptr(), &mut list_result);
            assert_eq!(list_result.code, result::FFIErrorCode::Ok);
            let lister = list_result.data_ptr;
            assert!(!lister.is_null());

            let mut paths = Vec::new();
            loop {
                let mut next_result = FFIResult::<c_char>::default();
                lister_next(lister, &mut next_result);
                assert_eq!(next_result.code, result::FFIErrorCode::Ok);
                if next_result.data_ptr.is_null() {
                    break;
                }
                paths.push(
                    CStr::from_ptr(next_result.data_ptr)
                        .to_string_lossy()
                        .into_owned(),
                );
                free_string(next_result.data_ptr);
            }
            assert!(paths.contains(&"ffi-list/a".to_string()));
            assert!(paths.contains(&"ffi-list/b".to_string()));

            free_lister(lister);
            free_operator(op);
        }
    }

    #[test]
    fn writer_roundtrip_releases_all_buffers() {
        unsafe {
            let op = memory_operator();
            let path = c"ffi-test/writer";

            let mut writer_result = FFIResult::<blocking::Writer>::default();
            blocking_writer(op, path.as_ptr(), &mut writer_result);
            assert_eq!(writer_result.code, result::FFIErrorCode::Ok);
            let writer = writer_result.data_ptr;
            assert!(!writer.is_null());

            let payload = c"hello writer";
            let mut write_result = FFIResult::<()>::default();
            writer_write(
                writer,
                payload.as_ptr(),
                payload.count_bytes(),
                &mut write_result,
            );
            assert_eq!(write_result.code, result::FFIErrorCode::Ok);

            let mut close_result = FFIResult::<Metadata>::default();
            writer_close(writer, &mut close_result);
            assert_eq!(close_result.code, result::FFIErrorCode::Ok);
            free_metadata(close_result.data_ptr);

            free_writer(writer);
            free_operator(op);
        }
    }

    #[test]
    fn concurrent_operator_ffi_calls_share_const_pointer() {
        struct SharedOp(*const od::blocking::Operator);
        // SAFETY: `od::blocking::Operator` is `Send + Sync`, and all FFI
        // entrypoints take `*const od::blocking::Operator` and only borrow `&*op`.
        unsafe impl Sync for SharedOp {}

        unsafe {
            let op = memory_operator();
            let shared = SharedOp(op.cast_const());

            std::thread::scope(|scope| {
                for worker in 0..8usize {
                    let shared = &shared;
                    scope.spawn(move || {
                        let path =
                            std::ffi::CString::new(format!("concurrent/worker-{worker}")).unwrap();
                        let payload = format!("payload-{worker}");

                        let mut write_result = FFIResult::<()>::default();
                        blocking_write(
                            shared.0,
                            path.as_ptr(),
                            payload.as_ptr() as *const c_char,
                            payload.len(),
                            &mut write_result,
                        );
                        assert_eq!(write_result.code, result::FFIErrorCode::Ok);

                        let mut exists_result = FFIResult::<bool>::default();
                        blocking_is_exist(shared.0, path.as_ptr(), &mut exists_result);
                        assert_eq!(exists_result.code, result::FFIErrorCode::Ok);
                        assert!(*exists_result.data_ptr);
                        free_bool(exists_result.data_ptr);

                        let mut stat_result = FFIResult::<Metadata>::default();
                        blocking_stat(shared.0, path.as_ptr(), &mut stat_result);
                        assert_eq!(stat_result.code, result::FFIErrorCode::Ok);
                        assert_eq!((*stat_result.data_ptr).content_length, payload.len() as u64);
                        free_metadata(stat_result.data_ptr);

                        let mut read_result = FFIResult::<ByteSlice>::default();
                        blocking_read(shared.0, path.as_ptr(), &mut read_result);
                        assert_eq!(read_result.code, result::FFIErrorCode::Ok);
                        let slice = &*read_result.data_ptr;
                        let view = std::slice::from_raw_parts(slice.data as *const u8, slice.len);
                        assert_eq!(view, payload.as_bytes());
                        free_byteslice(read_result.data_ptr);

                        let mut info_result = FFIResult::<c_char>::default();
                        operator_info(shared.0, &mut info_result);
                        assert_eq!(info_result.code, result::FFIErrorCode::Ok);
                        assert_eq!(CStr::from_ptr(info_result.data_ptr).to_bytes(), b"memory");
                        free_string(info_result.data_ptr);

                        let mut delete_result = FFIResult::<()>::default();
                        blocking_delete(shared.0, path.as_ptr(), &mut delete_result);
                        assert_eq!(delete_result.code, result::FFIErrorCode::Ok);
                    });
                }
            });

            free_operator(op);
        }
    }
}
