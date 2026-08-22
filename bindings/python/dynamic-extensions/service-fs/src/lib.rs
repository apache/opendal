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
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::slice;

use opendal_core::Operator;
use opendal_dynamic_extension_sdk::{
    ByteSlice, ExtensionApiV1, KeyValue, OutputBuffer, RUNTIME_PROTOCOL, STATUS_INVALID_ARGUMENT,
    STATUS_OK, STATUS_OPERATION_FAILED, write_output,
};
use opendal_service_fs::Fs;

static API: ExtensionApiV1 = ExtensionApiV1 {
    struct_size: size_of::<ExtensionApiV1>(),
    required_runtime_protocol: RUNTIME_PROTOCOL,
    opendal_version: ByteSlice::from_static(opendal_core::raw::VERSION),
    package_id: ByteSlice::from_static("opendal-service-fs-poc"),
    component_id: ByteSlice::from_static("fs"),
    entry_symbol: ByteSlice::from_static("opendal_service_fs_bootstrap_v1"),
    create_operator,
    destroy_operator,
    operator_info,
    operator_write,
    operator_read,
};

static TOKIO_RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> =
    std::sync::LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("prototype FS runtime should build")
    });

fn fail(error: *mut OutputBuffer, message: impl AsRef<str>) -> i32 {
    let _ = unsafe { write_output(error, message.as_ref().as_bytes()) };
    STATUS_OPERATION_FAILED
}

unsafe fn option_pairs<'a>(
    options: *const KeyValue,
    options_len: usize,
) -> Result<Vec<(&'a str, &'a str)>, &'static str> {
    if options_len == 0 {
        return Ok(Vec::new());
    }
    if options.is_null() {
        return Err("options pointer is null");
    }
    unsafe { slice::from_raw_parts(options, options_len) }
        .iter()
        .map(|pair| unsafe { Ok((pair.key.as_str()?, pair.value.as_str()?)) })
        .collect()
}

unsafe extern "C" fn create_operator(
    options: *const KeyValue,
    options_len: usize,
    operator: *mut *mut c_void,
    error: *mut OutputBuffer,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        if operator.is_null() {
            return STATUS_INVALID_ARGUMENT;
        }
        *operator = std::ptr::null_mut();
        let options = match option_pairs(options, options_len) {
            Ok(options) => options,
            Err(message) => return fail(error, message),
        };

        let mut builder = Fs::default();
        for (key, value) in options {
            builder = match key {
                "root" => builder.root(value),
                "atomic_write_dir" => builder.atomic_write_dir(value),
                _ => return fail(error, format!("unsupported FS option {key}")),
            };
        }

        match Operator::new(builder) {
            Ok(value) => {
                *operator = Box::into_raw(Box::new(value)).cast::<c_void>();
                STATUS_OK
            }
            Err(err) => fail(error, err.to_string()),
        }
    }))
    .unwrap_or_else(|_| fail(error, "FS operator construction panicked"))
}

unsafe extern "C" fn destroy_operator(operator: *mut c_void) {
    if operator.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(Box::from_raw(operator.cast::<Operator>()));
    }));
}

unsafe fn as_operator<'a>(operator: *mut c_void) -> Result<&'a Operator, &'static str> {
    if operator.is_null() {
        return Err("FS operator is null");
    }
    Ok(unsafe { &*operator.cast::<Operator>() })
}

unsafe extern "C" fn operator_info(
    operator: *mut c_void,
    output: *mut OutputBuffer,
    error: *mut OutputBuffer,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let operator = match as_operator(operator) {
            Ok(operator) => operator,
            Err(message) => return fail(error, message),
        };
        let info = operator.info();
        let value = serde_json::json!({
            "scheme": info.scheme().to_string(),
            "name": info.name(),
            "root": info.root(),
        });
        write_output(output, value.to_string().as_bytes())
    }))
    .unwrap_or_else(|_| fail(error, "FS operator info panicked"))
}

unsafe extern "C" fn operator_write(
    operator: *mut c_void,
    path: ByteSlice,
    data: ByteSlice,
    error: *mut OutputBuffer,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let operator = match as_operator(operator) {
            Ok(operator) => operator,
            Err(message) => return fail(error, message),
        };
        let path = match path.as_str() {
            Ok(path) => path,
            Err(message) => return fail(error, message),
        };
        let data = match data.as_bytes() {
            Ok(data) => data,
            Err(message) => return fail(error, message),
        };
        let _guard = TOKIO_RUNTIME.enter();
        let blocking = match opendal_core::blocking::Operator::new(operator.clone()) {
            Ok(blocking) => blocking,
            Err(err) => return fail(error, err.to_string()),
        };
        match blocking.write(path, data.to_vec()) {
            Ok(_) => STATUS_OK,
            Err(err) => fail(error, err.to_string()),
        }
    }))
    .unwrap_or_else(|_| fail(error, "FS write panicked"))
}

unsafe extern "C" fn operator_read(
    operator: *mut c_void,
    path: ByteSlice,
    output: *mut OutputBuffer,
    error: *mut OutputBuffer,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let operator = match as_operator(operator) {
            Ok(operator) => operator,
            Err(message) => return fail(error, message),
        };
        let path = match path.as_str() {
            Ok(path) => path,
            Err(message) => return fail(error, message),
        };
        let _guard = TOKIO_RUNTIME.enter();
        let blocking = match opendal_core::blocking::Operator::new(operator.clone()) {
            Ok(blocking) => blocking,
            Err(err) => return fail(error, err.to_string()),
        };
        match blocking.read(path) {
            Ok(value) => write_output(output, &value.to_vec()),
            Err(err) => fail(error, err.to_string()),
        }
    }))
    .unwrap_or_else(|_| fail(error, "FS read panicked"))
}

#[unsafe(no_mangle)]
/// Returns the FS extension function table.
///
/// # Safety
///
/// `extension` must point to writable storage for an extension table pointer.
pub unsafe extern "C" fn opendal_service_fs_bootstrap_v1(
    extension: *mut *const ExtensionApiV1,
) -> i32 {
    if extension.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }
    unsafe { *extension = &API };
    STATUS_OK
}
