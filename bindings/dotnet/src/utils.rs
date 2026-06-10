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

use std::os::raw::c_char;
use std::{collections::HashMap, ffi::CStr};

use crate::error::{ErrorCode, OpenDALError};

pub fn cstr_to_str<'a>(value: *const c_char) -> Option<&'a str> {
    if value.is_null() {
        return None;
    }

    let cstr = unsafe { std::ffi::CStr::from_ptr(value) };
    cstr.to_str().ok()
}

pub fn config_invalid_error(message: impl Into<String>) -> OpenDALError {
    OpenDALError::from_error(ErrorCode::ConfigInvalid, message.into())
}

pub fn invalid_utf8_message(field: &str) -> String {
    format!("{field} is null or invalid UTF-8")
}

pub fn invalid_utf8_message_at(field: &str, index: usize) -> String {
    format!("{field} at index {index} is null or invalid UTF-8")
}

pub fn require_cstr<'a>(value: *const c_char, field: &str) -> Result<&'a str, OpenDALError> {
    cstr_to_str(value).ok_or_else(|| {
        OpenDALError::from_error(ErrorCode::ConfigInvalid, invalid_utf8_message(field))
    })
}

pub fn require_operator<'a>(
    op: *const opendal::Operator,
) -> Result<&'a opendal::Operator, OpenDALError> {
    if op.is_null() {
        return Err(config_invalid_error("operator pointer is null"));
    }

    Ok(unsafe { &*op })
}

pub fn require_callback<T>(callback: Option<T>) -> Result<T, OpenDALError> {
    callback.ok_or_else(|| config_invalid_error("callback pointer is null"))
}

pub fn require_data_ptr(data: *const u8, len: usize) -> Result<(), OpenDALError> {
    if len > 0 && data.is_null() {
        return Err(config_invalid_error("data pointer is null while len > 0"));
    }

    Ok(())
}

/// # Safety
///
/// - When `len > 0`, `keys` and `values` must be non-null pointers to arrays
///   containing at least `len` C-string pointers.
/// - Each entry pointer must be non-null and valid UTF-8.
pub unsafe fn collect_options(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
) -> Result<HashMap<String, String>, OpenDALError> {
    if len == 0 {
        return Ok(HashMap::new());
    }

    if keys.is_null() {
        return Err(config_invalid_error("keys pointer is null while len > 0"));
    }

    if values.is_null() {
        return Err(config_invalid_error("values pointer is null while len > 0"));
    }

    let mut map = HashMap::with_capacity(len);
    for index in 0..len {
        let key_ptr = unsafe { *keys.add(index) };
        let value_ptr = unsafe { *values.add(index) };

        if key_ptr.is_null() {
            return Err(config_invalid_error(invalid_utf8_message_at("key", index)));
        }
        if value_ptr.is_null() {
            return Err(config_invalid_error(invalid_utf8_message_at(
                "value", index,
            )));
        }

        let key = unsafe { CStr::from_ptr(key_ptr) }
            .to_str()
            .map_err(|_| config_invalid_error(invalid_utf8_message_at("key", index)))?;
        let value = unsafe { CStr::from_ptr(value_ptr) }
            .to_str()
            .map_err(|_| config_invalid_error(invalid_utf8_message_at("value", index)))?;

        map.insert(key.to_string(), value.to_string());
    }

    Ok(map)
}

pub fn into_string_ptr(message: impl Into<String>) -> *mut c_char {
    match std::ffi::CString::new(message.into()) {
        Ok(msg) => msg.into_raw(),
        Err(_) => std::ffi::CString::new("invalid error message")
            .unwrap()
            .into_raw(),
    }
}
