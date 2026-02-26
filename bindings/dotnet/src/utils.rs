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

/// Convert a C string pointer into `&str`.
///
/// Returns `None` when the pointer is null or not valid UTF-8.
pub fn cstr_to_str<'a>(value: *const c_char) -> Option<&'a str> {
    if value.is_null() {
        return None;
    }

    let cstr = unsafe { std::ffi::CStr::from_ptr(value) };
    cstr.to_str().ok()
}

/// # Safety
///
/// - `data`, `len`, and `capacity` must come from `ByteBuffer::from_vec`.
/// - `buffer_free` must be called at most once for the same allocation.
/// - Callers must not access `data` after this function returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn buffer_free(data: *mut u8, len: usize, capacity: usize) {
    if data.is_null() {
        debug_assert_eq!(len, 0, "len must be zero when data is null");
        debug_assert_eq!(capacity, 0, "capacity must be zero when data is null");
        return;
    }

    if capacity == 0 {
        debug_assert!(
            capacity > 0,
            "capacity must be greater than zero when data is not null"
        );
        return;
    }

    if capacity < len {
        debug_assert!(
            capacity >= len,
            "capacity must be greater than or equal to len"
        );
        return;
    }

    unsafe {
        drop(Vec::from_raw_parts(data, len, capacity));
    }
}

/// # Safety
///
/// - `message` must be a pointer returned by Rust via `CString::into_raw`.
/// - `message_free` must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn message_free(message: *mut c_char) {
    if message.is_null() {
        return;
    }

    unsafe {
        drop(std::ffi::CString::from_raw(message));
    }
}
