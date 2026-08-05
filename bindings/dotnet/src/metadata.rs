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

use std::ffi::c_char;

use crate::utils::into_string_ptr;

#[repr(C)]
/// FFI representation of OpenDAL metadata.
pub struct OpendalMetadata {
    pub mode: i32,
    pub content_length: u64,
    pub content_disposition: *mut c_char,
    pub content_md5: *mut c_char,
    pub content_type: *mut c_char,
    pub content_encoding: *mut c_char,
    pub cache_control: *mut c_char,
    pub etag: *mut c_char,
    pub last_modified_has_value: u8,
    pub last_modified_second: i64,
    pub last_modified_nanosecond: i32,
    pub version: *mut c_char,
}

impl OpendalMetadata {
    pub fn from_metadata(metadata: opendal::Metadata) -> Self {
        let mode = match metadata.mode() {
            opendal::EntryMode::FILE => 0,
            opendal::EntryMode::DIR => 1,
            opendal::EntryMode::Unknown => 2,
        };

        let (last_modified_has_value, last_modified_second, last_modified_nanosecond) =
            if let Some(last_modified) = metadata.last_modified() {
                (
                    1,
                    last_modified.into_inner().as_second(),
                    last_modified.into_inner().subsec_nanosecond(),
                )
            } else {
                (0, 0, 0)
            };

        Self {
            mode,
            content_length: metadata.content_length(),
            content_disposition: optional_string_to_ptr(metadata.content_disposition()),
            content_md5: optional_string_to_ptr(metadata.content_md5()),
            content_type: optional_string_to_ptr(metadata.content_type()),
            content_encoding: optional_string_to_ptr(metadata.content_encoding()),
            cache_control: optional_string_to_ptr(metadata.cache_control()),
            etag: optional_string_to_ptr(metadata.etag()),
            last_modified_has_value,
            last_modified_second,
            last_modified_nanosecond,
            version: optional_string_to_ptr(metadata.version()),
        }
    }
}

/// Convert an optional Rust string into an owned UTF-8 C string pointer.
///
/// Returns null when the option is `None`.
fn optional_string_to_ptr(value: Option<&str>) -> *mut c_char {
    value
        .map(|v| into_string_ptr(v.to_string()))
        .unwrap_or(std::ptr::null_mut())
}

/// Release the heap-allocated string fields of a metadata value in place.
///
/// Entries store metadata inline, so their release path frees the strings
/// without also freeing a containing box.
/// # Safety
///
/// - Every string field must be null or produced by `into_string_ptr`.
/// - Must be called at most once for the same value.
pub(crate) unsafe fn metadata_release_fields(metadata: &mut OpendalMetadata) {
    unsafe fn release(field: &mut *mut c_char) {
        if field.is_null() {
            return;
        }

        drop(unsafe { std::ffi::CString::from_raw(*field) });
        *field = std::ptr::null_mut();
    }

    unsafe {
        release(&mut metadata.content_disposition);
        release(&mut metadata.content_md5);
        release(&mut metadata.content_type);
        release(&mut metadata.content_encoding);
        release(&mut metadata.cache_control);
        release(&mut metadata.etag);
        release(&mut metadata.version);
    }
}

/// # Safety
///
/// - `metadata` must be null or a pointer returned by Rust for `OpendalMetadata`.
/// - Must be called at most once for the same pointer.
pub(crate) unsafe fn metadata_free(metadata: *mut OpendalMetadata) {
    if metadata.is_null() {
        return;
    }

    unsafe {
        let mut metadata = Box::from_raw(metadata);
        metadata_release_fields(&mut metadata);
    }
}
