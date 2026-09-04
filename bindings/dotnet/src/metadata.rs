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
///
/// Fields follow the accessor order of `opendal::Metadata`.
pub struct OpendalMetadata {
    pub mode: i32,
    /// `1` when the service reported whether this version is the current one.
    pub is_current_has_value: u8,
    /// Meaningful only when `is_current_has_value` is `1`.
    pub is_current: u8,
    /// `1` when this metadata describes a deleted object or delete marker.
    pub is_deleted: u8,
    pub cache_control: *mut c_char,
    pub content_length: u64,
    pub content_md5: *mut c_char,
    pub content_type: *mut c_char,
    pub content_encoding: *mut c_char,
    pub last_modified_has_value: u8,
    pub last_modified_second: i64,
    pub last_modified_nanosecond: i32,
    pub etag: *mut c_char,
    pub content_disposition: *mut c_char,
    pub version: *mut c_char,
    /// `1` when the service reported user metadata, even if it is empty.
    pub user_metadata_has_value: u8,
    /// Parallel arrays of `user_metadata_len` owned C strings, or null when
    /// there are no pairs. Keys and values share the same index.
    pub user_metadata_keys: *mut *mut c_char,
    pub user_metadata_values: *mut *mut c_char,
    pub user_metadata_len: usize,
}

impl OpendalMetadata {
    pub fn from_metadata(metadata: opendal::Metadata) -> Self {
        let mode = match metadata.mode() {
            opendal::EntryMode::FILE => 0,
            opendal::EntryMode::DIR => 1,
            opendal::EntryMode::Unknown => 2,
        };

        let (is_current_has_value, is_current) = match metadata.is_current() {
            Some(value) => (1, u8::from(value)),
            None => (0, 0),
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

        let (user_metadata_has_value, user_metadata_keys, user_metadata_values, user_metadata_len) =
            match metadata.user_metadata() {
                Some(user_metadata) => {
                    let (keys, values, len) = string_pairs_to_ptrs(user_metadata);
                    (1, keys, values, len)
                }
                None => (0, std::ptr::null_mut(), std::ptr::null_mut(), 0),
            };

        Self {
            mode,
            is_current_has_value,
            is_current,
            is_deleted: u8::from(metadata.is_deleted()),
            cache_control: optional_string_to_ptr(metadata.cache_control()),
            content_length: metadata.content_length(),
            content_md5: optional_string_to_ptr(metadata.content_md5()),
            content_type: optional_string_to_ptr(metadata.content_type()),
            content_encoding: optional_string_to_ptr(metadata.content_encoding()),
            last_modified_has_value,
            last_modified_second,
            last_modified_nanosecond,
            etag: optional_string_to_ptr(metadata.etag()),
            content_disposition: optional_string_to_ptr(metadata.content_disposition()),
            version: optional_string_to_ptr(metadata.version()),
            user_metadata_has_value,
            user_metadata_keys,
            user_metadata_values,
            user_metadata_len,
        }
    }
}

/// Convert OpenDAL metadata into an owned FFI pointer.
///
/// The returned pointer must be released by `metadata_free`, which
/// `opendal_metadata_result_release` performs for result payloads.
pub fn into_metadata_ptr(metadata: opendal::Metadata) -> *mut OpendalMetadata {
    Box::into_raw(Box::new(OpendalMetadata::from_metadata(metadata)))
}

/// Convert an optional Rust string into an owned UTF-8 C string pointer.
///
/// Returns null when the option is `None`.
fn optional_string_to_ptr(value: Option<&str>) -> *mut c_char {
    value
        .map(|v| into_string_ptr(v.to_string()))
        .unwrap_or(std::ptr::null_mut())
}

/// Convert string pairs into two parallel owned arrays of C strings.
///
/// Both arrays are boxed slices of exactly `len` elements so that
/// `release_string_array` can rebuild them from the raw parts alone. Empty
/// input yields null pointers and a zero length.
fn string_pairs_to_ptrs<'a>(
    pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> (*mut *mut c_char, *mut *mut c_char, usize) {
    let mut keys = Vec::new();
    let mut values = Vec::new();

    for (key, value) in pairs {
        keys.push(into_string_ptr(key));
        values.push(into_string_ptr(value));
    }

    let len = keys.len();
    if len == 0 {
        return (std::ptr::null_mut(), std::ptr::null_mut(), 0);
    }

    let keys = Box::into_raw(keys.into_boxed_slice()) as *mut *mut c_char;
    let values = Box::into_raw(values.into_boxed_slice()) as *mut *mut c_char;
    (keys, values, len)
}

/// Release an owned array of `len` C strings produced by `string_pairs_to_ptrs`.
///
/// # Safety
///
/// - `array` must be null or a pointer produced by `string_pairs_to_ptrs`
///   together with the same `len`.
/// - Must be called at most once for the same pointer.
unsafe fn release_string_array(array: &mut *mut *mut c_char, len: usize) {
    if array.is_null() {
        return;
    }

    if len > 0 {
        let items = unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(*array, len)) };
        for item in items.iter() {
            if !item.is_null() {
                drop(unsafe { std::ffi::CString::from_raw(*item) });
            }
        }
    }

    *array = std::ptr::null_mut();
}

/// Release the heap-allocated string fields of a metadata value in place.
///
/// Entries store metadata inline, so their release path frees the strings
/// without also freeing a containing box.
/// # Safety
///
/// - Every string field must be null or produced by `into_string_ptr`.
/// - The user metadata arrays must be null or produced by
///   `string_pairs_to_ptrs` with `user_metadata_len` elements.
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
        release(&mut metadata.cache_control);
        release(&mut metadata.content_md5);
        release(&mut metadata.content_type);
        release(&mut metadata.content_encoding);
        release(&mut metadata.etag);
        release(&mut metadata.content_disposition);
        release(&mut metadata.version);

        let len = metadata.user_metadata_len;
        release_string_array(&mut metadata.user_metadata_keys, len);
        release_string_array(&mut metadata.user_metadata_values, len);
        metadata.user_metadata_len = 0;
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
