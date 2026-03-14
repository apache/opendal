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

use std::ffi::{c_char, c_void};

use crate::metadata::{OpendalMetadata, metadata_free};
use crate::utils::into_string_ptr;

#[repr(C)]
/// FFI representation of an OpenDAL entry.
///
/// String and metadata fields are heap-allocated and owned by Rust until
/// released via `entry_list_free`.
pub struct OpendalEntry {
    pub path: *mut c_char,
    pub metadata: *mut OpendalMetadata,
}

#[repr(C)]
/// FFI representation of a list of entries.
pub struct OpendalEntryList {
    pub entries: *mut *mut OpendalEntry,
    pub len: usize,
}

impl OpendalEntry {
    pub fn from_entry(entry: opendal::Entry) -> Self {
        let path = into_string_ptr(entry.path().to_string());
        let metadata = Box::into_raw(Box::new(OpendalMetadata::from_metadata(
            entry.metadata().clone(),
        )));

        Self { path, metadata }
    }
}

/// Convert OpenDAL entries into an owned FFI list pointer.
///
/// The returned pointer must be released by `entry_list_free`.
pub fn into_entry_list_ptr(entries: Vec<opendal::Entry>) -> *mut c_void {
    let mut entry_ptrs: Vec<*mut OpendalEntry> = entries
        .into_iter()
        .map(|entry| Box::into_raw(Box::new(OpendalEntry::from_entry(entry))))
        .collect();

    let len = entry_ptrs.len();
    let entries_ptr = entry_ptrs.as_mut_ptr();
    std::mem::forget(entry_ptrs);

    Box::into_raw(Box::new(OpendalEntryList {
        entries: entries_ptr,
        len,
    })) as *mut c_void
}

/// Release one FFI entry payload.
///
/// # Safety
///
/// - `entry` must be null or a pointer previously produced by this crate.
/// - This function must be called at most once per non-null pointer.
unsafe fn free_entry(entry: *mut OpendalEntry) {
    if entry.is_null() {
        return;
    }

    unsafe {
        let entry = Box::from_raw(entry);
        if !entry.path.is_null() {
            drop(std::ffi::CString::from_raw(entry.path));
        }
        if !entry.metadata.is_null() {
            metadata_free(entry.metadata);
        }
    }
}

/// # Safety
///
/// - `list` must be null or a pointer returned by Rust as `OpendalEntryList`.
/// - Must be called at most once for the same pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn entry_list_free(list: *mut OpendalEntryList) {
    if list.is_null() {
        return;
    }

    unsafe {
        let list = Box::from_raw(list);
        if !list.entries.is_null() {
            let entries = Vec::from_raw_parts(list.entries, list.len, list.len);
            for entry in entries {
                free_entry(entry);
            }
        }
    }
}
