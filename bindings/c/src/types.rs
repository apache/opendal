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

use std::collections::HashMap;
use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;

use opendal::options;
use opendal::Buffer;

/// \brief Frees a heap-allocated string returned by OpenDAL C APIs.
///
/// \note Only pass pointers returned from OpenDAL APIs that transfer string ownership.
#[no_mangle]
pub unsafe extern "C" fn opendal_string_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(unsafe { CString::from_raw(ptr) });
    }
}

/// \brief opendal_bytes carries raw-bytes with its length
///
/// The opendal_bytes type is a C-compatible substitute for Vec type
/// in Rust, it has to be manually freed. You have to call opendal_bytes_free()
/// to free the heap memory to avoid memory leak.
///
/// @see opendal_bytes_free
#[repr(C)]
pub struct opendal_bytes {
    /// Pointing to the byte array on heap
    pub data: *mut u8,
    /// The length of the byte array
    pub len: usize,
    /// The capacity of the byte array
    pub capacity: usize,
}

impl opendal_bytes {
    pub(crate) fn empty() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        }
    }

    /// Construct a [`opendal_bytes`] from the Rust [`Vec`] of bytes
    pub(crate) fn new(b: Buffer) -> Self {
        let mut b = std::mem::ManuallyDrop::new(b.to_vec());
        Self {
            data: b.as_mut_ptr(),
            len: b.len(),
            capacity: b.capacity(),
        }
    }

    /// \brief Frees the heap memory used by the opendal_bytes
    #[no_mangle]
    pub unsafe extern "C" fn opendal_bytes_free(ptr: *mut opendal_bytes) {
        unsafe {
            if !ptr.is_null() {
                let bs = &mut *ptr;
                if !bs.data.is_null() {
                    drop(Vec::from_raw_parts(bs.data, bs.len, bs.capacity));
                    bs.data = std::ptr::null_mut();
                    bs.len = 0;
                    bs.capacity = 0;
                }
            }
        }
    }
}

/// \brief The options for the list operation.
///
/// This struct carries the options for the list operation, including whether to
/// list recursively. Use `opendal_list_options_new()` to construct and
/// `opendal_list_options_free()` to free.
///
/// @see opendal_operator_list_with
/// @see opendal_list_options_new
/// @see opendal_list_options_free
/// @see opendal_list_options_set_recursive
#[repr(C)]
pub struct opendal_list_options {
    /// Whether to list recursively under the prefix; default false.
    pub recursive: bool,
}

impl opendal_list_options {
    /// \brief Construct a heap-allocated opendal_list_options with default values.
    ///
    /// @return A new opendal_list_options with all options set to their defaults.
    ///
    /// @see opendal_list_options_free
    #[no_mangle]
    pub extern "C" fn opendal_list_options_new() -> *mut Self {
        Box::into_raw(Box::new(Self { recursive: false }))
    }

    /// \brief Set the recursive option.
    ///
    /// @param opts The opendal_list_options to modify.
    /// @param recursive Whether to list recursively.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_list_options_set_recursive(
        opts: *mut opendal_list_options,
        recursive: bool,
    ) {
        if !opts.is_null() {
            (*opts).recursive = recursive;
        }
    }

    /// \brief Free the heap memory used by opendal_list_options.
    ///
    /// @param opts The opendal_list_options to free.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_list_options_free(opts: *mut opendal_list_options) {
        if !opts.is_null() {
            drop(Box::from_raw(opts));
        }
    }
}

/// \brief A key-value pair for write user metadata.
#[repr(C)]
pub struct opendal_write_user_metadata_pair {
    /// The metadata key.
    pub key: *const c_char,
    /// The metadata value.
    pub value: *const c_char,
}

/// \brief The options for write operations.
///
/// Use `opendal_write_options_new()` to construct and
/// `opendal_write_options_free()` to free.
#[repr(C)]
pub struct opendal_write_options {
    /// Append data to the existing file.
    pub append: bool,
    /// Cache-Control header value.
    pub cache_control: *const c_char,
    /// Content-Type header value.
    pub content_type: *const c_char,
    /// Content-Disposition header value.
    pub content_disposition: *const c_char,
    /// Content-Encoding header value.
    pub content_encoding: *const c_char,
    /// If-Match header value.
    pub if_match: *const c_char,
    /// If-None-Match header value.
    pub if_none_match: *const c_char,
    /// Only write if target does not exist.
    pub if_not_exists: bool,
    /// Concurrent write operations. `0` means sequential writes
    pub concurrent: usize,
    /// Whether `chunk` has been set.
    pub has_chunk: bool,
    /// Chunk size for buffered writes.
    pub chunk: usize,
    /// User metadata pairs.
    pub user_metadata: *const opendal_write_user_metadata_pair,
    /// User metadata pairs length.
    pub user_metadata_len: usize,
}

impl opendal_write_options {
    /// \brief Construct a heap-allocated opendal_write_options with default values.
    #[no_mangle]
    pub extern "C" fn opendal_write_options_new() -> *mut Self {
        Box::into_raw(Box::new(Self::default()))
    }

    /// \brief Free the heap memory used by opendal_write_options.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_free(opts: *mut opendal_write_options) {
        if !opts.is_null() {
            drop(Box::from_raw(opts));
        }
    }

    /// \brief Set append mode.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_append(
        opts: *mut opendal_write_options,
        append: bool,
    ) {
        if !opts.is_null() {
            (*opts).append = append;
        }
    }

    /// \brief Set Cache-Control.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_cache_control(
        opts: *mut opendal_write_options,
        cache_control: *const c_char,
    ) {
        if !opts.is_null() {
            (*opts).cache_control = cache_control;
        }
    }

    /// \brief Set Content-Type.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_content_type(
        opts: *mut opendal_write_options,
        content_type: *const c_char,
    ) {
        if !opts.is_null() {
            (*opts).content_type = content_type;
        }
    }

    /// \brief Set Content-Disposition.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_content_disposition(
        opts: *mut opendal_write_options,
        content_disposition: *const c_char,
    ) {
        if !opts.is_null() {
            (*opts).content_disposition = content_disposition;
        }
    }

    /// \brief Set Content-Encoding.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_content_encoding(
        opts: *mut opendal_write_options,
        content_encoding: *const c_char,
    ) {
        if !opts.is_null() {
            (*opts).content_encoding = content_encoding;
        }
    }

    /// \brief Set If-Match.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_if_match(
        opts: *mut opendal_write_options,
        if_match: *const c_char,
    ) {
        if !opts.is_null() {
            (*opts).if_match = if_match;
        }
    }

    /// \brief Set If-None-Match.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_if_none_match(
        opts: *mut opendal_write_options,
        if_none_match: *const c_char,
    ) {
        if !opts.is_null() {
            (*opts).if_none_match = if_none_match;
        }
    }

    /// \brief Set if_not_exists.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_if_not_exists(
        opts: *mut opendal_write_options,
        if_not_exists: bool,
    ) {
        if !opts.is_null() {
            (*opts).if_not_exists = if_not_exists;
        }
    }

    /// \brief Set concurrent.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_concurrent(
        opts: *mut opendal_write_options,
        concurrent: usize,
    ) {
        if !opts.is_null() {
            (*opts).concurrent = concurrent;
        }
    }

    /// \brief Set chunk.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_chunk(
        opts: *mut opendal_write_options,
        chunk: usize,
    ) {
        if !opts.is_null() {
            (*opts).has_chunk = true;
            (*opts).chunk = chunk;
        }
    }

    /// \brief Set user metadata.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_write_options_set_user_metadata(
        opts: *mut opendal_write_options,
        pairs: *const opendal_write_user_metadata_pair,
        len: usize,
    ) {
        if !opts.is_null() {
            (*opts).user_metadata = pairs;
            (*opts).user_metadata_len = len;
        }
    }
}

impl Default for opendal_write_options {
    fn default() -> Self {
        Self {
            append: false,
            cache_control: std::ptr::null(),
            content_type: std::ptr::null(),
            content_disposition: std::ptr::null(),
            content_encoding: std::ptr::null(),
            if_match: std::ptr::null(),
            if_none_match: std::ptr::null(),
            if_not_exists: false,
            concurrent: 0,
            has_chunk: false,
            chunk: 0,
            user_metadata: std::ptr::null(),
            user_metadata_len: 0,
        }
    }
}

unsafe fn optional_cstr(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        Some(
            CStr::from_ptr(ptr)
                .to_str()
                .expect("malformed option")
                .to_string(),
        )
    }
}

impl From<&opendal_write_options> for options::WriteOptions {
    fn from(value: &opendal_write_options) -> Self {
        let user_metadata = if value.user_metadata.is_null() || value.user_metadata_len == 0 {
            None
        } else {
            let pairs =
                unsafe { std::slice::from_raw_parts(value.user_metadata, value.user_metadata_len) };
            let mut metadata = HashMap::with_capacity(pairs.len());
            for pair in pairs {
                if pair.key.is_null() || pair.value.is_null() {
                    continue;
                }
                let key = unsafe { CStr::from_ptr(pair.key) }
                    .to_str()
                    .expect("malformed user metadata key")
                    .to_string();
                let value = unsafe { CStr::from_ptr(pair.value) }
                    .to_str()
                    .expect("malformed user metadata value")
                    .to_string();
                metadata.insert(key, value);
            }
            Some(metadata)
        };

        Self {
            append: value.append,
            cache_control: unsafe { optional_cstr(value.cache_control) },
            content_type: unsafe { optional_cstr(value.content_type) },
            content_disposition: unsafe { optional_cstr(value.content_disposition) },
            content_encoding: unsafe { optional_cstr(value.content_encoding) },
            user_metadata,
            if_match: unsafe { optional_cstr(value.if_match) },
            if_none_match: unsafe { optional_cstr(value.if_none_match) },
            if_not_exists: value.if_not_exists,
            concurrent: value.concurrent,
            chunk: value.has_chunk.then_some(value.chunk),
        }
    }
}

impl Drop for opendal_bytes {
    fn drop(&mut self) {
        unsafe {
            // Safety: the pointer is always valid
            Self::opendal_bytes_free(self);
        }
    }
}

impl From<&opendal_bytes> for Buffer {
    fn from(v: &opendal_bytes) -> Self {
        let slice = unsafe { std::slice::from_raw_parts(v.data, v.len) };
        Buffer::from(bytes::Bytes::copy_from_slice(slice))
    }
}

/// \brief The configuration for the initialization of opendal_operator.
///
/// \note This is also a heap-allocated struct, please free it after you use it
///
/// @see opendal_operator_new has an example of using opendal_operator_options
/// @see opendal_operator_options_new This function construct the operator
/// @see opendal_operator_options_free This function frees the heap memory of the operator
/// @see opendal_operator_options_set This function allow you to set the options
#[repr(C)]
pub struct opendal_operator_options {
    /// The pointer to the HashMap<String, String> in the Rust code.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_operator_options {
    pub(crate) fn deref(&self) -> &HashMap<String, String> {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &*(self.inner as *mut HashMap<String, String>) }
    }

    pub(crate) fn deref_mut(&mut self) -> &mut HashMap<String, String> {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.inner as *mut HashMap<String, String>) }
    }
}

impl opendal_operator_options {
    /// \brief Construct a heap-allocated opendal_operator_options
    ///
    /// @return An empty opendal_operator_option, which could be set by
    /// opendal_operator_option_set().
    ///
    /// @see opendal_operator_option_set
    #[no_mangle]
    pub extern "C" fn opendal_operator_options_new() -> *mut Self {
        let map: HashMap<String, String> = HashMap::default();
        let options = Self {
            inner: Box::into_raw(Box::new(map)) as _,
        };
        Box::into_raw(Box::new(options))
    }

    /// \brief Set a Key-Value pair inside opendal_operator_options
    ///
    /// # Safety
    ///
    /// This function is unsafe because it dereferences and casts the raw pointers
    /// Make sure the pointer of `key` and `value` point to a valid string.
    ///
    /// # Example
    ///
    /// ```C
    /// opendal_operator_options *options = opendal_operator_options_new();
    /// opendal_operator_options_set(options, "root", "/myroot");
    ///
    /// // .. use your opendal_operator_options
    ///
    /// opendal_operator_options_free(options);
    /// ```
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_options_set(
        &mut self,
        key: *const c_char,
        value: *const c_char,
    ) {
        let k = unsafe { std::ffi::CStr::from_ptr(key) }
            .to_str()
            .unwrap()
            .to_string();
        let v = unsafe { std::ffi::CStr::from_ptr(value) }
            .to_str()
            .unwrap()
            .to_string();
        self.deref_mut().insert(k, v);
    }

    /// \brief Free the allocated memory used by [`opendal_operator_options`]
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_options_free(ptr: *mut opendal_operator_options) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut HashMap<String, String>));
                drop(Box::from_raw(ptr));
            }
        }
    }
}
