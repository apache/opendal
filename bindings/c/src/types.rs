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
use std::mem;
use std::os::raw::c_char;

use ::opendal as od;

/// \brief Used to access almost all OpenDAL APIs. It represents a
/// operator that provides the unified interfaces provided by OpenDAL.
///
/// @see opendal_operator_new This function construct the operator
/// @see opendal_operator_free This function frees the heap memory of the operator
///
/// \note The opendal_operator_ptr actually owns a pointer to
/// a opendal::BlockingOperator, which is inside the Rust core code.
///
/// \remark You may use the field `ptr` to check whether this is a NULL
/// operator.
#[repr(C)]
pub struct opendal_operator_ptr {
    /// The pointer to the opendal::BlockingOperator in the Rust code.
    /// Only touch this on judging whether it is NULL.
    ptr: *const od::BlockingOperator,
}

impl opendal_operator_ptr {
    /// \brief Free the heap-allocated operator pointed by opendal_operator_ptr.
    ///
    /// Please only use this for a pointer pointing at a valid opendal_operator_ptr.
    /// Calling this function on NULL does nothing, but calling this function on pointers
    /// of other type will lead to segfault.
    ///
    /// # Example
    ///
    /// ```C
    /// opendal_operator_ptr *ptr = opendal_operator_new("fs", NULL);
    /// // ... use this ptr, maybe some reads and writes
    ///
    /// // free this operator
    /// opendal_operator_free(ptr);
    /// ```
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_free(op: *const opendal_operator_ptr) {
        if op.is_null() || unsafe { (*op).ptr.is_null() } {
            return;
        }
        let _ = unsafe { Box::from_raw((*op).ptr as *mut od::BlockingOperator) };
        let _ = unsafe { Box::from_raw(op as *mut opendal_operator_ptr) };
    }
}

impl opendal_operator_ptr {
    /// Returns a reference to the underlying [`od::BlockingOperator`]
    pub(crate) fn as_ref(&self) -> &od::BlockingOperator {
        unsafe { &*(self.ptr) }
    }
}

#[allow(clippy::from_over_into)]
impl From<&od::BlockingOperator> for opendal_operator_ptr {
    fn from(value: &od::BlockingOperator) -> Self {
        Self { ptr: value }
    }
}

#[allow(clippy::from_over_into)]
impl From<&mut od::BlockingOperator> for opendal_operator_ptr {
    fn from(value: &mut od::BlockingOperator) -> Self {
        Self { ptr: value }
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
    pub data: *const u8,
    /// The length of the byte array
    pub len: usize,
}

impl opendal_bytes {
    /// \brief Frees the heap memory used by the opendal_bytes
    #[no_mangle]
    pub extern "C" fn opendal_bytes_free(&self) {
        unsafe {
            // this deallocates the vector by reconstructing the vector and letting
            // it be dropped when its out of scope
            Vec::from_raw_parts(self.data as *mut u8, self.len, self.len);
        }
    }
}

impl opendal_bytes {
    /// Construct a [`opendal_bytes`] from the Rust [`Vec`] of bytes
    pub(crate) fn from_vec(vec: Vec<u8>) -> Self {
        let data = vec.as_ptr() as *const u8;
        let len = vec.len();
        std::mem::forget(vec); // To avoid deallocation of the vec.
        Self { data, len }
    }
}

#[allow(clippy::from_over_into)]
impl Into<bytes::Bytes> for opendal_bytes {
    fn into(self) -> bytes::Bytes {
        let slice = unsafe { std::slice::from_raw_parts(self.data, self.len) };
        bytes::Bytes::copy_from_slice(slice)
    }
}

/// \brief Carries all metadata associated with a path.
///
/// The metadata of the "thing" under a path. Please **only** use the opendal_metadata
/// with our provided API, e.g. opendal_metadata_content_length().
///
/// \note The metadata is also heap-allocated, please call opendal_metadata_free() on this
/// to free the heap memory.
///
/// @see opendal_metadata_free
#[repr(C)]
pub struct opendal_metadata {
    /// The pointer to the opendal::Metadata in the Rust code.
    /// Only touch this on judging whether it is NULL.
    pub inner: *mut od::Metadata,
}

impl opendal_metadata {
    /// Convert a Rust core [`od::Metadata`] into a heap allocated C-compatible
    /// [`opendal_metadata`]
    pub(crate) fn new(m: od::Metadata) -> Self {
        Self {
            inner: Box::into_raw(Box::new(m)),
        }
    }

    /// \brief Free the heap-allocated metadata used by opendal_metadata
    #[no_mangle]
    pub extern "C" fn opendal_metadata_free(&self) {
        if self.inner.is_null() {
            return;
        }

        unsafe {
            mem::drop(Box::from_raw(self.inner));
        }
    }

    /// \brief Return the content_length of the metadata
    ///
    /// # Example
    /// ```C
    /// // ... previously you wrote "Hello, World!" to path "/testpath"
    /// opendal_result_stat s = opendal_operator_stat(ptr, "/testpath");
    /// assert(s.code == OPENDAL_OK);
    ///
    /// opendal_metadata *meta = s.meta;
    /// assert(opendal_metadata_content_length(meta) == 13);
    /// ```
    #[no_mangle]
    pub extern "C" fn opendal_metadata_content_length(&self) -> u64 {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { (&*self.inner).content_length() }
    }

    /// \brief Return whether the path represents a file
    ///
    /// # Example
    /// ```C
    /// // ... previously you wrote "Hello, World!" to path "/testpath"
    /// opendal_result_stat s = opendal_operator_stat(ptr, "/testpath");
    /// assert(s.code == OPENDAL_OK);
    ///
    /// opendal_metadata *meta = s.meta;
    /// assert(opendal_metadata_is_file(meta));
    /// ```
    #[no_mangle]
    pub extern "C" fn opendal_metadata_is_file(&self) -> bool {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        let m = unsafe { &*self.inner };

        m.is_file()
    }

    /// \brief Return whether the path represents a directory
    ///
    /// # Example
    /// ```C
    /// // ... previously you wrote "Hello, World!" to path "/testpath"
    /// opendal_result_stat s = opendal_operator_stat(ptr, "/testpath");
    /// assert(s.code == OPENDAL_OK);
    ///
    /// opendal_metadata *meta = s.meta;
    ///
    /// // this is not a directory
    /// assert(!opendal_metadata_is_dir(meta));
    /// ```
    ///
    /// \todo This is not a very clear example. A clearer example will be added
    /// after we support opendal_operator_mkdir()
    #[no_mangle]
    pub extern "C" fn opendal_metadata_is_dir(&self) -> bool {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { (&*self.inner).is_dir() }
    }
}

/// \brief The configuration for the initialization of opendal_operator_ptr.
///
/// \note This is also a heap-allocated struct, please free it after you use it
///
/// @see opendal_operator_new has an example of using opendal_operator_options
/// @see opendal_operator_options_new This function construct the operator
/// @see opendal_operator_options_free This function frees the heap memory of the operator
/// @see opendal_operator_options_set This function allow you to set the options
#[repr(C)]
pub struct opendal_operator_options {
    /// The pointer to the Rust HashMap<String, String>
    /// Only touch this on judging whether it is NULL.
    inner: *mut HashMap<String, String>,
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
            inner: Box::leak(Box::new(map)),
        };

        Box::leak(Box::new(options))
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
        (*self.inner).insert(k, v);
    }

    /// Returns a reference to the underlying [`HashMap<String, String>`]
    pub(crate) fn as_ref(&self) -> &HashMap<String, String> {
        unsafe { &*(self.inner) }
    }

    /// \brief Free the allocated memory used by [`opendal_operator_options`]
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_options_free(
        options: *const opendal_operator_options,
    ) {
        if options.is_null() || unsafe { (*options).inner.is_null() } {
            return;
        }
        let _ = unsafe { Box::from_raw((*options).inner as *mut HashMap<String, String>) };
        let _ = unsafe { Box::from_raw(options as *mut opendal_operator_options) };
    }
}
