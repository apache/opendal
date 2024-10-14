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
use std::ffi::c_void;
use std::os::raw::c_char;

use opendal::Buffer;

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
        if !ptr.is_null() {
            drop(Box::from_raw((*ptr).inner as *mut HashMap<String, String>));
            drop(Box::from_raw(ptr));
        }
    }
}
