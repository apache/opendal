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

use ::opendal as core;
use std::ffi::c_void;

/// \brief Carries all metadata associated with a **path**.
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
    inner: *mut c_void,
}

impl opendal_metadata {
    fn deref(&self) -> &core::Metadata {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &*(self.inner as *mut core::Metadata) }
    }
}

impl opendal_metadata {
    /// Convert a Rust core [`od::Metadata`] into a heap allocated C-compatible
    /// [`opendal_metadata`]
    pub(crate) fn new(m: core::Metadata) -> Self {
        Self {
            inner: Box::into_raw(Box::new(m)) as _,
        }
    }

    /// \brief Free the heap-allocated metadata used by opendal_metadata
    #[no_mangle]
    pub unsafe extern "C" fn opendal_metadata_free(ptr: *mut opendal_metadata) {
        if !ptr.is_null() {
            drop(Box::from_raw((*ptr).inner as *mut core::Metadata));
            drop(Box::from_raw(ptr));
        }
    }

    /// \brief Return the content_length of the metadata
    ///
    /// # Example
    /// ```C
    /// // ... previously you wrote "Hello, World!" to path "/testpath"
    /// opendal_result_stat s = opendal_operator_stat(op, "/testpath");
    /// assert(s.error == NULL);
    ///
    /// opendal_metadata *meta = s.meta;
    /// assert(opendal_metadata_content_length(meta) == 13);
    /// ```
    #[no_mangle]
    pub extern "C" fn opendal_metadata_content_length(&self) -> u64 {
        self.deref().content_length()
    }

    /// \brief Return whether the path represents a file
    ///
    /// # Example
    /// ```C
    /// // ... previously you wrote "Hello, World!" to path "/testpath"
    /// opendal_result_stat s = opendal_operator_stat(op, "/testpath");
    /// assert(s.error == NULL);
    ///
    /// opendal_metadata *meta = s.meta;
    /// assert(opendal_metadata_is_file(meta));
    /// ```
    #[no_mangle]
    pub extern "C" fn opendal_metadata_is_file(&self) -> bool {
        self.deref().is_file()
    }

    /// \brief Return whether the path represents a directory
    ///
    /// # Example
    /// ```C
    /// // ... previously you wrote "Hello, World!" to path "/testpath"
    /// opendal_result_stat s = opendal_operator_stat(op, "/testpath");
    /// assert(s.error == NULL);
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
        self.deref().is_dir()
    }

    /// \brief Return the last_modified of the metadata, in milliseconds
    ///
    /// # Example
    /// ```C
    /// // ... previously you wrote "Hello, World!" to path "/testpath"
    /// opendal_result_stat s = opendal_operator_stat(op, "/testpath");
    /// assert(s.error == NULL);
    ///
    /// opendal_metadata *meta = s.meta;
    /// assert(opendal_metadata_last_modified_ms(meta) != -1);
    /// ```
    #[no_mangle]
    pub extern "C" fn opendal_metadata_last_modified_ms(&self) -> i64 {
        let mtime = self.deref().last_modified();
        match mtime {
            None => -1,
            Some(time) => time.timestamp_millis(),
        }
    }
}
