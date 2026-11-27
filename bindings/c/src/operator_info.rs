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

use std::ffi::CString;
use std::ffi::{c_char, c_void};

use ::opendal as core;

use crate::opendal_capability;
use crate::opendal_operator;

/// \brief Metadata for **operator**, users can use this metadata to get information
/// of operator.
#[repr(C)]
pub struct opendal_operator_info {
    /// The pointer to the opendal::OperatorInfo in the Rust code.
    /// Only touch this on judging whether it is NULL.
    inner: *mut c_void,
}

impl opendal_operator_info {
    fn deref(&self) -> &core::OperatorInfo {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &*(self.inner as *mut core::OperatorInfo) }
    }
}

impl opendal_operator_info {
    /// \brief Get information of underlying accessor.
    ///
    /// # Example
    ///
    /// ```C
    /// /// suppose you have a memory-backed opendal_operator* named op
    /// char *scheme;
    /// opendal_operator_info *info = opendal_operator_info_new(op);
    ///
    /// scheme = opendal_operator_info_get_scheme(info);
    /// assert(!strcmp(scheme, "memory"));
    ///
    /// /// free the heap memory
    /// opendal_string_free(scheme);
    /// opendal_operator_info_free(info);
    /// ```
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_info_new(op: &opendal_operator) -> *mut Self {
        let info = op.deref().info();
        Box::into_raw(Box::new(Self {
            inner: Box::into_raw(Box::new(info)) as _,
        }))
    }

    /// \brief Free the heap-allocated opendal_operator_info
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_info_free(ptr: *mut Self) {
        unsafe {
            if !ptr.is_null() {
                drop(Box::from_raw((*ptr).inner as *mut core::OperatorInfo));
                drop(Box::from_raw(ptr));
            }
        }
    }

    /// \brief Return the nul-terminated operator's scheme, i.e. service
    ///
    /// \note: The string is on heap, free it with opendal_string_free()
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_info_get_scheme(&self) -> *mut c_char {
        let scheme = self.deref().scheme().to_string();
        CString::new(scheme)
            .expect("CString::new failed in opendal_operator_info_get_root")
            .into_raw()
    }

    /// \brief Return the nul-terminated operator's working root path
    ///
    /// \note: The string is on heap, free it with opendal_string_free()
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_info_get_root(&self) -> *mut c_char {
        let root = self.deref().root();
        CString::new(root)
            .expect("CString::new failed in opendal_operator_info_get_root")
            .into_raw()
    }

    /// \brief Return the nul-terminated operator backend's name, could be empty if underlying backend has no
    /// namespace concept.
    ///
    /// \note: The string is on heap, free it with opendal_string_free()
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_info_get_name(&self) -> *mut c_char {
        let name = self.deref().name();
        CString::new(name)
            .expect("CString::new failed in opendal_operator_info_get_name")
            .into_raw()
    }

    /// \brief Write the operator's capability into `cap`.
    #[no_mangle]
    pub unsafe extern "C" fn opendal_operator_info_get_capability(
        &self,
        cap: *mut opendal_capability,
    ) {
        assert!(!cap.is_null());
        let capability = self.deref().capability();
        std::ptr::write(cap, capability.into());
    }
}
