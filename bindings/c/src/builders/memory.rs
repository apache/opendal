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

use ::opendal as od;

use super::CastBuilder;

use od::services::Memory;

/// The builder of building [`od::services::Memory`] service.
/// Fields' semantics could be checked in [`od::services::Memory`]
#[repr(C)]
pub struct opendal_builder_memory {
    root: *const c_char,
}

impl opendal_builder_memory {
    /// Construct a new [`opendal_builder_memory`], the memory is allocated on the heap.
    /// The memory will be deallocated when it is used to build the [`opendal_operator_ptr`],
    /// i.e. will become invalid.
    #[no_mangle]
    pub extern "C" fn opendal_builder_memory_new() -> *mut Self {
        let b = opendal_builder_memory {
            root: std::ptr::null(),
        };
        Box::leak(Box::new(b))
    }

    /// Set the `root` of the [`od::services::Memory`] service
    #[no_mangle]
    pub extern "C" fn opendal_builder_memory_set_root(&self, root: *const c_char) -> *mut Self {
        let mut b = unsafe { Box::from_raw(self as *const _ as *mut Self) };
        b.root = root;
        Box::leak(b)
    }
}

impl CastBuilder for opendal_builder_memory {
    type CastTo = Memory;

    fn cast_to_builder(&self) -> Memory {
        let boxed = unsafe { Box::from_raw(self as *const _ as *mut Self) };
        let root = unsafe { std::ffi::CStr::from_ptr(boxed.root).to_str().unwrap() };

        let mut b = Memory::default();
        b.root(root);
        b
    }
}
