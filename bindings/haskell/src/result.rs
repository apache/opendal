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

use std::{
    ffi::{c_char, CString},
    ptr,
};

#[repr(C)]
#[derive(Debug)]
pub struct FFIResult<T> {
    success: bool,
    data_ptr: *mut T,
    error_message: *mut c_char,
}

impl<T> FFIResult<T> {
    pub fn ok(data_ptr: *mut T) -> Self {
        FFIResult {
            success: true,
            data_ptr,
            error_message: ptr::null_mut(),
        }
    }

    pub fn err(error_message: &str) -> Self {
        let c_string = CString::new(error_message).unwrap();
        FFIResult {
            success: false,
            data_ptr: ptr::null_mut(),
            error_message: c_string.into_raw(),
        }
    }
}
