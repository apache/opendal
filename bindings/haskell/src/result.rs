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
use std::ffi::c_char;
use std::ptr;

use ::opendal as od;

#[repr(C)]
#[derive(Debug)]
pub struct FFIResult<T> {
    code: FFIErrorCode,
    data_ptr: *mut T,
    error_message: *mut c_char,
}

#[repr(C)]
#[derive(Debug)]
pub enum FFIErrorCode {
    Ok,
    FFIError,
    Unexpected,
    Unsupported,
    ConfigInvalid,
    NotFound,
    PermissionDenied,
    IsADirectory,
    NotADirectory,
    AlreadyExists,
    RateLimited,
    IsSameFile,
}

impl<T> FFIResult<T> {
    pub fn ok(data: T) -> Self {
        FFIResult {
            code: FFIErrorCode::Ok,
            data_ptr: Box::into_raw(Box::new(data)),
            error_message: ptr::null_mut(),
        }
    }

    pub fn err(error_message: &str) -> Self {
        let c_string = CString::new(error_message).unwrap();
        FFIResult {
            code: FFIErrorCode::FFIError,
            data_ptr: ptr::null_mut(),
            error_message: c_string.into_raw(),
        }
    }

    pub fn err_with_source(error_message: &str, source: od::Error) -> Self {
        let msg = format!("{error_message}, source error: {source}");
        let c_string = CString::new(msg).unwrap();
        FFIResult {
            code: source.kind().into(),
            data_ptr: ptr::null_mut(),
            error_message: c_string.into_raw(),
        }
    }
}

impl From<od::ErrorKind> for FFIErrorCode {
    fn from(kind: od::ErrorKind) -> Self {
        match kind {
            od::ErrorKind::Unexpected => FFIErrorCode::Unexpected,
            od::ErrorKind::Unsupported => FFIErrorCode::Unsupported,
            od::ErrorKind::ConfigInvalid => FFIErrorCode::ConfigInvalid,
            od::ErrorKind::NotFound => FFIErrorCode::NotFound,
            od::ErrorKind::PermissionDenied => FFIErrorCode::PermissionDenied,
            od::ErrorKind::IsADirectory => FFIErrorCode::IsADirectory,
            od::ErrorKind::NotADirectory => FFIErrorCode::NotADirectory,
            od::ErrorKind::AlreadyExists => FFIErrorCode::AlreadyExists,
            od::ErrorKind::RateLimited => FFIErrorCode::RateLimited,
            od::ErrorKind::IsSameFile => FFIErrorCode::IsSameFile,
            _ => FFIErrorCode::Unexpected,
        }
    }
}
