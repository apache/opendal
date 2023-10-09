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

use ::opendal as od;

use crate::types::opendal_bytes;

/// The wrapper type for opendal's error, wrapped because of the
/// orphan rule
struct opendal_internal_error(od::Error);

/// \brief The error code for all opendal APIs in C binding.
/// \todo The error handling is not complete, the error with error message will be
/// added in the future.
#[repr(C)]
pub(crate) enum opendal_code {
    /// returning it back. For example, s3 returns an internal service error.
    OPENDAL_UNEXPECTED,
    /// Underlying service doesn't support this operation.
    OPENDAL_UNSUPPORTED,
    /// The config for backend is invalid.
    OPENDAL_CONFIG_INVALID,
    /// The given path is not found.
    OPENDAL_NOT_FOUND,
    /// The given path doesn't have enough permission for this operation
    OPENDAL_PERMISSION_DENIED,
    /// The given path is a directory.
    OPENDAL_IS_A_DIRECTORY,
    /// The given path is not a directory.
    OPENDAL_NOT_A_DIRECTORY,
    /// The given path already exists thus we failed to the specified operation on it.
    OPENDAL_ALREADY_EXISTS,
    /// Requests that sent to this path is over the limit, please slow down.
    OPENDAL_RATE_LIMITED,
    /// The given file paths are same.
    OPENDAL_IS_SAME_FILE,
}

impl opendal_internal_error {
    /// Convert the [`od::ErrorKind`] of [`od::Error`] to [`opendal_code`]
    pub(crate) fn error_code(&self) -> opendal_code {
        let e = &self.0;
        match e.kind() {
            od::ErrorKind::Unexpected => opendal_code::OPENDAL_UNEXPECTED,
            od::ErrorKind::Unsupported => opendal_code::OPENDAL_UNSUPPORTED,
            od::ErrorKind::ConfigInvalid => opendal_code::OPENDAL_CONFIG_INVALID,
            od::ErrorKind::NotFound => opendal_code::OPENDAL_NOT_FOUND,
            od::ErrorKind::PermissionDenied => opendal_code::OPENDAL_PERMISSION_DENIED,
            od::ErrorKind::IsADirectory => opendal_code::OPENDAL_IS_A_DIRECTORY,
            od::ErrorKind::NotADirectory => opendal_code::OPENDAL_NOT_A_DIRECTORY,
            od::ErrorKind::AlreadyExists => opendal_code::OPENDAL_ALREADY_EXISTS,
            od::ErrorKind::RateLimited => opendal_code::OPENDAL_RATE_LIMITED,
            od::ErrorKind::IsSameFile => opendal_code::OPENDAL_IS_SAME_FILE,
            // if this is triggered, check the [`core`] crate and add a
            // new error code accordingly
            _ => panic!("The newly added ErrorKind in core crate is not handled in C bindings"),
        }
    }
}

#[repr(C)]
pub struct opendal_error {
    code: opendal_code,
    message: opendal_bytes,
}

// todo: add free
impl opendal_error {
    // The caller should sink the error to heap memory and return the pointer
    // that will not be freed by rustc
    pub(crate) fn from_opendal_error(error: od::Error) -> Self {
        let error = opendal_internal_error(error);
        let code = error.error_code();
        let c_str = format!("{}", error.0);
        let message = opendal_bytes::new(c_str.into_bytes());
        opendal_error { code, message }
    }
}
