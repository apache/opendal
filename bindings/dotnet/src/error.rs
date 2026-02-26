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

use opendal::ErrorKind;

#[repr(i32)]
#[derive(Clone, Copy)]
/// Error codes exposed to the .NET binding.
///
/// The numeric values are part of the FFI contract and must stay stable.
pub enum ErrorCode {
    Unexpected = 0,
    Unsupported = 1,
    ConfigInvalid = 2,
    NotFound = 3,
    PermissionDenied = 4,
    IsADirectory = 5,
    NotADirectory = 6,
    AlreadyExists = 7,
    RateLimited = 8,
    IsSameFile = 9,
    ConditionNotMatch = 10,
    RangeNotSatisfied = 11,
}

impl ErrorCode {
    /// Convert OpenDAL's internal error kind to an FFI-stable error code.
    pub fn from_error_kind(kind: ErrorKind) -> Self {
        match kind {
            ErrorKind::Unexpected => ErrorCode::Unexpected,
            ErrorKind::Unsupported => ErrorCode::Unsupported,
            ErrorKind::ConfigInvalid => ErrorCode::ConfigInvalid,
            ErrorKind::NotFound => ErrorCode::NotFound,
            ErrorKind::PermissionDenied => ErrorCode::PermissionDenied,
            ErrorKind::IsADirectory => ErrorCode::IsADirectory,
            ErrorKind::NotADirectory => ErrorCode::NotADirectory,
            ErrorKind::AlreadyExists => ErrorCode::AlreadyExists,
            ErrorKind::RateLimited => ErrorCode::RateLimited,
            ErrorKind::IsSameFile => ErrorCode::IsSameFile,
            ErrorKind::ConditionNotMatch => ErrorCode::ConditionNotMatch,
            ErrorKind::RangeNotSatisfied => ErrorCode::RangeNotSatisfied,
            _ => ErrorCode::Unexpected,
        }
    }
}
