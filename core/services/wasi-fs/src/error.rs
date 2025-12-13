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

use opendal_core::*;
use wasi::filesystem::types::ErrorCode;

/// Convert WASI filesystem errors to OpenDAL errors
pub fn parse_wasi_error(code: ErrorCode) -> Error {
    let (kind, message) = match code {
        ErrorCode::Access => (ErrorKind::PermissionDenied, "Access denied"),
        ErrorCode::WouldBlock => (ErrorKind::RateLimited, "Operation would block"),
        ErrorCode::Already => (ErrorKind::AlreadyExists, "Resource already exists"),
        ErrorCode::BadDescriptor => (ErrorKind::Unexpected, "Bad file descriptor"),
        ErrorCode::Busy => (ErrorKind::RateLimited, "Resource busy"),
        ErrorCode::Deadlock => (ErrorKind::Unexpected, "Deadlock detected"),
        ErrorCode::Quota => (ErrorKind::RateLimited, "Quota exceeded"),
        ErrorCode::Exist => (ErrorKind::AlreadyExists, "File exists"),
        ErrorCode::FileTooLarge => (ErrorKind::ContentTooLarge, "File too large"),
        ErrorCode::IllegalByteSequence => (ErrorKind::InvalidInput, "Invalid byte sequence"),
        ErrorCode::InProgress => (ErrorKind::RateLimited, "Operation in progress"),
        ErrorCode::Interrupted => (ErrorKind::Unexpected, "Operation interrupted"),
        ErrorCode::Invalid => (ErrorKind::InvalidInput, "Invalid argument"),
        ErrorCode::Io => (ErrorKind::Unexpected, "I/O error"),
        ErrorCode::IsDirectory => (ErrorKind::IsADirectory, "Is a directory"),
        ErrorCode::Loop => (ErrorKind::Unexpected, "Too many symbolic links"),
        ErrorCode::TooManyLinks => (ErrorKind::Unexpected, "Too many links"),
        ErrorCode::MessageSize => (ErrorKind::InvalidInput, "Message too large"),
        ErrorCode::NameTooLong => (ErrorKind::InvalidInput, "Name too long"),
        ErrorCode::NoDevice => (ErrorKind::NotFound, "No such device"),
        ErrorCode::NoEntry => (ErrorKind::NotFound, "No such file or directory"),
        ErrorCode::NoLock => (ErrorKind::Unexpected, "No locks available"),
        ErrorCode::InsufficientMemory => (ErrorKind::Unexpected, "Out of memory"),
        ErrorCode::InsufficientSpace => (ErrorKind::IsFull, "No space left on device"),
        ErrorCode::NotDirectory => (ErrorKind::NotADirectory, "Not a directory"),
        ErrorCode::NotEmpty => (ErrorKind::Unexpected, "Directory not empty"),
        ErrorCode::NotRecoverable => (ErrorKind::Unexpected, "State not recoverable"),
        ErrorCode::Unsupported => (ErrorKind::Unsupported, "Operation not supported"),
        ErrorCode::NoTty => (ErrorKind::Unexpected, "Not a terminal"),
        ErrorCode::NoSuchDevice => (ErrorKind::NotFound, "No such device"),
        ErrorCode::Overflow => (ErrorKind::Unexpected, "Value overflow"),
        ErrorCode::NotPermitted => (ErrorKind::PermissionDenied, "Operation not permitted"),
        ErrorCode::Pipe => (ErrorKind::Unexpected, "Broken pipe"),
        ErrorCode::ReadOnly => (ErrorKind::PermissionDenied, "Read-only filesystem"),
        ErrorCode::InvalidSeek => (ErrorKind::InvalidInput, "Invalid seek"),
        ErrorCode::TextFileBusy => (ErrorKind::RateLimited, "Text file busy"),
        ErrorCode::CrossDevice => (ErrorKind::Unsupported, "Cross-device link"),
    };

    Error::new(kind, message).with_context("source", "wasi-filesystem")
}

/// Convert WASI I/O stream errors
pub fn parse_stream_error(err: wasi::io::streams::StreamError) -> Error {
    match err {
        wasi::io::streams::StreamError::LastOperationFailed(e) => {
            Error::new(ErrorKind::Unexpected, "Stream operation failed")
                .with_context("details", format!("{:?}", e))
        }
        wasi::io::streams::StreamError::Closed => {
            Error::new(ErrorKind::Unexpected, "Stream closed")
        }
    }
}
