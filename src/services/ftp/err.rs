// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::anyhow;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use suppaftp::FtpError;
use suppaftp::Status;

use crate::error::other;
use crate::error::ObjectError;

/// Parse error response into io::Error.
///
/// # TODO
///
/// In the future, we may have our own error struct.
///

// Create error happened during connection to ftp server.
pub fn new_request_connection_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("connection request: {err:?}"),
    ))
}

// Create error happened during swtiching to secure mode.
pub fn new_request_secure_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("switching to secure mode request: {err:?}"),
    ))
}

// Create error happened during signing to ftp server.
pub fn new_request_sign_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("signing request: {err:?}"),
    ))
}

// Create error happened during quitting ftp server.
pub fn new_request_quit_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(op, path, anyhow!("quit request: {err:?}")))
}

// Create error happened during retrieving from ftp server.
pub fn new_request_retr_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("retrieve request: {err:?}"),
    ))
}

// Create error happened during putting to ftp server.
pub fn new_request_put_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(op, path, anyhow!("put request: {err:?}")))
}

// Create error happened during finalizing stream.
pub fn new_request_finalize_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("finalize request: {err:?}"),
    ))
}

// Create error happended uring appending to ftp server.
pub fn new_request_append_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("append request: {err:?}"),
    ))
}

// Create error happened during removeing from ftp server.
pub fn new_request_remove_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("remove request: {err:?}"),
    ))
}

// Create error happened during listing from ftp server.
pub fn new_request_list_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(op, path, anyhow!("list request: {err:?}")))
}

// Create error happened during making directory to ftp server.
pub fn new_request_mkdir_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("mkdir request: {err:?}"),
    ))
}

// Create error happened during  retrieving modification time of the file to ftp server.
pub fn new_request_mdtm_error(op: &'static str, path: &str, err: FtpError) -> Error {
    other(ObjectError::new(op, path, anyhow!("mdtm request: {err:?}")))
}
pub struct ErrorResponse {
    status: Status,
    body: Vec<u8>,
}

impl ErrorResponse {
    pub fn status_code(&self) -> Status {
        self.status
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write! {
            f,
            "status code: {:?}, body: {:?}",
            self.status_code(),
            String::from_utf8_lossy(self.body())
        }
    }
}

pub fn parse_error(op: &'static str, path: &str, err: FtpError) -> Error {
    let kind = match err {
        FtpError::BadResponse => ErrorKind::InvalidData,
        FtpError::ConnectionError(ref e) => e.kind(),
        FtpError::InvalidAddress(_) => ErrorKind::AddrNotAvailable,
        FtpError::SecureError(_) => ErrorKind::PermissionDenied,
        FtpError::UnexpectedResponse(_) => ErrorKind::ConnectionRefused,
        _ => ErrorKind::Other,
    };
    Error::new(kind, ObjectError::new(op, path, anyhow!(err)))
}
pub fn parse_io_error(err: Error, op: &'static str, path: &str) -> Error {
    Error::new(err.kind(), ObjectError::new(op, path, err))
}
