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

use bb8::RunError;
use openssh::Error as SshError;
use openssh_sftp_client::{error::SftpErrorKind, Error as SftpClientError};

use crate::{Error, ErrorKind};

#[derive(Debug)]
pub enum SftpError {
    SftpClientError(SftpClientError),
    SshError(SshError),
}

impl From<SftpClientError> for Error {
    fn from(e: SftpClientError) -> Self {
        let kind = match &e {
            SftpClientError::UnsupportedSftpProtocol { version: _ } => ErrorKind::Unsupported,
            SftpClientError::SftpError(kind, _msg) => match kind {
                SftpErrorKind::NoSuchFile => ErrorKind::NotFound,
                SftpErrorKind::PermDenied => ErrorKind::PermissionDenied,
                SftpErrorKind::OpUnsupported => ErrorKind::Unsupported,
                _ => ErrorKind::Unexpected,
            },
            _ => ErrorKind::Unexpected,
        };

        let err = Error::new(kind, "sftp error").set_source(e);

        err
    }
}

impl From<SshError> for Error {
    fn from(e: SshError) -> Self {
        let kind = ErrorKind::Unexpected; // todo

        let err = Error::new(kind, "ssh error").set_source(e);

        err
    }
}

impl From<SftpClientError> for SftpError {
    fn from(e: SftpClientError) -> Self {
        SftpError::SftpClientError(e)
    }
}

impl From<SshError> for SftpError {
    fn from(e: SshError) -> Self {
        SftpError::SshError(e)
    }
}

impl From<SftpError> for Error {
    fn from(e: SftpError) -> Self {
        match e {
            SftpError::SftpClientError(e) => e.into(),
            SftpError::SshError(e) => e.into(),
        }
    }
}

impl From<RunError<SftpError>> for Error {
    fn from(e: RunError<SftpError>) -> Self {
        match e {
            RunError::User(err) => err.into(),
            RunError::TimedOut => {
                Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary()
            }
        }
    }
}

pub(super) fn is_not_found(e: &SftpClientError) -> bool {
    match e {
        SftpClientError::SftpError(kind, _msg) => match kind {
            SftpErrorKind::NoSuchFile => true,
            _ => false,
        },
        _ => false,
    }
}

pub(super) fn is_sftp_protocol_error(e: &SftpClientError) -> bool {
    match e {
        SftpClientError::SftpError(_kind, _msg) => true,
        _ => false,
    }
}
