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

use openssh::Error as SshError;
use openssh_sftp_client::error::SftpErrorKind;
use openssh_sftp_client::Error as SftpClientError;

use crate::Error;
use crate::ErrorKind;

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

        Error::new(kind, "sftp error").set_source(e)
    }
}

/// REMOVE ME: it's not allowed to impl `<T>` for Error.
impl From<SshError> for Error {
    fn from(e: SshError) -> Self {
        Error::new(ErrorKind::Unexpected, "ssh error").set_source(e)
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

pub(super) fn is_not_found(e: &SftpClientError) -> bool {
    matches!(e, SftpClientError::SftpError(SftpErrorKind::NoSuchFile, _))
}

pub(super) fn is_sftp_protocol_error(e: &SftpClientError) -> bool {
    matches!(e, SftpClientError::SftpError(_, _))
}
