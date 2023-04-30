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

use std::io;

use bb8::RunError;
use openssh::Error as SshError;
use openssh_sftp_client::Error as SftpClientError;

use crate::{Error, ErrorKind};

use super::backend::Manager;

#[derive(Debug)]
pub enum SftpError {
    SftpClientError(SftpClientError),
    SshError(SshError),
}

impl From<SftpClientError> for Error {
    fn from(e: SftpClientError) -> Self {
        let kind = match e {
            SftpClientError::UnsupportedSftpProtocol { version: _ } => ErrorKind::Unsupported,
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

/// Parse all io related errors.
pub fn parse_io_error(err: io::Error) -> Error {
    use io::ErrorKind::*;

    let (kind, retryable) = match err.kind() {
        NotFound => (ErrorKind::NotFound, false),
        PermissionDenied => (ErrorKind::PermissionDenied, false),
        Interrupted | UnexpectedEof | TimedOut | WouldBlock => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, true),
    };

    let mut err = Error::new(kind, &err.kind().to_string()).set_source(err);

    if retryable {
        err = err.set_temporary();
    }

    err
}
