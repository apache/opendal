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

use smb::Error as SmbError;
use smb::error::TimedOutTask;
use smb::transport::TransportError;

use opendal_core::Error;
use opendal_core::ErrorKind;

const STATUS_ACCESS_DENIED: u32 = 0xC000_0022;
const STATUS_DELETE_PENDING: u32 = 0xC000_0056;
const STATUS_FILE_IS_A_DIRECTORY: u32 = 0xC000_00BA;
const STATUS_NOT_A_DIRECTORY: u32 = 0xC000_0103;
const STATUS_OBJECT_NAME_COLLISION: u32 = 0xC000_0035;
const STATUS_OBJECT_NAME_NOT_FOUND: u32 = 0xC000_0034;
const STATUS_OBJECT_PATH_NOT_FOUND: u32 = 0xC000_003A;

pub fn parse_smb_error(err: SmbError) -> Error {
    let kind = match &err {
        SmbError::NotFound(_) => ErrorKind::NotFound,
        SmbError::MissingPermissions(_) => ErrorKind::PermissionDenied,
        SmbError::UnsupportedOperation(_) => ErrorKind::Unsupported,
        SmbError::InvalidConfiguration(_) => ErrorKind::ConfigInvalid,
        SmbError::InvalidArgument(_) => ErrorKind::Unexpected,
        SmbError::UnexpectedMessageStatus(status) | SmbError::ReceivedErrorMessage(status, _) => {
            match *status {
                STATUS_ACCESS_DENIED => ErrorKind::PermissionDenied,
                STATUS_DELETE_PENDING => ErrorKind::NotFound,
                STATUS_FILE_IS_A_DIRECTORY => ErrorKind::IsADirectory,
                STATUS_NOT_A_DIRECTORY => ErrorKind::NotADirectory,
                STATUS_OBJECT_NAME_COLLISION => ErrorKind::AlreadyExists,
                STATUS_OBJECT_NAME_NOT_FOUND | STATUS_OBJECT_PATH_NOT_FOUND => ErrorKind::NotFound,
                _ => ErrorKind::Unexpected,
            }
        }
        SmbError::OperationTimeout(TimedOutTask::ReceiveNextMessage, _)
        | SmbError::TransportError(TransportError::Timeout(_)) => ErrorKind::Unexpected,
        _ => ErrorKind::Unexpected,
    };

    let mut e = Error::new(kind, "smb error").set_source(err);
    if kind == ErrorKind::Unexpected {
        e = e.set_temporary();
    }
    e
}

pub(super) fn is_already_exists(err: &SmbError) -> bool {
    matches!(
        err,
        SmbError::UnexpectedMessageStatus(STATUS_OBJECT_NAME_COLLISION)
            | SmbError::ReceivedErrorMessage(STATUS_OBJECT_NAME_COLLISION, _)
    )
}

pub(super) fn is_not_found(err: &SmbError) -> bool {
    matches!(
        err,
        SmbError::NotFound(_)
            | SmbError::UnexpectedMessageStatus(
                STATUS_DELETE_PENDING | STATUS_OBJECT_NAME_NOT_FOUND | STATUS_OBJECT_PATH_NOT_FOUND
            )
            | SmbError::ReceivedErrorMessage(
                STATUS_DELETE_PENDING | STATUS_OBJECT_NAME_NOT_FOUND | STATUS_OBJECT_PATH_NOT_FOUND,
                _
            )
    )
}
