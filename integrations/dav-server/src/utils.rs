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

use futures::StreamExt;
use opendal_core::{ErrorKind, Operator};

pub fn convert_error(opendal_error: opendal_core::Error) -> dav_server::fs::FsError {
    match opendal_error.kind() {
        opendal_core::ErrorKind::Unsupported => dav_server::fs::FsError::NotImplemented,
        opendal_core::ErrorKind::NotFound => dav_server::fs::FsError::NotFound,
        opendal_core::ErrorKind::PermissionDenied
        | opendal_core::ErrorKind::IsADirectory
        | opendal_core::ErrorKind::NotADirectory => dav_server::fs::FsError::Forbidden,
        opendal_core::ErrorKind::AlreadyExists
        | opendal_core::ErrorKind::IsSameFile
        | opendal_core::ErrorKind::ConditionNotMatch => dav_server::fs::FsError::Exists,
        _ => dav_server::fs::FsError::GeneralFailure,
    }
}

/// Return whether `dir_path` (ending with `/`) exists as an implicit prefix
/// containing at least one entry.
pub(crate) async fn implicit_dir_exists(
    op: &Operator,
    dir_path: &str,
) -> dav_server::fs::FsResult<bool> {
    if !op.info().capability().list {
        return Ok(false);
    }

    let mut lister = match op.lister_with(dir_path).limit(1).await {
        Ok(lister) => lister,
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::NotFound | ErrorKind::NotADirectory | ErrorKind::Unsupported
            ) =>
        {
            return Ok(false);
        }
        Err(err) => return Err(convert_error(err)),
    };

    match lister.next().await {
        Some(Ok(_)) => Ok(true),
        Some(Err(err))
            if matches!(
                err.kind(),
                ErrorKind::NotFound | ErrorKind::NotADirectory | ErrorKind::Unsupported
            ) =>
        {
            Ok(false)
        }
        Some(Err(err)) => Err(convert_error(err)),
        None => Ok(false),
    }
}

/// Return whether `dir_path` (ending with `/`) exists as an explicit
/// directory marker or an implicit prefix with children.
pub(crate) async fn dir_exists(op: &Operator, dir_path: &str) -> dav_server::fs::FsResult<bool> {
    if op.info().capability().stat {
        match op.stat(dir_path).await {
            Ok(meta) => {
                if meta.is_dir() {
                    return Ok(true);
                }
            }
            Err(err) if err.kind() == ErrorKind::NotADirectory => return Ok(false),
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(convert_error(err)),
        }
    }

    implicit_dir_exists(op, dir_path).await
}
