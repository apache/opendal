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

use hdfs_native::HdfsError;

use crate::*;

/// Parse hdfs-native error into opendal::Error.
pub fn parse_hdfs_error(hdfs_error: HdfsError) -> Error {
    let (kind, retryable, msg) = match &hdfs_error {
        HdfsError::IOError(err) => (ErrorKind::Unexpected, false, err.to_string()),
        HdfsError::DataTransferError(msg) => (ErrorKind::Unexpected, false, msg.clone()),
        HdfsError::ChecksumError => (
            ErrorKind::Unexpected,
            false,
            "checksums didn't match".to_string(),
        ),
        HdfsError::UrlParseError(err) => (ErrorKind::Unexpected, false, err.to_string()),
        HdfsError::AlreadyExists(msg) => (ErrorKind::AlreadyExists, false, msg.clone()),
        HdfsError::OperationFailed(msg) => (ErrorKind::Unexpected, false, msg.clone()),
        HdfsError::RPCError(msg0, msg1) => {
            if msg0.contains("java.io.FileNotFoundException") {
                (ErrorKind::NotFound, false, msg1.clone())
            } else {
                (ErrorKind::Unexpected, false, msg1.clone())
            }
        }
        HdfsError::FileNotFound(msg) => (ErrorKind::NotFound, false, msg.clone()),
        HdfsError::BlocksNotFound(msg) => (ErrorKind::NotFound, false, msg.clone()),
        HdfsError::IsADirectoryError(msg) => (ErrorKind::IsADirectory, false, msg.clone()),
        _ => (
            ErrorKind::Unexpected,
            false,
            "unexpected error from hdfs".to_string(),
        ),
    };

    let mut err = Error::new(kind, msg).set_source(hdfs_error);

    if retryable {
        err = err.set_temporary();
    }

    err
}
