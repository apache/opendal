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

/// Map goosefs-sdk Error to OpenDAL Error.
///
/// This is the bridge between Layer 3 (goosefs-sdk) error types
/// and Layer 2 (OpenDAL) error types.
pub(super) fn parse_error(err: goosefs_sdk::error::Error) -> Error {
    use goosefs_sdk::error::Error as GfsError;

    let (kind, message, temporary) = match &err {
        GfsError::NotFound { path } => (ErrorKind::NotFound, format!("not found: {}", path), false),

        GfsError::AlreadyExists { path } => (
            ErrorKind::AlreadyExists,
            format!("already exists: {}", path),
            false,
        ),

        GfsError::PermissionDenied { message } => {
            (ErrorKind::PermissionDenied, message.clone(), false)
        }

        GfsError::InvalidArgument { message } => (ErrorKind::ConfigInvalid, message.clone(), false),

        GfsError::ConfigError { message } => (ErrorKind::ConfigInvalid, message.clone(), false),

        GfsError::NoWorkerAvailable { message } => {
            // No worker available is a transient error
            (
                ErrorKind::Unexpected,
                format!("no worker available: {}", message),
                true,
            )
        }

        GfsError::MasterUnavailable { message } => (
            ErrorKind::Unexpected,
            format!("master unavailable: {}", message),
            true,
        ),

        // Authentication failures are transient — the SASL stream expired
        // (e.g. after process fork or long idle). The goosefs-sdk layer
        // should have already attempted reconnection, but if the error
        // propagates up to OpenDAL, mark it as temporary so upper layers
        // (e.g. RetryLayer) can retry the entire operation.
        GfsError::AuthenticationFailed { message } => (
            ErrorKind::Unexpected,
            format!("authentication failed (retriable): {}", message),
            true,
        ),

        // For GrpcError, the goosefs_sdk::error::Error::From<tonic::Status>
        // already maps NotFound/AlreadyExists/PermissionDenied/InvalidArgument
        // to specific error variants above. GrpcError only contains codes that
        // were NOT mapped (Unavailable, DeadlineExceeded, Internal, etc.)
        GfsError::GrpcError { message, .. } => (ErrorKind::Unexpected, message.clone(), false),

        GfsError::TransportError { message, .. } => (ErrorKind::Unexpected, message.clone(), true),

        _ => (ErrorKind::Unexpected, format!("{}", err), false),
    };

    let mut error = Error::new(kind, message).set_source(err);
    if temporary {
        error = error.set_temporary();
    }
    error
}
