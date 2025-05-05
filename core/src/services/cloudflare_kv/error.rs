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

use bytes::Buf;
use http::Response;
use http::StatusCode;
use serde_json::de;

use super::backend::CfKvError;
use super::backend::CfKvResponse;
use crate::raw::*;
use crate::*;

/// Parse error response into Error.
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let (mut kind, mut retryable) = match parts.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        // Some services (like owncloud) return 403 while file locked.
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, true),
        // Allowing retry for resource locked.
        StatusCode::LOCKED => (ErrorKind::Unexpected, true),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let (message, err) = de::from_reader::<_, CfKvResponse>(bs.clone().reader())
        .map(|err| (format!("{err:?}"), Some(err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(err) = err {
        (kind, retryable) = parse_cfkv_error_code(err.errors).unwrap_or((kind, retryable));
    }

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

pub(super) fn parse_cfkv_error_code(errors: Vec<CfKvError>) -> Option<(ErrorKind, bool)> {
    if errors.is_empty() {
        return None;
    }

    match errors[0].code {
        // The request is malformed: failed to decode id.
        7400 => Some((ErrorKind::Unexpected, false)),
        // no such column: Xxxx.
        7500 => Some((ErrorKind::NotFound, false)),
        // Authentication error.
        10000 => Some((ErrorKind::PermissionDenied, false)),
        _ => None,
    }
}
