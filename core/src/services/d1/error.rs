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

use super::model::D1Error;
use super::model::D1Response;
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

    let (message, d1_err) = de::from_reader::<_, D1Response>(bs.clone().reader())
        .map(|d1_err| (format!("{d1_err:?}"), Some(d1_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(d1_err) = d1_err {
        (kind, retryable) = parse_d1_error_code(d1_err.errors).unwrap_or((kind, retryable));
    }

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

pub fn parse_d1_error_code(errors: Vec<D1Error>) -> Option<(ErrorKind, bool)> {
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
