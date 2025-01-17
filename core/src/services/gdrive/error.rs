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

use http::Response;
use http::StatusCode;
use serde::Deserialize;

use crate::raw::*;
use crate::*;

#[derive(Default, Debug, Deserialize)]
struct GdriveError {
    error: GdriveInnerError,
}

#[derive(Default, Debug, Deserialize)]
struct GdriveInnerError {
    message: String,
}

/// Parse error response into Error.
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let (mut kind, mut retryable) = match parts.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT
        // Gdrive sometimes return METHOD_NOT_ALLOWED for our requests for abuse detection.
        | StatusCode::METHOD_NOT_ALLOWED => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let (message, gdrive_err) = serde_json::from_slice::<GdriveError>(bs.as_ref())
        .map(|gdrive_err| (format!("{gdrive_err:?}"), Some(gdrive_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(gdrive_err) = gdrive_err {
        (kind, retryable) =
            parse_gdrive_error_code(gdrive_err.error.message.as_str()).unwrap_or((kind, retryable));
    }

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

pub fn parse_gdrive_error_code(message: &str) -> Option<(ErrorKind, bool)> {
    match message {
        // > Please reduce your request rate.
        //
        // It's Ok to retry since later on the request rate may get reduced.
        "User rate limit exceeded." => Some((ErrorKind::RateLimited, true)),
        _ => None,
    }
}
