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
use serde_json::from_slice;

use crate::raw::*;
use crate::*;

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
/// The error returned by Supabase
struct SupabaseError {
    status_code: String,
    error: String,
    message: String,
}

/// Parse the supabase error type to the OpenDAL error type
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    // Check HTTP status code first/
    let (mut kind, mut retryable) = match parts.status.as_u16() {
        500 | 502 | 503 | 504 => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    // Than extrace the error message.
    let (message, _) = from_slice::<SupabaseError>(&bs)
        .map(|sb_err| {
            (kind, retryable) = parse_supabase_error(&sb_err);
            (format!("{sb_err:?}"), Some(sb_err))
        })
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

// Return the error kind and whether it is retryable
fn parse_supabase_error(err: &SupabaseError) -> (ErrorKind, bool) {
    let code = err.status_code.parse::<u16>().unwrap();
    let status_code = StatusCode::from_u16(code).unwrap();
    match status_code {
        StatusCode::CONFLICT => (ErrorKind::AlreadyExists, false),
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::PRECONDITION_FAILED | StatusCode::NOT_MODIFIED => {
            (ErrorKind::ConditionNotMatch, false)
        }
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    }
}
