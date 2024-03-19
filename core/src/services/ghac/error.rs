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

use crate::raw::*;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

/// Parse error response into Error.
pub async fn parse_error(resp: Response<oio::Buffer>) -> Result<Error> {
    let (parts, mut body) = resp.into_parts();

    let (kind, retryable) = match parts.status {
        StatusCode::NOT_FOUND | StatusCode::NO_CONTENT => (ErrorKind::NotFound, false),
        StatusCode::CONFLICT => (ErrorKind::AlreadyExists, false),
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::TOO_MANY_REQUESTS => (ErrorKind::RateLimited, true),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let bs = body.copy_to_bytes(body.remaining());
    let message = String::from_utf8_lossy(&bs);

    let mut err = Error::new(kind, &message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    Ok(err)
}
