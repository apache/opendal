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

use crate::raw::*;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

use serde_json::de;

use super::model::D1Response;

/// Parse error response into Error.
pub async fn parse_error(resp: Response<IncomingAsyncBody>) -> Result<Error> {
    let (parts, body) = resp.into_parts();
    let bs = body.bytes().await?;

    let (kind, retryable) = match parts.status {
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

    let message = "failed to parse error response";
    let Ok(body) = de::from_slice::<D1Response>(&bs) else {
        return Ok(Error::new(kind, &message));
    };

    let message = body.errors.get(0).map_or(message.to_string(), |e| {
        e.get("message").map_or(message.to_string(), |m| {
            m.as_str().unwrap_or(message).to_string()
        })
    });
    let mut err = Error::new(kind, &message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    Ok(err)
}

/// Parse error D1Response into Error.
pub async fn parse_d1_error(resp: &D1Response) -> Result<Error> {
    let message = "failed to parse error response";
    let message = resp.errors.get(0).map_or(message.to_string(), |e| {
        e.get("message").map_or(message.to_string(), |m| {
            m.as_str().unwrap_or(message).to_string()
        })
    });
    Ok(Error::new(ErrorKind::Unexpected, &message))
}
