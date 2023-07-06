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

use super::response::DropboxErrorResponse;
use crate::raw::*;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

/// Parse error response into Error.
pub async fn parse_error(resp: Response<IncomingAsyncBody>) -> Result<Error> {
    let (parts, body) = resp.into_parts();
    let bs = body.bytes().await?;

    let (kind, retryable) = match parts.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let dropbox_error =
        serde_json::from_slice::<DropboxErrorResponse>(&bs).map_err(new_json_deserialize_error);
    match dropbox_error {
        Ok(dropbox_error) => {
            // We cannot get the error type from the response header when the status code is 409.
            // Because Dropbox API v2 will put error summary in the response body,
            // we need to parse it to get the correct error type and then error kind.
            // See https://www.dropbox.com/developers/documentation/http/documentation#error-handling
            let error_summary = dropbox_error.error_summary.as_str();

            let mut err = Error::new(
                match parts.status {
                    // 409 Conflict means that Endpoint-specific error.
                    // Look to the JSON response body for the specifics of the error.
                    StatusCode::CONFLICT => {
                        if error_summary.contains("path/not_found")
                            || error_summary.contains("path_lookup/not_found")
                        {
                            ErrorKind::NotFound
                        } else if error_summary.contains("path/conflict") {
                            ErrorKind::AlreadyExists
                        } else {
                            ErrorKind::Unexpected
                        }
                    }
                    // Otherwise, we can get the error type from the response status code.
                    _ => kind,
                },
                error_summary,
            )
            .with_context("response", format!("{parts:?}"));

            if retryable {
                err = err.set_temporary();
            }

            Ok(err)
        }
        Err(_err) => {
            let mut err = Error::new(kind, &String::from_utf8_lossy(&bs))
                .with_context("response", format!("{parts:?}"));

            if retryable {
                err = err.set_temporary();
            }

            Ok(err)
        }
    }
}
