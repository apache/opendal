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

use std::fmt::Display;
use std::fmt::Formatter;

use anyhow::anyhow;
use http::response::Parts;
use http::HeaderMap;
use http::HeaderValue;
use http::Response;
use http::StatusCode;

use super::IncomingAsyncBody;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

/// ErrorResponse carries HTTP status code, headers and body.
///
/// This struct should only be used to parse error response which is small.
pub struct ErrorResponse {
    parts: Parts,
    body: Vec<u8>,
}

impl ErrorResponse {
    /// Get http status code
    pub fn status_code(&self) -> StatusCode {
        self.parts.status
    }

    /// Get http headers
    pub fn headers(&self) -> &HeaderMap<HeaderValue> {
        &self.parts.headers
    }

    /// Get http error response body content (in bytes).
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "status code: {:?}, headers: {:?}, body: {:?}",
            self.status_code(),
            self.headers(),
            String::from_utf8_lossy(self.body())
        )
    }
}

/// parse_error_response will parse response into `ErrorResponse`.
///
/// # NOTE
///
/// Please only use this for parsing error response hence it will read the
/// entire body into memory.
pub async fn parse_error_response(resp: Response<IncomingAsyncBody>) -> Result<ErrorResponse> {
    let (parts, body) = resp.into_parts();
    let bs = body.bytes().await.map_err(|err| {
        Error::new(ErrorKind::Unexpected, "reading error response")
            .with_operation("http_util::parse_error_response")
            .set_source(anyhow!(err))
    })?;

    Ok(ErrorResponse {
        parts,
        body: bs.to_vec(),
    })
}

/// Create a new error happened during building request.
pub fn new_request_build_error(err: http::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "building http request")
        .with_operation("http::Request::build")
        .set_source(err)
}

/// Create a new error happened during signing request.
pub fn new_request_sign_error(err: anyhow::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "signing http request")
        .with_operation("reqsign::Sign")
        .set_source(err)
}
