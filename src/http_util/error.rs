// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;
use std::fmt::Formatter;

use std::io;
use std::io::Error;





use crate::http_util::AsyncBody;
use anyhow::anyhow;

use http::response::Parts;
use http::HeaderMap;
use http::HeaderValue;
use http::Response;
use http::StatusCode;

use crate::error::other;
use crate::error::ObjectError;
use crate::ops::Operation;

/// Create error happened during building http request.
pub fn new_request_build_error(op: Operation, path: &str, err: http::Error) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("building request: {err:?}"),
    ))
}

/// Create error happened during signing http request.
pub fn new_request_sign_error(op: Operation, path: &str, err: anyhow::Error) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("signing request: {err:?}"),
    ))
}

/// Create error happened during sending http request.
pub fn new_request_send_error(op: Operation, path: &str, err: Error) -> Error {
    Error::new(
        err.kind(),
        ObjectError::new(op, path, anyhow!("sending request:  {err:?}")),
    )
}

/// Create error happened during consuming http response.
pub fn new_response_consume_error(op: Operation, path: &str, err: Error) -> Error {
    Error::new(
        err.kind(),
        ObjectError::new(op, path, anyhow!("consuming response: {err:?}")),
    )
}

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
pub async fn parse_error_response(resp: Response<AsyncBody>) -> io::Result<ErrorResponse> {
    let (parts, body) = resp.into_parts();
    let bs = body.bytes().await?;

    Ok(ErrorResponse {
        parts,
        body: bs.to_vec(),
    })
}
