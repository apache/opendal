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

use anyhow::anyhow;
use http::response::Parts;
use http::HeaderMap;
use http::HeaderValue;
use http::Response;
use http::StatusCode;

use crate::error::new_unexpected_object_error;
use crate::http_util::IncomingAsyncBody;
use crate::ops::Operation;
use crate::Error;
use crate::Scheme;

/// Create error happened during building http request.
pub fn new_request_build_error(
    scheme: Scheme,
    op: Operation,
    path: &str,
    err: http::Error,
) -> Error {
    new_unexpected_object_error(scheme, op, path, "building request").with_source(anyhow!(err))
}

/// Create error happened during signing http request.
pub fn new_request_sign_error(
    scheme: Scheme,
    op: Operation,
    path: &str,
    err: anyhow::Error,
) -> Error {
    new_unexpected_object_error(scheme, op, path, "signing request").with_source(err)
}

/// Create error happened during sending http request.
pub fn new_request_send_error(
    scheme: Scheme,
    op: Operation,
    path: &str,
    err: ureq::Error,
) -> Error {
    let retryable = {
        match err {
            ureq::Error::Transport(transport) => match transport.kind() {
                ureq::ErrorKind::Dns | ureq::ErrorKind::ConnectionFailed | ureq::ErrorKind::Io => {
                    true
                }
                _ => false,
            },
            _ => false,
        }
    };

    new_unexpected_object_error(scheme, op, path, "sending request")
        .with_source(anyhow!(err))
        .with_retryable(retryable)
}

/// Create error happened during sending http request.
pub fn new_request_send_async_error(
    scheme: Scheme,
    op: Operation,
    path: &str,
    err: reqwest::Error,
) -> Error {
    let retryable = {
        if err.is_builder() {
            // Builder related error should not be retried.
            false
        } else if err.is_redirect() {
            // Error returned by RedirectPolicy.
            //
            // We don't set this by hand, just don't allow retry.
            false
        } else if err.is_status() {
            // We never use `Response::error_for_status`, just don't allow retry.
            //
            // Status should be checked by our services.
            false
        } else {
            true
        }
    };

    new_unexpected_object_error(scheme, op, path, "sending request")
        .with_source(anyhow!(err))
        .with_retryable(retryable)
}

/// Create error happened during consuming http response.
pub fn new_response_consume_error(
    scheme: Scheme,
    op: Operation,
    path: &str,
    err: io::Error,
) -> Error {
    new_unexpected_object_error(scheme, op, path, "consuming response").with_source(anyhow!(err))
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
pub async fn parse_error_response(resp: Response<IncomingAsyncBody>) -> io::Result<ErrorResponse> {
    let (parts, body) = resp.into_parts();
    let bs = body.bytes().await?;

    Ok(ErrorResponse {
        parts,
        body: bs.to_vec(),
    })
}
