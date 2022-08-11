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

use std::future::Future;
use std::io::Error;
use std::io::ErrorKind;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use futures::ready;
use futures::AsyncRead;
use http::response::Parts;
use http::Response;
use http::StatusCode;

use crate::error::ObjectError;

/// Parse isahc error into `ErrorKind`.
pub fn parse_error_kind(err: &isahc::Error) -> ErrorKind {
    match err.kind() {
        // The HTTP client failed to initialize.
        //
        // This error can occur when trying to create a client with invalid
        // configuration, if there were insufficient resources to create the
        // client, or if a system error occurred when trying to initialize an I/O
        // driver.
        isahc::error::ErrorKind::ConnectionFailed => ErrorKind::Interrupted,
        // Failed to resolve a host name.
        //
        // This could be caused by any number of problems, including failure to
        // reach a DNS server, misconfigured resolver configuration, or the
        // hostname simply does not exist.
        isahc::error::ErrorKind::NameResolution => ErrorKind::Interrupted,
        // An I/O error either sending the request or reading the response. This
        // could be caused by a problem on the client machine, a problem on the
        // server machine, or a problem with the network between the two.
        //
        // You can get more details about the underlying I/O error with
        // [`Error::source`][std::error::Error::source].
        isahc::error::ErrorKind::Io => ErrorKind::Interrupted,
        // A request or operation took longer than the configured timeout time.
        isahc::error::ErrorKind::Timeout => ErrorKind::Interrupted,
        _ => ErrorKind::Other,
    }
}

/// parse_http_error_code will parse HTTP status code into `ErrorKind`
pub fn parse_http_error_code(code: StatusCode) -> ErrorKind {
    match code {
        StatusCode::NOT_FOUND => ErrorKind::NotFound,
        StatusCode::FORBIDDEN => ErrorKind::PermissionDenied,
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => ErrorKind::Interrupted,
        _ => ErrorKind::Other,
    }
}

/// parse_error_response will try to read and parse error response.
pub fn parse_error_response(
    op: &'static str,
    path: &str,
    parser: fn(StatusCode) -> ErrorKind,
    resp: Response<isahc::AsyncBody>,
) -> ParseErrorResponse {
    let (parts, body) = resp.into_parts();

    ParseErrorResponse {
        op,
        path: path.to_string(),
        parser,
        parts,
        body,
        buf: Vec::with_capacity(1024),
    }
}

pub struct ParseErrorResponse {
    op: &'static str,
    path: String,
    parser: fn(StatusCode) -> ErrorKind,
    parts: Parts,
    body: isahc::AsyncBody,

    buf: Vec<u8>,
}

impl Future for ParseErrorResponse {
    type Output = Error;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut data = vec![0; 1024];
        match ready!(Pin::new(&mut self.body).poll_read(cx, &mut data)) {
            Ok(0) => Poll::Ready(Error::new(
                (self.parser)(self.parts.status),
                ObjectError::new(
                    self.op,
                    &self.path,
                    anyhow!(
                        "status code: {:?}, headers: {:?}, body: {:?}",
                        self.parts.status,
                        self.parts.headers,
                        String::from_utf8_lossy(&self.buf)
                    ),
                ),
            )),
            Ok(size) => {
                // Only read 4KiB from the response to avoid broken services.
                if self.buf.len() < 4 * 1024 {
                    self.buf.extend_from_slice(&data[..size]);
                }

                // Make sure the whole body consumed, even we don't need them.
                self.poll(cx)
            }
            Err(e) => Poll::Ready(Error::new(
                (self.parser)(self.parts.status),
                ObjectError::new(
                    self.op,
                    &self.path,
                    anyhow!(
                        "status code: {:?}, headers: {:?}, read body: {:?}, remaining {:?}",
                        self.parts.status,
                        self.parts.headers,
                        String::from_utf8_lossy(&self.buf),
                        e
                    ),
                ),
            )),
        }
    }
}
