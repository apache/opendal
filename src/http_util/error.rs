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

use crate::error::ObjectError;
use anyhow::anyhow;
use futures::ready;
use http::response::Parts;
use http::{Response, StatusCode};
use hyper::body::HttpBody;
use hyper::Body;
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Parse hyper error into `ErrorKind`.
///
/// It's safe to retry the following errors:
///
/// - is_canceled(): Request that was canceled.
/// - is_connect(): an error from Connect.
/// - is_timeout(): the error was caused by a timeout.
pub fn parse_error_kind(err: &hyper::Error) -> ErrorKind {
    if err.is_canceled() || err.is_connect() || err.is_timeout() {
        ErrorKind::Interrupted
    } else {
        ErrorKind::Other
    }
}

/// parse_error_response will try to read and parse error response.
pub fn parse_error_response(
    op: &'static str,
    path: &str,
    parser: fn(StatusCode) -> ErrorKind,
    resp: Response<Body>,
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
    body: Body,

    buf: Vec<u8>,
}

impl Future for ParseErrorResponse {
    type Output = Error;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(Pin::new(&mut self.body).poll_data(cx)) {
            None => Poll::Ready(Error::new(
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
            Some(Ok(data)) => {
                // Only read 4KiB from the response to avoid broken services.
                if self.buf.len() < 4 * 1024 {
                    self.buf.extend_from_slice(&data);
                }

                // Make sure the whole body consumed, even we don't need them.
                self.poll(cx)
            }
            Some(Err(e)) => Poll::Ready(Error::new(
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
