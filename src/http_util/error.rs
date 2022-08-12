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
use std::io;
use std::io::Error;
use std::io::ErrorKind;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use futures::future::BoxFuture;
use futures::ready;
use futures::AsyncRead;
use http::response::Parts;
use http::StatusCode;
use http::{HeaderMap, HeaderValue, Response};
use isahc::AsyncBody;

use crate::error::other;
use crate::error::ObjectError;

/// Create error happened during building http request.
pub fn new_request_build_error(op: &'static str, path: &str, err: http::Error) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("building request: {err:?}"),
    ))
}

/// Create error happened during signing http request.
pub fn new_request_sign_error(op: &'static str, path: &str, err: anyhow::Error) -> Error {
    other(ObjectError::new(
        op,
        path,
        anyhow!("signing request: {err:?}"),
    ))
}

/// Create error happened during sending http request.
pub fn new_request_send_error(op: &'static str, path: &str, err: isahc::Error) -> Error {
    let kind = match err.kind() {
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
    };

    Error::new(
        kind,
        ObjectError::new(op, path, anyhow!("sending request:  {err:?}")),
    )
}

pub struct ErrorResponse {
    parts: Parts,
    body: Vec<u8>,
}

impl ErrorResponse {
    pub fn status_code(&self) -> StatusCode {
        self.parts.status
    }

    pub fn headers(&self) -> &HeaderMap<HeaderValue> {
        &self.parts.headers
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

pub struct ErrorResponseFuture {
    resp: Option<Response<AsyncBody>>,
    state: State,
}

enum State {
    Idle,
    Reading(BoxFuture<'static, io::Result<ErrorResponse>>),
}

pub fn parse_error_response_x(resp: Response<AsyncBody>) -> ErrorResponseFuture {
    ErrorResponseFuture {
        resp: Some(resp),
        state: State::Idle,
    }
}

impl Future for ErrorResponseFuture {
    type Output = io::Result<ErrorResponse>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            State::Idle => {
                let (parts, mut body) = self
                    .resp
                    .take()
                    .expect("ErrorResponseFuture in idle state must contains valid response")
                    .into_parts();

                let mut buf = match body.len() {
                    None => Vec::new(),
                    Some(v) => Vec::with_capacity(v as usize),
                };

                let future = async move {
                    futures::io::copy(&mut body, &mut buf).await?;
                    Ok(ErrorResponse { parts, body: buf })
                };
                self.state = State::Reading(Box::pin(future));
                self.poll(cx)
            }
            State::Reading(fut) => Pin::new(fut).poll(cx),
        }
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
