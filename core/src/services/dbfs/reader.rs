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

use std::cmp::Ordering;
use std::io;
use std::mem;
use std::str::FromStr;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use base64::engine::general_purpose;
use base64::Engine;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use futures::StreamExt;
use futures::TryStreamExt;
use http::Request;
use http::Response;
use serde::Deserialize;

use crate::raw::*;
use crate::*;

impl HttpClient {
    /// Send a request in async way.
    pub async fn send_dbfs(
        &self,
        req: Request<AsyncBody>,
    ) -> Result<Response<IncomingDbfsAsyncBody>> {
        // Uri stores all string alike data in `Bytes` which means
        // the clone here is cheap.
        let uri = req.uri().clone();
        let is_head = req.method() == http::Method::HEAD;

        let (parts, body) = req.into_parts();

        let mut req_builder = self
            .client
            .request(
                parts.method,
                reqwest::Url::from_str(&uri.to_string()).expect("input request url must be valid"),
            )
            .version(parts.version)
            .headers(parts.headers);

        req_builder = match body {
            AsyncBody::Empty => req_builder.body(reqwest::Body::from("")),
            AsyncBody::Bytes(bs) => req_builder.body(reqwest::Body::from(bs)),
            AsyncBody::ChunkedBytes(bs) => req_builder.body(reqwest::Body::wrap_stream(bs)),
            AsyncBody::Stream(s) => req_builder.body(reqwest::Body::wrap_stream(s)),
        };

        let mut resp = req_builder.send().await.map_err(|err| {
            let is_temporary = !(
                // Builder related error should not be retried.
                err.is_builder() ||
                    // Error returned by RedirectPolicy.
                    //
                    // We don't set this by hand, just don't allow retry.
                    err.is_redirect() ||
                    // We never use `Response::error_for_status`, just don't allow retry.
                    //
                    // Status should be checked by our services.
                    err.is_status()
            );

            let mut oerr = Error::new(ErrorKind::Unexpected, "send async request")
                .with_operation("http_util::Client::send_async")
                .with_context("url", uri.to_string())
                .set_source(err);
            if is_temporary {
                oerr = oerr.set_temporary();
            }

            oerr
        })?;

        // Get content length from header so that we can check it.
        // If the request method is HEAD, we will ignore this.
        let content_length = if is_head {
            None
        } else {
            parse_content_length(resp.headers()).expect("response content length must be valid")
        };

        let mut hr = Response::builder()
            .version(resp.version())
            .status(resp.status())
            // Insert uri into response extension so that we can fetch
            // it later.
            .extension(uri.clone());
        // Swap headers directly instead of copy the entire map.
        mem::swap(hr.headers_mut().unwrap(), resp.headers_mut());

        let stream = resp.bytes_stream().map_err(move |err| {
            // If stream returns a body related error, we can convert
            // it to interrupt so we can retry it.
            Error::new(ErrorKind::Unexpected, "read data from http stream")
                .map(|v| if err.is_body() { v.set_temporary() } else { v })
                .with_context("url", uri.to_string())
                .set_source(err)
        });

        let body = IncomingDbfsAsyncBody::new(Box::new(oio::into_stream(stream)), content_length);

        let resp = hr.body(body).expect("response must build succeed");

        Ok(resp)
    }
}

/// IncomingDbfsAsyncBody carries the content returned by remote servers.
///
/// # Notes
///
/// Client SHOULD NEVER construct this body.
pub struct IncomingDbfsAsyncBody {
    inner: oio::Streamer,
    size: Option<u64>,
    consumed: u64,
    chunk: Option<Bytes>,
}

impl IncomingDbfsAsyncBody {
    /// Construct a new incoming async body
    pub fn new(s: oio::Streamer, size: Option<u64>) -> Self {
        Self {
            inner: s,
            size,
            consumed: 0,
            chunk: None,
        }
    }

    /// Consume the entire body.
    pub async fn consume(mut self) -> Result<()> {
        use oio::ReadExt;

        while let Some(bs) = self.next().await {
            bs.map_err(|err| {
                Error::new(ErrorKind::Unexpected, "fetch bytes from stream")
                    .with_operation("http_util::IncomingDbfsAsyncBody::consume")
                    .set_source(err)
            })?;
        }

        Ok(())
    }

    /// Consume the response to bytes.
    pub async fn bytes(mut self) -> Result<Bytes> {
        use oio::ReadExt;

        // If there's only 1 chunk, we can just return Buf::to _bytes()
        let mut first = if let Some(buf) = self.next().await {
            buf?
        } else {
            return Ok(Bytes::new());
        };

        let second = if let Some(buf) = self.next().await {
            buf?
        } else {
            return Ok(first.copy_to_bytes(first.remaining()));
        };

        // With more than 1 buf, we gotta flatten into a Vec first.
        let cap = first.remaining() + second.remaining() + self.size.unwrap_or_default() as usize;
        let mut vec = Vec::with_capacity(cap);
        vec.put(first);
        vec.put(second);

        while let Some(buf) = self.next().await {
            vec.put(buf?);
        }

        Ok(vec.into())
    }

    #[inline]
    fn check(expect: u64, actual: u64) -> Result<()> {
        match actual.cmp(&expect) {
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(Error::new(
                ErrorKind::ContentIncomplete,
                &format!("reader got too less data, expect: {expect}, actual: {actual}"),
            )
            .set_temporary()),
            Ordering::Greater => Err(Error::new(
                ErrorKind::ContentTruncated,
                &format!("reader got too much data, expect: {expect}, actual: {actual}"),
            )
            .set_temporary()),
        }
    }
}

impl oio::Read for IncomingDbfsAsyncBody {
    fn poll_read(&mut self, cx: &mut Context<'_>, mut buf: &mut [u8]) -> Poll<Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        // We must get a valid bytes from underlying stream
        let bs = loop {
            match ready!(self.poll_next(cx)) {
                Some(Ok(bs)) if bs.is_empty() => continue,
                Some(Ok(bs)) => break bs,
                Some(Err(err)) => return Poll::Ready(Err(err)),
                None => return Poll::Ready(Ok(0)),
            }
        };

        // TODO: reqwest::Body::bytes_stream() in poll_next() will not return whole response at once if the file is too big, which causes serde failed
        let mut response_body = match serde_json::from_slice::<ReadContentJsonResponse>(&bs) {
            Ok(v) => v,
            Err(err) => {
                return Poll::Ready(Err(Error::new(
                    ErrorKind::Unexpected,
                    "parse response content failed",
                )
                .with_operation("http_util::IncomingDbfsAsyncBody::poll_read")
                .set_source(err)));
            }
        };

        response_body.data = general_purpose::STANDARD
            .decode(response_body.data)
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "decode response content failed")
                    .with_operation("http_util::IncomingDbfsAsyncBody::poll_read")
                    .set_source(err)
            })
            .and_then(|v| {
                String::from_utf8(v).map_err(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "response data contains invalid utf8 bytes",
                    )
                    .with_operation("http_util::IncomingDbfsAsyncBody::poll_read")
                    .set_source(err)
                })
            })?;

        let amt = response_body.bytes_read as usize;

        buf.put_slice(response_body.data.as_ref());

        // TODO: will handle chunk here till we find a way to get whole bytes at once

        Poll::Ready(Ok(amt))
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        let (_, _) = (cx, pos);

        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support seeking",
        )))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if let Some(bs) = self.chunk.take() {
            return Poll::Ready(Some(Ok(bs)));
        }

        // NOTE: inner: Stream<Item = Result<Bytes>>
        // Reader::poll_next() -> Stream::poll_next()
        // control by reqwest::Body::bytes_stream()
        let res = match ready!(self.inner.poll_next_unpin(cx)) {
            Some(Ok(bs)) => {
                self.consumed += bs.len() as u64;
                Some(Ok(bs))
            }
            Some(Err(err)) => Some(Err(err)),
            None => {
                if let Some(size) = self.size {
                    Self::check(size, self.consumed)?;
                }

                None
            }
        };

        Poll::Ready(res)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ReadContentJsonResponse {
    bytes_read: u64,
    data: String,
}
