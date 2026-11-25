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

use futures::Stream;
use futures::StreamExt;

use crate::raw::BoxedFuture;
use crate::raw::oio;
use crate::raw::oio::ReadStream;
use crate::types::Buffer;
use crate::types::Error;
use crate::types::ErrorKind;
use crate::types::Result;

type StreamNext<'a> = BoxedFuture<'a, Option<Result<Buffer>>>;
type WrapStreamNext = dyn for<'a> Fn(StreamNext<'a>) -> StreamNext<'a> + Send + Sync;

/// The streaming body returned by [`HttpTransporter`].
///
/// We implement [`oio::ReadStream`] for `HttpBody`. Services can use
/// `HttpBody` as [`Service::Reader`].
///
/// [`HttpTransporter`]: super::HttpTransporter
/// [`Service::Reader`]: crate::raw::Service::Reader
pub struct HttpBody {
    #[cfg(not(target_arch = "wasm32"))]
    stream: Box<dyn Stream<Item = Result<Buffer>> + Send + Sync + Unpin + 'static>,
    #[cfg(target_arch = "wasm32")]
    stream: Box<dyn Stream<Item = Result<Buffer>> + Unpin + 'static>,
    size: Option<u64>,
    consumed: u64,
    wrap_stream_next: Option<Box<WrapStreamNext>>,
}

/// # Safety
///
/// `HttpBody` is `Send` on non-wasm targets.
unsafe impl Send for HttpBody {}

/// # Safety
///
/// `HttpBody` is `Sync` on non-wasm targets.
unsafe impl Sync for HttpBody {}

impl HttpBody {
    /// Create a new `HttpBody` with given stream and optional size.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new<S>(stream: S, size: Option<u64>) -> Self
    where
        S: Stream<Item = Result<Buffer>> + Send + Sync + Unpin + 'static,
    {
        HttpBody {
            stream: Box::new(stream),
            size,
            consumed: 0,
            wrap_stream_next: None,
        }
    }

    /// Create a new `HttpBody` with given stream and optional size.
    #[cfg(target_arch = "wasm32")]
    pub fn new<S>(stream: S, size: Option<u64>) -> Self
    where
        S: Stream<Item = Result<Buffer>> + Unpin + 'static,
    {
        HttpBody {
            stream: Box::new(stream),
            size,
            consumed: 0,
            wrap_stream_next: None,
        }
    }

    /// Map the inner stream.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn map_inner(
        mut self,
        f: impl FnOnce(
            Box<dyn Stream<Item = Result<Buffer>> + Send + Sync + Unpin + 'static>,
        )
            -> Box<dyn Stream<Item = Result<Buffer>> + Send + Sync + Unpin + 'static>,
    ) -> Self {
        self.stream = f(self.stream);
        self
    }

    /// Map the inner stream.
    #[cfg(target_arch = "wasm32")]
    pub fn map_inner(
        mut self,
        f: impl FnOnce(
            Box<dyn Stream<Item = Result<Buffer>> + Unpin + 'static>,
        ) -> Box<dyn Stream<Item = Result<Buffer>> + Unpin + 'static>,
    ) -> Self {
        self.stream = f(self.stream);
        self
    }

    /// Wrap each stream read future, including any wrapper already installed.
    /// The wrapper future is dropped when the caller cancels the read.
    #[doc(hidden)]
    pub fn wrap_stream_next(
        mut self,
        wrap: impl for<'a> Fn(StreamNext<'a>) -> StreamNext<'a> + Send + Sync + 'static,
    ) -> Self {
        self.wrap_stream_next = Some(match self.wrap_stream_next.take() {
            Some(previous) => Box::new(move |next| wrap(previous(next))),
            None => Box::new(wrap),
        });
        self
    }

    /// Read all data from the stream.
    pub async fn to_buffer(&mut self) -> Result<Buffer> {
        self.read_all().await
    }

    #[inline]
    fn check(&self) -> Result<()> {
        let Some(expect) = self.size else {
            return Ok(());
        };

        let actual = self.consumed;
        match actual.cmp(&expect) {
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(Error::new(
                ErrorKind::Unexpected,
                format!("http response got too little data, expect: {expect}, actual: {actual}"),
            )
            .set_temporary()),
            Ordering::Greater => Err(Error::new(
                ErrorKind::Unexpected,
                format!("http response got too much data, expect: {expect}, actual: {actual}"),
            )
            .set_temporary()),
        }
    }
}

impl oio::ReadStream for HttpBody {
    async fn read(&mut self) -> Result<Buffer> {
        loop {
            let next = self.stream.next();
            let chunk = match &self.wrap_stream_next {
                Some(wrap) => wrap(Box::pin(next)).await,
                None => next.await,
            };
            match chunk.transpose()? {
                Some(buf) if buf.is_empty() => continue,
                Some(buf) => {
                    self.consumed += buf.len() as u64;
                    return Ok(buf);
                }
                None => {
                    self.check()?;
                    return Ok(Buffer::new());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering as AtomicOrdering;

    use futures::stream;

    use super::*;

    #[tokio::test]
    async fn test_http_body_skips_empty_chunks() {
        let mut body = HttpBody::new(
            stream::iter([
                Ok(Buffer::new()),
                Ok(Buffer::from("ab")),
                Ok(Buffer::new()),
                Ok(Buffer::from("cd")),
            ]),
            Some(4),
        );

        let buf = body.to_buffer().await.unwrap();
        assert_eq!(buf.to_bytes().as_ref(), b"abcd");
    }

    #[tokio::test]
    async fn test_http_body_only_empty_chunks_fails_when_size_expected() {
        let mut body = HttpBody::new(stream::iter([Ok(Buffer::new())]), Some(5));

        let err = body.to_buffer().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_temporary());
        assert_eq!(
            err.message(),
            "http response got too little data, expect: 5, actual: 0"
        );
    }

    #[tokio::test]
    async fn test_http_body_empty_with_zero_size() {
        let mut empty_stream = HttpBody::new(stream::iter(std::iter::empty()), Some(0));
        let buf = empty_stream.to_buffer().await.unwrap();
        assert!(buf.is_empty());

        let mut empty_chunk = HttpBody::new(stream::iter([Ok(Buffer::new())]), Some(0));
        let buf = empty_chunk.to_buffer().await.unwrap();
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn test_http_body_wrap_stream_next_across_empty_chunks() {
        let polls = Arc::new(AtomicUsize::new(0));
        let polls_clone = polls.clone();

        let mut body = HttpBody::new(
            stream::iter([Ok(Buffer::new()), Ok(Buffer::from("hi"))]),
            Some(2),
        )
        .wrap_stream_next(move |next| {
            polls_clone.fetch_add(1, AtomicOrdering::SeqCst);
            next
        });

        let chunk = body.read().await.unwrap();
        assert_eq!(chunk.to_bytes().as_ref(), b"hi");
        assert_eq!(polls.load(AtomicOrdering::SeqCst), 2);

        let eof = body.read().await.unwrap();
        assert!(eof.is_empty());
        assert_eq!(polls.load(AtomicOrdering::SeqCst), 3);
    }
}
