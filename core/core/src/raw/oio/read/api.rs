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

use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;

use bytes::Bytes;
use futures::Future;

use crate::raw::*;
use crate::*;

/// Reader is a type erased [`Read`].
pub type Reader = Box<dyn ReadDyn>;

/// ReadStreamBox is a type erased [`ReadStream`].
pub type ReadStreamBox = Box<dyn ReadStreamDyn>;

/// Read is the internal trait used by OpenDAL to read ranges from storage.
///
/// Users should not use or import this trait unless they are implementing an `Accessor`.
pub trait Read: Unpin + Send + Sync {
    /// Open a range stream for the given range.
    fn open(
        &self,
        range: BytesRange,
    ) -> impl Future<Output = Result<(RpRead, ReadStreamBox)>> + MaybeSend;

    /// Read an exact bounded range into [`Buffer`].
    fn read(
        &self,
        range: BytesRange,
    ) -> impl Future<Output = Result<(RpRead, Buffer)>> + MaybeSend {
        async move {
            if range.size().is_none() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "read requires a bounded range",
                ));
            }

            let (rp, mut stream) = self.open(range).await?;
            let buffer = stream.read_all().await?;
            Ok((rp, buffer))
        }
    }

    /// Read multiple exact bounded ranges into [`Buffer`].
    fn fetch(
        &self,
        ranges: Vec<BytesRange>,
    ) -> impl Future<Output = Result<(RpRead, Vec<Buffer>)>> + MaybeSend {
        async move {
            let mut metadata = None;
            let mut buffers = Vec::with_capacity(ranges.len());

            for range in ranges {
                let (rp, buffer) = self.read(range).await?;
                if metadata.is_none() {
                    metadata = rp.into_metadata();
                }
                buffers.push(buffer);
            }

            Ok((metadata.map(RpRead::new).unwrap_or_default(), buffers))
        }
    }
}

impl Read for () {
    async fn open(&self, _: BytesRange) -> Result<(RpRead, ReadStreamBox)> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support open",
        ))
    }
}

/// ReadDyn is the dyn version of [`Read`] make it possible to use as
/// `Box<dyn ReadDyn>`.
pub trait ReadDyn: Unpin + Send + Sync {
    /// The dyn version of [`Read::open`].
    fn open_dyn(&self, range: BytesRange) -> BoxedFuture<'_, Result<(RpRead, ReadStreamBox)>>;

    /// The dyn version of [`Read::read`].
    fn read_dyn(&self, range: BytesRange) -> BoxedFuture<'_, Result<(RpRead, Buffer)>>;

    /// The dyn version of [`Read::fetch`].
    fn fetch_dyn(&self, ranges: Vec<BytesRange>) -> BoxedFuture<'_, Result<(RpRead, Vec<Buffer>)>>;
}

impl<T: Read + ?Sized> ReadDyn for T {
    fn open_dyn(&self, range: BytesRange) -> BoxedFuture<'_, Result<(RpRead, ReadStreamBox)>> {
        Box::pin(self.open(range))
    }

    fn read_dyn(&self, range: BytesRange) -> BoxedFuture<'_, Result<(RpRead, Buffer)>> {
        Box::pin(self.read(range))
    }

    fn fetch_dyn(&self, ranges: Vec<BytesRange>) -> BoxedFuture<'_, Result<(RpRead, Vec<Buffer>)>> {
        Box::pin(self.fetch(ranges))
    }
}

impl<T: ReadDyn + ?Sized> Read for Box<T> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, ReadStreamBox)> {
        self.deref().open_dyn(range).await
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        self.deref().read_dyn(range).await
    }

    async fn fetch(&self, ranges: Vec<BytesRange>) -> Result<(RpRead, Vec<Buffer>)> {
        self.deref().fetch_dyn(ranges).await
    }
}

/// RangeRead is a compatibility helper for backends whose first implementation
/// still opens independent range streams.
pub trait RangeRead: Clone + Unpin + Send + Sync + 'static {
    /// RangeReader is the range stream returned by this backend.
    type RangeReader: ReadStream;

    /// Open a range-scoped stream.
    fn open_range(
        &self,
        path: &str,
        args: OpRead,
        range: BytesRange,
    ) -> impl Future<Output = Result<(RpRead, Self::RangeReader)>> + MaybeSend;
}

/// RangeReader turns a range-scoped backend implementation into reusable raw [`Read`].
pub struct RangeReader<R> {
    inner: R,
    path: String,
    args: OpRead,
}

impl<R> RangeReader<R> {
    /// Create a new [`RangeReader`].
    pub fn new(inner: R, path: &str, args: OpRead) -> Self {
        Self {
            inner,
            path: path.to_string(),
            args,
        }
    }
}

impl<R: RangeRead> Read for RangeReader<R> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, ReadStreamBox)> {
        self.inner
            .open_range(&self.path, self.args.clone(), range)
            .await
            .map(|(rp, stream)| (rp, Box::new(stream) as ReadStreamBox))
    }
}

/// ReadStream is the internal trait used by OpenDAL to stream data from storage.
///
/// Users should not use or import this trait unless they are implementing an `Accessor`.
///
/// # Notes
///
/// ## Object Safety
///
/// `ReadStream` uses `async in trait`, making it not object safe, preventing the use of
/// `Box<dyn ReadStream>`.
/// To address this, we've introduced [`ReadStreamDyn`] and its compatible type
/// `Box<dyn ReadStreamDyn>`.
///
/// `ReadStreamDyn` uses `Box::pin()` to transform the returned future into a [`BoxedFuture`],
/// introducing an additional layer of indirection and an extra allocation. Ideally,
/// `ReadStreamDyn` should occur only once, at the outermost level of our API.
pub trait ReadStream: Unpin + Send + Sync {
    /// Read the next data chunk from the stream.
    fn read(&mut self) -> impl Future<Output = Result<Buffer>> + MaybeSend;

    /// Read all data from the reader.
    fn read_all(&mut self) -> impl Future<Output = Result<Buffer>> + MaybeSend {
        async {
            let mut bufs = vec![];
            loop {
                match self.read().await {
                    Ok(buf) if buf.is_empty() => break,
                    Ok(buf) => bufs.push(buf),
                    Err(err) => return Err(err),
                }
            }
            Ok(bufs.into_iter().flatten().collect())
        }
    }
}

impl ReadStream for () {
    async fn read(&mut self) -> Result<Buffer> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support read",
        ))
    }
}

impl ReadStream for Bytes {
    async fn read(&mut self) -> Result<Buffer> {
        Ok(Buffer::from(self.split_off(0)))
    }
}

impl ReadStream for Buffer {
    async fn read(&mut self) -> Result<Buffer> {
        Ok(mem::take(self))
    }
}

/// ReadStreamDyn is the dyn version of [`ReadStream`] make it possible to use as
/// `Box<dyn ReadStreamDyn>`.
pub trait ReadStreamDyn: Unpin + Send + Sync {
    /// The dyn version of [`ReadStream::read`].
    ///
    /// This function returns a boxed future to make it object safe.
    fn read_dyn(&mut self) -> BoxedFuture<'_, Result<Buffer>>;

    /// The dyn version of [`ReadStream::read_all`].
    fn read_all_dyn(&mut self) -> BoxedFuture<'_, Result<Buffer>>;
}

impl<T: ReadStream + ?Sized> ReadStreamDyn for T {
    fn read_dyn(&mut self) -> BoxedFuture<'_, Result<Buffer>> {
        Box::pin(self.read())
    }

    fn read_all_dyn(&mut self) -> BoxedFuture<'_, Result<Buffer>> {
        Box::pin(self.read_all())
    }
}

/// # NOTE
///
/// Take care about the `deref_mut()` here. This makes sure that we are calling functions
/// upon `&mut T` instead of `&mut Box<T>`. The later could result in infinite recursion.
impl<T: ReadStreamDyn + ?Sized> ReadStream for Box<T> {
    async fn read(&mut self) -> Result<Buffer> {
        self.deref_mut().read_dyn().await
    }

    async fn read_all(&mut self) -> Result<Buffer> {
        self.deref_mut().read_all_dyn().await
    }
}
