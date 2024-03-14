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

use std::pin::Pin;
use std::sync::Arc;

use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::Future;
use futures::FutureExt;
use futures::StreamExt;

use crate::raw::*;
use crate::*;

/// RangeWrite is used to implement [`Write`] based on range write.
///
/// # Services
///
/// Services like gcs support range write via [GCS Resumable Upload](https://cloud.google.com/storage/docs/resumable-uploads).
///
/// GCS will support upload content by specifying the range of the file in `CONTENT-RANGE`.
///
/// Most range based services will have the following limitations:
///
/// - The size of chunk per upload must be aligned to a certain size. For example, GCS requires
///   to align with 256KiB.
/// - Some services requires to complete the write at the last chunk with the total size.
///
/// # Architecture
///
/// The architecture after adopting [`RangeWrite`]:
///
/// - Services impl `RangeWrite`
/// - `RangeWriter` impl `Write`
/// - Expose `RangeWriter` as `Accessor::Writer`
///
/// # Requirements
///
/// Services that implement `RangeWrite` must fulfill the following requirements:
///
/// - Must be a http service that could accept `AsyncBody`.
/// - Need initialization before writing.
/// - Writing data based on range: `offset`, `size`.
pub trait RangeWrite: Send + Sync + Unpin + 'static {
    /// write_once is used to write the data to underlying storage at once.
    ///
    /// RangeWriter will call this API when:
    ///
    /// - All the data has been written to the buffer and we can perform the upload at once.
    fn write_once(&self, size: u64, body: AsyncBody) -> impl Future<Output = Result<()>> + Send;

    /// Initiate range the range write, the returning value is the location.
    fn initiate_range(&self) -> impl Future<Output = Result<String>> + Send;

    /// write_range will write a range of data.
    fn write_range(
        &self,
        location: &str,
        offset: u64,
        size: u64,
        body: AsyncBody,
    ) -> impl Future<Output = Result<()>> + Send;

    /// complete_range will complete the range write by uploading the last chunk.
    fn complete_range(
        &self,
        location: &str,
        offset: u64,
        size: u64,
        body: AsyncBody,
    ) -> impl Future<Output = Result<()>> + Send;

    /// abort_range will abort the range write by abort all already uploaded data.
    fn abort_range(&self, location: &str) -> impl Future<Output = Result<()>> + Send;
}

/// WritePartResult is the result returned by [`WriteRangeFuture`].
///
/// The error part will carries input `(offset, bytes, err)` so caller can retry them.
type WriteRangeResult = std::result::Result<(), (u64, oio::ChunkedBytes, Error)>;

struct WriteRangeFuture(BoxedStaticFuture<WriteRangeResult>);

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this WriteRangeFuture.
unsafe impl Send for WriteRangeFuture {}

/// # Safety
///
/// We will only take `&mut Self` reference for WriteRangeFuture.
unsafe impl Sync for WriteRangeFuture {}

impl Future for WriteRangeFuture {
    type Output = WriteRangeResult;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_unpin(cx)
    }
}

impl WriteRangeFuture {
    pub fn new<W: RangeWrite>(
        w: Arc<W>,
        location: Arc<String>,
        offset: u64,
        bytes: oio::ChunkedBytes,
    ) -> Self {
        let fut = async move {
            w.write_range(
                &location,
                offset,
                bytes.len() as u64,
                AsyncBody::ChunkedBytes(bytes.clone()),
            )
            .await
            .map_err(|err| (offset, bytes, err))
        };

        WriteRangeFuture(Box::pin(fut))
    }
}

/// RangeWriter will implements [`Write`] based on range write.
pub struct RangeWriter<W: RangeWrite> {
    location: Option<Arc<String>>,
    next_offset: u64,
    /// TODO: Use Bytes directly.
    buffer: Option<oio::ChunkedBytes>,
    futures: ConcurrentFutures<WriteRangeFuture>,

    w: Arc<W>,
}

impl<W: RangeWrite> RangeWriter<W> {
    /// Create a new MultipartWriter.
    pub fn new(inner: W, concurrent: usize) -> Self {
        Self {
            w: Arc::new(inner),

            futures: ConcurrentFutures::new(1.max(concurrent)),
            buffer: None,
            location: None,
            next_offset: 0,
        }
    }

    fn fill_cache(&mut self, bs: Bytes) -> usize {
        let size = bs.len();
        let bs = oio::ChunkedBytes::from_vec(vec![bs]);
        assert!(self.buffer.is_none());
        self.buffer = Some(bs);
        size
    }
}

impl<W: RangeWrite> oio::Write for RangeWriter<W> {
    async fn write(&mut self, bs: Bytes) -> Result<usize> {
        let location = match self.location.clone() {
            Some(location) => location,
            None => {
                // Fill cache with the first write.
                if self.buffer.is_none() {
                    let size = self.fill_cache(bs);
                    return Ok(size);
                }

                let location = self.w.initiate_range().await?;
                let location = Arc::new(location);
                self.location = Some(location.clone());
                location
            }
        };

        loop {
            if self.futures.has_remaining() {
                let cache = self.buffer.take().expect("cache must be valid");
                let offset = self.next_offset;
                self.next_offset += cache.len() as u64;
                self.futures.push_back(WriteRangeFuture::new(
                    self.w.clone(),
                    location,
                    offset,
                    cache,
                ));

                let size = self.fill_cache(bs);
                return Ok(size);
            }

            if let Some(Err((offset, bytes, err))) = self.futures.next().await {
                self.futures.push_front(WriteRangeFuture::new(
                    self.w.clone(),
                    location,
                    offset,
                    bytes,
                ));
                return Err(err);
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        let Some(location) = self.location.clone() else {
            let (size, body) = match self.buffer.clone() {
                Some(cache) => (cache.len(), AsyncBody::ChunkedBytes(cache)),
                None => (0, AsyncBody::Empty),
            };
            // Call write_once if there is no data in buffer and no location.
            return self.w.write_once(size as u64, body).await;
        };

        if !self.futures.is_empty() {
            while let Some(result) = self.futures.next().await {
                if let Err((offset, bytes, err)) = result {
                    self.futures.push_front(WriteRangeFuture::new(
                        self.w.clone(),
                        location,
                        offset,
                        bytes,
                    ));
                    return Err(err);
                };
            }
        }

        if let Some(buffer) = self.buffer.clone() {
            let offset = self.next_offset;
            self.w
                .complete_range(
                    &location,
                    offset,
                    buffer.len() as u64,
                    AsyncBody::ChunkedBytes(buffer),
                )
                .await?;
            self.buffer = None;
        }

        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        let Some(location) = self.location.clone() else {
            return Ok(());
        };

        self.futures.clear();
        self.w.abort_range(&location).await?;
        // Clean cache when abort_range returns success.
        self.buffer = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Mutex;

    use pretty_assertions::assert_eq;
    use rand::thread_rng;
    use rand::Rng;
    use rand::RngCore;

    use super::*;
    use crate::raw::oio::Write;

    struct TestWrite {
        length: u64,
        bytes: HashSet<u64>,
    }

    impl TestWrite {
        pub fn new() -> Arc<Mutex<Self>> {
            let v = Self {
                bytes: HashSet::new(),
                length: 0,
            };

            Arc::new(Mutex::new(v))
        }
    }

    impl RangeWrite for Arc<Mutex<TestWrite>> {
        async fn write_once(&self, size: u64, _: AsyncBody) -> Result<()> {
            let mut test = self.lock().unwrap();
            test.length += size;
            test.bytes.extend(0..size);

            Ok(())
        }

        async fn initiate_range(&self) -> Result<String> {
            Ok("test".to_string())
        }

        async fn write_range(&self, _: &str, offset: u64, size: u64, _: AsyncBody) -> Result<()> {
            // We will have 50% percent rate for write part to fail.
            if thread_rng().gen_bool(5.0 / 10.0) {
                return Err(Error::new(ErrorKind::Unexpected, "I'm a crazy monkey!"));
            }

            let mut test = self.lock().unwrap();
            test.length += size;

            let input = (offset..offset + size).collect::<HashSet<_>>();

            assert!(
                test.bytes.is_disjoint(&input),
                "input should not have overlap"
            );
            test.bytes.extend(input);

            Ok(())
        }

        async fn complete_range(
            &self,
            _: &str,
            offset: u64,
            size: u64,
            _: AsyncBody,
        ) -> Result<()> {
            let mut test = self.lock().unwrap();
            test.length += size;

            let input = (offset..offset + size).collect::<HashSet<_>>();
            assert!(
                test.bytes.is_disjoint(&input),
                "input should not have overlap"
            );
            test.bytes.extend(input);

            Ok(())
        }

        async fn abort_range(&self, _: &str) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_range_writer_with_concurrent_errors() {
        let mut rng = thread_rng();

        let mut w = RangeWriter::new(TestWrite::new(), 8);
        let mut total_size = 0u64;

        for _ in 0..1000 {
            let size = rng.gen_range(1..1024);
            total_size += size as u64;

            let mut bs = vec![0; size];
            rng.fill_bytes(&mut bs);

            loop {
                match w.write(Bytes::copy_from_slice(&bs)).await {
                    Ok(_) => break,
                    Err(_) => continue,
                }
            }
        }

        loop {
            match w.close().await {
                Ok(_) => break,
                Err(_) => continue,
            }
        }

        let actual_bytes = w.w.lock().unwrap().bytes.clone();
        let expected_bytes: HashSet<_> = (0..total_size).collect();
        assert_eq!(actual_bytes, expected_bytes);

        let actual_size = w.w.lock().unwrap().length;
        assert_eq!(actual_size, total_size);
    }
}
