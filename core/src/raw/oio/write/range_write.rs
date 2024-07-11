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

use std::sync::Arc;

use futures::FutureExt;
use futures::{select, Future};

use crate::raw::*;
use crate::*;

/// RangeWrite is used to implement [`oio::Write`] based on range write.
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
    fn write_once(&self, body: Buffer) -> impl Future<Output = Result<()>> + MaybeSend;

    /// Initiate range the range write, the returning value is the location.
    fn initiate_range(&self) -> impl Future<Output = Result<String>> + MaybeSend;

    /// write_range will write a range of data.
    fn write_range(
        &self,
        location: &str,
        offset: u64,
        body: Buffer,
    ) -> impl Future<Output = Result<()>> + MaybeSend;

    /// complete_range will complete the range write by uploading the last chunk.
    fn complete_range(
        &self,
        location: &str,
        offset: u64,
        body: Buffer,
    ) -> impl Future<Output = Result<()>> + MaybeSend;

    /// abort_range will abort the range write by abort all already uploaded data.
    fn abort_range(&self, location: &str) -> impl Future<Output = Result<()>> + MaybeSend;
}

struct WriteInput<W: RangeWrite> {
    w: Arc<W>,
    executor: Executor,

    location: Arc<String>,
    offset: u64,
    bytes: Buffer,
}

/// RangeWriter will implements [`oio::Write`] based on range write.
pub struct RangeWriter<W: RangeWrite> {
    w: Arc<W>,
    executor: Executor,

    location: Option<Arc<String>>,
    next_offset: u64,
    cache: Option<Buffer>,
    tasks: ConcurrentTasks<WriteInput<W>, ()>,
}

impl<W: RangeWrite> RangeWriter<W> {
    /// Create a new MultipartWriter.
    pub fn new(inner: W, executor: Option<Executor>, concurrent: usize) -> Self {
        let executor = executor.unwrap_or_default();

        Self {
            w: Arc::new(inner),
            executor: executor.clone(),
            location: None,
            next_offset: 0,
            cache: None,

            tasks: ConcurrentTasks::new(executor, concurrent, |input| {
                Box::pin(async move {
                    let fut =
                        input
                            .w
                            .write_range(&input.location, input.offset, input.bytes.clone());
                    match input.executor.timeout() {
                        None => {
                            let result = fut.await;
                            (input, result)
                        }
                        Some(timeout) => {
                            let result = select! {
                                result = fut.fuse() => {
                                    result
                                }
                                _ = timeout.fuse() => {
                                      Err(Error::new(
                                            ErrorKind::Unexpected, "write range timeout")
                                                .with_context("offset", input.offset.to_string())
                                                .set_temporary())
                                }
                            };
                            (input, result)
                        }
                    }
                })
            }),
        }
    }

    fn fill_cache(&mut self, bs: Buffer) -> usize {
        let size = bs.len();
        assert!(self.cache.is_none());
        self.cache = Some(bs);
        size
    }
}

impl<W: RangeWrite> oio::Write for RangeWriter<W> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let location = match self.location.clone() {
            Some(location) => location,
            None => {
                // Fill cache with the first write.
                if self.cache.is_none() {
                    self.fill_cache(bs);
                    return Ok(());
                }

                let location = self.w.initiate_range().await?;
                let location = Arc::new(location);
                self.location = Some(location.clone());
                location
            }
        };

        let bytes = self.cache.clone().expect("pending write must exist");
        let length = bytes.len() as u64;
        let offset = self.next_offset;

        self.tasks
            .execute(WriteInput {
                w: self.w.clone(),
                executor: self.executor.clone(),
                location,
                offset,
                bytes,
            })
            .await?;
        self.cache = None;
        self.next_offset += length;
        self.fill_cache(bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let Some(location) = self.location.clone() else {
            let body = self.cache.clone().unwrap_or_default();
            // Call write_once if there is no data in buffer and no location.
            self.w.write_once(body).await?;
            self.cache = None;
            return Ok(());
        };

        // Make sure all tasks are finished.
        while self.tasks.next().await.transpose()?.is_some() {}

        if let Some(buffer) = self.cache.clone() {
            let offset = self.next_offset;
            self.w.complete_range(&location, offset, buffer).await?;
            self.cache = None;
        }

        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        let Some(location) = self.location.clone() else {
            return Ok(());
        };

        self.tasks.clear();
        self.cache = None;
        self.w.abort_range(&location).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use rand::thread_rng;
    use rand::Rng;
    use rand::RngCore;
    use tokio::time::sleep;

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
        async fn write_once(&self, body: Buffer) -> Result<()> {
            let mut test = self.lock().unwrap();
            let size = body.len() as u64;
            test.length += size;
            test.bytes.extend(0..size);

            Ok(())
        }

        async fn initiate_range(&self) -> Result<String> {
            Ok("test".to_string())
        }

        async fn write_range(&self, _: &str, offset: u64, body: Buffer) -> Result<()> {
            // Add an async sleep here to enforce some pending.
            sleep(Duration::from_millis(50)).await;

            // We will have 10% percent rate for write part to fail.
            if thread_rng().gen_bool(1.0 / 10.0) {
                return Err(
                    Error::new(ErrorKind::Unexpected, "I'm a crazy monkey!").set_temporary()
                );
            }

            let mut test = self.lock().unwrap();
            let size = body.len() as u64;
            test.length += size;

            let input = (offset..offset + size).collect::<HashSet<_>>();

            assert!(
                test.bytes.is_disjoint(&input),
                "input should not have overlap"
            );
            test.bytes.extend(input);

            Ok(())
        }

        async fn complete_range(&self, _: &str, offset: u64, body: Buffer) -> Result<()> {
            // Add an async sleep here to enforce some pending.
            sleep(Duration::from_millis(50)).await;

            // We will have 10% percent rate for write part to fail.
            if thread_rng().gen_bool(1.0 / 10.0) {
                return Err(
                    Error::new(ErrorKind::Unexpected, "I'm a crazy monkey!").set_temporary()
                );
            }

            let mut test = self.lock().unwrap();
            let size = body.len() as u64;
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

        let mut w = RangeWriter::new(TestWrite::new(), Some(Executor::new()), 200);
        let mut total_size = 0u64;

        for _ in 0..1000 {
            let size = rng.gen_range(1..1024);
            total_size += size as u64;

            let mut bs = vec![0; size];
            rng.fill_bytes(&mut bs);

            loop {
                match w.write(bs.clone().into()).await {
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
