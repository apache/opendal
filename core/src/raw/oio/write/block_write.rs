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

use futures::select;
use futures::Future;
use futures::FutureExt;
use futures::TryFutureExt;
use uuid::Uuid;

use crate::raw::*;
use crate::*;

/// BlockWrite is used to implement [`oio::Write`] based on block
/// uploads. By implementing BlockWrite, services don't need to
/// care about the details of uploading blocks.
///
/// # Architecture
///
/// The architecture after adopting [`BlockWrite`]:
///
/// - Services impl `BlockWrite`
/// - `BlockWriter` impl `Write`
/// - Expose `BlockWriter` as `Accessor::Writer`
///
/// # Notes
///
/// `BlockWrite` has an oneshot optimization when `write` has been called only once:
///
/// ```no_build
/// w.write(bs).await?;
/// w.close().await?;
/// ```
///
/// We will use `write_once` instead of starting a new block upload.
///
/// # Requirements
///
/// Services that implement `BlockWrite` must fulfill the following requirements:
///
/// - Must be a http service that could accept `AsyncBody`.
/// - Don't need initialization before writing.
/// - Block ID is generated by caller `BlockWrite` instead of services.
/// - Complete block by an ordered block id list.
pub trait BlockWrite: Send + Sync + Unpin + 'static {
    /// write_once is used to write the data to underlying storage at once.
    ///
    /// BlockWriter will call this API when:
    ///
    /// - All the data has been written to the buffer and we can perform the upload at once.
    fn write_once(
        &self,
        size: u64,
        body: Buffer,
    ) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// write_block will write a block of the data.
    ///
    /// BlockWriter will call this API and stores the result in
    /// order.
    ///
    /// - block_id is the id of the block.
    fn write_block(
        &self,
        block_id: Uuid,
        size: u64,
        body: Buffer,
    ) -> impl Future<Output = Result<()>> + MaybeSend;

    /// complete_block will complete the block upload to build the final
    /// file.
    fn complete_block(
        &self,
        block_ids: Vec<Uuid>,
    ) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// abort_block will cancel the block upload and purge all data.
    fn abort_block(&self, block_ids: Vec<Uuid>) -> impl Future<Output = Result<()>> + MaybeSend;
}

struct WriteInput<W: BlockWrite> {
    w: Arc<W>,
    executor: Executor,
    block_id: Uuid,
    bytes: Buffer,
}

/// BlockWriter will implement [`oio::Write`] based on block
/// uploads.
pub struct BlockWriter<W: BlockWrite> {
    w: Arc<W>,
    executor: Executor,

    started: bool,
    block_ids: Vec<Uuid>,
    cache: Option<Buffer>,
    tasks: ConcurrentTasks<WriteInput<W>, Uuid>,
}

impl<W: BlockWrite> BlockWriter<W> {
    /// Create a new BlockWriter.
    pub fn new(inner: W, executor: Option<Executor>, concurrent: usize) -> Self {
        let executor = executor.unwrap_or_default();

        Self {
            w: Arc::new(inner),
            executor: executor.clone(),
            started: false,
            block_ids: Vec::new(),
            cache: None,

            tasks: ConcurrentTasks::new(executor, concurrent, |input| {
                Box::pin(async move {
                    let fut = input
                        .w
                        .write_block(
                            input.block_id,
                            input.bytes.len() as u64,
                            input.bytes.clone(),
                        )
                        .map_ok(|_| input.block_id);
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
                                            ErrorKind::Unexpected, "write block timeout")
                                                .with_context("block_id", input.block_id.to_string())
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

impl<W> oio::Write for BlockWriter<W>
where
    W: BlockWrite,
{
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        if !self.started && self.cache.is_none() {
            self.fill_cache(bs);
            return Ok(());
        }

        // The block upload process has been started.
        self.started = true;

        let bytes = self.cache.clone().expect("pending write must exist");
        self.tasks
            .execute(WriteInput {
                w: self.w.clone(),
                executor: self.executor.clone(),
                block_id: Uuid::new_v4(),
                bytes,
            })
            .await?;
        self.cache = None;
        self.fill_cache(bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        if !self.started {
            let (size, body) = match self.cache.clone() {
                Some(cache) => (cache.len(), cache),
                None => (0, Buffer::new()),
            };

            let meta = self.w.write_once(size as u64, body).await?;
            self.cache = None;
            return Ok(meta);
        }

        if let Some(cache) = self.cache.clone() {
            self.tasks
                .execute(WriteInput {
                    w: self.w.clone(),
                    executor: self.executor.clone(),
                    block_id: Uuid::new_v4(),
                    bytes: cache,
                })
                .await?;
            self.cache = None;
        }

        loop {
            let Some(result) = self.tasks.next().await.transpose()? else {
                break;
            };
            self.block_ids.push(result);
        }

        let block_ids = self.block_ids.clone();
        self.w.complete_block(block_ids).await
    }

    async fn abort(&mut self) -> Result<()> {
        if !self.started {
            return Ok(());
        }

        self.tasks.clear();
        self.cache = None;
        self.w.abort_block(self.block_ids.clone()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
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
        bytes: HashMap<Uuid, Buffer>,
        content: Option<Buffer>,
    }

    impl TestWrite {
        pub fn new() -> Arc<Mutex<Self>> {
            let v = Self {
                length: 0,
                bytes: HashMap::new(),
                content: None,
            };

            Arc::new(Mutex::new(v))
        }
    }

    impl BlockWrite for Arc<Mutex<TestWrite>> {
        async fn write_once(&self, _: u64, _: Buffer) -> Result<Metadata> {
            Ok(Metadata::default())
        }

        async fn write_block(&self, block_id: Uuid, size: u64, body: Buffer) -> Result<()> {
            // Add an async sleep here to enforce some pending.
            sleep(Duration::from_millis(50)).await;

            // We will have 10% percent rate for write part to fail.
            if thread_rng().gen_bool(1.0 / 10.0) {
                return Err(
                    Error::new(ErrorKind::Unexpected, "I'm a crazy monkey!").set_temporary()
                );
            }

            let mut this = self.lock().unwrap();
            this.length += size;
            this.bytes.insert(block_id, body);

            Ok(())
        }

        async fn complete_block(&self, block_ids: Vec<Uuid>) -> Result<Metadata> {
            let mut this = self.lock().unwrap();
            let mut bs = Vec::new();
            for id in block_ids {
                bs.push(this.bytes[&id].clone());
            }
            this.content = Some(bs.into_iter().flatten().collect());

            Ok(Metadata::default())
        }

        async fn abort_block(&self, _: Vec<Uuid>) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_block_writer_with_concurrent_errors() {
        let mut rng = thread_rng();

        let mut w = BlockWriter::new(TestWrite::new(), Some(Executor::new()), 8);
        let mut total_size = 0u64;
        let mut expected_content = Vec::new();

        for _ in 0..1000 {
            let size = rng.gen_range(1..1024);
            total_size += size as u64;

            let mut bs = vec![0; size];
            rng.fill_bytes(&mut bs);

            expected_content.extend_from_slice(&bs);

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

        let inner = w.w.lock().unwrap();

        assert_eq!(total_size, inner.length, "length must be the same");
        assert!(inner.content.is_some());
        assert_eq!(
            expected_content,
            inner.content.clone().unwrap().to_bytes(),
            "content must be the same"
        );
    }
}
