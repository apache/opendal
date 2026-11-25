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

use std::future::Future;
use std::sync::Arc;

use futures::FutureExt;
use futures::select;
use uuid::Uuid;

use crate::raw::*;
use crate::*;

/// BlockCopy is used to implement [`oio::Copy`] based on block copy.
///
/// By implementing BlockCopy, services only need to provide service-specific
/// block copy operations. [`BlockCopier`] will drive source metadata loading,
/// block id generation, block queue, completion, and abort state.
pub trait BlockCopy: Send + Sync + Unpin + 'static {
    /// source_metadata returns source metadata for planning block copy.
    ///
    /// BlockCopier will call this API when source content length hint is not
    /// provided.
    fn source_metadata(&self) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// copy_once is used to copy the source object at once.
    ///
    /// BlockCopier will call this API when the source object can be copied
    /// without starting block copy.
    fn copy_once(&self) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// copy_block copies one source range into one block.
    fn copy_block(
        &self,
        block_id: Uuid,
        range: BytesRange,
    ) -> impl Future<Output = Result<()>> + MaybeSend;

    /// complete_block completes the block copy with the ordered block id list.
    fn complete_block(
        &self,
        block_ids: Vec<Uuid>,
    ) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// abort_block cancels the pending block copy and purges intermediate state.
    fn abort_block(&self, block_ids: Vec<Uuid>) -> impl Future<Output = Result<()>> + MaybeSend;
}

struct CopyInput<C: BlockCopy> {
    copier: Arc<C>,
    executor: Executor,
    block_id: Uuid,
    block_number: usize,
    range: BytesRange,
}

struct CopiedBlock {
    block_id: Uuid,
    block_number: usize,
    size: u64,
}

enum State {
    /// The copy is active and can be advanced or completed.
    Running,
    /// An abort has been requested and server-side cleanup is pending retry.
    Aborting,
    /// The copy completed and produced server-side metadata.
    Completed(Metadata),
    /// The copy was aborted and any server-side intermediate state was cleaned up.
    Aborted,
}

/// BlockCopier implements [`oio::Copy`] based on block copy.
pub struct BlockCopier<C: BlockCopy> {
    copier: Arc<C>,
    executor: Executor,

    block_ids: Vec<(usize, Uuid)>,
    scheduled_block_ids: Vec<Uuid>,
    next_block_number: usize,
    next_offset: u64,
    source_size: Option<u64>,
    copy_once_threshold: u64,
    block_size: u64,
    concurrent: usize,
    state: State,

    tasks: ConcurrentTasks<CopyInput<C>, CopiedBlock>,
}

impl<C: BlockCopy> BlockCopier<C> {
    /// Create a new BlockCopier.
    pub fn new(
        executor: impl Into<Executor>,
        inner: C,
        source_content_length_hint: Option<u64>,
        copy_once_threshold: u64,
        block_size: u64,
        concurrent: usize,
    ) -> Self {
        let executor = executor.into();
        let copier = Arc::new(inner);
        let concurrent = concurrent.max(1);

        Self {
            copier,
            executor: executor.clone(),
            block_ids: Vec::new(),
            scheduled_block_ids: Vec::new(),
            next_block_number: 0,
            next_offset: 0,
            source_size: source_content_length_hint,
            copy_once_threshold,
            block_size,
            concurrent,
            state: State::Running,

            tasks: ConcurrentTasks::new(executor, concurrent, 0, |input| {
                Box::pin(async move {
                    let size = input.range.size().expect("block copy range must be sized");
                    let fut = input.copier.copy_block(input.block_id, input.range);

                    let result = match input.executor.timeout() {
                        None => fut.await.map(|_| CopiedBlock {
                            block_id: input.block_id,
                            block_number: input.block_number,
                            size,
                        }),
                        Some(timeout) => {
                            select! {
                                result = fut.fuse() => {
                                    result.map(|_| CopiedBlock {
                                        block_id: input.block_id,
                                        block_number: input.block_number,
                                        size,
                                    })
                                }
                                _ = timeout.fuse() => {
                                    Err(Error::new(
                                        ErrorKind::Unexpected, "copy block timeout")
                                            .with_context("block_id", input.block_id.to_string())
                                            .set_temporary())
                                }
                            }
                        }
                    };

                    (input, result)
                })
            }),
        }
    }

    async fn source_size(&mut self) -> Result<u64> {
        match self.source_size {
            Some(size) => Ok(size),
            None => {
                let size = self.copier.source_metadata().await?.content_length();
                self.source_size = Some(size);
                Ok(size)
            }
        }
    }

    async fn fill_tasks(&mut self, source_size: u64) -> Result<()> {
        let mut scheduled = 0;

        while self.next_offset < source_size
            && self.tasks.has_remaining()
            && scheduled < self.concurrent
        {
            let size = self.block_size.min(source_size - self.next_offset);
            let range = BytesRange::new(self.next_offset, Some(size));

            let block_id = Uuid::new_v4();
            let input = CopyInput {
                copier: self.copier.clone(),
                executor: self.executor.clone(),
                block_id,
                block_number: self.next_block_number,
                range,
            };

            // Propagate errors to the caller (e.g. RetryLayer) instead of
            // retrying in place. ConcurrentTasks keeps failed task inputs in
            // the queue, and this input is rebuilt from next_offset on the
            // next call, so a retry preserves progress.
            self.tasks.execute(input).await?;

            self.scheduled_block_ids.push(block_id);
            self.next_offset += size;
            self.next_block_number += 1;
            scheduled += 1;

            if self.tasks.has_result() {
                break;
            }
        }

        Ok(())
    }
}

impl<C> oio::Copy for BlockCopier<C>
where
    C: BlockCopy,
{
    async fn next(&mut self) -> Result<Option<usize>> {
        match &self.state {
            State::Completed(_) => return Ok(None),
            State::Aborting | State::Aborted => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "block copier has already been aborted",
                ));
            }
            State::Running => {}
        }

        let source_size = self.source_size().await?;

        if self.scheduled_block_ids.is_empty() && source_size <= self.copy_once_threshold {
            let metadata = self.copier.copy_once().await?;
            self.state = State::Completed(metadata);
            return Ok(None);
        }

        self.fill_tasks(source_size).await?;

        if let Some(result) = self.tasks.next().await.transpose()? {
            let size = result
                .size
                .try_into()
                .map_err(|_| Error::new(ErrorKind::Unexpected, "block copy size exceeds usize"))?;
            self.block_ids.push((result.block_number, result.block_id));
            return Ok(Some(size));
        }

        if self.block_ids.len() != self.next_block_number {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "block copy numbers mismatch, please report bug to opendal",
            )
            .with_context("expected", self.next_block_number)
            .with_context("actual", self.block_ids.len()));
        }

        self.block_ids
            .sort_by_key(|(block_number, _)| *block_number);
        let block_ids = self
            .block_ids
            .iter()
            .map(|(_, block_id)| *block_id)
            .collect();
        let metadata = self.copier.complete_block(block_ids).await?;
        self.scheduled_block_ids.clear();
        self.block_ids.clear();
        self.state = State::Completed(metadata);
        Ok(None)
    }

    async fn close(&mut self) -> Result<Metadata> {
        match &self.state {
            State::Aborting | State::Aborted => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "block copier has already been aborted and can't be closed",
                ));
            }
            State::Running | State::Completed(_) => {}
        }

        while matches!(self.state, State::Running) {
            self.next().await?;
        }

        let State::Completed(metadata) = &self.state else {
            unreachable!("block copier must be completed after draining next()");
        };
        let metadata = metadata.clone();
        let Some(source_size) = self.source_size else {
            return Ok(metadata);
        };
        if metadata.is_file() {
            if metadata.content_length() != source_size {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "block copy result content length does not match source",
                )
                .with_context("expected", source_size)
                .with_context("actual", metadata.content_length()));
            }
            return Ok(metadata);
        }

        let mut builder = metadata.into_builder();
        builder.set_file(source_size);
        Ok(builder.build())
    }

    async fn abort(&mut self) -> Result<()> {
        match &self.state {
            State::Completed(_) | State::Aborted => return Ok(()),
            State::Running | State::Aborting => {}
        }

        self.state = State::Aborting;
        self.tasks.clear();

        // Keep scheduled_block_ids in place until abort_block succeeds so a
        // failed abort can be retried with the complete list.
        if !self.scheduled_block_ids.is_empty() {
            self.copier
                .abort_block(self.scheduled_block_ids.clone())
                .await?;
            self.scheduled_block_ids.clear();
        }

        self.block_ids.clear();
        self.state = State::Aborted;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use tokio::time::Duration;
    use tokio::time::sleep;
    use tokio::time::timeout;

    use super::*;
    use crate::raw::oio::Copy;

    #[derive(Default)]
    struct TestState {
        ranges: HashMap<Uuid, BytesRange>,
        completed_ranges: Vec<BytesRange>,
        source_metadata_calls: usize,
        copy_once_calls: usize,
        complete_block_calls: usize,
        abort_calls: Vec<Vec<Uuid>>,
        abort_failures_remaining: usize,
        delay_first_block: bool,
    }

    struct TestCopy {
        state: Arc<Mutex<TestState>>,
    }

    impl BlockCopy for TestCopy {
        async fn source_metadata(&self) -> Result<Metadata> {
            self.state
                .lock()
                .expect("test state mutex poisoned")
                .source_metadata_calls += 1;
            Ok({
                let mut metadata = MetadataBuilder::unknown();
                metadata.set_file(4);
                metadata.build()
            })
        }

        async fn copy_once(&self) -> Result<Metadata> {
            self.state
                .lock()
                .expect("test state mutex poisoned")
                .copy_once_calls += 1;
            Ok(MetadataBuilder::unknown().build())
        }

        async fn copy_block(&self, block_id: Uuid, range: BytesRange) -> Result<()> {
            let should_delay = range.offset() == 0
                && self
                    .state
                    .lock()
                    .expect("test state mutex poisoned")
                    .delay_first_block;
            if should_delay {
                sleep(Duration::from_millis(50)).await;
            }

            self.state
                .lock()
                .expect("test state mutex poisoned")
                .ranges
                .insert(block_id, range);

            Ok(())
        }

        async fn complete_block(&self, block_ids: Vec<Uuid>) -> Result<Metadata> {
            let mut state = self.state.lock().expect("test state mutex poisoned");
            state.complete_block_calls += 1;
            state.completed_ranges = block_ids
                .into_iter()
                .map(|block_id| state.ranges[&block_id])
                .collect();
            Ok(MetadataBuilder::unknown().build())
        }

        async fn abort_block(&self, block_ids: Vec<Uuid>) -> Result<()> {
            let mut state = self.state.lock().expect("test state mutex poisoned");
            state.abort_calls.push(block_ids);
            if state.abort_failures_remaining > 0 {
                state.abort_failures_remaining -= 1;
                return Err(Error::new(ErrorKind::Unexpected, "503 SlowDown").set_temporary());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_block_copier_completes_blocks_in_source_order() -> Result<()> {
        let state = Arc::new(Mutex::new(TestState {
            delay_first_block: true,
            ..Default::default()
        }));
        let inner = TestCopy {
            state: state.clone(),
        };
        let mut copier = BlockCopier::new(crate::test_executor(), inner, None, 0, 2, 2);

        while copier.next().await?.is_some() {}

        let completed_ranges = state
            .lock()
            .expect("test state mutex poisoned")
            .completed_ranges
            .clone();
        assert_eq!(
            completed_ranges,
            vec![BytesRange::new(0, Some(2)), BytesRange::new(2, Some(2))]
        );
        let metadata = copier.close().await?;
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 4);

        Ok(())
    }

    #[derive(Default)]
    struct FlakyCopyState {
        /// Number of `copy_block` calls that should still fail temporarily.
        failures_remaining: usize,
        copy_block_calls: usize,
        ranges: HashMap<Uuid, BytesRange>,
        completed_ranges: Vec<BytesRange>,
    }

    /// A BlockCopy that fails `copy_block` temporarily for the first
    /// `failures` calls and then succeeds.
    struct FlakyCopy {
        state: Arc<Mutex<FlakyCopyState>>,
    }

    impl FlakyCopy {
        fn new(failures: usize) -> Self {
            Self {
                state: Arc::new(Mutex::new(FlakyCopyState {
                    failures_remaining: failures,
                    ..Default::default()
                })),
            }
        }
    }

    impl BlockCopy for FlakyCopy {
        async fn source_metadata(&self) -> Result<Metadata> {
            unreachable!()
        }

        async fn copy_once(&self) -> Result<Metadata> {
            unreachable!()
        }

        async fn copy_block(&self, block_id: Uuid, range: BytesRange) -> Result<()> {
            let mut state = self.state.lock().expect("test state mutex poisoned");
            state.copy_block_calls += 1;
            if state.failures_remaining > 0 {
                state.failures_remaining -= 1;
                return Err(Error::new(ErrorKind::Unexpected, "503 SlowDown").set_temporary());
            }

            state.ranges.insert(block_id, range);
            Ok(())
        }

        async fn complete_block(&self, block_ids: Vec<Uuid>) -> Result<Metadata> {
            let mut state = self.state.lock().expect("test state mutex poisoned");
            state.completed_ranges = block_ids
                .into_iter()
                .map(|block_id| state.ranges[&block_id])
                .collect();
            Ok(MetadataBuilder::file(4).build())
        }

        async fn abort_block(&self, _: Vec<Uuid>) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_temporary_copy_block_error_is_returned_immediately() -> Result<()> {
        let inner = FlakyCopy::new(usize::MAX);
        let state = inner.state.clone();
        let mut copier = BlockCopier::new(
            crate::test_executor(),
            inner,
            Some(4),
            /*copy_once_threshold=*/ 0,
            /*block_size=*/ 2,
            /*concurrent=*/ 1,
        );

        // A persistent temporary error must surface to the caller (RetryLayer)
        // instead of spinning inside `next` forever.
        let err = timeout(Duration::from_secs(10), copier.next())
            .await
            .expect("next must not hang on temporary errors")
            .expect_err("next must propagate the temporary error");
        assert!(err.is_temporary());
        let state = state.lock().expect("test state mutex poisoned");
        assert_eq!(state.copy_block_calls, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_temporary_copy_block_error_retries_without_losing_blocks() -> Result<()> {
        // Fail the very first copy_block call once, then succeed.
        let inner = FlakyCopy::new(1);
        let state = inner.state.clone();
        let mut copier = BlockCopier::new(
            crate::test_executor(),
            inner,
            Some(4),
            /*copy_once_threshold=*/ 0,
            /*block_size=*/ 2,
            /*concurrent=*/ 2,
        );

        // Mimic RetryLayer: keep calling next() while it fails temporarily.
        loop {
            match timeout(Duration::from_secs(10), copier.next())
                .await
                .expect("next must not hang on temporary errors")
            {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(err) => assert!(err.is_temporary()),
            }
        }

        let metadata = copier.close().await?;
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 4);

        let state = state.lock().expect("test state mutex poisoned");
        // One failed call plus two successful blocks.
        assert_eq!(state.copy_block_calls, 3);
        assert_eq!(
            state.completed_ranges,
            vec![BytesRange::new(0, Some(2)), BytesRange::new(2, Some(2))]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_before_schedule_prevents_copy_and_close() -> Result<()> {
        let state = Arc::new(Mutex::new(TestState::default()));
        let inner = TestCopy {
            state: state.clone(),
        };
        let mut copier = BlockCopier::new(
            crate::test_executor(),
            inner,
            Some(4),
            /*copy_once_threshold=*/ 0,
            /*block_size=*/ 2,
            /*concurrent=*/ 1,
        );

        copier.abort().await?;
        copier.abort().await?;

        let err = copier.next().await.expect_err("next after abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let err = copier
            .close()
            .await
            .expect_err("close after abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let state = state.lock().expect("test state mutex poisoned");
        assert_eq!(state.source_metadata_calls, 0);
        assert_eq!(state.copy_once_calls, 0);
        assert_eq!(state.complete_block_calls, 0);
        assert!(state.abort_calls.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_after_schedule_clears_blocks_and_rejects_close() -> Result<()> {
        let state = Arc::new(Mutex::new(TestState::default()));
        let inner = TestCopy {
            state: state.clone(),
        };
        let mut copier = BlockCopier::new(
            crate::test_executor(),
            inner,
            Some(4),
            /*copy_once_threshold=*/ 0,
            /*block_size=*/ 2,
            /*concurrent=*/ 1,
        );

        assert_eq!(copier.next().await?, Some(2));

        copier.abort().await?;
        copier.abort().await?;

        let err = copier
            .close()
            .await
            .expect_err("close after abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let err = copier.next().await.expect_err("next after abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let state = state.lock().expect("test state mutex poisoned");
        assert_eq!(state.complete_block_calls, 0);
        assert_eq!(state.abort_calls.len(), 1);
        assert_eq!(state.abort_calls[0].len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_failure_is_retryable_and_blocks_copy_and_close() -> Result<()> {
        let state = Arc::new(Mutex::new(TestState {
            abort_failures_remaining: 1,
            ..Default::default()
        }));
        let inner = TestCopy {
            state: state.clone(),
        };
        let mut copier = BlockCopier::new(
            crate::test_executor(),
            inner,
            Some(4),
            /*copy_once_threshold=*/ 0,
            /*block_size=*/ 2,
            /*concurrent=*/ 1,
        );

        assert_eq!(copier.next().await?, Some(2));

        let err = copier
            .abort()
            .await
            .expect_err("first abort should surface temporary failure");
        assert!(err.is_temporary());

        let err = copier
            .next()
            .await
            .expect_err("next after failed abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let err = copier
            .close()
            .await
            .expect_err("close after failed abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        copier.abort().await?;
        copier.abort().await?;

        let err = copier
            .close()
            .await
            .expect_err("close after retried abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let state = state.lock().expect("test state mutex poisoned");
        assert_eq!(state.complete_block_calls, 0);
        assert_eq!(state.abort_calls.len(), 2);
        assert_eq!(state.abort_calls[0], state.abort_calls[1]);
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_after_complete_is_noop_and_keeps_completed_metadata() -> Result<()> {
        let state = Arc::new(Mutex::new(TestState::default()));
        let inner = TestCopy {
            state: state.clone(),
        };
        let mut copier = BlockCopier::new(
            crate::test_executor(),
            inner,
            Some(4),
            /*copy_once_threshold=*/ 0,
            /*block_size=*/ 2,
            /*concurrent=*/ 1,
        );

        while copier.next().await?.is_some() {}
        assert_eq!(
            state
                .lock()
                .expect("test state mutex poisoned")
                .complete_block_calls,
            1
        );

        copier.abort().await?;
        assert!(
            state
                .lock()
                .expect("test state mutex poisoned")
                .abort_calls
                .is_empty()
        );
        assert_eq!(copier.next().await?, None);

        let metadata = copier.close().await?;
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 4);

        copier.abort().await?;
        assert!(
            state
                .lock()
                .expect("test state mutex poisoned")
                .abort_calls
                .is_empty()
        );

        let metadata = copier.close().await?;
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 4);
        assert_eq!(
            state
                .lock()
                .expect("test state mutex poisoned")
                .complete_block_calls,
            1
        );
        Ok(())
    }
}
