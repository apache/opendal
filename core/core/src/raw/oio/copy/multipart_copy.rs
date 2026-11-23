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

use crate::raw::oio::MultipartPart;
use crate::raw::*;
use crate::*;

/// MultipartCopy is used to implement [`oio::Copy`] based on multipart copy.
///
/// By implementing MultipartCopy, services only need to provide the
/// service-specific multipart copy operations. [`MultipartCopier`] will drive
/// the upload id, part queue, completion, and abort state.
pub trait MultipartCopy: Send + Sync + Unpin + 'static {
    /// source_metadata returns source metadata for planning multipart copy.
    ///
    /// MultipartCopier will call this API when source content length hint is not
    /// provided.
    fn source_metadata(&self) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// copy_once is used to copy the source object at once.
    ///
    /// MultipartCopier will call this API when the source object can be copied
    /// without starting a multipart copy.
    fn copy_once(&self) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// initiate_copy starts a multipart copy and returns the upload id.
    fn initiate_copy(&self) -> impl Future<Output = Result<String>> + MaybeSend;

    /// copy_part copies one source range into one multipart upload part.
    ///
    /// - part_number is the index of the part, starting from 0.
    fn copy_part(
        &self,
        upload_id: &str,
        part_number: usize,
        range: BytesRange,
    ) -> impl Future<Output = Result<MultipartPart>> + MaybeSend;

    /// complete_copy completes the multipart copy with the ordered part list.
    fn complete_copy(
        &self,
        upload_id: &str,
        parts: &[MultipartPart],
    ) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// abort_copy cancels the pending multipart copy and purges intermediate state.
    fn abort_copy(&self, upload_id: &str) -> impl Future<Output = Result<()>> + MaybeSend;
}

struct CopyInput<C: MultipartCopy> {
    copier: Arc<C>,
    executor: Executor,
    upload_id: Arc<String>,
    part_number: usize,
    range: BytesRange,
}

/// Build executor and capability inputs for [`MultipartCopier`].
pub trait IntoMultipartCopyContext {
    /// Convert into executor and capability.
    fn into_multipart_copy_context(self) -> (Executor, Capability);
}

impl IntoMultipartCopyContext for Executor {
    fn into_multipart_copy_context(self) -> (Executor, Capability) {
        (self, Capability::default())
    }
}

impl IntoMultipartCopyContext for (Executor, Capability) {
    fn into_multipart_copy_context(self) -> (Executor, Capability) {
        self
    }
}

struct CopiedPart {
    part: MultipartPart,
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

/// MultipartCopier implements [`oio::Copy`] based on multipart copy.
pub struct MultipartCopier<C: MultipartCopy> {
    copier: Arc<C>,
    executor: Executor,
    capability: Capability,

    upload_id: Option<Arc<String>>,
    parts: Vec<MultipartPart>,
    next_part_number: usize,
    next_offset: u64,
    source_size: Option<u64>,
    copy_once_threshold: u64,
    part_size: u64,
    concurrent: usize,
    state: State,

    tasks: ConcurrentTasks<CopyInput<C>, CopiedPart>,
}

impl<C: MultipartCopy> MultipartCopier<C> {
    /// Create a new MultipartCopier.
    pub fn new(
        context: impl IntoMultipartCopyContext,
        inner: C,
        source_content_length_hint: Option<u64>,
        copy_once_threshold: u64,
        part_size: u64,
        concurrent: usize,
    ) -> Self {
        let (executor, capability) = context.into_multipart_copy_context();
        let copier = Arc::new(inner);
        let concurrent = concurrent.max(1);

        Self {
            copier,
            executor: executor.clone(),
            capability,
            upload_id: None,
            parts: Vec::new(),
            next_part_number: 0,
            next_offset: 0,
            source_size: source_content_length_hint,
            copy_once_threshold,
            part_size,
            concurrent,
            state: State::Running,

            tasks: ConcurrentTasks::new(executor, concurrent, 0, |input| {
                Box::pin(async move {
                    let size = input
                        .range
                        .size()
                        .expect("multipart copy range must be sized");
                    let fut =
                        input
                            .copier
                            .copy_part(&input.upload_id, input.part_number, input.range);

                    let result = match input.executor.timeout() {
                        None => fut.await.map(|part| CopiedPart { part, size }),
                        Some(timeout) => {
                            select! {
                                result = fut.fuse() => {
                                    result.map(|part| CopiedPart { part, size })
                                }
                                _ = timeout.fuse() => {
                                    Err(Error::new(
                                        ErrorKind::Unexpected, "copy part timeout")
                                            .with_context("upload_id", input.upload_id.to_string())
                                            .with_context("part_number", input.part_number.to_string())
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

    /// Ensure the planned multipart copy does not exceed the service's derived part-count limit.
    ///
    /// This is called before `initiate_copy` so we fail a copy operation before we make any IO.
    fn validate_part_count(&self, source_size: u64) -> Result<()> {
        let capability = self.capability;
        let (Some(max_total_size), Some(max_part_size)) = (
            capability.write_total_max_size,
            capability.write_multi_max_size,
        ) else {
            return Ok(());
        };

        if max_part_size == 0 {
            return Ok(());
        }

        let max_parts = (max_total_size as u64).div_ceil(max_part_size as u64);
        let part_count = source_size.div_ceil(self.part_size);
        if part_count > max_parts {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "multipart copy part count exceeds service limit, please increase `OpCopy::chunk`",
            )
            .with_context("source_size", source_size)
            .with_context("part_size", self.part_size)
            .with_context("part_count", part_count)
            .with_context("max_parts", max_parts));
        }

        Ok(())
    }

    async fn upload_id(&mut self) -> Result<Arc<String>> {
        match self.upload_id.clone() {
            Some(upload_id) => Ok(upload_id),
            None => {
                let upload_id = self.copier.initiate_copy().await?;
                let upload_id = Arc::new(upload_id);
                self.upload_id = Some(upload_id.clone());
                Ok(upload_id)
            }
        }
    }

    async fn fill_tasks(&mut self, upload_id: Arc<String>, source_size: u64) -> Result<()> {
        let mut scheduled = 0;
        let executor = self.executor.clone();

        while self.next_offset < source_size
            && self.tasks.has_remaining()
            && scheduled < self.concurrent
        {
            let size = self.part_size.min(source_size - self.next_offset);
            let range = BytesRange::new(self.next_offset, Some(size));

            let input = CopyInput {
                copier: self.copier.clone(),
                executor: executor.clone(),
                upload_id: upload_id.clone(),
                part_number: self.next_part_number,
                range,
            };

            // Propagate errors to the caller (e.g. RetryLayer) instead of
            // retrying in place. ConcurrentTasks keeps failed task inputs in
            // the queue, and this input is rebuilt from next_offset on the
            // next call, so a retry preserves progress.
            self.tasks.execute(input).await?;

            self.next_offset += size;
            self.next_part_number += 1;
            scheduled += 1;

            if self.tasks.has_result() {
                break;
            }
        }

        Ok(())
    }
}

impl<C> oio::Copy for MultipartCopier<C>
where
    C: MultipartCopy,
{
    async fn next(&mut self) -> Result<Option<usize>> {
        match &self.state {
            State::Completed(_) => return Ok(None),
            State::Aborting | State::Aborted => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "multipart copier has already been aborted",
                ));
            }
            State::Running => {}
        }

        let source_size = self.source_size().await?;

        if self.upload_id.is_none() && source_size <= self.copy_once_threshold {
            let metadata = self.copier.copy_once().await?;
            self.state = State::Completed(metadata);
            return Ok(None);
        }

        // Validate part count before performing any copy requests.
        if self.upload_id.is_none() {
            self.validate_part_count(source_size)?;
        }

        let upload_id = self.upload_id().await?;
        self.fill_tasks(upload_id.clone(), source_size).await?;

        if let Some(result) = self.tasks.next().await.transpose()? {
            let size = result.size.try_into().map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "multipart copy part size exceeds usize",
                )
            })?;
            self.parts.push(result.part);
            return Ok(Some(size));
        }

        if self.parts.len() != self.next_part_number {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "multipart copy part numbers mismatch, please report bug to opendal",
            )
            .with_context("expected", self.next_part_number)
            .with_context("actual", self.parts.len())
            .with_context("upload_id", upload_id.to_string()));
        }

        self.parts.sort_by_key(|part| part.part_number);
        let metadata = self.copier.complete_copy(&upload_id, &self.parts).await?;
        self.upload_id = None;
        self.parts.clear();
        self.state = State::Completed(metadata);
        Ok(None)
    }

    async fn close(&mut self) -> Result<Metadata> {
        match &self.state {
            State::Aborting | State::Aborted => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "multipart copier has already been aborted and can't be closed",
                ));
            }
            State::Running | State::Completed(_) => {}
        }

        while matches!(self.state, State::Running) {
            self.next().await?;
        }

        let State::Completed(metadata) = &self.state else {
            unreachable!("multipart copier must be completed after draining next()");
        };
        let metadata = metadata.clone();
        let Some(source_size) = self.source_size else {
            return Ok(metadata);
        };
        if metadata.is_file() {
            if metadata.content_length() != source_size {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "multipart copy result content length does not match source",
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

        // Keep upload_id in place until abort_copy succeeds so a failed abort
        // can be retried for the same multipart copy.
        if let Some(upload_id) = self.upload_id.clone() {
            self.copier.abort_copy(&upload_id).await?;
            self.upload_id = None;
        }

        self.parts.clear();
        self.state = State::Aborted;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use tokio::time::Duration;
    use tokio::time::timeout;

    use super::*;
    use crate::raw::oio::Copy;

    struct TestCopy {
        source_size: u64,
        source_metadata_calls: AtomicUsize,
        copy_once_calls: AtomicUsize,
        initiate_copy_calls: AtomicUsize,
        complete_copy_calls: AtomicUsize,
        abort_copy_calls: AtomicUsize,
        abort_failures: usize,
    }

    impl TestCopy {
        fn new(source_size: u64) -> Arc<Self> {
            Self::with_abort_failures(source_size, 0)
        }

        fn with_abort_failures(source_size: u64, abort_failures: usize) -> Arc<Self> {
            Arc::new(Self {
                source_size,
                source_metadata_calls: AtomicUsize::new(0),
                copy_once_calls: AtomicUsize::new(0),
                initiate_copy_calls: AtomicUsize::new(0),
                complete_copy_calls: AtomicUsize::new(0),
                abort_copy_calls: AtomicUsize::new(0),
                abort_failures,
            })
        }
    }

    impl MultipartCopy for Arc<TestCopy> {
        async fn source_metadata(&self) -> Result<Metadata> {
            self.source_metadata_calls.fetch_add(1, Ordering::Relaxed);
            Ok({
                let mut metadata = MetadataBuilder::unknown();
                metadata.set_file(self.source_size);
                metadata.build()
            })
        }

        async fn copy_once(&self) -> Result<Metadata> {
            self.copy_once_calls.fetch_add(1, Ordering::Relaxed);
            Ok(MetadataBuilder::unknown().build())
        }

        async fn initiate_copy(&self) -> Result<String> {
            self.initiate_copy_calls.fetch_add(1, Ordering::Relaxed);
            Ok("upload_id".to_string())
        }

        async fn copy_part(
            &self,
            _: &str,
            part_number: usize,
            range: BytesRange,
        ) -> Result<MultipartPart> {
            Ok(MultipartPart {
                part_number,
                etag: "etag".to_string(),
                checksum: None,
                size: range.size(),
            })
        }

        async fn complete_copy(&self, _: &str, _: &[MultipartPart]) -> Result<Metadata> {
            self.complete_copy_calls.fetch_add(1, Ordering::Relaxed);
            Ok(MetadataBuilder::unknown().build())
        }

        async fn abort_copy(&self, _: &str) -> Result<()> {
            let call = self.abort_copy_calls.fetch_add(1, Ordering::Relaxed);
            if call < self.abort_failures {
                return Err(Error::new(ErrorKind::Unexpected, "503 SlowDown").set_temporary());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_content_length_hint_skips_source_metadata() -> Result<()> {
        let inner = TestCopy::new(8);
        let mut copier =
            MultipartCopier::new(crate::test_executor(), inner.clone(), Some(8), 8, 8, 1);

        let metadata = copier.close().await?;
        assert_eq!(inner.source_metadata_calls.load(Ordering::Relaxed), 0);
        assert_eq!(inner.copy_once_calls.load(Ordering::Relaxed), 1);
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 8);
        Ok(())
    }

    #[tokio::test]
    async fn test_missing_content_length_hint_loads_source_metadata() -> Result<()> {
        let inner = TestCopy::new(8);
        let mut copier = MultipartCopier::new(crate::test_executor(), inner.clone(), None, 8, 8, 1);

        let metadata = copier.close().await?;
        assert_eq!(inner.source_metadata_calls.load(Ordering::Relaxed), 1);
        assert_eq!(inner.copy_once_calls.load(Ordering::Relaxed), 1);
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 8);
        Ok(())
    }

    #[tokio::test]
    async fn test_validate_part_count_rejects_before_initiate() -> Result<()> {
        let capability = Capability {
            write_total_max_size: Some(2),
            write_multi_max_size: Some(1),
            ..Default::default()
        };

        // source_size=10, part_size=1, copy_once_threshold=0 -> 10 parts needed, only 2 allowed.
        let inner = TestCopy::new(10);
        let mut copier = MultipartCopier::new(
            /*context=*/ (crate::test_executor(), capability),
            /*inner=*/ inner.clone(),
            /*source_content_length_hint=*/ Some(10),
            /*copy_once_threshold=*/ 0,
            /*part_size=*/ 1,
            /*concurrent=*/ 1,
        );

        let err = copier
            .next()
            .await
            .expect_err("part count should exceed max_parts");
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        // Validation must reject before any multipart state is created on the server.
        assert_eq!(inner.copy_once_calls.load(Ordering::Relaxed), 0);
        assert_eq!(inner.initiate_copy_calls.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[derive(Default)]
    struct FlakyCopyState {
        /// Number of `copy_part` calls that should still fail temporarily.
        failures_remaining: usize,
        copy_part_calls: usize,
        completed_part_numbers: Vec<usize>,
        completed_ranges: Vec<BytesRange>,
    }

    /// A MultipartCopy that fails `copy_part` temporarily for the first
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

    impl MultipartCopy for FlakyCopy {
        async fn source_metadata(&self) -> Result<Metadata> {
            unreachable!()
        }

        async fn copy_once(&self) -> Result<Metadata> {
            unreachable!()
        }

        async fn initiate_copy(&self) -> Result<String> {
            Ok("upload_id".to_string())
        }

        async fn copy_part(
            &self,
            _: &str,
            part_number: usize,
            range: BytesRange,
        ) -> Result<MultipartPart> {
            let mut state = self.state.lock().expect("test state mutex poisoned");
            state.copy_part_calls += 1;
            if state.failures_remaining > 0 {
                state.failures_remaining -= 1;
                return Err(Error::new(ErrorKind::Unexpected, "503 SlowDown").set_temporary());
            }

            Ok(MultipartPart {
                part_number,
                etag: "etag".to_string(),
                checksum: None,
                size: range.size(),
            })
        }

        async fn complete_copy(&self, _: &str, parts: &[MultipartPart]) -> Result<Metadata> {
            let mut state = self.state.lock().expect("test state mutex poisoned");
            state.completed_part_numbers = parts.iter().map(|part| part.part_number).collect();
            state.completed_ranges = parts
                .iter()
                .map(|part| BytesRange::new(part.part_number as u64 * 4, part.size))
                .collect();
            Ok(MetadataBuilder::file(8).build())
        }

        async fn abort_copy(&self, _: &str) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_temporary_copy_part_error_is_returned_immediately() -> Result<()> {
        let inner = FlakyCopy::new(usize::MAX);
        let state = inner.state.clone();
        let mut copier = MultipartCopier::new(
            crate::test_executor(),
            inner,
            Some(8),
            /*copy_once_threshold=*/ 0,
            /*part_size=*/ 4,
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
        assert_eq!(state.copy_part_calls, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_temporary_copy_part_error_retries_without_losing_parts() -> Result<()> {
        // Fail the very first copy_part call once, then succeed.
        let inner = FlakyCopy::new(1);
        let state = inner.state.clone();
        let mut copier = MultipartCopier::new(
            crate::test_executor(),
            inner,
            Some(8),
            /*copy_once_threshold=*/ 0,
            /*part_size=*/ 4,
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
        assert_eq!(metadata.content_length(), 8);

        let state = state.lock().expect("test state mutex poisoned");
        // One failed call plus two successful parts.
        assert_eq!(state.copy_part_calls, 3);
        assert_eq!(state.completed_part_numbers, vec![0, 1]);
        assert_eq!(
            state.completed_ranges,
            vec![BytesRange::new(0, Some(4)), BytesRange::new(4, Some(4))]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_before_initiate_prevents_copy_and_close() -> Result<()> {
        let inner = TestCopy::new(8);
        let mut copier = MultipartCopier::new(
            crate::test_executor(),
            inner.clone(),
            Some(8),
            /*copy_once_threshold=*/ 0,
            /*part_size=*/ 4,
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

        assert_eq!(inner.source_metadata_calls.load(Ordering::Relaxed), 0);
        assert_eq!(inner.copy_once_calls.load(Ordering::Relaxed), 0);
        assert_eq!(inner.initiate_copy_calls.load(Ordering::Relaxed), 0);
        assert_eq!(inner.complete_copy_calls.load(Ordering::Relaxed), 0);
        assert_eq!(inner.abort_copy_calls.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_after_initiate_rejects_close_and_is_idempotent() -> Result<()> {
        let inner = TestCopy::new(8);
        let mut copier = MultipartCopier::new(
            crate::test_executor(),
            inner.clone(),
            Some(8),
            /*copy_once_threshold=*/ 0,
            /*part_size=*/ 4,
            /*concurrent=*/ 1,
        );

        assert_eq!(copier.next().await?, Some(4));
        assert_eq!(inner.initiate_copy_calls.load(Ordering::Relaxed), 1);

        copier.abort().await?;
        copier.abort().await?;
        assert_eq!(inner.abort_copy_calls.load(Ordering::Relaxed), 1);

        let err = copier
            .close()
            .await
            .expect_err("close after abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let err = copier.next().await.expect_err("next after abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());
        assert_eq!(inner.complete_copy_calls.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_failure_is_retryable_and_blocks_copy_and_close() -> Result<()> {
        let inner = TestCopy::with_abort_failures(8, 1);
        let mut copier = MultipartCopier::new(
            crate::test_executor(),
            inner.clone(),
            Some(8),
            /*copy_once_threshold=*/ 0,
            /*part_size=*/ 4,
            /*concurrent=*/ 1,
        );

        assert_eq!(copier.next().await?, Some(4));

        let err = copier
            .abort()
            .await
            .expect_err("first abort should surface temporary failure");
        assert!(err.is_temporary());
        assert_eq!(inner.abort_copy_calls.load(Ordering::Relaxed), 1);

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
        assert_eq!(inner.abort_copy_calls.load(Ordering::Relaxed), 2);
        assert_eq!(inner.complete_copy_calls.load(Ordering::Relaxed), 0);

        let err = copier
            .close()
            .await
            .expect_err("close after retried abort must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());
        Ok(())
    }

    #[tokio::test]
    async fn test_abort_after_complete_is_noop_and_keeps_completed_metadata() -> Result<()> {
        let inner = TestCopy::new(8);
        let mut copier = MultipartCopier::new(
            crate::test_executor(),
            inner.clone(),
            Some(8),
            /*copy_once_threshold=*/ 0,
            /*part_size=*/ 4,
            /*concurrent=*/ 1,
        );

        while copier.next().await?.is_some() {}
        assert_eq!(inner.complete_copy_calls.load(Ordering::Relaxed), 1);

        copier.abort().await?;
        assert_eq!(inner.abort_copy_calls.load(Ordering::Relaxed), 0);
        assert_eq!(copier.next().await?, None);

        let metadata = copier.close().await?;
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 8);

        copier.abort().await?;
        assert_eq!(inner.abort_copy_calls.load(Ordering::Relaxed), 0);

        let metadata = copier.close().await?;
        assert!(metadata.is_file());
        assert_eq!(metadata.content_length(), 8);
        assert_eq!(inner.complete_copy_calls.load(Ordering::Relaxed), 1);
        Ok(())
    }
}
