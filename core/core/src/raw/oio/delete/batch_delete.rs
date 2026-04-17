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

use crate::raw::*;
use crate::*;

/// BatchDelete is used to implement [`oio::Delete`] based on batch delete operation.
///
/// OneShotDeleter will perform delete operation while calling `close`.
pub trait BatchDelete: Send + Sync + Unpin + 'static {
    /// delete_once delete one path at once.
    ///
    /// Implementations should make sure that the data is deleted correctly at once.
    ///
    /// BatchDeleter may call this method while there are only one path to delete.
    fn delete_once(
        &self,
        path: String,
        args: OpDelete,
    ) -> impl Future<Output = Result<()>> + MaybeSend;

    /// delete_batch delete multiple paths at once.
    ///
    /// - Implementations should report success/failure using indices into the input `batch` Vec.
    /// - Implementations should return error if no path is deleted.
    fn delete_batch(
        &self,
        batch: Vec<(String, OpDelete)>,
    ) -> impl Future<Output = Result<BatchDeleteResult>> + MaybeSend;
}

/// BatchDeleteResult is the result of batch delete operation.
///
/// Results are tracked by index into the input batch Vec, avoiding reliance on
/// `OpDelete` equality which can fail when services reconstruct `OpDelete` from
/// responses without preserving all fields.
#[derive(Default)]
pub struct BatchDeleteResult {
    /// Indices of successfully deleted items in the input batch.
    pub succeeded: Vec<usize>,
    /// Indices of failed deletions with their errors.
    pub failed: Vec<(usize, Error)>,
}

/// BatchDeleter is used to implement [`oio::Delete`] based on batch delete.
pub struct BatchDeleter<D: BatchDelete> {
    inner: D,
    buffer: Vec<(String, OpDelete)>,
    max_batch_size: usize,
}

impl<D: BatchDelete> BatchDeleter<D> {
    /// Create a new batch deleter.
    pub fn new(inner: D, max_batch_size: Option<usize>) -> Self {
        debug_assert!(
            max_batch_size.is_some(),
            "BatchDeleter requires delete_max_size to be configured"
        );
        let max_batch_size = max_batch_size.unwrap_or(1);

        Self {
            inner,
            buffer: Vec::new(),
            max_batch_size,
        }
    }

    async fn flush_buffer(&mut self) -> Result<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        if self.buffer.len() == 1 {
            let (path, args) = self.buffer.remove(0);
            self.inner.delete_once(path, args).await?;
            return Ok(1);
        }

        let batch: Vec<_> = self.buffer.drain(..).collect();
        let result = match self.inner.delete_batch(batch.clone()).await {
            Ok(result) => result,
            Err(err) => {
                // Restore all items back to buffer since the entire call failed.
                self.buffer = batch;
                return Err(err);
            }
        };

        if result.succeeded.is_empty() {
            // Restore all items back to buffer since nothing was deleted.
            self.buffer = batch;
            return Err(Error::new(
                ErrorKind::Unexpected,
                "batch delete returned zero successes",
            ));
        }
        if result.succeeded.len() + result.failed.len() != batch.len() {
            // Restore all items back to buffer since result is inconsistent.
            self.buffer = batch;
            return Err(Error::new(
                ErrorKind::Unexpected,
                "batch delete result size mismatch",
            ));
        }

        let deleted = result.succeeded.len();

        // Put failed items back into the buffer for retry.
        for (idx, err) in result.failed {
            if !err.is_temporary() {
                let (path, op) = &batch[idx];
                return Err(err
                    .with_context("path", path)
                    .with_context("version", op.version().unwrap_or("<latest>")));
            }
            self.buffer.push(batch[idx].clone());
        }

        Ok(deleted)
    }
}

impl<D: BatchDelete> oio::Delete for BatchDeleter<D> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.buffer.push((path.to_string(), args));
        if self.buffer.len() >= self.max_batch_size {
            let _ = self.flush_buffer().await?;
            return Ok(());
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        loop {
            let deleted = self.flush_buffer().await?;

            if self.buffer.is_empty() {
                break;
            }

            if deleted == 0 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "batch delete made no progress while buffer remains",
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::oio::Delete;

    /// A mock BatchDelete implementation that reports all items as successfully
    /// deleted by index.
    struct MockBatchDelete;

    impl BatchDelete for MockBatchDelete {
        async fn delete_once(&self, _path: String, _args: OpDelete) -> Result<()> {
            Ok(())
        }

        async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
            Ok(BatchDeleteResult {
                succeeded: (0..batch.len()).collect(),
                failed: vec![],
            })
        }
    }

    /// Regression test: previously, BatchDeleter used a HashSet with
    /// `(String, OpDelete)` equality to track progress. If a service
    /// reconstructed `OpDelete` from responses without preserving all fields
    /// (e.g., missing `recursive`), `buffer.remove()` would silently fail,
    /// leaving items stuck in the buffer forever and triggering a "no progress"
    /// error. With index-based tracking, this is no longer possible.
    #[tokio::test]
    async fn test_batch_deleter_index_based_progress() -> Result<()> {
        let mut deleter = BatchDeleter::new(MockBatchDelete, Some(3));

        deleter
            .delete("a", OpDelete::new().with_recursive(true))
            .await?;
        deleter
            .delete("b", OpDelete::new().with_recursive(true))
            .await?;
        deleter
            .delete("c", OpDelete::new().with_recursive(true))
            .await?;

        deleter.close().await?;
        Ok(())
    }

    /// A mock that fails one item with a temporary error on the first call,
    /// then succeeds on retry.
    struct MockPartialFailBatchDelete {
        call_count: std::sync::atomic::AtomicUsize,
    }

    impl BatchDelete for MockPartialFailBatchDelete {
        async fn delete_once(&self, _path: String, _args: OpDelete) -> Result<()> {
            Ok(())
        }

        async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
            let count = self
                .call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if count == 0 {
                let last = batch.len() - 1;
                Ok(BatchDeleteResult {
                    succeeded: (0..last).collect(),
                    failed: vec![(
                        last,
                        Error::new(ErrorKind::Unexpected, "temporary failure").set_temporary(),
                    )],
                })
            } else {
                Ok(BatchDeleteResult {
                    succeeded: (0..batch.len()).collect(),
                    failed: vec![],
                })
            }
        }
    }

    /// Test that failed items are properly retained and retried.
    #[tokio::test]
    async fn test_batch_deleter_partial_failure_retry() -> Result<()> {
        let mock = MockPartialFailBatchDelete {
            call_count: std::sync::atomic::AtomicUsize::new(0),
        };
        let mut deleter = BatchDeleter::new(mock, Some(3));

        deleter.delete("a", OpDelete::new()).await?;
        deleter.delete("b", OpDelete::new()).await?;
        deleter.delete("c", OpDelete::new()).await?;

        deleter.close().await?;
        Ok(())
    }

    /// A mock that fails the entire delete_batch call on the first attempt,
    /// then succeeds on retry.
    struct MockTransportFailBatchDelete {
        call_count: std::sync::atomic::AtomicUsize,
    }

    impl BatchDelete for MockTransportFailBatchDelete {
        async fn delete_once(&self, _path: String, _args: OpDelete) -> Result<()> {
            Ok(())
        }

        async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
            let count = self
                .call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if count == 0 {
                Err(Error::new(ErrorKind::Unexpected, "transport error").set_temporary())
            } else {
                Ok(BatchDeleteResult {
                    succeeded: (0..batch.len()).collect(),
                    failed: vec![],
                })
            }
        }
    }

    /// Regression test: when delete_batch() itself returns Err (e.g., transport
    /// failure), the buffer must be restored so close() can retry successfully.
    #[tokio::test]
    async fn test_batch_deleter_transport_error_restores_buffer() -> Result<()> {
        let mock = MockTransportFailBatchDelete {
            call_count: std::sync::atomic::AtomicUsize::new(0),
        };
        let mut deleter = BatchDeleter::new(mock, Some(3));

        deleter.delete("a", OpDelete::new()).await?;
        deleter.delete("b", OpDelete::new()).await?;
        // Third delete triggers flush, which fails. Buffer should be restored.
        let err = deleter.delete("c", OpDelete::new()).await;
        assert!(err.is_err());

        // close() retries: the batch is still in buffer, now succeeds.
        deleter.close().await?;
        Ok(())
    }
}
