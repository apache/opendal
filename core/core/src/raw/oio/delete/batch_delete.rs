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

use std::collections::HashSet;
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
    /// Implementations must account for every input path in either
    /// `succeeded` or `failed`; `BatchDeleter` rejects a result that drops
    /// or invents paths.
    fn delete_batch(
        &self,
        batch: Vec<(String, OpDelete)>,
    ) -> impl Future<Output = Result<BatchDeleteResult>> + MaybeSend;
}

/// BatchDeleteResult is the result of batch delete operation.
#[derive(Default)]
pub struct BatchDeleteResult {
    /// Collection of successful deletions, containing tuples of (path, args)
    pub succeeded: Vec<(String, OpDelete)>,
    /// Collection of failed deletions, containing tuples of (path, args, error)
    pub failed: Vec<(String, OpDelete, Error)>,
}

/// BatchDeleter is used to implement [`oio::Delete`] based on batch delete.
pub struct BatchDeleter<D: BatchDelete> {
    inner: D,
    buffer: HashSet<(String, OpDelete)>,
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
            buffer: HashSet::default(),
            max_batch_size,
        }
    }

    async fn flush_buffer(&mut self) -> Result<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        if self.buffer.len() == 1 {
            let (path, args) = self
                .buffer
                .iter()
                .next()
                .expect("the delete buffer size must be 1")
                .clone();
            self.inner.delete_once(path, args).await?;
            self.buffer.clear();
            return Ok(1);
        }

        let batch_len = self.buffer.len();
        let batch = self.buffer.iter().cloned().collect();
        let result = self.inner.delete_batch(batch).await?;

        // The backend must account for every input path. A result that
        // drops or invents paths is a bug, so fail instead of losing
        // buffered deletes or retrying forever.
        if result.succeeded.len() + result.failed.len() != batch_len {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "batch delete result size mismatch",
            ));
        }

        for item in result.succeeded {
            self.buffer.remove(&item);
        }
        let deleted = batch_len - self.buffer.len();

        // Failed paths remain queued so a later flush can retry them.
        let mut first_temporary_error = None;
        for (path, op, err) in result.failed {
            let err = err
                .with_context("path", path)
                .with_context("version", op.version().unwrap_or("<latest>"));
            if !err.is_temporary() {
                return Err(err);
            }
            first_temporary_error.get_or_insert(err);
        }

        if deleted == 0 {
            return Err(first_temporary_error.unwrap_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "batch delete removed zero buffered paths",
                )
            }));
        }

        Ok(deleted)
    }
}

impl<D: BatchDelete> oio::Delete for BatchDeleter<D> {
    async fn delete(&mut self, path: &str, mut args: OpDelete) -> Result<()> {
        // A concurrent task fills its own handle, then lets close own the flush
        // and any layer-level retries of that flush.
        let deferred = args.take_deferred_batch_flush();
        if deferred && self.buffer.len() >= self.max_batch_size {
            // An outer layer may advertise a larger batch than this backend accepts.
            self.flush_buffer().await?;
        }
        self.buffer.insert((path.to_string(), args));
        if !deferred && self.buffer.len() >= self.max_batch_size {
            self.flush_buffer().await?;
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
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use crate::raw::oio::Delete;

    type BatchResponse = Box<dyn Fn(Vec<(String, OpDelete)>) -> BatchDeleteResult + Send + Sync>;

    /// MockBatchDelete replays scripted `delete_batch` responses and records
    /// calls so tests can assert how `BatchDeleter` retries failed paths.
    struct MockBatchDelete {
        delete_batch_calls: Mutex<Vec<Vec<String>>>,
        delete_once_paths: Mutex<Vec<String>>,
        responses: Mutex<VecDeque<BatchResponse>>,
    }

    impl MockBatchDelete {
        fn new() -> Self {
            Self {
                delete_batch_calls: Mutex::new(Vec::new()),
                delete_once_paths: Mutex::new(Vec::new()),
                responses: Mutex::new(VecDeque::new()),
            }
        }

        fn push_response(
            &self,
            respond: impl Fn(Vec<(String, OpDelete)>) -> BatchDeleteResult + Send + Sync + 'static,
        ) {
            self.responses.lock().unwrap().push_back(Box::new(respond));
        }
    }

    impl BatchDelete for MockBatchDelete {
        async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
            self.delete_once_paths.lock().unwrap().push(path);
            Ok(())
        }

        async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
            let paths = batch.iter().map(|(path, _)| path.clone()).collect();
            self.delete_batch_calls.lock().unwrap().push(paths);
            let respond = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("delete_batch called without a scripted response");
            Ok(respond(batch))
        }
    }

    /// Split a batch by path into `succeeded` and `failed`. Items listed in
    /// neither are dropped from the result, like a buggy backend would.
    /// `failed` entries are `(path, kind, temporary)`.
    fn split(
        succeeded: Vec<&'static str>,
        failed: Vec<(&'static str, ErrorKind, bool)>,
    ) -> impl Fn(Vec<(String, OpDelete)>) -> BatchDeleteResult + Send + Sync {
        move |batch| {
            let mut result = BatchDeleteResult::default();
            for (path, op) in batch {
                if let Some((_, kind, temporary)) =
                    failed.iter().find(|(p, ..)| *p == path.as_str())
                {
                    result.failed.push((
                        path,
                        op,
                        Error::new(*kind, "delete failed").with_temporary(*temporary),
                    ));
                } else if succeeded.iter().any(|p| *p == path) {
                    result.succeeded.push((path, op));
                }
            }
            result
        }
    }

    /// A batch where every item fails permanently must surface the real
    /// per-object error, not a masked `Unexpected`.
    #[tokio::test]
    async fn test_batch_delete_all_failed_permanent() {
        let inner = MockBatchDelete::new();
        inner.push_response(split(
            vec![],
            vec![
                ("a", ErrorKind::ConditionNotMatch, false),
                ("b", ErrorKind::PermissionDenied, false),
                ("c", ErrorKind::ConditionNotMatch, false),
            ],
        ));
        let mut deleter = BatchDeleter::new(inner, Some(10));

        for path in ["a", "b", "c"] {
            deleter.delete(path, OpDelete::new()).await.unwrap();
        }
        let err = deleter.close().await.unwrap_err();

        assert!(
            matches!(
                err.kind(),
                ErrorKind::ConditionNotMatch | ErrorKind::PermissionDenied
            ),
            "permanent batch failure must keep its original kind: {err:?}"
        );
        assert!(!err.is_temporary());
        let rendered = err.to_string();
        assert!(rendered.contains("path: "), "missing path context: {err}");
        assert!(rendered.contains("version: <latest>"));
    }

    /// A batch where every item fails with a temporary error must return a
    /// temporary error and keep all paths queued, so a retry layer can
    /// replay the same batch on a later `close`.
    #[tokio::test]
    async fn test_batch_delete_all_failed_temporary_retries() {
        let inner = MockBatchDelete::new();
        inner.push_response(split(
            vec![],
            vec![
                ("a", ErrorKind::RateLimited, true),
                ("b", ErrorKind::RateLimited, true),
            ],
        ));
        inner.push_response(split(vec!["a", "b"], vec![]));
        let mut deleter = BatchDeleter::new(inner, Some(10));

        deleter.delete("a", OpDelete::new()).await.unwrap();
        deleter.delete("b", OpDelete::new()).await.unwrap();
        let err = deleter.close().await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert!(
            err.is_temporary(),
            "temporary batch failure must stay retryable: {err:?}"
        );
        assert!(err.to_string().contains("path: "));
        assert_eq!(deleter.buffer.len(), 2);

        deleter.close().await.unwrap();
        assert_eq!(deleter.inner.delete_batch_calls.lock().unwrap().len(), 2);
        assert!(deleter.buffer.is_empty());
    }

    /// A result accounting for zero input paths is a backend bug and must
    /// fail fast instead of looping forever.
    #[tokio::test]
    async fn test_batch_delete_empty_result_is_unexpected() {
        let inner = MockBatchDelete::new();
        inner.push_response(|_| BatchDeleteResult::default());
        let mut deleter = BatchDeleter::new(inner, Some(10));

        deleter.delete("a", OpDelete::new()).await.unwrap();
        deleter.delete("b", OpDelete::new()).await.unwrap();
        let err = deleter.close().await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());
        assert_eq!(deleter.inner.delete_batch_calls.lock().unwrap().len(), 1);
    }

    /// A permanent per-object error surfaces with its original kind even
    /// when the rest of the batch succeeded.
    #[tokio::test]
    async fn test_batch_delete_partial_failure_permanent() {
        let inner = MockBatchDelete::new();
        inner.push_response(split(
            vec!["a", "b"],
            vec![("c", ErrorKind::ConditionNotMatch, false)],
        ));
        let mut deleter = BatchDeleter::new(inner, Some(10));

        for path in ["a", "b", "c"] {
            deleter.delete(path, OpDelete::new()).await.unwrap();
        }
        let err = deleter.close().await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(err.to_string().contains("path: c"));
        // "c" stays queued; only the failed path remains in the buffer.
        assert_eq!(deleter.buffer.len(), 1);
        assert!(deleter.buffer.iter().all(|(p, _)| p == "c"));
    }

    /// Temporary per-object errors alongside progress keep the failed
    /// paths queued, so `close` retries them until the buffer drains.
    #[tokio::test]
    async fn test_batch_delete_partial_failure_temporary_retries() {
        let inner = MockBatchDelete::new();
        inner.push_response(split(
            vec!["a"],
            vec![
                ("b", ErrorKind::RateLimited, true),
                ("c", ErrorKind::RateLimited, true),
            ],
        ));
        inner.push_response(split(vec!["b"], vec![("c", ErrorKind::RateLimited, true)]));
        let mut deleter = BatchDeleter::new(inner, Some(10));

        for path in ["a", "b", "c"] {
            deleter.delete(path, OpDelete::new()).await.unwrap();
        }
        deleter.close().await.unwrap();

        assert_eq!(deleter.inner.delete_batch_calls.lock().unwrap().len(), 2);
        // "c" drains through the single-object `delete_once` path.
        assert_eq!(
            deleter.inner.delete_once_paths.lock().unwrap().as_slice(),
            &["c"]
        );
        assert!(deleter.buffer.is_empty());
    }
}
