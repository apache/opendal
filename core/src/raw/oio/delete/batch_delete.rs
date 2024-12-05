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

use crate::raw::*;
use crate::*;
use std::collections::HashSet;
use std::future::Future;

/// BatchDelete is used to implement [`oio::Delete`] based on batch delete operation.
///
/// OneShotDeleter will perform delete operation while calling `flush`.
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
    /// - Implementations should make sure that the length of `batch` equals to the return result's length.
    /// - Implementations should return error no path is deleted.
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
}

impl<D: BatchDelete> BatchDeleter<D> {
    /// Create a new batch deleter.
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            buffer: HashSet::default(),
        }
    }
}

impl<D: BatchDelete> oio::Delete for BatchDeleter<D> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.buffer.insert((path.to_string(), args));
        Ok(())
    }

    async fn flush(&mut self) -> Result<usize> {
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

        let batch = self.buffer.iter().cloned().collect();
        let result = self.inner.delete_batch(batch).await?;
        debug_assert!(
            !result.succeeded.is_empty(),
            "the number of succeeded operations must be greater than 0"
        );
        debug_assert_eq!(
            result.succeeded.len() + result.failed.len(),
            self.buffer.len(),
            "the number of succeeded and failed operations must be equal to the input batch size"
        );

        // Remove all succeeded operations from the buffer.
        let deleted = result.succeeded.len();
        for i in result.succeeded {
            self.buffer.remove(&i);
        }

        // Return directly if there are non-temporary errors.
        for (path, op, err) in result.failed {
            if !err.is_temporary() {
                return Err(err
                    .with_context("path", path)
                    .with_context("version", op.version().unwrap_or("<latest>")));
            }
        }

        // Return the number of succeeded operations to allow users to decide whether
        // to retry or push more delete operations.
        Ok(deleted)
    }
}
