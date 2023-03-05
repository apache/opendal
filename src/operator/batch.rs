// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::stream;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// BatchOperator is used to take batch operations like `remove_all`.
///
/// # Examples
///
/// ```
/// # use anyhow::Result;
/// # use futures::io;
/// # use opendal::Operator;
/// #
/// # #[tokio::main]
/// # async fn test(op: Operator) -> Result<()> {
/// op.batch()
///     .with_limit(1000)
///     .remove_all("dir/to/delete")
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct BatchOperator {
    src: Operator,
    meta: OperatorMetadata,

    limit: usize,
}

impl BatchOperator {
    pub(crate) fn new(op: Operator) -> Self {
        let meta = op.metadata();

        BatchOperator {
            src: op,
            meta,
            limit: 1000,
        }
    }

    /// Specify the batch limit.
    ///
    /// Default: 1000
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// remove will given paths.
    ///
    /// # Notes
    ///
    /// If underlying services support delete in batch, we will use batch
    /// delete instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.batch()
    ///     .remove(vec!["abc".to_string(), "def".to_string()])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove(&self, paths: Vec<String>) -> Result<()> {
        self.remove_via(stream::iter(paths)).await
    }

    /// remove_via will remove objects via given stream.
    ///
    /// We will delete by chunks with given batch limit on the stream.
    ///
    /// # Notes
    ///
    /// If underlying services support delete in batch, we will use batch
    /// delete instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// use futures::stream;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let stream = stream::iter(vec!["abc".to_string(), "def".to_string()]);
    /// op.batch().remove_via(stream).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_via(&self, mut input: impl Stream<Item = String> + Unpin) -> Result<()> {
        if self.meta.can_batch() {
            let mut input = input.map(|v| (v, OpDelete::default())).chunks(self.limit);

            while let Some(batches) = input.next().await {
                let results = self
                    .src
                    .inner()
                    .batch(OpBatch::new(BatchOperations::Delete(batches)))
                    .await?;

                let BatchedResults::Delete(results) = results.into_results();

                // TODO: return error here directly seems not a good idea?
                for (_, result) in results {
                    let _ = result?;
                }
            }
        } else {
            while let Some(path) = input.next().await {
                self.src.inner().delete(&path, OpDelete::default()).await?;
            }
        }

        Ok(())
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Notes
    ///
    /// If underlying services support delete in batch, we will use batch
    /// delete instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.batch().remove_all("path/to/dir").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_all(&self, path: &str) -> Result<()> {
        let parent = self.src.object(path);
        let meta = parent.stat().await?;

        if meta.mode() != ObjectMode::DIR {
            return parent.delete().await;
        }

        let obs = parent.scan().await?;

        if self.meta.can_batch() {
            let mut obs = obs.try_chunks(self.limit);

            while let Some(batches) = obs.next().await {
                let batches = batches
                    .map_err(|err| err.1)?
                    .into_iter()
                    .map(|v| (v.path().to_string(), OpDelete::default()))
                    .collect();

                let results = self
                    .src
                    .inner()
                    .batch(OpBatch::new(BatchOperations::Delete(batches)))
                    .await?;

                let BatchedResults::Delete(results) = results.into_results();

                // TODO: return error here directly seems not a good idea?
                for (_, result) in results {
                    let _ = result?;
                }
            }
        } else {
            obs.try_for_each(|v| async move { v.delete().await })
                .await?;
        }

        Ok(())
    }
}
