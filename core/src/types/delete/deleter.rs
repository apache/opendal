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

use crate::raw::oio::Delete;
use crate::raw::*;
use crate::*;
use futures::{Stream, StreamExt};
use std::future::IntoFuture;
use std::pin::pin;

/// Deleter is designed to continuously remove content from storage.
///
/// It leverages batch deletion capabilities provided by storage services for efficient removal.
///
/// # Usage
///
/// [`Deleter`] provides several ways to delete files:
///
/// ## Direct Deletion
///
/// Use the `delete` method to remove a single file:
///
/// ```rust
/// use opendal::Operator;
/// use opendal::Result;
///
/// async fn example(op: Operator) -> Result<()> {
///     let mut d = op.deleter().await?;
///     d.delete("path/to/file").await?;
///     d.close().await?;
///     Ok(())
/// }
/// ```
///
/// Delete multiple files via a stream:
///
/// ```rust
/// use opendal::Operator;
/// use opendal::Result;
/// use futures::stream;
///
/// async fn example(op: Operator) -> Result<()> {
///     let mut d = op.deleter().await?;
///     d.delete_from(stream::iter(vec!["path/to/file"])).await?;
///     d.close().await?;
///     Ok(())
/// }
/// ```
///
/// ## Using as a Sink
///
/// Deleter can be used as a Sink for file deletion:
///
/// ```rust
/// use opendal::Operator;
/// use opendal::Result;
/// use futures::stream;
/// use futures::SinkExt;
///
/// async fn example(op: Operator) -> Result<()> {
///     let mut sink = op.deleter().await?.into_sink();
///     sink.send_all(&mut stream::iter(vec!["path/to/file"])).await?;
///     sink.close().await?;
///     Ok(())
/// }
/// ```
pub struct Deleter {
    deleter: oio::Deleter,

    max_size: usize,
    cur_size: usize,
}

impl Deleter {
    pub(crate) async fn create(acc: Accessor) -> Result<Self> {
        let max_size = acc.info().full_capability().delete_max_size.unwrap_or(1);
        let (_, deleter) = acc.delete().await?;

        Ok(Self {
            deleter,
            max_size,
            cur_size: 0,
        })
    }

    /// Delete a path.
    pub async fn delete(&mut self, input: impl IntoDeleteInput) -> Result<()> {
        if self.cur_size >= self.max_size {
            let deleted = self.deleter.flush().await?;
            self.cur_size -= deleted;
        }

        let input = input.into_delete_input();
        let mut op = OpDelete::default();
        if let Some(version) = &input.version {
            op = op.with_version(version);
        }

        self.deleter.delete(&input.path, op)?;
        self.cur_size += 1;
        Ok(())
    }

    /// Delete a stream of paths.
    pub async fn delete_from<S, E>(&mut self, mut stream: S) -> Result<()>
    where
        S: Stream<Item = Result<E>>,
        E: IntoDeleteInput,
    {
        let mut stream = pin!(stream);
        loop {
            match stream.next().await {
                Some(Ok(entry)) => {
                    self.delete(entry).await?;
                }
                Some(Err(err)) => return Err(err),
                None => break,
            }
        }

        Ok(())
    }

    /// Flush the deleter, returns the number of deleted paths.
    pub async fn flush(&mut self) -> Result<usize> {
        let deleted = self.deleter.flush().await?;
        self.cur_size -= deleted;
        Ok(deleted)
    }

    /// Close the deleter, this will flush the deleter and wait until all paths are deleted.
    pub async fn close(&mut self) -> Result<()> {
        loop {
            self.deleter.flush().await?;
            if self.cur_size == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Convert the deleter into a sink.
    pub fn into_sink(self) -> FuturesDeleteSink {
        FuturesDeleteSink::new(self)
    }
}
