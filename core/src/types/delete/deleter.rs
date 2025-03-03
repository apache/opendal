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

use crate::raw::oio::DeleteDyn;
use crate::raw::*;
use crate::*;
use futures::{Stream, StreamExt};
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
///     d.delete_stream(stream::iter(vec!["path/to/file"])).await?;
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
/// use futures::{stream, Sink};
/// use futures::SinkExt;
///
/// async fn example(op: Operator) -> Result<()> {
///     let mut sink = op.deleter().await?.into_sink();
///     sink.send("path/to/file").await?;
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
            let deleted = self.deleter.flush_dyn().await?;
            self.cur_size -= deleted;
        }

        let input = input.into_delete_input();
        let mut op = OpDelete::default();
        if let Some(version) = &input.version {
            op = op.with_version(version);
        }

        self.deleter.delete_dyn(&input.path, op)?;
        self.cur_size += 1;
        Ok(())
    }

    /// Delete an infallible iterator of paths.
    ///
    /// Also see:
    ///
    /// - [`Deleter::delete_try_iter`]: delete a fallible iterator of paths.
    /// - [`Deleter::delete_stream`]: delete an infallible stream of paths.
    /// - [`Deleter::delete_try_stream`]: delete a fallible stream of paths.
    pub async fn delete_iter<I, D>(&mut self, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = D>,
        D: IntoDeleteInput,
    {
        for entry in iter {
            self.delete(entry).await?;
        }
        Ok(())
    }

    /// Delete a fallible iterator of paths.
    ///
    /// Also see:
    ///
    /// - [`Deleter::delete_iter`]: delete an infallible iterator of paths.
    /// - [`Deleter::delete_stream`]: delete an infallible stream of paths.
    /// - [`Deleter::delete_try_stream`]: delete a fallible stream of paths.
    pub async fn delete_try_iter<I, D>(&mut self, try_iter: I) -> Result<()>
    where
        I: IntoIterator<Item = Result<D>>,
        D: IntoDeleteInput,
    {
        for entry in try_iter {
            self.delete(entry?).await?;
        }

        Ok(())
    }

    /// Delete an infallible stream of paths.
    ///
    /// Also see:
    ///
    /// - [`Deleter::delete_iter`]: delete an infallible iterator of paths.
    /// - [`Deleter::delete_try_iter`]: delete a fallible iterator of paths.
    /// - [`Deleter::delete_try_stream`]: delete a fallible stream of paths.
    pub async fn delete_stream<S, D>(&mut self, mut stream: S) -> Result<()>
    where
        S: Stream<Item = D>,
        D: IntoDeleteInput,
    {
        let mut stream = pin!(stream);
        while let Some(entry) = stream.next().await {
            self.delete(entry).await?;
        }

        Ok(())
    }

    /// Delete a fallible stream of paths.
    ///
    /// Also see:
    ///
    /// - [`Deleter::delete_iter`]: delete an infallible iterator of paths.
    /// - [`Deleter::delete_try_iter`]: delete a fallible iterator of paths.
    /// - [`Deleter::delete_stream`]: delete an infallible stream of paths.
    pub async fn delete_try_stream<S, D>(&mut self, mut try_stream: S) -> Result<()>
    where
        S: Stream<Item = Result<D>>,
        D: IntoDeleteInput,
    {
        let mut stream = pin!(try_stream);
        while let Some(entry) = stream.next().await.transpose()? {
            self.delete(entry).await?;
        }

        Ok(())
    }

    /// Flush the deleter, returns the number of deleted paths.
    pub async fn flush(&mut self) -> Result<usize> {
        let deleted = self.deleter.flush_dyn().await?;
        self.cur_size -= deleted;
        Ok(deleted)
    }

    /// Close the deleter, this will flush the deleter and wait until all paths are deleted.
    pub async fn close(&mut self) -> Result<()> {
        loop {
            self.flush().await?;
            if self.cur_size == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Convert the deleter into a sink.
    pub fn into_sink<T: IntoDeleteInput>(self) -> FuturesDeleteSink<T> {
        FuturesDeleteSink::new(self)
    }
}
