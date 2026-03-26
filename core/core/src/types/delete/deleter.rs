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

use std::pin::pin;

use futures::Stream;
use futures::StreamExt;

use crate::raw::oio::DeleteDyn;
use crate::raw::*;
use crate::*;

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
/// use opendal_core::Operator;
/// use opendal_core::Result;
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
/// use futures::stream;
/// use opendal_core::Operator;
/// use opendal_core::Result;
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
/// use futures::stream;
/// use futures::Sink;
/// use futures::SinkExt;
/// use opendal_core::Operator;
/// use opendal_core::Result;
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
}

impl Deleter {
    pub(crate) async fn create(acc: Accessor) -> Result<Self> {
        let (_, deleter) = acc.delete().await?;

        Ok(Self { deleter })
    }

    /// Delete a path.
    pub async fn delete(&mut self, input: impl IntoDeleteInput) -> Result<()> {
        let input = input.into_delete_input();
        let mut op = OpDelete::default();
        if let Some(version) = &input.version {
            op = op.with_version(version);
        }
        if input.recursive {
            op = op.with_recursive(true);
        }

        self.deleter.delete_dyn(&input.path, op).await?;
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
    pub async fn delete_stream<S, D>(&mut self, stream: S) -> Result<()>
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
    pub async fn delete_try_stream<S, D>(&mut self, try_stream: S) -> Result<()>
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

    /// Close the deleter, this will flush the deleter and wait until all paths are deleted.
    pub async fn close(&mut self) -> Result<()> {
        self.deleter.close_dyn().await
    }

    /// Convert the deleter into a sink.
    pub fn into_sink<T: IntoDeleteInput>(self) -> FuturesDeleteSink<T> {
        FuturesDeleteSink::new(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use futures::SinkExt;

    use super::*;
    use crate::raw::OpDelete;
    use crate::raw::oio;

    struct MockBatchDeleter {
        buffer: Vec<String>,
        flushed: Arc<Mutex<Vec<String>>>,
    }

    impl oio::Delete for MockBatchDeleter {
        async fn delete(&mut self, path: &str, _args: OpDelete) -> Result<()> {
            self.buffer.push(path.to_string());
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            let mut flushed = self.flushed.lock().unwrap();
            flushed.extend(self.buffer.drain(..));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_sink_close_must_flush_buffered_deletes() {
        let flushed = Arc::new(Mutex::new(Vec::<String>::new()));

        let mock = MockBatchDeleter {
            buffer: Vec::new(),
            flushed: flushed.clone(),
        };

        let deleter = Deleter {
            deleter: Box::new(mock),
        };
        let mut sink = deleter.into_sink::<String>();

        sink.send("file_a".to_string()).await.unwrap();
        sink.send("file_b".to_string()).await.unwrap();
        sink.close().await.unwrap();

        let flushed = flushed.lock().unwrap();
        assert!(
            flushed.contains(&"file_a".to_string()),
            "file_a should have been flushed by close, got: {:?}",
            *flushed
        );
        assert!(
            flushed.contains(&"file_b".to_string()),
            "file_b should have been flushed by close, got: {:?}",
            *flushed
        );
    }

    /// Mock that reconstructs OpDelete from scratch (losing `recursive`),
    /// just like the real S3 deleter does.
    struct S3LikeMock;

    impl oio::BatchDelete for S3LikeMock {
        async fn delete_once(&self, _: String, _: OpDelete) -> Result<()> {
            Ok(())
        }
        async fn delete_batch(
            &self,
            batch: Vec<(String, OpDelete)>,
        ) -> Result<oio::BatchDeleteResult> {
            Ok(oio::BatchDeleteResult {
                succeeded: batch
                    .into_iter()
                    .map(|(p, _)| (p, OpDelete::new()))
                    .collect(),
                failed: vec![],
            })
        }
    }

    #[derive(Debug)]
    struct S3LikeBackend;

    impl Access for S3LikeBackend {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;

        fn info(&self) -> Arc<AccessorInfo> {
            let info = AccessorInfo::default();
            info.set_native_capability(Capability {
                delete: true,
                delete_max_size: Some(1000),
                delete_with_recursive: true,
                ..Default::default()
            });
            info.into()
        }

        async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
            Ok((
                RpDelete::default(),
                Box::new(oio::BatchDeleter::new(S3LikeMock, Some(1000))),
            ))
        }
    }

    #[tokio::test]
    async fn test_operator_delete_recursive_with_multiple_directories() {
        let op = OperatorBuilder::new(S3LikeBackend).finish();

        let mut d = op.deleter().await.unwrap();
        d.delete(DeleteInput {
            path: "dir_a/".to_string(),
            recursive: true,
            ..Default::default()
        })
        .await
        .unwrap();
        d.delete(DeleteInput {
            path: "dir_b/".to_string(),
            recursive: true,
            ..Default::default()
        })
        .await
        .unwrap();
        d.close().await.unwrap();
    }
}
