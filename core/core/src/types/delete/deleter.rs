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

use super::concurrent::ConcurrentDeleter;
use crate::raw::oio::Delete;
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
    scheme: &'static str,
    capability: Capability,
}

impl Deleter {
    pub(crate) fn create(ctx: OperationContext, srv: Servicer) -> Result<Self> {
        Self::create_with(ctx, srv, 1)
    }

    pub(crate) fn create_with(
        ctx: OperationContext,
        srv: Servicer,
        concurrent: usize,
    ) -> Result<Self> {
        let scheme = srv.info().scheme();
        let capability = srv.capability();
        let mut deleter = srv.delete(&ctx)?;
        if concurrent > 1 {
            deleter = Box::new(ConcurrentDeleter::new(
                ctx,
                srv,
                deleter,
                concurrent,
                capability.delete_max_size.unwrap_or(1).max(1),
            ));
        }

        Ok(Self {
            deleter,
            scheme,
            capability,
        })
    }

    /// Delete a path with default or per-entry options.
    ///
    /// Pass a path directly to use [`options::DeleteOptions::default`]. Pass
    /// `(String, DeleteOptions)` when this entry needs version or condition
    /// options.
    ///
    /// A successful call accepts the entry. Call [`Self::close`] to wait for
    /// all accepted deletions to complete. With concurrency greater than one,
    /// an error from an earlier backend task is returned before this entry is
    /// accepted. Canceling a wait for an available task slot leaves this entry
    /// unaccepted. If an interrupted close is pending, this call resumes it
    /// before accepting another entry.
    pub async fn delete(&mut self, input: impl IntoDeleteInput) -> Result<()> {
        let (path, options) = input.into_delete_input();
        let path = normalize_path(&path);
        let op = OpDelete::from_options(&self.capability, options)
            .map_err(|err| err.with_context("service", self.scheme))?;
        self.deleter.delete(&path, op).await?;
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

    /// Flush accepted entries and wait for their deletions to complete.
    ///
    /// With concurrency greater than one, this waits for every dispatched
    /// task and returns the first error it observes, if any. Each call can
    /// retry work whose failure was reported by an earlier call, using the
    /// original paths and options. The deleter retains failed entries and
    /// entries whose outcome is unknown. Dropping the future returned by
    /// `close` preserves progress so the next call can resume it. Dropping
    /// the deleter discards buffered work and cancels local tasks. Already
    /// issued requests may still complete on the backend.
    pub async fn close(&mut self) -> Result<()> {
        self.deleter.close().await
    }

    /// Convert the deleter into a sink.
    pub fn into_sink<T: IntoDeleteInput>(self) -> FuturesDeleteSink<T> {
        FuturesDeleteSink::new(self)
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;

    use futures::Sink;
    use futures::SinkExt;
    use futures::task::noop_waker;

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
            scheme: "memory",
            capability: Capability::default(),
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

    struct FailCloseOnce<D> {
        inner: D,
        failed: bool,
    }

    impl<D: oio::Delete> oio::Delete for FailCloseOnce<D> {
        async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
            self.inner.delete(path, args).await
        }

        async fn close(&mut self) -> Result<()> {
            if !self.failed {
                self.failed = true;
                return Err(Error::new(ErrorKind::Unexpected, "injected close failure"));
            }
            self.inner.close().await
        }
    }

    struct PendingCloseOnce<D> {
        inner: D,
    }

    impl<D: oio::Delete> oio::Delete for PendingCloseOnce<D> {
        async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
            self.inner.delete(path, args).await
        }

        async fn close(&mut self) -> Result<()> {
            let mut pending = true;
            poll_fn(|cx| {
                if pending {
                    pending = false;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            self.inner.close().await
        }
    }

    async fn pending_close_sink(op: &Operator) -> FuturesDeleteSink<String> {
        let Deleter {
            deleter,
            scheme,
            capability,
        } = op.deleter().await.unwrap();
        Deleter {
            deleter: Box::new(PendingCloseOnce { inner: deleter }),
            scheme,
            capability,
        }
        .into_sink()
    }

    #[tokio::test]
    async fn test_closed_sink_rejects_sends_and_allows_flush_and_close() {
        let op = Operator::new(services::Memory::default()).unwrap();
        op.write("file_a", "a").await.unwrap();
        op.write("file_b", "b").await.unwrap();
        let mut sink = op.deleter().await.unwrap().into_sink::<String>();

        sink.send("file_a".to_string()).await.unwrap();
        sink.close().await.unwrap();
        assert!(!op.exists("file_a").await.unwrap());

        let err = sink
            .send("file_b".to_string())
            .await
            .expect_err("closed sink must reject sends");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(op.exists("file_b").await.unwrap());

        sink.flush().await.expect("closed sink is already flushed");
        sink.close().await.expect("closed sink is already closed");

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let err = match Pin::new(&mut sink).poll_ready(&mut cx) {
            Poll::Ready(Err(err)) => err,
            result => panic!("closed sink must reject ready, got {result:?}"),
        };
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        let err = Pin::new(&mut sink)
            .start_send("file_b".to_string())
            .expect_err("closed sink must reject start_send");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(op.exists("file_b").await.unwrap());
    }

    #[tokio::test]
    async fn test_sink_close_can_retry_after_error() {
        let op = Operator::new(services::Memory::default()).unwrap();
        op.write("file_a", "a").await.unwrap();
        op.write("file_b", "b").await.unwrap();
        let Deleter {
            deleter,
            scheme,
            capability,
        } = op.deleter().await.unwrap();
        let mut sink = Deleter {
            deleter: Box::new(FailCloseOnce {
                inner: deleter,
                failed: false,
            }),
            scheme,
            capability,
        }
        .into_sink::<String>();

        sink.send("file_a".to_string()).await.unwrap();
        let err = sink.close().await.expect_err("first close must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.message().contains("injected close failure"));

        sink.close().await.expect("close retry must succeed");
        assert!(!op.exists("file_a").await.unwrap());
        let err = sink
            .send("file_b".to_string())
            .await
            .expect_err("successful close retry must close the sink");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(op.exists("file_b").await.unwrap());
    }

    #[tokio::test]
    async fn test_sink_ready_and_flush_can_finish_a_pending_close() {
        let op = Operator::new(services::Memory::default()).unwrap();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut sink = pending_close_sink(&op).await;
        assert!(Pin::new(&mut sink).poll_close(&mut cx).is_pending());
        let err = match Pin::new(&mut sink).poll_ready(&mut cx) {
            Poll::Ready(Err(err)) => err,
            result => panic!("ready must reject a completed close, got {result:?}"),
        };
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        let mut sink = pending_close_sink(&op).await;
        assert!(Pin::new(&mut sink).poll_close(&mut cx).is_pending());
        let result = Pin::new(&mut sink).poll_flush(&mut cx);
        assert!(matches!(result, Poll::Ready(Ok(()))));
        let result = Pin::new(&mut sink).poll_ready(&mut cx);
        assert!(matches!(result, Poll::Ready(Err(_))));
    }

    #[tokio::test]
    async fn test_delete_normalizes_path() {
        let op = Operator::new(services::Memory::default()).unwrap();
        op.write("dir/file", "data").await.unwrap();

        let mut deleter = op.deleter().await.unwrap();
        deleter.delete("/dir/file").await.unwrap();
        deleter.close().await.unwrap();

        assert!(!op.exists("dir/file").await.unwrap());
    }
}
