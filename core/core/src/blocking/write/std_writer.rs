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

use std::io::Write;

use futures::AsyncWriteExt;

use crate::*;

/// `StdWriter` adapts a [`crate::blocking::Writer`] to [`std::io::Write`].
///
/// Users can use this adapter in cases where they need to use [`std::io::Write`] related trait.
///
/// # Notes
///
/// Files are automatically closed when they go out of scope. Errors detected on closing are ignored
/// by the implementation of Drop. Use the method `close` if these errors must be manually handled.
///
/// Drop does not close a writer whose `write`, `flush`, or `close` call returned an error. Closing
/// such a writer could store data that the caller was told was not stored. Call `close` again to
/// retry a failed `close`.
pub struct StdWriter {
    handle: tokio::runtime::Handle,
    w: Option<FuturesAsyncWriter>,
    /// Whether an attempted `close` left the sink open.
    ///
    /// A failed `close` leaves the sink open, so Drop must not close it again: the retry could
    /// store the data the caller was told was not stored, or block the dropping thread on another
    /// attempt.
    close_failed: bool,
    /// Whether the last `write` or `flush` call returned an error.
    ///
    /// Closing after a failed call can publish a truncated object, because bytes accepted before
    /// the error may never have reached the service. A later successful call clears this flag so
    /// a recovered writer still closes on drop.
    has_error: bool,
}

impl StdWriter {
    /// NOTE: don't allow users to create directly.
    #[inline]
    pub(crate) fn new(handle: tokio::runtime::Handle, w: Writer) -> Self {
        StdWriter {
            handle,
            w: Some(w.into_futures_async_write()),
            close_failed: false,
            has_error: false,
        }
    }

    /// Close the internal writer and make sure all data have been stored.
    pub fn close(&mut self) -> std::io::Result<()> {
        // Assume the close fails until the sink confirms it, so a `close` that panics through this
        // method also leaves a writer that Drop will not retry.
        self.close_failed = true;

        let Some(w) = self.w.as_mut() else {
            return Err(Error::new(ErrorKind::Unexpected, "writer has been dropped").into());
        };

        let result = self.handle.block_on(w.close());
        self.close_failed = result.is_err();
        result
    }
}

impl Write for StdWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let Some(w) = self.w.as_mut() else {
            return Err(Error::new(ErrorKind::Unexpected, "writer has been dropped").into());
        };

        let written = self.handle.block_on(w.write(buf));
        // An empty write transfers nothing, so it cannot prove the writer recovered.
        if !buf.is_empty() {
            self.has_error = written.is_err();
        }
        written
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let Some(w) = self.w.as_mut() else {
            return Err(Error::new(ErrorKind::Unexpected, "writer has been dropped").into());
        };

        let flushed = self.handle.block_on(w.flush());
        self.has_error = flushed.is_err();
        flushed
    }
}

/// Make sure the inner writer is dropped in async context.
impl Drop for StdWriter {
    fn drop(&mut self) {
        let Some(mut v) = self.w.take() else {
            return;
        };

        // Only close a writer the caller left healthy. Retrying a failed `close`, `write`, or
        // `flush` here could store data the caller knows is incomplete or was told had failed.
        let should_close = !self.close_failed && !self.has_error;

        self.handle.block_on(async move {
            // Data that has been written but not sent yet lives in the writer's
            // internal buffer, so dropping without closing would lose it. Close
            // errors are ignored here, as documented for this adapter.
            //
            // A writer that `close` already closed is skipped: closing a closed
            // writer returns an error instead of closing it again.
            if should_close && !v.is_closed() {
                let _ = v.close().await;
            }

            drop(v);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::blocking;
    use crate::raw::*;
    use crate::services;

    const PAYLOAD: &[u8] = b"hello, world";

    /// Buffer capacity inside [`FuturesAsyncWriter`]. Writes that fill it leave data the sink still
    /// has to send.
    const WRITER_BUFFER_SIZE: usize = 256 * 1024;

    fn new_operator() -> crate::Result<blocking::Operator> {
        let op = Operator::new(services::Memory::default())?;
        blocking::Operator::new(op)
    }

    /// Run a blocking write body and wait for its result.
    async fn run_blocking<T, F>(f: F) -> std::io::Result<T>
    where
        F: FnOnce() -> std::io::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(f)
            .await
            .expect("blocking write task must complete")
    }

    /// An `oio::Write` that counts `close` calls and can inject failures.
    struct CountingWriter {
        close_calls: Arc<AtomicUsize>,
        /// Number of upcoming `write` calls that fail before later calls succeed.
        write_failures: usize,
        fail_close: bool,
    }

    impl oio::Write for CountingWriter {
        async fn write(&mut self, _: Buffer) -> Result<()> {
            if self.write_failures > 0 {
                self.write_failures -= 1;
                return Err(Error::new(ErrorKind::Unexpected, "injected write failure"));
            }

            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);

            if self.fail_close {
                return Err(Error::new(ErrorKind::Unexpected, "injected close failure"));
            }

            Ok(MetadataBuilder::unknown().build())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// Build a [`StdWriter`] around a [`CountingWriter`].
    ///
    /// Call this from a blocking context: [`StdWriter`] blocks on the runtime for every operation,
    /// including `Drop`.
    fn counting_std_writer(
        close_calls: Arc<AtomicUsize>,
        write_failures: usize,
        fail_close: bool,
    ) -> StdWriter {
        let writer = CountingWriter {
            close_calls,
            write_failures,
            fail_close,
        };
        let write_state = WriteState::new(Box::new(writer), None, false);

        StdWriter {
            handle: tokio::runtime::Handle::current(),
            w: Some(FuturesAsyncWriter::new(write_state)),
            close_failed: false,
            has_error: false,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_without_close_persists_data() {
        let op = new_operator().expect("memory operator must be created");

        let written = run_blocking(move || -> std::io::Result<Vec<u8>> {
            let path = "test_drop_without_close_persists_data";
            let mut w = op.writer(path).map_err(io::Error::from)?.into_std_write();
            w.write_all(PAYLOAD)?;

            drop(w);

            Ok(op.read(path).map_err(io::Error::from)?.to_vec())
        })
        .await
        .expect("blocking write must succeed");

        assert_eq!(written, PAYLOAD);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_after_flush_persists_data() {
        let op = new_operator().expect("memory operator must be created");

        let written = run_blocking(move || -> std::io::Result<Vec<u8>> {
            let path = "test_drop_after_flush_persists_data";
            let mut w = op.writer(path).map_err(io::Error::from)?.into_std_write();
            w.write_all(PAYLOAD)?;
            w.flush()?;

            drop(w);

            Ok(op.read(path).map_err(io::Error::from)?.to_vec())
        })
        .await
        .expect("blocking write must succeed");

        assert_eq!(written, PAYLOAD);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_after_close_does_not_close_again() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let calls = close_calls.clone();

        run_blocking(move || -> std::io::Result<()> {
            let mut w = counting_std_writer(calls, 0, false);
            w.write_all(PAYLOAD)?;
            w.close()?;

            drop(w);

            Ok(())
        })
        .await
        .expect("blocking write must succeed");

        assert_eq!(
            close_calls.load(Ordering::SeqCst),
            1,
            "drop must not close a writer that close already closed"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_after_failed_close_does_not_close_again() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let calls = close_calls.clone();

        run_blocking(move || -> std::io::Result<()> {
            let mut w = counting_std_writer(calls, 0, true);
            let err = w.close().expect_err("injected close failure must surface");
            assert!(err.to_string().contains("injected close failure"), "{err}");

            drop(w);

            Ok(())
        })
        .await
        .expect("blocking write must succeed");

        assert_eq!(
            close_calls.load(Ordering::SeqCst),
            1,
            "drop must not retry a failed close"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_after_failed_flush_does_not_close() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let calls = close_calls.clone();

        run_blocking(move || -> std::io::Result<()> {
            let mut w = counting_std_writer(calls, 1, false);
            w.write_all(PAYLOAD)?;
            let err = w.flush().expect_err("injected write failure must surface");
            assert!(err.to_string().contains("injected write failure"), "{err}");

            drop(w);

            Ok(())
        })
        .await
        .expect("blocking write must succeed");

        assert_eq!(
            close_calls.load(Ordering::SeqCst),
            0,
            "drop must not commit after a failed flush"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_after_failed_write_does_not_close() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let calls = close_calls.clone();

        run_blocking(move || -> std::io::Result<()> {
            let mut w = counting_std_writer(calls, 1, false);
            let chunk = vec![b'a'; WRITER_BUFFER_SIZE];

            // The first write fills the adapter buffer, the second moves it into the sink and
            // refills the buffer, and the third has to send the buffered data through the writer.
            w.write_all(&chunk)?;
            w.write_all(&chunk)?;
            let err = w
                .write_all(&chunk)
                .expect_err("injected write failure must surface");
            assert!(err.to_string().contains("injected write failure"), "{err}");

            // An empty write transfers nothing, so it must not mark the writer healthy again.
            assert_eq!(w.write(b"")?, 0);

            drop(w);

            Ok(())
        })
        .await
        .expect("blocking write must succeed");

        assert_eq!(
            close_calls.load(Ordering::SeqCst),
            0,
            "drop must not commit a truncated object"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_closes_after_write_error_is_recovered() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let calls = close_calls.clone();

        run_blocking(move || -> std::io::Result<()> {
            let mut w = counting_std_writer(calls, 1, false);
            let chunk = vec![b'a'; WRITER_BUFFER_SIZE];

            w.write_all(&chunk)?;
            w.write_all(&chunk)?;
            w.write_all(&chunk)
                .expect_err("injected write failure must surface");

            // The adapter keeps the rejected buffer and sends it again, so this write succeeds and
            // marks the writer healthy.
            w.write_all(&chunk)?;

            drop(w);

            Ok(())
        })
        .await
        .expect("blocking write must succeed");

        assert_eq!(
            close_calls.load(Ordering::SeqCst),
            1,
            "drop must close a writer that recovered from a failed write"
        );
    }
}
