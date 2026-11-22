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

use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::ready;

use futures::AsyncWrite;
use futures::SinkExt;

use crate::raw::*;
use crate::*;

/// Adapts a [`Writer`] to [`AsyncWrite`].
///
/// The adapter copies input into a 256 KiB buffer. Writing beyond that buffer
/// or calling `flush()` can produce multiple chunks. Sending multiple chunks
/// to the service requires [`Capability::write_can_multi`]. Call `close()` to
/// send any remaining data and commit the write.
///
/// When this capability is false, pass the complete [`Buffer`] to
/// [`Operator::write`] or to a single [`Writer::write`] call on a default writer.
/// To use this adapter with a known payload size, set `chunk(size)` with
/// [`Operator::writer_with`] so the underlying writer buffers the full payload
/// in one chunk.
pub struct FuturesAsyncWriter {
    sink: BufferSink,
    buf: oio::FlexBuf,
}

impl fmt::Debug for FuturesAsyncWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FuturesAsyncWriter").finish_non_exhaustive()
    }
}

impl FuturesAsyncWriter {
    /// NOTE: don't allow users to create directly.
    #[inline]
    pub(crate) fn new(w: WriteGenerator<oio::Writer>) -> Self {
        FuturesAsyncWriter {
            sink: BufferSink::new(w),
            buf: oio::FlexBuf::new(256 * 1024),
        }
    }
}

impl AsyncWrite for FuturesAsyncWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        if this.sink.is_closed() {
            return Poll::Ready(Err(format_std_io_error(Error::new(
                ErrorKind::Unexpected,
                "state invalid: sink has been closed",
            ))));
        }

        loop {
            let n = this.buf.put(buf);
            if n > 0 {
                return Poll::Ready(Ok(n));
            }

            ready!(this.sink.poll_ready_unpin(cx)).map_err(format_std_io_error)?;

            let bs = this.buf.get().expect("frozen buffer must be valid");
            this.sink
                .start_send_unpin(Buffer::from(bs))
                .map_err(format_std_io_error)?;
            this.buf.clean();
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        loop {
            // Make sure buf has been frozen.
            this.buf.freeze();
            let Some(bs) = this.buf.get() else {
                return this.sink.poll_flush_unpin(cx).map_err(format_std_io_error);
            };

            ready!(this.sink.poll_ready_unpin(cx)).map_err(format_std_io_error)?;
            this.sink
                .start_send_unpin(Buffer::from(bs))
                .map_err(format_std_io_error)?;
            this.buf.clean();
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        loop {
            // Make sure buf has been frozen.
            this.buf.freeze();
            let Some(bs) = this.buf.get() else {
                return this.sink.poll_close_unpin(cx).map_err(format_std_io_error);
            };

            ready!(this.sink.poll_ready_unpin(cx)).map_err(format_std_io_error)?;
            this.sink
                .start_send_unpin(Buffer::from(bs))
                .map_err(format_std_io_error)?;
            this.buf.clean();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use futures::AsyncWriteExt;

    use super::*;
    use crate::raw::MaybeSend;
    use crate::raw::oio;

    struct RecordingWriter {
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
        fail_write: bool,
    }

    impl oio::Write for RecordingWriter {
        async fn write(&mut self, bs: Buffer) -> Result<()> {
            if self.fail_write {
                return Err(Error::new(ErrorKind::Unexpected, "injected write failure"));
            }

            self.writes.lock().unwrap().push(bs.to_vec());
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(MetadataBuilder::unknown().build())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn new_async_writer(writer: RecordingWriter) -> FuturesAsyncWriter {
        let write_gen = WriteGenerator::new(Box::new(writer), None, false);
        FuturesAsyncWriter::new(write_gen)
    }

    #[tokio::test]
    async fn test_flush_drives_underlying_write() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut w = new_async_writer(RecordingWriter {
            writes: writes.clone(),
            fail_write: false,
        });

        w.write_all(b"hello").await.unwrap();
        assert!(
            writes.lock().unwrap().is_empty(),
            "write must remain buffered until flush"
        );

        w.flush().await.unwrap();
        assert_eq!(*writes.lock().unwrap(), vec![b"hello".to_vec()]);

        w.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_flush_surfaces_underlying_write_error() {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut w = new_async_writer(RecordingWriter {
            writes: writes.clone(),
            fail_write: true,
        });

        w.write_all(b"hello").await.unwrap();
        let err = w
            .flush()
            .await
            .expect_err("flush must surface write errors");
        assert!(err.to_string().contains("injected write failure"), "{err}");
    }

    #[tokio::test]
    async fn test_write_and_flush_after_close_return_error() {
        let op = Operator::via_iter(services::MEMORY_SCHEME, []).unwrap();
        let mut w = op
            .writer("test_write_and_flush_after_close_return_error")
            .await
            .unwrap()
            .into_futures_async_write();

        w.close().await.unwrap();

        let err = w
            .write_all(b"after close")
            .await
            .expect_err("write must fail");
        assert!(err.to_string().contains("sink has been closed"), "{err}");

        let err = w.flush().await.expect_err("flush must fail");
        assert!(err.to_string().contains("sink has been closed"), "{err}");
    }

    #[tokio::test]
    async fn test_trait() {
        let op = Operator::via_iter(services::MEMORY_SCHEME, []).unwrap();

        let ctx = op.context().clone();
        let srv = op.service().clone();
        let ctx = Arc::new(WriteContext::new(
            ctx,
            srv,
            "test".to_string(),
            OpWrite::new(),
            OpWriter::new().with_chunk(1),
        ));
        let write_gen = WriteGenerator::create(ctx).unwrap();

        let v = FuturesAsyncWriter::new(write_gen);

        let _: Box<dyn Unpin + MaybeSend + Sync + 'static> = Box::new(v);
    }

    #[tokio::test]
    async fn test_writer_debug_omits_path_and_payload() {
        let op = Operator::via_iter(services::MEMORY_SCHEME, []).unwrap();
        let path = "private/object";
        let writer = op.writer_with(path).chunk(8).await.unwrap();

        let output = format!("{writer:?}");
        assert!(output.starts_with("Writer {"), "{output}");
        assert!(output.contains("chunk: Some(8)"), "{output}");
        assert!(output.contains("append: false"), "{output}");
        assert!(!output.contains(path), "{output}");

        let payload = "private payload";
        let mut writer = writer.into_futures_async_write();
        writer.write_all(payload.as_bytes()).await.unwrap();
        let output = format!("{writer:?}");
        assert!(output.starts_with("FuturesAsyncWriter {"), "{output}");
        assert!(!output.contains(path), "{output}");
        assert!(!output.contains(payload), "{output}");
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_empty_write() {
        let op = Operator::via_iter(services::MEMORY_SCHEME, []).unwrap();

        let mut w = op
            .writer("test_empty_write")
            .await
            .unwrap()
            .into_futures_async_write();

        assert_eq!(w.write(b"").await.unwrap(), 0);
        w.write_all(b"hello").await.unwrap();
        assert_eq!(w.write(b"").await.unwrap(), 0);
        w.close().await.unwrap();

        assert_eq!(
            op.read("test_empty_write").await.unwrap().to_vec(),
            b"hello"
        );
    }
}
