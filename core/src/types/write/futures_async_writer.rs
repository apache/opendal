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

use std::io;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use futures::AsyncWrite;
use futures::SinkExt;

use crate::raw::*;
use crate::*;

/// FuturesIoAsyncWriter is the adapter of [`AsyncWrite`] for [`Writer`].
///
/// Users can use this adapter in cases where they need to use [`AsyncWrite`] related trait.
///
/// FuturesIoAsyncWriter also implements [`Unpin`], [`Send`] and [`Sync`]
pub struct FuturesAsyncWriter {
    sink: BufferSink,
    buf: oio::FlexBuf,
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

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        if bufs.is_empty() {
            return Poll::Ready(Ok(0));
        }

        loop {
            let mut total_size = 0;
            let mut all_empty = true;
            for buf in bufs {
                if buf.is_empty() {
                    continue;
                }
                all_empty = false;
                let n = this.buf.put(buf);
                total_size += n;
                if n < buf.len() {
                    break;
                }
            }
            if total_size > 0 || all_empty {
                return Poll::Ready(Ok(total_size));
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
                return Poll::Ready(Ok(()));
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
    use super::*;
    use crate::raw::MaybeSend;
    use futures::AsyncWriteExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_trait() {
        let op = Operator::via_iter(Scheme::Memory, []).unwrap();

        let acc = op.into_inner();
        let ctx = Arc::new(WriteContext::new(
            acc,
            "test".to_string(),
            OpWrite::new(),
            OpWriter::new().with_chunk(1),
        ));
        let write_gen = WriteGenerator::create(ctx).await.unwrap();

        let v = FuturesAsyncWriter::new(write_gen);

        let _: Box<dyn Unpin + MaybeSend + Sync + 'static> = Box::new(v);
    }

    #[tokio::test]
    async fn test_poll_write_vectored() {
        let op = Operator::via_iter(Scheme::Memory, []).unwrap();

        let acc = op.clone().into_inner();
        let path = "test";
        let ctx = Arc::new(WriteContext::new(
            acc,
            path.to_string(),
            OpWrite::new(),
            OpWriter::new().with_chunk(1),
        ));
        let write_gen = WriteGenerator::create(ctx).await.unwrap();

        let mut v = FuturesAsyncWriter::new(write_gen);
        let _ = v
            .write_vectored(&[io::IoSlice::new(b"Hello"), io::IoSlice::new(b"World")])
            .await
            .unwrap();
        v.flush().await.unwrap();
        v.close().await.unwrap();

        let data = op.read(path).await.unwrap();
        assert_eq!(data.to_vec(), b"HelloWorld");
    }

    #[tokio::test]
    async fn test_poll_write_vectored_with_concurrent() {
        let op = Operator::via_iter(Scheme::Memory, []).unwrap();

        let acc = op.clone().into_inner();
        let path = "test";
        let ctx = Arc::new(WriteContext::new(
            acc,
            path.to_string(),
            OpWrite::new().with_concurrent(3),
            OpWriter::new().with_chunk(2),
        ));
        let write_gen = WriteGenerator::create(ctx).await.unwrap();

        let mut v = FuturesAsyncWriter::new(write_gen);
        let _ = v
            .write_vectored(&[io::IoSlice::new(b"Hello"), io::IoSlice::new(b"World")])
            .await
            .unwrap();
        v.flush().await.unwrap();
        v.close().await.unwrap();

        let data = op.read(path).await.unwrap();
        assert_eq!(data.to_vec(), b"HelloWorld");
    }
}
