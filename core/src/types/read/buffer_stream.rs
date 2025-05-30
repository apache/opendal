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

use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::ready;
use futures::Stream;

use crate::raw::oio::Read as _;
use crate::raw::*;
use crate::*;

/// StreamingReader will stream the content of the file without reading into
/// memory first.
///
/// StreamingReader is good for small memory footprint and optimized for latency.
pub struct StreamingReader {
    generator: ReadGenerator,
    reader: Option<oio::Reader>,
}

impl StreamingReader {
    /// Create a new streaming reader.
    #[inline]
    fn new(ctx: Arc<ReadContext>, range: BytesRange) -> Self {
        let generator = ReadGenerator::new(ctx.clone(), range.offset(), range.size());
        Self {
            generator,
            reader: None,
        }
    }
}

impl oio::Read for StreamingReader {
    async fn read(&mut self) -> Result<Buffer> {
        loop {
            if self.reader.is_none() {
                self.reader = self.generator.next_reader().await?;
            }
            let Some(r) = self.reader.as_mut() else {
                return Ok(Buffer::new());
            };

            let buf = r.read().await?;
            // Reset reader to None if this reader returns empty buffer.
            if buf.is_empty() {
                self.reader = None;
                continue;
            } else {
                return Ok(buf);
            }
        }
    }
}

/// ChunkedReader will read the file in chunks.
///
/// ChunkedReader is good for concurrent read and optimized for throughput.
pub struct ChunkedReader {
    generator: ReadGenerator,
    tasks: ConcurrentTasks<oio::Reader, Buffer>,
    done: bool,
}

impl ChunkedReader {
    /// Create a new chunked reader.
    ///
    /// # Notes
    ///
    /// We don't need to handle `Executor::timeout` since we are outside the layer.
    fn new(ctx: Arc<ReadContext>, range: BytesRange) -> Self {
        let tasks = ConcurrentTasks::new(
            ctx.accessor().info().executor(),
            ctx.options().concurrent(),
            |mut r: oio::Reader| {
                Box::pin(async {
                    match r.read_all().await {
                        Ok(buf) => (r, Ok(buf)),
                        Err(err) => (r, Err(err)),
                    }
                })
            },
        );
        let generator = ReadGenerator::new(ctx, range.offset(), range.size());
        Self {
            generator,
            tasks,
            done: false,
        }
    }
}

impl oio::Read for ChunkedReader {
    async fn read(&mut self) -> Result<Buffer> {
        while self.tasks.has_remaining() && !self.done {
            if let Some(r) = self.generator.next_reader().await? {
                self.tasks.execute(r).await?;
            } else {
                self.done = true;
                break;
            }
            if self.tasks.has_result() {
                break;
            }
        }
        Ok(self.tasks.next().await.transpose()?.unwrap_or_default())
    }
}

/// BufferStream is a stream of buffers, created by [`Reader::into_stream`]
///
/// `BufferStream` implements `Stream` trait.
pub struct BufferStream {
    /// # Notes to maintainers
    ///
    /// The underlying reader is either a StreamingReader or a ChunkedReader.
    ///
    /// - If chunk is None, BufferStream will use StreamingReader to iterate
    ///   data in streaming way.
    /// - Otherwise, BufferStream will use ChunkedReader to read data in chunks.
    state: State,
}

enum State {
    Idle(Option<TwoWays<StreamingReader, ChunkedReader>>),
    Reading(BoxedStaticFuture<(TwoWays<StreamingReader, ChunkedReader>, Result<Buffer>)>),
}

impl BufferStream {
    /// Create a new buffer stream with already calculated offset and size.
    pub(crate) fn new(ctx: Arc<ReadContext>, offset: u64, size: Option<u64>) -> Self {
        debug_assert!(
            size.is_some() || ctx.options().chunk().is_none(),
            "size must be known if chunk is set"
        );

        let reader = if ctx.options().chunk().is_some() {
            TwoWays::Two(ChunkedReader::new(ctx, BytesRange::new(offset, size)))
        } else {
            TwoWays::One(StreamingReader::new(ctx, BytesRange::new(offset, size)))
        };

        Self {
            state: State::Idle(Some(reader)),
        }
    }

    /// Create a new buffer stream with given range bound.
    ///
    /// If users is going to perform chunked read but the read size is unknown, we will parse
    /// into range first.
    pub(crate) async fn create(
        ctx: Arc<ReadContext>,
        range: impl RangeBounds<u64>,
    ) -> Result<Self> {
        let reader = if ctx.options().chunk().is_some() {
            let range = ctx.parse_into_range(range).await?;
            TwoWays::Two(ChunkedReader::new(ctx, range.into()))
        } else {
            TwoWays::One(StreamingReader::new(ctx, range.into()))
        };

        Ok(Self {
            state: State::Idle(Some(reader)),
        })
    }
}

impl Stream for BufferStream {
    type Item = Result<Buffer>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                State::Idle(reader) => {
                    let mut reader = reader.take().unwrap();
                    let fut = async move {
                        let ret = reader.read().await;
                        (reader, ret)
                    };
                    this.state = State::Reading(Box::pin(fut));
                }
                State::Reading(fut) => {
                    let fut = fut.as_mut();
                    let (reader, buf) = ready!(fut.poll(cx));
                    this.state = State::Idle(Some(reader));
                    return match buf {
                        Ok(buf) if buf.is_empty() => Poll::Ready(None),
                        Ok(buf) => Poll::Ready(Some(Ok(buf))),
                        Err(err) => Poll::Ready(Some(Err(err))),
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Buf;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use super::*;

    #[tokio::test]
    async fn test_trait() -> Result<()> {
        let acc = Operator::via_iter(Scheme::Memory, [])?.into_inner();
        let ctx = Arc::new(ReadContext::new(
            acc,
            "test".to_string(),
            OpRead::new(),
            OpReader::new(),
        ));
        let v = BufferStream::create(ctx, 4..8).await?;

        let _: Box<dyn Unpin + MaybeSend + 'static> = Box::new(v);

        Ok(())
    }

    #[tokio::test]
    async fn test_buffer_stream() -> Result<()> {
        let op = Operator::via_iter(Scheme::Memory, [])?;
        op.write(
            "test",
            Buffer::from(vec![Bytes::from("Hello"), Bytes::from("World")]),
        )
        .await?;

        let acc = op.into_inner();
        let ctx = Arc::new(ReadContext::new(
            acc,
            "test".to_string(),
            OpRead::new(),
            OpReader::new(),
        ));

        let s = BufferStream::create(ctx, 4..8).await?;
        let bufs: Vec<_> = s.try_collect().await.unwrap();
        assert_eq!(bufs.len(), 1);
        assert_eq!(bufs[0].chunk(), "o".as_bytes());

        let buf: Buffer = bufs.into_iter().flatten().collect();
        assert_eq!(buf.len(), 4);
        assert_eq!(&buf.to_vec(), "oWor".as_bytes());

        Ok(())
    }
}
