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

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::raw::oio::Read;
use futures::{ready, Stream};

use crate::raw::*;
use crate::*;

/// ReaderGenerator is used to generate new readers.
///
/// If chunk is None, ReaderGenerator will only return one reader.
/// Otherwise, ReaderGenerator will return multiple readers, each with size
/// of chunk.
///
/// It's design that we didn't implement the generator as a stream, because
/// we don't expose the generator to the user. Instead, we use the async method
/// directly to keep it simple and easy to understand.
struct ReaderGenerator {
    acc: Accessor,
    path: Arc<String>,
    args: OpRead,
    options: OpReader,

    offset: u64,
    end: u64,
}

impl ReaderGenerator {
    /// Create a new ReaderGenerator.
    #[inline]
    fn new(
        acc: Accessor,
        path: Arc<String>,
        args: OpRead,
        options: OpReader,
        range: Range<u64>,
    ) -> Self {
        Self {
            acc,
            path,
            args,
            options,
            offset: range.start,
            end: range.end,
        }
    }

    /// Generate next reader.
    async fn next(&mut self) -> Result<Option<oio::Reader>> {
        if self.offset >= self.end {
            return Ok(None);
        }

        let offset = self.offset;
        let mut size = (self.end - self.offset) as usize;
        if let Some(chunk) = self.options.chunk() {
            size = size.min(chunk)
        }

        // Update self.offset before building future.
        self.offset += size as u64;
        let args = self
            .args
            .clone()
            .with_range(BytesRange::new(offset, Some(size as u64)));
        let (_, r) = self.acc.read(&self.path, args).await?;
        Ok(Some(r))
    }
}

/// StreamingReader will stream the content of the file without reading into
/// memory first.
///
/// StreamingReader is good for small memory footprint and optimized for latency.
pub struct StreamingReader {
    generator: ReaderGenerator,
    reader: Option<oio::Reader>,
}

impl StreamingReader {
    /// Create a new streaming reader.
    #[inline]
    fn new(generator: ReaderGenerator) -> Self {
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
                self.reader = self.generator.next().await?;
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
    generator: ReaderGenerator,
    tasks: ConcurrentTasks<oio::Reader, Buffer>,
}

impl ChunkedReader {
    /// Create a new chunked reader.
    fn new(generator: ReaderGenerator) -> Self {
        let tasks = ConcurrentTasks::new(
            generator.args.executor().cloned(),
            generator.options.concurrent(),
            |mut r: oio::Reader| {
                Box::pin(async {
                    match r.read_all().await {
                        Ok(buf) => (r, Ok(buf)),
                        Err(err) => (r, Err(err)),
                    }
                })
            },
        );
        Self { generator, tasks }
    }
}

impl oio::Read for ChunkedReader {
    async fn read(&mut self) -> Result<Buffer> {
        while self.tasks.has_remaining() {
            if let Some(r) = self.generator.next().await? {
                self.tasks.execute(r).await?;
            }
            if self.tasks.has_result() {
                break;
            }
        }
        Ok(self.tasks.next().await.transpose()?.unwrap_or_default())
    }
}

/// BufferStream is a stream of buffers.
///
/// # Notes
///
/// The underlying reader is either a StreamingReader or a ChunkedReader.
///
/// - If chunk is None, BufferStream will use StreamingReader to iterate
/// data in streaming way.
/// - Otherwise, BufferStream will use ChunkedReader to read data in chunks.
pub struct BufferStream {
    state: State,
}

enum State {
    Idle(Option<TwoWays<StreamingReader, ChunkedReader>>),
    Reading(BoxedStaticFuture<(TwoWays<StreamingReader, ChunkedReader>, Result<Buffer>)>),
}

impl BufferStream {
    /// Create a new buffer stream.
    pub fn new(
        acc: Accessor,
        path: Arc<String>,
        args: OpRead,
        options: OpReader,
        range: Range<u64>,
    ) -> Self {
        let generator = ReaderGenerator::new(acc, path, args, options, range);
        let reader = if generator.options.chunk().is_some() {
            TwoWays::Two(ChunkedReader::new(generator))
        } else {
            TwoWays::One(StreamingReader::new(generator))
        };
        Self {
            state: State::Idle(Some(reader)),
        }
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

#[cfg(test_xx)]
mod tests {
    use bytes::Buf;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_trait() {
        let v = BufferStream::new(Box::new(Buffer::new()), OpReader::new(), 4..8);

        let _: Box<dyn Unpin + MaybeSend + 'static> = Box::new(v);
    }

    #[test]
    fn test_future_iterator() {
        let r: oio::Reader = Box::new(Buffer::new());

        let it = FutureIterator::new(r.clone(), Some(1), 1..3);
        let futs: Vec<_> = it.collect();
        assert_eq!(futs.len(), 2);
    }

    #[tokio::test]
    async fn test_buffer_stream() {
        let r: oio::Reader = Box::new(Buffer::from(vec![
            Bytes::from("Hello"),
            Bytes::from("World"),
        ]));

        let s = BufferStream::new(r, OpReader::new(), 4..8);
        let bufs: Vec<_> = s.try_collect().await.unwrap();
        assert_eq!(bufs.len(), 1);
        assert_eq!(bufs[0].chunk(), "o".as_bytes());

        let buf: Buffer = bufs.into_iter().flatten().collect();
        assert_eq!(buf.len(), 4);
        assert_eq!(&buf.to_vec(), "oWor".as_bytes());
    }
}
