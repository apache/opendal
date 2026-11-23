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

use futures::Stream;
use futures::ready;

use crate::layers::check_complete;
use crate::raw::oio::Read as _;
use crate::raw::oio::ReadStream as _;
use crate::raw::*;
use crate::*;

/// The range a [`StreamingReader`] opens over its lifetime.
#[derive(Clone, Copy)]
enum PendingRange {
    /// A caller-provided range, opened as-is.
    Range(BytesRange),
    /// A suffix range resolved via `stat`, revalidated on open.
    Suffix(SuffixRevalidation),
    /// The range has been opened; later reads drain the open reader.
    Opened,
}

/// StreamingReader will stream the content of the file without reading into
/// memory first.
///
/// StreamingReader is good for small memory footprint and optimized for latency.
pub struct StreamingReader {
    ctx: Arc<ReadContext>,
    pending: PendingRange,
    /// The suffix range after revalidation, reported to `BufferStream`.
    revalidated_range: Option<Range<u64>>,
    reader: Option<Box<dyn oio::ReadStreamDyn>>,
}

impl StreamingReader {
    /// Create a new streaming reader.
    #[inline]
    fn new(ctx: Arc<ReadContext>, range: BytesRange) -> Self {
        Self {
            ctx,
            pending: PendingRange::Range(range),
            revalidated_range: None,
            reader: None,
        }
    }

    #[inline]
    fn with_suffix_revalidation(ctx: Arc<ReadContext>, reval: SuffixRevalidation) -> Self {
        Self {
            ctx,
            pending: PendingRange::Suffix(reval),
            revalidated_range: None,
            reader: None,
        }
    }

    fn take_revalidated_range(&mut self) -> Option<Range<u64>> {
        self.revalidated_range.take()
    }

    /// Open the pending range.
    ///
    /// The reader advances only after `open` succeeds, so a failed or
    /// cancelled open retries the same range instead of skipping it.
    async fn open(&mut self) -> Result<Option<Box<dyn oio::ReadStreamDyn>>> {
        match self.pending {
            PendingRange::Opened => Ok(None),
            PendingRange::Range(range) if range.size() == Some(0) => {
                self.pending = PendingRange::Opened;
                Ok(None)
            }
            PendingRange::Range(range) => {
                let (rp, reader) = self.ctx.reader().open(range).await?;
                self.pending = PendingRange::Opened;
                self.ctx.observe_read_response(rp);
                Ok(Some(reader))
            }
            PendingRange::Suffix(mut reval) => {
                let opened = self
                    .ctx
                    .open_revalidated_suffix(&mut reval, |full| {
                        (full.start < full.end)
                            .then(|| BytesRange::new(full.start, Some(full.end - full.start)))
                    })
                    .await;

                match opened {
                    Ok(Some(RevalidatedSuffixOpen {
                        full_range,
                        rp,
                        reader,
                        ..
                    })) => {
                        self.pending = PendingRange::Opened;
                        self.revalidated_range = Some(full_range);
                        self.ctx.observe_read_response(rp);
                        Ok(Some(reader))
                    }
                    Ok(None) => {
                        self.pending = PendingRange::Opened;
                        self.revalidated_range = Some(reval.resolved_range());
                        Ok(None)
                    }
                    Err(err) => {
                        self.pending = PendingRange::Suffix(reval);
                        Err(err)
                    }
                }
            }
        }
    }

    async fn prepare_metadata(&mut self) -> Result<()> {
        if self.ctx.metadata().is_some() {
            return Ok(());
        }

        if self.reader.is_none() {
            self.reader = self.open().await?;
        }

        Ok(())
    }

    async fn metadata(&mut self) -> Result<Metadata> {
        self.prepare_metadata().await?;

        self.ctx
            .metadata()
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::Unsupported, "read metadata is not available"))
    }
}

impl oio::ReadStream for StreamingReader {
    async fn read(&mut self) -> Result<Buffer> {
        loop {
            if self.reader.is_none() {
                self.reader = self.open().await?;
            }

            let Some(r) = self.reader.as_mut() else {
                return Ok(Buffer::new());
            };

            let buf = r.read_dyn().await?;
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

struct ChunkedReadInput {
    ctx: Arc<ReadContext>,
    range: BytesRange,
    reader: Option<Box<dyn oio::ReadStreamDyn>>,
}

/// A planned chunk and the chunked reader state after it is scheduled.
#[derive(Clone, Copy)]
struct PlannedRange {
    range: BytesRange,
    next_offset: u64,
    next_remaining: Option<u64>,
}

/// A chunk opened by `prepare_metadata` whose range is not committed yet.
///
/// The range advances only after the chunk is successfully scheduled, so a
/// failed or cancelled read retries this chunk instead of skipping it.
struct OpenedChunk {
    input: ChunkedReadInput,
    planned: PlannedRange,
}

/// ChunkedReader will read the file in chunks.
///
/// ChunkedReader is good for concurrent read and optimized for throughput.
pub struct ChunkedReader {
    ctx: Arc<ReadContext>,
    offset: u64,
    remaining: Option<u64>,
    suffix_revalidation: Option<SuffixRevalidation>,
    revalidated_range: Option<Range<u64>>,
    opened: Option<Box<OpenedChunk>>,
    tasks: ConcurrentTasks<ChunkedReadInput, Buffer>,
    done: bool,
}

impl ChunkedReader {
    /// Create a new chunked reader.
    ///
    /// # Notes
    ///
    /// We don't need to handle `Executor::timeout` since we are outside the layer.
    fn new(ctx: Arc<ReadContext>, range: BytesRange) -> Self {
        Self::with_revalidation(ctx, range, None)
    }

    fn with_suffix_revalidation(ctx: Arc<ReadContext>, reval: SuffixRevalidation) -> Self {
        Self::with_revalidation(ctx, reval.resolved_range().into(), Some(reval))
    }

    fn with_revalidation(
        ctx: Arc<ReadContext>,
        range: BytesRange,
        suffix_revalidation: Option<SuffixRevalidation>,
    ) -> Self {
        let tasks = ConcurrentTasks::new(
            ctx.context().executor().clone(),
            ctx.options().concurrent(),
            ctx.options().prefetch(),
            |mut input: ChunkedReadInput| {
                Box::pin(async move {
                    let result = if let Some(mut reader) = input.reader.take() {
                        reader.read_all().await
                    } else {
                        match input.ctx.reader().read(input.range).await {
                            Ok((rp, buffer)) => {
                                input.ctx.observe_read_response(rp);
                                Ok(buffer)
                            }
                            Err(err) => Err(err),
                        }
                    };
                    let result = result.and_then(|buffer| {
                        check_complete(input.range.size(), buffer.len() as u64)?;
                        Ok(buffer)
                    });
                    (input, result)
                })
            },
        );
        Self {
            ctx,
            offset: range.offset(),
            remaining: range.size(),
            suffix_revalidation,
            revalidated_range: None,
            opened: None,
            tasks,
            done: false,
        }
    }

    fn take_revalidated_range(&mut self) -> Option<Range<u64>> {
        self.revalidated_range.take()
    }

    async fn open_revalidated_first_chunk(&mut self) -> Result<()> {
        let Some(mut reval) = self.suffix_revalidation else {
            return Ok(());
        };

        let chunk = self.ctx.options().chunk();
        let opened = self
            .ctx
            .open_revalidated_suffix(&mut reval, |full| {
                let remaining = full.end - full.start;
                if remaining == 0 {
                    return None;
                }
                let read_size = chunk.map_or(remaining, |c| remaining.min(c as u64));
                Some(BytesRange::new(full.start, Some(read_size)))
            })
            .await;

        match opened {
            Ok(Some(RevalidatedSuffixOpen {
                full_range,
                first_range,
                rp,
                reader,
            })) => {
                let read_size = first_range
                    .size()
                    .expect("revalidated suffix chunk must be bounded");
                let total = full_range.end - full_range.start;
                self.offset = full_range.start;
                self.remaining = Some(total);
                self.revalidated_range = Some(full_range);
                self.suffix_revalidation = None;
                self.ctx.observe_read_response(rp);
                self.opened = Some(Box::new(OpenedChunk {
                    input: ChunkedReadInput {
                        ctx: self.ctx.clone(),
                        range: first_range,
                        reader: Some(reader),
                    },
                    planned: PlannedRange {
                        range: first_range,
                        next_offset: self.offset + read_size,
                        next_remaining: Some(total - read_size),
                    },
                }));
                Ok(())
            }
            Ok(None) => {
                let full_range = reval.resolved_range();
                self.offset = full_range.start;
                self.remaining = Some(0);
                self.revalidated_range = Some(full_range);
                self.suffix_revalidation = None;
                self.done = true;
                Ok(())
            }
            Err(err) => {
                self.suffix_revalidation = Some(reval);
                Err(err)
            }
        }
    }

    async fn prepare_metadata(&mut self) -> Result<()> {
        if self.ctx.metadata().is_some() && self.suffix_revalidation.is_none() {
            return Ok(());
        }

        if self.suffix_revalidation.is_some() {
            self.open_revalidated_first_chunk().await?;
            return Ok(());
        }

        if self.opened.is_none() {
            if let Some(planned) = self.plan_next_range() {
                let range = planned.range;
                let (rp, reader) = self.ctx.reader().open(range).await?;
                self.ctx.observe_read_response(rp);
                self.opened = Some(Box::new(OpenedChunk {
                    input: ChunkedReadInput {
                        ctx: self.ctx.clone(),
                        range,
                        reader: Some(reader),
                    },
                    planned,
                }));
            } else {
                self.done = true;
            }
        }

        Ok(())
    }

    async fn metadata(&mut self) -> Result<Metadata> {
        self.prepare_metadata().await?;

        self.ctx
            .metadata()
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::Unsupported, "read metadata is not available"))
    }

    /// Plan the next range to read without advancing chunked reader state.
    fn plan_next_range(&self) -> Option<PlannedRange> {
        if self.remaining == Some(0) {
            return None;
        }

        let (next_size, next_offset, next_remaining) = match self.remaining {
            None => (None, self.offset, Some(0)),
            Some(remaining) => {
                let read_size = self
                    .ctx
                    .options()
                    .chunk()
                    .map_or(remaining, |chunk| remaining.min(chunk as u64));
                (
                    Some(read_size),
                    self.offset + read_size,
                    Some(remaining - read_size),
                )
            }
        };

        Some(PlannedRange {
            range: BytesRange::new(self.offset, next_size),
            next_offset,
            next_remaining,
        })
    }

    /// Advance chunked reader state after the planned range is scheduled.
    fn commit_range(&mut self, planned: PlannedRange) {
        self.offset = planned.next_offset;
        self.remaining = planned.next_remaining;
    }
}

impl oio::ReadStream for ChunkedReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.suffix_revalidation.is_some() {
            self.open_revalidated_first_chunk().await?;
        }

        while self.tasks.has_remaining() && !self.done {
            if let Some(opened) = self.opened.take() {
                let OpenedChunk { input, planned } = *opened;
                self.tasks.execute(input).await?;
                self.commit_range(planned);
            } else if let Some(planned) = self.plan_next_range() {
                self.tasks
                    .execute(ChunkedReadInput {
                        ctx: self.ctx.clone(),
                        range: planned.range,
                        reader: None,
                    })
                    .await?;
                self.commit_range(planned);
            } else {
                self.done = true;
                break;
            }
            if self.tasks.has_result() {
                break;
            }
        }

        let Some(buffer) = self.tasks.next().await.transpose()? else {
            return Ok(Buffer::new());
        };

        Ok(buffer)
    }
}

/// BufferStream is a stream of buffers, created by [`Reader::into_stream`]
///
/// `BufferStream` implements `Stream` trait.
pub struct BufferStream {
    ctx: Arc<ReadContext>,
    /// # Notes to maintainers
    ///
    /// The underlying reader is either a StreamingReader or a ChunkedReader.
    ///
    /// - If chunk is None, BufferStream will use StreamingReader to iterate
    ///   data in streaming way.
    /// - Otherwise, BufferStream will use ChunkedReader to read data in chunks.
    state: State,
    size: Option<u64>,
    exact: bool,
    read: u64,
    revalidated_suffix_range: Option<Range<u64>>,
}

#[allow(clippy::large_enum_variant)]
enum State {
    Idle(Option<TwoWays<StreamingReader, ChunkedReader>>),
    Reading(BoxedStaticFuture<(TwoWays<StreamingReader, ChunkedReader>, Result<Buffer>)>),
    /// A metadata preparation future that owns the reader.
    ///
    /// Keeping the future in the state preserves the reader and the progress it
    /// already made when a [`BufferStream::metadata`] future is dropped.
    PreparingMetadata(
        BoxedStaticFuture<(TwoWays<StreamingReader, ChunkedReader>, Result<Metadata>)>,
    ),
}

impl BufferStream {
    /// Create a new buffer stream with already calculated offset and size.
    pub(crate) fn new(ctx: Arc<ReadContext>, offset: u64, size: Option<u64>) -> Self {
        debug_assert!(
            size.is_some() || ctx.options().chunk().is_none(),
            "size must be known if chunk is set"
        );

        Self::from_range(ctx, BytesRange::new(offset, size), None, None)
    }

    /// Create a new buffer stream for a suffix range resolved via `stat`,
    /// revalidating the object size on the first read open.
    pub(crate) fn new_suffix(ctx: Arc<ReadContext>, reval: SuffixRevalidation) -> Self {
        let resolved = reval.resolved_range();
        let suffix_revalidation = (resolved.end > resolved.start).then_some(reval);
        Self::from_range(ctx, resolved.into(), None, suffix_revalidation)
    }

    /// Create a stream, resolving the range first if chunk planning needs its size.
    pub(crate) async fn create(
        ctx: Arc<ReadContext>,
        range: impl Into<BytesRange>,
        limit: Option<u64>,
    ) -> Result<Self> {
        let mut range = range.into();
        let mut suffix_revalidation = None;
        if ctx.options().chunk().is_some() {
            if let BytesRange::Suffix { size } = range
                && ctx.args().content_length_hint().is_none()
            {
                let expected_len = ctx.content_length().await?;
                let reval = SuffixRevalidation::new(size, expected_len);
                let resolved = reval.resolved_range();
                if resolved.end > resolved.start {
                    suffix_revalidation = Some(reval);
                }
                range = resolved.into();
            } else {
                range = ctx.parse_into_range(range).await?.into();
            }
        }
        Ok(Self::from_range(ctx, range, limit, suffix_revalidation))
    }

    fn from_range(
        ctx: Arc<ReadContext>,
        range: BytesRange,
        limit: Option<u64>,
        suffix_revalidation: Option<SuffixRevalidation>,
    ) -> Self {
        let exact = limit.is_none() && !range.is_suffix() && range.size().is_some();
        let size = if exact { range.size() } else { limit };
        let reader = if ctx.options().chunk().is_some() {
            match suffix_revalidation {
                Some(reval) => {
                    TwoWays::Two(ChunkedReader::with_suffix_revalidation(ctx.clone(), reval))
                }
                None => TwoWays::Two(ChunkedReader::new(ctx.clone(), range)),
            }
        } else if let Some(reval) = suffix_revalidation {
            TwoWays::One(StreamingReader::with_suffix_revalidation(
                ctx.clone(),
                reval,
            ))
        } else {
            TwoWays::One(StreamingReader::new(ctx.clone(), range))
        };

        Self {
            ctx,
            state: State::Idle(Some(reader)),
            size,
            exact,
            read: 0,
            revalidated_suffix_range: None,
        }
    }

    fn sync_revalidated_suffix(&mut self, reader: &mut TwoWays<StreamingReader, ChunkedReader>) {
        let revalidated = match reader {
            TwoWays::One(v) => v.take_revalidated_range(),
            TwoWays::Two(v) => v.take_revalidated_range(),
        };
        if let Some(range) = revalidated {
            if self.exact {
                self.size = Some(range.end - range.start);
            }
            self.revalidated_suffix_range = Some(range);
        }
    }

    pub(crate) fn take_revalidated_suffix_range(&mut self) -> Option<Range<u64>> {
        self.revalidated_suffix_range.take()
    }

    /// Get metadata for this stream.
    ///
    /// Calling this method opens the underlying read request if needed.
    /// Returns [`ErrorKind::Unsupported`] if the underlying service doesn't
    /// return metadata while opening the read operation.
    ///
    /// Dropping this future before it completes does not lose the stream's
    /// reader; later calls resume the same preparation.
    pub async fn metadata(&mut self) -> Result<Metadata> {
        if let Some(metadata) = self.ctx.metadata() {
            return Ok(metadata.clone());
        }

        match &mut self.state {
            State::Idle(reader) => {
                let mut reader = reader.take().expect("reader must exist while idle");
                let fut = async move {
                    let ret = match &mut reader {
                        TwoWays::One(v) => v.metadata().await,
                        TwoWays::Two(v) => v.metadata().await,
                    };
                    (reader, ret)
                };
                self.state = State::PreparingMetadata(Box::pin(fut));
            }
            State::PreparingMetadata(_) => {}
            State::Reading(_) => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "read metadata is not available",
                ));
            }
        }

        let State::PreparingMetadata(fut) = &mut self.state else {
            unreachable!("metadata state must be preparing")
        };
        let (mut reader, result) = fut.as_mut().await;
        self.sync_revalidated_suffix(&mut reader);
        self.state = State::Idle(Some(reader));
        result
    }
}

impl Stream for BufferStream {
    type Item = Result<Buffer>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let remaining = if this.exact {
            None
        } else {
            this.size.map(|size| size - this.read)
        };
        if remaining == Some(0) {
            return Poll::Ready(None);
        }

        loop {
            match &mut this.state {
                State::Idle(reader) => {
                    let mut reader = reader.take().expect("reader must exist while idle");
                    let fut = async move {
                        let ret = reader.read().await;
                        (reader, ret)
                    };
                    this.state = State::Reading(Box::pin(fut));
                }
                State::Reading(fut) => {
                    let fut = fut.as_mut();
                    let (mut reader, buf) = ready!(fut.poll(cx));
                    this.sync_revalidated_suffix(&mut reader);
                    this.state = State::Idle(Some(reader));
                    return match buf {
                        Ok(buf) if buf.is_empty() => {
                            let error = if this.exact {
                                check_complete(this.size, this.read).err()
                            } else {
                                None
                            };
                            Poll::Ready(error.map(Err))
                        }
                        Ok(mut buf) => {
                            if let Some(remaining) = remaining {
                                buf.truncate((buf.len() as u64).min(remaining) as usize);
                            }
                            this.read += buf.len() as u64;
                            Poll::Ready(Some(Ok(buf)))
                        }
                        Err(err) => Poll::Ready(Some(Err(err))),
                    };
                }
                State::PreparingMetadata(fut) => {
                    let (mut reader, result) = ready!(fut.as_mut().poll(cx));
                    this.sync_revalidated_suffix(&mut reader);
                    this.state = State::Idle(Some(reader));
                    match result {
                        Ok(_) => {}
                        Err(err) if err.kind() == ErrorKind::Unsupported => {}
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use bytes::Buf;
    use bytes::Bytes;
    use futures::StreamExt;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::types::read::test_utils::ReadFault;
    use crate::types::read::test_utils::new_mock_read_context;

    struct PendingOnceReader {
        content: Bytes,
        pending: AtomicBool,
    }

    impl PendingOnceReader {
        fn new(content: Bytes) -> Self {
            Self {
                content,
                pending: AtomicBool::new(true),
            }
        }
    }

    impl oio::Read for PendingOnceReader {
        async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            if self.pending.swap(false, Ordering::SeqCst) {
                let mut polled = false;
                std::future::poll_fn(|cx| {
                    if polled {
                        Poll::Ready(())
                    } else {
                        polled = true;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
            }

            let start = range.offset() as usize;
            let size = range.size().expect("range must be bounded") as usize;
            let metadata = MetadataBuilder::file(self.content.len() as u64).build();

            Ok((
                RpRead::new(metadata),
                Box::new(self.content.slice(start..start + size)),
            ))
        }

        async fn read(&self, _: BytesRange) -> Result<(RpRead, Buffer)> {
            Err(Error::new(ErrorKind::Unsupported, "read is not supported"))
        }
    }

    fn new_pending_once_read_context(content: Bytes) -> Arc<ReadContext> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, []).unwrap();
        let ctx = op.context().clone();
        let srv = op.service().clone();
        Arc::new(ReadContext::new(
            ctx,
            srv,
            "test".to_string(),
            OpRead::new(),
            OpReader::new(),
            Box::new(PendingOnceReader::new(content)),
        ))
    }

    fn new_read_context(
        ctx: OperationContext,
        srv: Servicer,
        path: &str,
        options: crate::raw::OpReader,
    ) -> crate::Result<ReadContext> {
        let args = crate::raw::OpRead::new();
        let reader = srv.read(&ctx, path, args.clone())?;
        Ok(ReadContext::new(
            ctx,
            srv,
            path.to_string(),
            args,
            options,
            reader,
        ))
    }

    #[tokio::test]
    async fn test_trait() -> Result<()> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])?;
        let ctx = op.context().clone();
        let srv = op.service().clone();
        let ctx = Arc::new(new_read_context(ctx, srv, "test", OpReader::new())?);
        let v = BufferStream::create(ctx, 4..8, None).await?;

        let _: Box<dyn Unpin + MaybeSend + 'static> = Box::new(v);

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_cancel_then_metadata() -> Result<()> {
        let ctx = new_pending_once_read_context(Bytes::from_static(b"HelloWorld"));
        let mut stream = BufferStream::create(ctx, 4..8, None).await?;

        {
            let mut metadata = std::pin::pin!(stream.metadata());
            assert!(futures::poll!(&mut metadata).is_pending());
        }

        let metadata = stream.metadata().await?;
        assert_eq!(metadata.content_length(), 10);

        let buf = stream.next().await.expect("stream must yield a buffer")?;
        assert_eq!(buf.to_bytes(), Bytes::from_static(b"oWor"));
        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_cancel_then_read() -> Result<()> {
        let ctx = new_pending_once_read_context(Bytes::from_static(b"HelloWorld"));
        let mut stream = BufferStream::create(ctx, 4..8, None).await?;

        {
            let mut metadata = std::pin::pin!(stream.metadata());
            assert!(futures::poll!(&mut metadata).is_pending());
        }

        let buf = stream.next().await.expect("stream must yield a buffer")?;
        assert_eq!(buf.to_bytes(), Bytes::from_static(b"oWor"));

        let metadata = stream.metadata().await?;
        assert_eq!(metadata.content_length(), 10);
        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_while_reading() -> Result<()> {
        let ctx = new_pending_once_read_context(Bytes::from_static(b"HelloWorld"));
        let mut stream = BufferStream::create(ctx, 4..8, None).await?;

        {
            let mut next = std::pin::pin!(stream.next());
            assert!(futures::poll!(&mut next).is_pending());
        }

        let err = stream
            .metadata()
            .await
            .expect_err("metadata must be unavailable while reading");
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        let buf = stream.next().await.expect("stream must yield a buffer")?;
        assert_eq!(buf.to_bytes(), Bytes::from_static(b"oWor"));

        let metadata = stream.metadata().await?;
        assert_eq!(metadata.content_length(), 10);
        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_buffer_stream() -> Result<()> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])?;
        op.write(
            "test",
            Buffer::from(vec![Bytes::from("Hello"), Bytes::from("World")]),
        )
        .await?;

        let ctx = op.context().clone();
        let srv = op.service().clone();
        let ctx = Arc::new(new_read_context(ctx, srv, "test", OpReader::new())?);

        let s = BufferStream::create(ctx, 4..8, None).await?;
        let bufs: Vec<_> = s.try_collect().await.unwrap();
        assert_eq!(bufs.len(), 1);
        assert_eq!(bufs[0].chunk(), "o".as_bytes());

        let buf: Buffer = bufs.into_iter().flatten().collect();
        assert_eq!(buf.len(), 4);
        assert_eq!(&buf.to_vec(), "oWor".as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_reader_retries_open_after_error() -> Result<()> {
        let content = b"HelloWorld";
        let fault = Arc::new(ReadFault::new().with_failed_open_range(BytesRange::new(0, Some(4))));
        let ctx = Arc::new(new_mock_read_context(
            content,
            fault.clone(),
            OpReader::new(),
        ));
        let mut stream = BufferStream::create(ctx, 0..4, None).await?;

        let err = stream
            .next()
            .await
            .expect("stream must yield")
            .expect_err("first open must fail");
        assert!(err.is_temporary());

        let buf = stream
            .next()
            .await
            .expect("stream must yield")
            .expect("retry must succeed");
        assert_eq!(buf.to_bytes().as_ref(), &content[..4]);
        assert!(stream.next().await.is_none());
        assert_eq!(
            fault.opened_ranges(),
            vec![BytesRange::new(0, Some(4)), BytesRange::new(0, Some(4))]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_chunked_reader_retries_failed_chunk() -> Result<()> {
        let fault = Arc::new(ReadFault::new().with_failed_read_range(BytesRange::new(4, Some(4))));
        let ctx = Arc::new(new_mock_read_context(
            b"HelloWorld",
            fault.clone(),
            OpReader::new().with_chunk(4),
        ));
        let mut stream = BufferStream::create(ctx, 0..10, None).await?;

        let mut data = Vec::new();
        loop {
            match stream.try_next().await {
                Ok(Some(buf)) => data.extend_from_slice(&buf.to_vec()),
                Ok(None) => break,
                Err(err) if err.is_temporary() => continue,
                Err(err) => return Err(err),
            }
        }

        assert_eq!(data, b"HelloWorld");
        assert_eq!(
            fault.read_ranges(),
            vec![
                BytesRange::new(0, Some(4)),
                BytesRange::new(4, Some(4)),
                BytesRange::new(4, Some(4)),
                BytesRange::new(8, Some(2)),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_chunked_reader_metadata_cancel_does_not_skip_chunk() -> Result<()> {
        let fault = Arc::new(ReadFault::new().with_pending_open());
        let ctx = Arc::new(new_mock_read_context(
            b"HelloWorld",
            fault.clone(),
            OpReader::new().with_chunk(4),
        ));
        let mut reader = ChunkedReader::new(ctx, BytesRange::new(0, Some(10)));

        {
            let mut metadata = Box::pin(reader.metadata());
            assert!(futures::poll!(metadata.as_mut()).is_pending());
        }

        let metadata = reader.metadata().await?;
        assert_eq!(metadata.content_length(), 10);

        let data = reader.read_all().await?;
        assert_eq!(data.to_vec(), b"HelloWorld");
        assert_eq!(
            fault.opened_ranges(),
            vec![BytesRange::new(0, Some(4)), BytesRange::new(0, Some(4)),]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_buffer_stream_metadata_cancel_keeps_chunked_reader() -> Result<()> {
        let fault = Arc::new(ReadFault::new().with_pending_open());
        let ctx = Arc::new(new_mock_read_context(
            b"HelloWorld",
            fault.clone(),
            OpReader::new().with_chunk(4),
        ));
        let mut stream = BufferStream::create(ctx, 0..10, None).await?;

        {
            let mut metadata = Box::pin(stream.metadata());
            assert!(futures::poll!(metadata.as_mut()).is_pending());
        }

        let metadata = stream.metadata().await?;
        assert_eq!(metadata.content_length(), 10);

        let bufs: Vec<Buffer> = stream.try_collect().await?;
        let data: Buffer = bufs.into_iter().flatten().collect();

        assert_eq!(data.to_vec(), b"HelloWorld");
        assert_eq!(fault.opened_ranges(), vec![BytesRange::new(0, Some(4))]);
        Ok(())
    }

    #[tokio::test]
    async fn test_chunked_reader_retries_metadata_chunk_after_read_error() -> Result<()> {
        for concurrent in [1, 2] {
            let fault =
                Arc::new(ReadFault::new().with_failed_read_range(BytesRange::new(0, Some(4))));
            let ctx = Arc::new(new_mock_read_context(
                b"0123456789",
                fault.clone(),
                OpReader::new().with_chunk(4).with_concurrent(concurrent),
            ));
            let mut reader = ChunkedReader::new(ctx, BytesRange::new(0, Some(10)));

            let metadata = reader.metadata().await?;
            assert_eq!(metadata.content_length(), 10);

            let mut data = Vec::new();
            loop {
                match reader.read().await {
                    Ok(buf) if buf.is_empty() => break,
                    Ok(buf) => data.extend_from_slice(&buf.to_vec()),
                    Err(err) if err.is_temporary() => continue,
                    Err(err) => return Err(err),
                }
            }

            assert_eq!(data, b"0123456789", "concurrent={concurrent}");
            assert_eq!(
                fault.opened_ranges(),
                vec![BytesRange::new(0, Some(4))],
                "concurrent={concurrent}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_chunked_reader_retries_metadata_chunk_after_read_cancel() -> Result<()> {
        for concurrent in [1, 2] {
            let fault = Arc::new(ReadFault::new().with_pending_read());
            let ctx = Arc::new(new_mock_read_context(
                b"0123456789",
                fault.clone(),
                OpReader::new().with_chunk(4).with_concurrent(concurrent),
            ));
            let mut reader = ChunkedReader::new(ctx, BytesRange::new(0, Some(10)));

            let metadata = reader.metadata().await?;
            assert_eq!(metadata.content_length(), 10);

            {
                let mut read = Box::pin(reader.read());
                assert!(
                    futures::poll!(read.as_mut()).is_pending(),
                    "first chunk read should stay pending for concurrent={concurrent}"
                );
            }
            fault.release_pending_read();

            let mut data = Vec::new();
            loop {
                match reader.read().await {
                    Ok(buf) if buf.is_empty() => break,
                    Ok(buf) => data.extend_from_slice(&buf.to_vec()),
                    Err(err) => return Err(err),
                }
            }

            assert_eq!(data, b"0123456789", "concurrent={concurrent}");
            assert_eq!(
                fault.opened_ranges(),
                vec![BytesRange::new(0, Some(4))],
                "concurrent={concurrent}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_buffer_stream_chunk_revalidates_suffix_on_size_change() -> Result<()> {
        use std::collections::VecDeque;
        use std::sync::Mutex;

        struct ChangingStreamRead {
            contents: Arc<Mutex<VecDeque<Bytes>>>,
            emit_metadata: bool,
        }

        impl oio::StreamRead for ChangingStreamRead {
            async fn open(
                &self,
                range: BytesRange,
            ) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
                let data = self
                    .contents
                    .lock()
                    .expect("mutex must not poison")
                    .pop_front()
                    .expect("unexpected open call");
                let content_range = range.to_content_range(data.len())?;
                let slice = data.slice(content_range);
                let rp = if self.emit_metadata {
                    RpRead::new(MetadataBuilder::file(data.len() as u64).build())
                } else {
                    RpRead::default()
                };
                Ok((
                    rp,
                    Box::new(Buffer::from(slice)) as Box<dyn oio::ReadStreamDyn>,
                ))
            }
        }

        #[derive(Debug)]
        struct ChangingSizeService {
            stat_lens: Arc<Mutex<VecDeque<u64>>>,
            contents: Arc<Mutex<VecDeque<Bytes>>>,
            emit_metadata: bool,
        }

        impl Service for ChangingSizeService {
            type Reader = oio::StreamReader<ChangingStreamRead>;
            type Writer = ();
            type Lister = ();
            type Deleter = ();
            type Copier = ();
            type Composer = ();

            fn info(&self) -> ServiceInfo {
                ServiceInfo::with_scheme("mock")
            }

            fn capability(&self) -> Capability {
                Capability {
                    stat: true,
                    read: true,
                    read_with_suffix: false,
                    ..Default::default()
                }
            }

            async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
                let len = self
                    .stat_lens
                    .lock()
                    .expect("mutex must not poison")
                    .pop_front()
                    .expect("unexpected stat call");
                Ok(RpStat::new(MetadataBuilder::file(len).build()))
            }

            fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
                Ok(oio::StreamReader::new(ChangingStreamRead {
                    contents: self.contents.clone(),
                    emit_metadata: self.emit_metadata,
                }))
            }

            async fn create_dir(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpCreateDir,
            ) -> Result<RpCreateDir> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn copy(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpCopy,
            ) -> Result<Self::Copier> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn rename(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpRename,
            ) -> Result<RpRename> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn presign(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpPresign,
            ) -> Result<RpPresign> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }
        }

        let make_op = |stat_lens: Vec<u64>, contents: Vec<&'static [u8]>, emit_metadata: bool| {
            let srv = Arc::new(ChangingSizeService {
                stat_lens: Arc::new(Mutex::new(stat_lens.into())),
                contents: Arc::new(Mutex::new(
                    contents.into_iter().map(Bytes::from_static).collect(),
                )),
                emit_metadata,
            }) as Servicer;
            let ctx = OperationContext::default().with_executor(crate::test_executor());
            Operator::from_parts(ctx, srv)
                .layer(crate::layers::SimulateLayer::default())
                .layer(crate::layers::CompleteLayer)
        };

        // Growth (8 -> 12) with chunk(2) and concurrent(2): Suffix(4) returns b"89AB".
        let op = make_op(vec![8], vec![b"0123456789AB"; 3], true);
        let buf = op
            .read_with("f")
            .range(BytesRange::suffix(4))
            .chunk(2)
            .concurrent(2)
            .await?;
        assert_eq!(buf.to_bytes(), &b"89AB"[..]);

        // Growth (8 -> 12) with Suffix(100) and chunk(4): returns full 12 bytes.
        let op = make_op(vec![8], vec![b"0123456789AB"; 4], true);
        let buf = op
            .read_with("f")
            .range(BytesRange::suffix(100))
            .chunk(4)
            .await?;
        assert_eq!(buf.to_bytes(), &b"0123456789AB"[..]);

        // Shrink (12 -> 6) with chunk(2): RangeNotSatisfied re-stats and returns b"2345".
        let op = make_op(vec![12, 6], vec![b"012345"; 3], true);
        let buf = op
            .read_with("f")
            .range(BytesRange::suffix(4))
            .chunk(2)
            .await?;
        assert_eq!(buf.to_bytes(), &b"2345"[..]);

        // Shrink to empty (8 -> 0) with chunk(2): returns empty without check_complete error.
        let op = make_op(vec![8, 0], vec![b""], true);
        let buf = op
            .read_with("f")
            .range(BytesRange::suffix(4))
            .chunk(2)
            .await?;
        assert!(buf.is_empty());

        // Repeated growth (8 -> 12 -> 16) with chunk(2) fails with temporary Conflict + context.
        let op = make_op(vec![8], vec![b"0123456789AB", b"0123456789ABCDEF"], true);
        let err = op
            .read_with("f")
            .range(BytesRange::suffix(4))
            .chunk(2)
            .await
            .expect_err("repeated growth must fail");
        assert_eq!(err.kind(), ErrorKind::Conflict);
        assert!(err.is_temporary());
        let msg = err.to_string();
        assert!(msg.contains("path: f"), "missing path in {msg}");
        assert!(
            msg.contains("expected_size: 12"),
            "missing expected_size in {msg}"
        );
        assert!(
            msg.contains("actual_size: 16"),
            "missing actual_size in {msg}"
        );

        // RpRead::default() (no metadata) with chunk(2) does not false-error.
        let op = make_op(vec![8], vec![b"01234567"; 2], false);
        let buf = op
            .read_with("f")
            .range(BytesRange::suffix(4))
            .chunk(2)
            .await?;
        assert_eq!(buf.to_bytes(), &b"4567"[..]);

        Ok(())
    }
}
