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
use std::sync::Arc;
use std::sync::OnceLock;

use crate::raw::oio::Read as _;
use crate::raw::*;
use crate::*;

/// ReadContext holds the immutable context for give read operation.
pub struct ReadContext {
    /// The composed context used to execute operations.
    ctx: OperationContext,
    /// The composed service to the underlying object storage.
    srv: Servicer,
    /// Path to the file.
    path: String,
    /// Arguments for the read operation.
    args: OpRead,
    /// Options for the reader.
    options: OpReader,
    /// Raw reader returned by [`Access::read`].
    reader: oio::Reader,
    /// Complete object metadata observed from successful read opens.
    metadata: OnceLock<Metadata>,
}

impl ReadContext {
    /// Create a new ReadContext.
    #[inline]
    pub fn new(
        ctx: OperationContext,
        srv: Servicer,
        path: String,
        args: OpRead,
        options: OpReader,
        reader: oio::Reader,
    ) -> Self {
        Self {
            ctx,
            srv,
            path,
            args,
            options,
            reader,
            metadata: OnceLock::new(),
        }
    }

    /// Get the composed context.
    #[inline]
    pub(crate) fn context(&self) -> &OperationContext {
        &self.ctx
    }

    /// Get the path.
    #[inline]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the arguments.
    #[inline]
    pub fn args(&self) -> &OpRead {
        &self.args
    }

    /// Get the options.
    #[inline]
    pub fn options(&self) -> &OpReader {
        &self.options
    }

    /// Get the raw reader.
    #[inline]
    pub fn reader(&self) -> &oio::Reader {
        &self.reader
    }

    /// Get complete object metadata observed by this reader.
    #[inline]
    pub fn metadata(&self) -> Option<&Metadata> {
        self.metadata.get()
    }

    /// Set cached object metadata observed from successful read opens once.
    pub(crate) fn set_metadata(&self, metadata: Metadata) {
        let _ = self.metadata.set(metadata);
    }

    /// Observe read response and cache metadata if available.
    pub(crate) fn observe_read_response(&self, rp: RpRead) {
        if let Some(metadata) = rp.into_metadata()
            && self.metadata().is_none()
        {
            self.set_metadata(metadata);
        }
    }

    pub(crate) async fn content_length(&self) -> Result<u64> {
        if let Some(v) = self
            .args()
            .content_length_hint()
            .or_else(|| self.metadata().map(|v| v.content_length()))
        {
            return Ok(v);
        }

        let op_stat = OpStat::from_read(self.args());

        Ok(self
            .srv
            .stat(&self.ctx, self.path(), op_stat)
            .await?
            .into_metadata()
            .content_length())
    }

    /// Parse the range into an absolute bounded range.
    pub(crate) async fn parse_into_range(
        &self,
        range: impl Into<BytesRange>,
    ) -> Result<Range<u64>> {
        let range = range.into();
        let (start, end) = match range {
            BytesRange::Range { offset, size } => {
                let end = match size {
                    Some(size) => offset.checked_add(size).ok_or_else(|| {
                        Error::new(
                            ErrorKind::RangeNotSatisfied,
                            "range end overflow: offset + size exceeds u64::MAX",
                        )
                    })?,
                    None => self.content_length().await?,
                };
                (offset, end)
            }
            BytesRange::Suffix { size } => {
                let content_length = self.content_length().await?;
                (content_length.saturating_sub(size), content_length)
            }
        };

        if start > end {
            return Err(Error::new(
                ErrorKind::RangeNotSatisfied,
                "range starts after the end of the object",
            )
            .with_context("offset", start)
            .with_context("content_length", end));
        }

        Ok(start..end)
    }
}

/// ReadGenerator is used to generate new readers.
///
/// If chunk is None, ReaderGenerator will only return one reader.
/// Otherwise, ReaderGenerator will return multiple readers, each with size
/// of chunk.
///
/// It's design that we didn't implement the generator as a stream, because
/// we don't expose the generator to the user. Instead, we use the async method
/// directly to keep it simple and easy to understand.
pub struct ReadGenerator {
    ctx: Arc<ReadContext>,

    range: BytesRange,
    done: bool,
}

/// A planned read range and the generator state after opening it.
#[derive(Clone, Copy)]
struct PlannedRange {
    range: BytesRange,
    next_range: BytesRange,
    done: bool,
}

impl ReadGenerator {
    /// Create a new ReadGenerator.
    #[inline]
    pub fn new(ctx: Arc<ReadContext>, range: BytesRange) -> Self {
        Self {
            ctx,
            range,
            done: false,
        }
    }

    /// Plan the next range to read without advancing the generator.
    fn plan_next_range(&self) -> Option<PlannedRange> {
        if self.done || self.range.size() == Some(0) {
            return None;
        }

        let Some(chunk) = self.ctx.options.chunk() else {
            return Some(PlannedRange {
                range: self.range,
                next_range: self.range,
                done: true,
            });
        };

        match self.range {
            BytesRange::Suffix { .. } => Some(PlannedRange {
                range: self.range,
                next_range: self.range,
                done: true,
            }),
            BytesRange::Range { offset, size } => {
                let Some(remaining) = size else {
                    return Some(PlannedRange {
                        range: self.range,
                        next_range: self.range,
                        done: true,
                    });
                };

                let read_size = remaining.min(chunk as u64);
                Some(PlannedRange {
                    range: BytesRange::new(offset, Some(read_size)),
                    next_range: BytesRange::new(offset + read_size, Some(remaining - read_size)),
                    done: false,
                })
            }
        }
    }

    /// Advance generator state after the planned range has been opened.
    fn commit_range(&mut self, planned: PlannedRange) {
        self.range = planned.next_range;
        self.done = planned.done;
    }

    /// Generate next reader.
    ///
    /// The generator advances only after `open` succeeds, so a failed or
    /// cancelled open retries the same range instead of skipping it.
    pub async fn next_reader(&mut self) -> Result<Option<Box<dyn oio::ReadStreamDyn>>> {
        let Some(planned) = self.plan_next_range() else {
            return Ok(None);
        };

        let (rp, r) = self.ctx.reader().open(planned.range).await?;
        self.commit_range(planned);
        self.ctx.observe_read_response(rp);
        Ok(Some(r))
    }

    /// Get metadata observed by generated readers.
    pub(crate) fn metadata(&self) -> Option<&Metadata> {
        self.ctx.metadata()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::raw::oio::ReadStream as _;
    use crate::types::read::test_utils::ReadFault;
    use crate::types::read::test_utils::new_mock_read_context;

    fn new_read_context(
        ctx: OperationContext,
        srv: Servicer,
        path: &str,
        options: crate::raw::OpReader,
    ) -> crate::Result<ReadContext> {
        new_read_context_with_args(ctx, srv, path, crate::raw::OpRead::new(), options)
    }

    fn new_read_context_with_args(
        ctx: OperationContext,
        srv: Servicer,
        path: &str,
        args: crate::raw::OpRead,
        options: crate::raw::OpReader,
    ) -> crate::Result<ReadContext> {
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
    async fn test_next_reader() -> Result<()> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])?;
        op.write(
            "test",
            Buffer::from(vec![Bytes::from("Hello"), Bytes::from("World")]),
        )
        .await?;

        let ctx = op.context().clone();
        let srv = op.service().clone();
        let ctx = Arc::new(new_read_context(
            ctx,
            srv,
            "test",
            OpReader::new().with_chunk(3),
        )?);
        let mut generator = ReadGenerator::new(ctx, BytesRange::new(0, Some(10)));
        let mut readers = vec![];
        while let Some(r) = generator.next_reader().await? {
            readers.push(r);
        }

        pretty_assertions::assert_eq!(readers.len(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_next_reader_without_size() -> Result<()> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])?;
        op.write(
            "test",
            Buffer::from(vec![Bytes::from("Hello"), Bytes::from("World")]),
        )
        .await?;

        let ctx = op.context().clone();
        let srv = op.service().clone();
        let ctx = Arc::new(new_read_context(
            ctx,
            srv,
            "test",
            OpReader::new().with_chunk(3),
        )?);
        let mut generator = ReadGenerator::new(ctx, BytesRange::new(0, None));
        let mut readers = vec![];
        while let Some(r) = generator.next_reader().await? {
            readers.push(r);
        }

        pretty_assertions::assert_eq!(readers.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_next_reader_retries_range_after_open_error() -> Result<()> {
        let fault = Arc::new(ReadFault::new().with_failed_open_range(BytesRange::new(0, Some(4))));
        let ctx = Arc::new(new_mock_read_context(
            b"HelloWorld",
            fault.clone(),
            OpReader::new().with_chunk(4),
        ));
        let mut generator = ReadGenerator::new(ctx, BytesRange::new(0, Some(10)));

        let err = generator
            .next_reader()
            .await
            .err()
            .expect("first open should fail");
        assert!(err.is_temporary());

        let data = read_generator_to_end(&mut generator).await?;

        pretty_assertions::assert_eq!(data, b"HelloWorld");
        pretty_assertions::assert_eq!(
            fault.opened_ranges(),
            vec![
                BytesRange::new(0, Some(4)),
                BytesRange::new(0, Some(4)),
                BytesRange::new(4, Some(4)),
                BytesRange::new(8, Some(2)),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_next_reader_is_cancel_safe() -> Result<()> {
        let fault = Arc::new(ReadFault::new().with_pending_open());
        let ctx = Arc::new(new_mock_read_context(
            b"HelloWorld",
            fault.clone(),
            OpReader::new().with_chunk(4),
        ));
        let mut generator = ReadGenerator::new(ctx, BytesRange::new(0, Some(10)));

        let mut fut = Box::pin(generator.next_reader());
        assert!(
            futures::poll!(fut.as_mut()).is_pending(),
            "first open should stay pending"
        );
        drop(fut);

        let data = read_generator_to_end(&mut generator).await?;

        pretty_assertions::assert_eq!(data, b"HelloWorld");
        pretty_assertions::assert_eq!(
            fault.opened_ranges(),
            vec![
                BytesRange::new(0, Some(4)),
                BytesRange::new(0, Some(4)),
                BytesRange::new(4, Some(4)),
                BytesRange::new(8, Some(2)),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_next_reader_retries_failed_second_chunk() -> Result<()> {
        let fault = Arc::new(ReadFault::new().with_failed_open_range(BytesRange::new(4, Some(4))));
        let ctx = Arc::new(new_mock_read_context(
            b"0123456789",
            fault.clone(),
            OpReader::new().with_chunk(4),
        ));
        let mut generator = ReadGenerator::new(ctx, BytesRange::new(0, Some(10)));

        let mut first = generator.next_reader().await?.expect("first chunk reader");
        let first = first.read_all().await?;
        pretty_assertions::assert_eq!(first.to_vec(), b"0123");

        let err = generator
            .next_reader()
            .await
            .err()
            .expect("second chunk open should fail");
        assert!(err.is_temporary());

        let mut data = first.to_vec();
        while let Some(mut reader) = generator.next_reader().await? {
            data.extend_from_slice(&reader.read_all().await?.to_vec());
        }

        pretty_assertions::assert_eq!(data, b"0123456789");
        pretty_assertions::assert_eq!(
            fault.opened_ranges(),
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
    async fn test_next_reader_preserves_range_boundaries() -> Result<()> {
        let fault = Arc::new(ReadFault::new());
        let ctx = Arc::new(new_mock_read_context(
            b"HelloWorld",
            fault.clone(),
            OpReader::new().with_chunk(5),
        ));

        let mut generator = ReadGenerator::new(ctx.clone(), BytesRange::new(0, Some(10)));
        let data = read_generator_to_end(&mut generator).await?;

        pretty_assertions::assert_eq!(data, b"HelloWorld");
        pretty_assertions::assert_eq!(
            fault.opened_ranges(),
            vec![BytesRange::new(0, Some(5)), BytesRange::new(5, Some(5)),]
        );

        let mut empty = ReadGenerator::new(ctx, BytesRange::new(0, Some(0)));
        assert!(empty.next_reader().await?.is_none());
        pretty_assertions::assert_eq!(
            fault.opened_ranges(),
            vec![BytesRange::new(0, Some(5)), BytesRange::new(5, Some(5)),]
        );
        Ok(())
    }

    async fn read_generator_to_end(generator: &mut ReadGenerator) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        while let Some(mut reader) = generator.next_reader().await? {
            data.extend_from_slice(&reader.read_all().await?.to_vec());
        }
        Ok(data)
    }

    #[tokio::test]
    async fn test_parse_into_range_bounded_end_overflow() -> Result<()> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])?;
        op.write("test", Buffer::from(Bytes::new())).await?;

        let ctx = op.context().clone();
        let srv = op.service().clone();
        let ctx = new_read_context(ctx, srv, "test", OpReader::new())?;

        let result = ctx
            .parse_into_range(BytesRange::new(u64::MAX, Some(1)))
            .await;
        assert!(
            result.is_err(),
            "bounded range whose end overflows should return error"
        );
        assert_eq!(result.unwrap_err().kind(), ErrorKind::RangeNotSatisfied);
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_into_range_uses_content_length_hint() -> Result<()> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])?;
        let (_, args, options) = options::ReadOptions {
            content_length_hint: Some(42),
            ..Default::default()
        }
        .into();

        let ctx = op.context().clone();
        let srv = op.service().clone();
        let ctx = new_read_context_with_args(ctx, srv, "test", args, options)?;

        let range = ctx.parse_into_range(10..).await?;

        pretty_assertions::assert_eq!(range, 10..42);
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_into_range_suffix_uses_content_length_hint() -> Result<()> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])?;
        let (_, args, options) = options::ReadOptions {
            content_length_hint: Some(42),
            ..Default::default()
        }
        .into();

        let ctx = op.context().clone();
        let srv = op.service().clone();
        let ctx = new_read_context_with_args(ctx, srv, "test", args, options)?;

        let range = ctx.parse_into_range(BytesRange::suffix(10)).await?;

        pretty_assertions::assert_eq!(range, 32..42);
        Ok(())
    }
}
