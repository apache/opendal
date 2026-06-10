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

use std::ops::Bound;
use std::ops::Range;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::OnceLock;

use crate::raw::oio::Read as _;
use crate::raw::*;
use crate::*;

/// ReadContext holds the immutable context for give read operation.
pub struct ReadContext {
    /// The accessor to the storage services.
    acc: Accessor,
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
        acc: Accessor,
        path: String,
        args: OpRead,
        options: OpReader,
        reader: oio::Reader,
    ) -> Self {
        Self {
            acc,
            path,
            args,
            options,
            reader,
            metadata: OnceLock::new(),
        }
    }

    /// Get the accessor.
    #[inline]
    pub fn accessor(&self) -> &Accessor {
        &self.acc
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
        if let Some(metadata) = rp.into_metadata() {
            if self.metadata().is_none() {
                self.set_metadata(metadata);
            }
        }
    }

    /// Parse the range bounds into a range.
    pub(crate) async fn parse_into_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<Range<u64>> {
        let start = match range.start_bound() {
            Bound::Included(v) => *v,
            Bound::Excluded(v) => v.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "range start overflow: excluded bound at u64::MAX cannot be incremented",
                )
            })?,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(v) => v.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "range end overflow: inclusive bound at u64::MAX cannot be incremented",
                )
            })?,
            Bound::Excluded(v) => *v,
            Bound::Unbounded => {
                if let Some(v) = self.options().content_length_hint() {
                    v
                } else {
                    let mut op_stat = OpStat::new();

                    if let Some(v) = self.args().version() {
                        op_stat = op_stat.with_version(v);
                    }

                    self.accessor()
                        .stat(self.path(), op_stat)
                        .await?
                        .into_metadata()
                        .content_length()
                }
            }
        };

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

    offset: u64,
    size: Option<u64>,
}

impl ReadGenerator {
    /// Create a new ReadGenerator.
    #[inline]
    pub fn new(ctx: Arc<ReadContext>, offset: u64, size: Option<u64>) -> Self {
        Self { ctx, offset, size }
    }

    /// Generate next range to read.
    fn next_range(&mut self) -> Option<BytesRange> {
        if self.size == Some(0) {
            return None;
        }

        let next_offset = self.offset;
        let next_size = match self.size {
            // Given size is None, read all data.
            None => {
                // Update size to Some(0) to indicate that there is no more data to read.
                self.size = Some(0);
                None
            }
            Some(remaining) => {
                // If chunk is set, read data in chunks.
                let read_size = self
                    .ctx
                    .options
                    .chunk()
                    .map_or(remaining, |chunk| remaining.min(chunk as u64));
                // Update (offset, size) before building future.
                self.offset += read_size;
                self.size = Some(remaining - read_size);
                Some(read_size)
            }
        };

        Some(BytesRange::new(next_offset, next_size))
    }

    /// Generate next reader.
    pub async fn next_reader(&mut self) -> Result<Option<Box<dyn oio::ReadStreamDyn>>> {
        let Some(range) = self.next_range() else {
            return Ok(None);
        };

        let (rp, r) = self.ctx.reader().open(range).await?;
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

    async fn new_read_context(
        acc: crate::raw::Accessor,
        path: &str,
        options: crate::raw::OpReader,
    ) -> crate::Result<ReadContext> {
        let args = crate::raw::OpRead::new();
        let (_, reader) = acc.read(path, args.clone()).await?;
        Ok(ReadContext::new(
            acc,
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

        let acc = op.into_inner();
        let ctx = Arc::new(new_read_context(acc, "test", OpReader::new().with_chunk(3)).await?);
        let mut generator = ReadGenerator::new(ctx, 0, Some(10));
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

        let acc = op.into_inner();
        let ctx = Arc::new(new_read_context(acc, "test", OpReader::new().with_chunk(3)).await?);
        let mut generator = ReadGenerator::new(ctx, 0, None);
        let mut readers = vec![];
        while let Some(r) = generator.next_reader().await? {
            readers.push(r);
        }

        pretty_assertions::assert_eq!(readers.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_into_range_inclusive_end_u64_max() -> Result<()> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])?;
        op.write("test", Buffer::from(Bytes::new())).await?;

        let acc = op.into_inner();
        let ctx = new_read_context(acc, "test", OpReader::new()).await?;

        let result = ctx.parse_into_range(..=u64::MAX).await;
        assert!(
            result.is_err(),
            "..=u64::MAX should return error, not overflow"
        );
        assert_eq!(result.unwrap_err().kind(), ErrorKind::RangeNotSatisfied);
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_into_range_excluded_start_u64_max() -> Result<()> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])?;
        op.write("test", Buffer::from(Bytes::new())).await?;

        let acc = op.into_inner();
        let ctx = new_read_context(acc, "test", OpReader::new()).await?;

        let result = ctx
            .parse_into_range((Bound::Excluded(u64::MAX), Bound::Unbounded))
            .await;
        assert!(
            result.is_err(),
            "Excluded(u64::MAX) start should return error, not overflow"
        );
        assert_eq!(result.unwrap_err().kind(), ErrorKind::RangeNotSatisfied);
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_into_range_uses_content_length_hint() -> Result<()> {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])?;

        let acc = op.into_inner();
        let ctx =
            new_read_context(acc, "test", OpReader::new().with_content_length_hint(42)).await?;

        let range = ctx.parse_into_range(10..).await?;

        pretty_assertions::assert_eq!(range, 10..42);
        Ok(())
    }
}
