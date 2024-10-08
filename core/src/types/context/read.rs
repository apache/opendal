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

use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;

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
}

impl ReadContext {
    /// Create a new ReadContext.
    #[inline]
    pub fn new(acc: Accessor, path: String, args: OpRead, options: OpReader) -> Self {
        Self {
            acc,
            path,
            args,
            options,
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

    /// Parse the range bounds into a range.
    pub(crate) async fn parse_into_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<Range<u64>> {
        let start = match range.start_bound() {
            Bound::Included(v) => *v,
            Bound::Excluded(v) => v + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(v) => v + 1,
            Bound::Excluded(v) => *v,
            Bound::Unbounded => {
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
    pub async fn next_reader(&mut self) -> Result<Option<oio::Reader>> {
        let Some(range) = self.next_range() else {
            return Ok(None);
        };

        let args = self.ctx.args.clone().with_range(range);
        let (_, r) = self.ctx.acc.read(&self.ctx.path, args).await?;
        Ok(Some(r))
    }

    /// Generate next blocking reader.
    pub fn next_blocking_reader(&mut self) -> Result<Option<oio::BlockingReader>> {
        let Some(range) = self.next_range() else {
            return Ok(None);
        };

        let args = self.ctx.args.clone().with_range(range);
        let (_, r) = self.ctx.acc.blocking_read(&self.ctx.path, args)?;
        Ok(Some(r))
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn test_next_reader() -> Result<()> {
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
            OpReader::new().with_chunk(3),
        ));
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
            OpReader::new().with_chunk(3),
        ));
        let mut generator = ReadGenerator::new(ctx, 0, None);
        let mut readers = vec![];
        while let Some(r) = generator.next_reader().await? {
            readers.push(r);
        }

        pretty_assertions::assert_eq!(readers.len(), 1);
        Ok(())
    }

    #[test]
    fn test_next_blocking_reader() -> Result<()> {
        let op = Operator::via_iter(Scheme::Memory, [])?;
        op.blocking().write(
            "test",
            Buffer::from(vec![Bytes::from("Hello"), Bytes::from("World")]),
        )?;

        let acc = op.into_inner();
        let ctx = Arc::new(ReadContext::new(
            acc,
            "test".to_string(),
            OpRead::new(),
            OpReader::new().with_chunk(3),
        ));
        let mut generator = ReadGenerator::new(ctx, 0, Some(10));
        let mut readers = vec![];
        while let Some(r) = generator.next_blocking_reader()? {
            readers.push(r);
        }

        pretty_assertions::assert_eq!(readers.len(), 4);
        Ok(())
    }
}
