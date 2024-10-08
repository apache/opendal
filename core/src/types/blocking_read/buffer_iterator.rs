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
use std::sync::Arc;

use crate::raw::*;
use crate::Buffer;
use crate::*;

struct IteratingReader {
    generator: ReadGenerator,
    reader: Option<oio::BlockingReader>,
}

impl IteratingReader {
    /// Create a new iterating reader.
    #[inline]
    fn new(ctx: Arc<ReadContext>, range: BytesRange) -> Self {
        let generator = ReadGenerator::new(ctx.clone(), range.offset(), range.size());
        Self {
            generator,
            reader: None,
        }
    }
}

impl oio::BlockingRead for IteratingReader {
    fn read(&mut self) -> Result<Buffer> {
        loop {
            if self.reader.is_none() {
                self.reader = self.generator.next_blocking_reader()?;
            }
            let Some(r) = self.reader.as_mut() else {
                return Ok(Buffer::new());
            };

            let buf = r.read()?;
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

/// BufferIterator is an iterator of buffers.
///
/// # TODO
///
/// We can support chunked reader for concurrent read in the future.
pub struct BufferIterator {
    inner: IteratingReader,
}

impl BufferIterator {
    /// Create a new buffer iterator.
    #[inline]
    pub fn new(ctx: Arc<ReadContext>, range: impl RangeBounds<u64>) -> Self {
        Self {
            inner: IteratingReader::new(ctx, range.into()),
        }
    }
}

impl Iterator for BufferIterator {
    type Item = Result<Buffer>;

    fn next(&mut self) -> Option<Self::Item> {
        use oio::BlockingRead;

        match self.inner.read() {
            Ok(buf) if buf.is_empty() => None,
            Ok(buf) => Some(Ok(buf)),
            Err(err) => Some(Err(err)),
        }
    }
}
