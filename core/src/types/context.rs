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

use crate::raw::*;
use crate::*;
use std::ops::Range;
use std::sync::Arc;

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
    end: u64,
}

impl ReadGenerator {
    /// Create a new ReadGenerator.
    #[inline]
    pub fn new(ctx: Arc<ReadContext>, range: Range<u64>) -> Self {
        Self {
            ctx,
            offset: range.start,
            end: range.end,
        }
    }

    /// Generate next reader.
    pub async fn next_reader(&mut self) -> Result<Option<oio::Reader>> {
        if self.offset >= self.end {
            return Ok(None);
        }

        let offset = self.offset;
        let mut size = (self.end - self.offset) as usize;
        if let Some(chunk) = self.ctx.options.chunk() {
            size = size.min(chunk)
        }

        // Update self.offset before building future.
        self.offset += size as u64;
        let args = self
            .ctx
            .args
            .clone()
            .with_range(BytesRange::new(offset, Some(size as u64)));
        let (_, r) = self.ctx.acc.read(&self.ctx.path, args).await?;
        Ok(Some(r))
    }

    /// Generate next blocking reader.
    pub fn next_blocking_reader(&mut self) -> Result<Option<oio::BlockingReader>> {
        if self.offset >= self.end {
            return Ok(None);
        }

        let offset = self.offset;
        let mut size = (self.end - self.offset) as usize;
        if let Some(chunk) = self.ctx.options.chunk() {
            size = size.min(chunk)
        }

        // Update self.offset before building future.
        self.offset += size as u64;
        let args = self
            .ctx
            .args
            .clone()
            .with_range(BytesRange::new(offset, Some(size as u64)));
        let (_, r) = self.ctx.acc.blocking_read(&self.ctx.path, args)?;
        Ok(Some(r))
    }
}
