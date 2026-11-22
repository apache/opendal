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

use super::backend::*;
use super::core::{ErrorContext, parse_error};
use http::Response;
use opendal_core::raw::*;
use opendal_core::*;

/// Alluxio serves file content in pages of this size.
///
/// alluxio-py reads by pages of 1MB (`ALLUXIO_PAGE_SIZE_DEFAULT_VALUE`).
/// The proxy stream read returns the file from the start, so a range is
/// served by skipping the leading pages and taking the requested bytes.
const ALLUXIO_PAGE_SIZE: u64 = 1024 * 1024;

/// Reader adapter that serves a byte range out of a full file stream.
///
/// The adapter skips `offset` bytes, then yields at most `size` bytes, one
/// Alluxio page at a time. It ends early if the file ends first, so a range
/// past the end of the file reads short instead of failing.
struct PageRangeReader {
    inner: Box<dyn oio::ReadStreamDyn>,
    to_skip: u64,
    remaining: Option<u64>,
    pending: Option<Buffer>,
}

impl PageRangeReader {
    fn new(inner: Box<dyn oio::ReadStreamDyn>, range: BytesRange) -> Self {
        Self {
            inner,
            to_skip: range.offset(),
            remaining: range.size(),
            pending: None,
        }
    }
}

impl oio::ReadStream for PageRangeReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.remaining == Some(0) {
            return Ok(Buffer::new());
        }

        loop {
            let mut chunk = match self.pending.take() {
                Some(chunk) => chunk,
                None => match self.inner.read().await? {
                    buf if buf.is_empty() => return Ok(Buffer::new()),
                    buf => buf,
                },
            };

            if self.to_skip > 0 {
                let skip = self.to_skip.min(chunk.len() as u64) as usize;
                chunk = chunk.split_off(skip);
                self.to_skip -= skip as u64;
                if chunk.is_empty() {
                    continue;
                }
            }

            let mut take = chunk.len() as u64;
            if let Some(remaining) = self.remaining {
                take = take.min(remaining);
            }
            take = take.min(ALLUXIO_PAGE_SIZE);
            let out = chunk.split_to(take as usize);
            if let Some(remaining) = self.remaining.as_mut() {
                *remaining -= take;
            }
            if !chunk.is_empty() {
                self.pending = Some(chunk);
            }
            return Ok(out);
        }
    }
}

/// Reader returned by this backend.
pub struct AlluxioReader {
    backend: AlluxioBackend,
    ctx: OperationContext,
    path: String,
}

impl AlluxioReader {
    pub(super) fn new(
        backend: AlluxioBackend,
        ctx: OperationContext,
        path: &str,
        _: OpRead,
    ) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for AlluxioReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let metadata: Metadata = backend.core.get_status(&self.ctx, path).await?.try_into()?;
        let stream_id = backend.core.open_file(&self.ctx, path).await?;

        // The proxy read endpoint cannot serve a suffix range, so resolve
        // it against the file length first.
        let range = if range.is_suffix() {
            let size = range.size().unwrap_or(0);
            let start = metadata.content_length().saturating_sub(size);
            BytesRange::new(start, Some(size))
        } else {
            range
        };

        let resp = backend.core.read(&self.ctx, stream_id).await?;
        if !resp.status().is_success() {
            let (part, mut body) = resp.into_parts();
            let buf = body.to_buffer().await?;
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("ReadStream")),
                Response::from_parts(part, buf),
            ));
        }

        let rp = RpRead::new(metadata);
        let stream = resp.into_body();
        if range.is_full() {
            return Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>));
        }

        let stream = PageRangeReader::new(Box::new(stream), range);
        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::core::test_utils::*;
    use super::*;
    use opendal_core::raw::oio::ReadStream;
    use opendal_core::raw::oio::StreamRead;

    async fn read_all(range: BytesRange, data: Vec<u8>, chunk: usize) -> Vec<u8> {
        let (core, ctx) = test_core_with(MockTransport::new(data, chunk));
        let backend = AlluxioBackend {
            core: Arc::new(core),
        };
        let reader = AlluxioReader::new(backend, ctx, "file", OpRead::default());
        let (_, mut stream) = reader.open(range).await.unwrap();
        stream.read_all().await.unwrap().to_vec()
    }

    fn pattern_data(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8).collect()
    }

    #[tokio::test]
    async fn test_read_full_range() {
        let data = pattern_data(1024);
        let out = read_all(BytesRange::default(), data.clone(), 13).await;
        assert_eq!(out, data);
    }

    #[tokio::test]
    async fn test_read_range_middle() {
        let data = pattern_data(1024);
        let out = read_all(BytesRange::new(100, Some(50)), data.clone(), 13).await;
        assert_eq!(out, data[100..150]);
    }

    #[tokio::test]
    async fn test_read_range_offset_only() {
        let data = pattern_data(1024);
        let out = read_all(BytesRange::new(1000, None), data.clone(), 13).await;
        assert_eq!(out, data[1000..]);
    }

    #[tokio::test]
    async fn test_read_range_past_end() {
        let data = pattern_data(256);
        let out = read_all(BytesRange::new(250, Some(100)), data.clone(), 13).await;
        assert_eq!(out, data[250..]);
    }

    #[tokio::test]
    async fn test_read_range_spanning_pages() {
        let n = 3 * ALLUXIO_PAGE_SIZE as usize + 123;
        let data = pattern_data(n);
        let offset = ALLUXIO_PAGE_SIZE as usize + 100;
        let size = ALLUXIO_PAGE_SIZE as usize + 50;
        let out = read_all(
            BytesRange::new(offset as u64, Some(size as u64)),
            data.clone(),
            65536,
        )
        .await;
        assert_eq!(out, data[offset..offset + size]);
    }

    #[tokio::test]
    async fn test_read_suffix_range() {
        let data = pattern_data(1024);
        let out = read_all(BytesRange::suffix(10), data.clone(), 13).await;
        assert_eq!(out, data[1014..]);
    }

    #[tokio::test]
    async fn test_read_range_empty_file() {
        let out = read_all(BytesRange::new(0, Some(10)), vec![], 13).await;
        assert!(out.is_empty());
    }
}
