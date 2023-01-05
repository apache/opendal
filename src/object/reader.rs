// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::AsyncRead;
use futures::AsyncSeek;
use futures::Stream;
use parking_lot::Mutex;

use crate::error::Result;
use crate::raw::*;
use crate::ObjectMetadata;
use crate::OpRead;
use crate::OpStat;

/// ObjectReader
pub struct ObjectReader {
    inner: OutputBytesReader,
}

impl ObjectReader {
    /// Create a new object reader.
    pub(crate) async fn create(
        acc: Arc<dyn Accessor>,
        path: &str,
        meta: Arc<Mutex<ObjectMetadata>>,
        op: OpRead,
    ) -> Result<Self> {
        let acc_meta = acc.metadata();

        let r = if acc_meta.hints().contains(AccessorHint::ReadIsSeekable) {
            let (_, r) = acc.read(path, op).await?;
            r
        } else {
            let (offset, size) = match (op.range().offset(), op.range().size()) {
                (Some(offset), Some(size)) => (offset, size),
                (Some(offset), None) => {
                    let total_size = get_total_size(acc.clone(), path, meta).await?;
                    (offset, total_size - offset)
                }
                (None, Some(size)) => {
                    let total_size = get_total_size(acc.clone(), path, meta).await?;
                    if size > total_size {
                        (0, total_size)
                    } else {
                        (total_size - size, size)
                    }
                }
                (None, None) => {
                    let total_size = get_total_size(acc.clone(), path, meta).await?;
                    (0, total_size)
                }
            };

            Box::new(into_seekable_reader::by_range(acc, path, offset, size))
        };

        let r = if acc_meta.hints().contains(AccessorHint::ReadIsStreamable) {
            r
        } else {
            // Make this capacity configurable.
            Box::new(into_seekable_stream(r, 256 * 1024))
        };

        Ok(ObjectReader { inner: r })
    }
}

impl OutputBytesRead for ObjectReader {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        self.inner.poll_read(cx, buf)
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<io::Result<u64>> {
        self.inner.poll_seek(cx, pos)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
        self.inner.poll_next(cx)
    }
}

impl AsyncRead for ObjectReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncSeek for ObjectReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.inner).poll_seek(cx, pos)
    }
}

impl Stream for ObjectReader {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// get_total_size will get total size via stat.
async fn get_total_size(
    acc: Arc<dyn Accessor>,
    path: &str,
    meta: Arc<Mutex<ObjectMetadata>>,
) -> Result<u64> {
    if let Some(v) = meta.lock().content_length_raw() {
        return Ok(v);
    }

    let om = acc.stat(path, OpStat::new()).await?.into_metadata();
    let size = om.content_length();
    *(meta.lock()) = om;
    Ok(size)
}
