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

use std::mem;
use std::sync::Arc;
use std::sync::Mutex;

use asyncband::once::OnceCell;
use bytes::Bytes;
use bytes::BytesMut;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use opendal_core::raw::*;
use opendal_core::*;

use super::core::HdfsNativeCore;
use super::core::parse_hdfs_error;
use super::lister::HdfsNativeLister;
use super::writer::HdfsNativeWriter;

// Preserve coarse buffers for reads to EOF so collecting a large stream does
// not retain a separate allocation for every DataNode packet.
const READ_TO_END_BUFFER_SIZE: usize = 512 * 1024;

/// Reader returned by this backend.
pub struct HdfsNativeReader {
    core: Arc<HdfsNativeCore>,
    path: String,
    file: OnceCell<hdfs_native::file::FileReader>,
}

impl HdfsNativeReader {
    pub(super) fn new(core: Arc<HdfsNativeCore>, path: &str) -> Self {
        Self {
            core,
            path: path.to_string(),
            file: OnceCell::new(),
        }
    }
}

impl oio::Read for HdfsNativeReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        if range.size() == Some(0) {
            return Ok((RpRead::default(), Box::new(Buffer::new())));
        }

        let file = self
            .file
            .get_or_try_init(|| self.core.hdfs_open(&self.path))
            .await?;
        let read_to_end = range.size().is_none();
        let range = range.to_content_range(file.file_length())?;
        if range.is_empty() {
            return Ok((RpRead::default(), Box::new(Buffer::new())));
        }

        let stream = file
            .read_range_stream(range.start, range.len())
            .fuse()
            .boxed();
        Ok((
            RpRead::default(),
            Box::new(HdfsNativeReadStream {
                stream: Mutex::new(stream),
                buffer: BytesMut::new(),
                buffer_size: if read_to_end {
                    range.len().min(READ_TO_END_BUFFER_SIZE)
                } else {
                    0
                },
            }),
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let size = range
            .size()
            .ok_or_else(|| Error::new(ErrorKind::Unsupported, "read requires a bounded range"))?;
        let (rp, mut stream) = self.open(range).await?;
        let buffer = stream.read_all_dyn().await?;
        if !range.is_suffix() && buffer.len() as u64 != size {
            return Err(
                Error::new(ErrorKind::RangeNotSatisfied, "range exceeds content length")
                    .with_context("offset", range.offset())
                    .with_context("expect", size)
                    .with_context("actual", buffer.len()),
            );
        }
        Ok((rp, buffer))
    }
}

struct HdfsNativeReadStream {
    // The upstream stream is Send. Exclusive access lets us poll it without locking.
    stream: Mutex<BoxStream<'static, hdfs_native::Result<Bytes>>>,
    buffer: BytesMut,
    buffer_size: usize,
}

impl oio::ReadStream for HdfsNativeReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        let stream = self
            .stream
            .get_mut()
            .expect("stream mutex must not be poisoned");
        while let Some(packet) = stream.try_next().await.map_err(parse_hdfs_error)? {
            if self.buffer.is_empty() {
                if packet.len() >= self.buffer_size {
                    return Ok(Buffer::from(packet));
                }
                self.buffer.reserve(self.buffer_size);
            }
            self.buffer.extend_from_slice(&packet);
            if self.buffer.len() >= self.buffer_size {
                break;
            }
        }
        Ok(Buffer::from(mem::take(&mut self.buffer).freeze()))
    }
}

pub struct HdfsNativeLazyWriter {
    core: Arc<HdfsNativeCore>,
    path: String,
    args: OpWrite,
    inner: Option<HdfsNativeWriter>,
}

impl HdfsNativeLazyWriter {
    pub(super) fn new(core: Arc<HdfsNativeCore>, path: &str, args: OpWrite) -> Self {
        Self {
            core,
            path: path.to_string(),
            args,
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut HdfsNativeWriter> {
        if self.inner.is_none() {
            let (f, initial_size) = self.core.hdfs_write(&self.path, &self.args).await?;
            self.inner = Some(HdfsNativeWriter::new(f, initial_size));
        }

        Ok(self.inner.as_mut().expect("writer must be initialized"))
    }
}

impl oio::Write for HdfsNativeLazyWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner().await?.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner().await?.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        match &mut self.inner {
            Some(w) => w.abort().await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "HdfsNativeWriter doesn't support abort",
            )),
        }
    }
}

pub struct HdfsNativeLazyLister {
    core: Arc<HdfsNativeCore>,
    path: String,
    inner: Option<Option<HdfsNativeLister>>,
}

impl HdfsNativeLazyLister {
    pub(super) fn new(core: Arc<HdfsNativeCore>, path: &str) -> Self {
        Self {
            core,
            path: path.to_string(),
            inner: None,
        }
    }
}

impl oio::List for HdfsNativeLazyLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.inner.is_none() {
            self.inner = Some(match self.core.hdfs_list(&self.path).await? {
                Some((p, current_path)) => Some(HdfsNativeLister::new(
                    &self.core.root,
                    &self.core.client,
                    &p,
                    current_path,
                )),
                None => None,
            });
        }

        match self.inner.as_mut().expect("lister must be initialized") {
            Some(lister) => lister.next().await,
            None => Ok(None),
        }
    }
}
