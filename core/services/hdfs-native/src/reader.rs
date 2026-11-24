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
use futures::Stream;
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

fn open_range<S, F>(
    file_length: usize,
    range: BytesRange,
    open_stream: F,
) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)>
where
    S: Stream<Item = hdfs_native::Result<Bytes>> + Send + 'static,
    F: FnOnce(usize, usize) -> S,
{
    let rp = RpRead::new(MetadataBuilder::file(file_length as u64).build());
    let read_to_end = range.size().is_none();
    let content_range = range.to_content_range(file_length)?;
    if content_range.is_empty() {
        return Ok((rp, Box::new(Buffer::new())));
    }

    let stream = open_stream(content_range.start, content_range.len())
        .fuse()
        .boxed();
    Ok((
        rp,
        Box::new(HdfsNativeReadStream {
            stream: Mutex::new(stream),
            buffer: BytesMut::new(),
            buffer_size: if read_to_end {
                content_range.len().min(READ_TO_END_BUFFER_SIZE)
            } else {
                0
            },
        }),
    ))
}

impl oio::Read for HdfsNativeReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        if range.size() == Some(0) {
            let rp = self
                .file
                .get()
                .map(|file| RpRead::new(MetadataBuilder::file(file.file_length() as u64).build()))
                .unwrap_or_default();
            return Ok((rp, Box::new(Buffer::new())));
        }

        let file = self
            .file
            .get_or_try_init(|| self.core.hdfs_open(&self.path))
            .await?;
        open_range(file.file_length(), range, |start, len| {
            file.read_range_stream(start, len)
        })
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::stream;
    use opendal_core::raw::oio::ReadStreamDyn;

    use super::*;

    #[tokio::test]
    async fn open_range_populates_file_metadata_for_empty_and_non_empty_ranges() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (rp, mut stream) = open_range(0, BytesRange::from(..), |_, _| {
                stream::empty::<hdfs_native::Result<Bytes>>()
            })
            .unwrap();
            let meta = rp.metadata().expect("empty file should return metadata");
            assert!(meta.is_file());
            assert_eq!(meta.content_length(), 0);
            assert!(stream.read_all_dyn().await.unwrap().is_empty());

            let (rp, mut stream) = open_range(10, BytesRange::from(10..), |_, _| {
                stream::empty::<hdfs_native::Result<Bytes>>()
            })
            .unwrap();
            let meta = rp.metadata().expect("eof range should return metadata");
            assert!(meta.is_file());
            assert_eq!(meta.content_length(), 10);
            assert!(stream.read_all_dyn().await.unwrap().is_empty());

            let (rp, mut stream) = open_range(10, BytesRange::from(2..7), |start, len| {
                assert_eq!((start, len), (2, 5));
                stream::iter(vec![
                    Ok(Bytes::from_static(b"23")),
                    Ok(Bytes::from_static(b"456")),
                ])
            })
            .unwrap();
            let meta = rp
                .metadata()
                .expect("non-empty range should return metadata");
            assert!(meta.is_file());
            assert_eq!(meta.content_length(), 10);
            assert_eq!(stream.read_all_dyn().await.unwrap().to_vec(), b"23456");
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn complete_layer_detects_truncated_stream_using_rp_read_metadata() {
        #[derive(Debug, Default)]
        struct TruncatedReadService {
            file_length: usize,
            payload: Bytes,
        }

        struct TruncatedReader {
            file_length: usize,
            payload: Bytes,
        }

        impl oio::Read for TruncatedReader {
            async fn open(
                &self,
                range: BytesRange,
            ) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
                let payload = self.payload.clone();
                open_range(self.file_length, range, move |_, _| {
                    stream::iter(vec![Ok(payload)])
                })
            }

            async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
                let (rp, mut stream) = self.open(range).await?;
                let buf = stream.read_all_dyn().await?;
                Ok((rp, buf))
            }
        }

        impl Builder for TruncatedReadService {
            type Config = ();

            fn build(self) -> Result<impl Service> {
                Ok(self)
            }
        }

        impl Service for TruncatedReadService {
            type Reader = TruncatedReader;
            type Writer = ();
            type Lister = ();
            type Deleter = ();
            type Copier = ();
            type Composer = ();

            fn info(&self) -> ServiceInfo {
                ServiceInfo::new("hdfs-native-test", "/", "")
            }

            fn capability(&self) -> Capability {
                Capability {
                    read: true,
                    ..Default::default()
                }
            }

            async fn create_dir(
                &self,
                _ctx: &OperationContext,
                _path: &str,
                _args: OpCreateDir,
            ) -> Result<RpCreateDir> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn stat(
                &self,
                _ctx: &OperationContext,
                _path: &str,
                _args: OpStat,
            ) -> Result<RpStat> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn read(
                &self,
                _ctx: &OperationContext,
                _path: &str,
                _args: OpRead,
            ) -> Result<Self::Reader> {
                Ok(TruncatedReader {
                    file_length: self.file_length,
                    payload: self.payload.clone(),
                })
            }

            fn write(
                &self,
                _ctx: &OperationContext,
                _path: &str,
                _args: OpWrite,
            ) -> Result<Self::Writer> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn list(
                &self,
                _ctx: &OperationContext,
                _path: &str,
                _args: OpList,
            ) -> Result<Self::Lister> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn copy(
                &self,
                _ctx: &OperationContext,
                _from: &str,
                _to: &str,
                _args: OpCopy,
            ) -> Result<Self::Copier> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn rename(
                &self,
                _ctx: &OperationContext,
                _from: &str,
                _to: &str,
                _args: OpRename,
            ) -> Result<RpRename> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn presign(
                &self,
                _ctx: &OperationContext,
                _path: &str,
                _args: OpPresign,
            ) -> Result<RpPresign> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            let op = Operator::new(TruncatedReadService {
                file_length: 10,
                payload: Bytes::from_static(b"abcd"),
            })
            .unwrap();

            let err = op.read("file.bin").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(
                err.to_string().contains("too little data"),
                "expected truncation error, got: {err}"
            );
        })
        .await
        .unwrap();
    }
}
