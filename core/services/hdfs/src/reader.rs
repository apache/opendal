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

use super::core::HdfsCore;
use super::writer::HdfsWriter;
use opendal_core::raw::*;
use opendal_core::*;
use std::future::Future;
use std::sync::Arc;

/// Reader returned by this backend.
pub struct HdfsReader {
    core: Arc<HdfsCore>,
    path: String,
}

impl HdfsReader {
    pub(super) fn new(core: Arc<HdfsCore>, path: &str) -> Self {
        Self {
            core,
            path: path.to_string(),
        }
    }
}

pub struct HdfsReaderHandle {
    file: Arc<hdrs::File>,
}

impl HdfsReaderHandle {
    pub(super) fn new(file: hdrs::File) -> Self {
        Self { file: file.into() }
    }
}

impl oio::PositionRead for HdfsReader {
    type Handle = HdfsReaderHandle;

    async fn open(&self) -> Result<Self::Handle> {
        let file = self.core.hdfs_open(&self.path).await?;
        Ok(HdfsReaderHandle::new(file))
    }

    async fn read_at(handle: &Self::Handle, offset: u64, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Ok(Buffer::new());
        }

        let file = handle.file.clone();
        let buf = tokio::task::spawn_blocking(move || {
            let mut buf = vec![0; size];
            let n = file.read_at(&mut buf, offset).map_err(new_std_io_error)?;
            buf.truncate(n);
            Ok::<_, Error>(Buffer::from(buf))
        })
        .await
        .map_err(|e| Error::new(ErrorKind::Unexpected, "tokio task join failed").set_source(e))??;

        Ok(buf)
    }
}

pub struct HdfsLazyWriter<C: HdfsWriteOpener = HdfsCore> {
    core: Arc<C>,
    path: String,
    op: OpWrite,
    inner: Option<HdfsWriter<hdrs::AsyncFile>>,
}

impl<C: HdfsWriteOpener> HdfsLazyWriter<C> {
    pub(super) fn new(core: Arc<C>, path: &str, op: OpWrite) -> Self {
        Self {
            core,
            path: path.to_string(),
            op,
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut HdfsWriter<hdrs::AsyncFile>> {
        if self.inner.is_none() {
            self.inner = Some(self.core.open(&self.path, &self.op).await?);
        }

        Ok(self.inner.as_mut().expect("writer must be initialized"))
    }
}

impl<C: HdfsWriteOpener> oio::Write for HdfsLazyWriter<C> {
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
                "HdfsWriter doesn't support abort",
            )),
        }
    }
}

/// Opens the underlying HDFS writer on the first write or close.
///
/// A fake opener lets tests verify that aborting an unused writer does not
/// connect to HDFS.
pub(super) trait HdfsWriteOpener: Send + Sync + 'static {
    fn open(
        &self,
        path: &str,
        op: &OpWrite,
    ) -> impl Future<Output = Result<HdfsWriter<hdrs::AsyncFile>>> + Send;
}

impl HdfsWriteOpener for HdfsCore {
    async fn open(&self, path: &str, op: &OpWrite) -> Result<HdfsWriter<hdrs::AsyncFile>> {
        let (target_path, tmp_path, f, target_exists, initial_size) =
            self.hdfs_write(path, op).await?;

        Ok(HdfsWriter::new(
            target_path,
            tmp_path,
            f,
            Arc::clone(&self.client),
            target_exists,
            initial_size,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use opendal_core::raw::oio::Write;

    use super::*;

    struct FakeOpener {
        calls: Arc<AtomicUsize>,
    }

    impl HdfsWriteOpener for FakeOpener {
        async fn open(&self, _: &str, _: &OpWrite) -> Result<HdfsWriter<hdrs::AsyncFile>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(Error::new(
                ErrorKind::Unexpected,
                "unused writer must not open the target",
            ))
        }
    }

    #[tokio::test]
    async fn abort_unused_writer_does_not_connect() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut writer = HdfsLazyWriter::new(
            Arc::new(FakeOpener {
                calls: Arc::clone(&calls),
            }),
            "new-dir/nested/file.txt",
            OpWrite::default(),
        );

        let err = writer.abort().await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Unsupported);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
