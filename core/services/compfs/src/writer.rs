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

use std::io::Cursor;
use std::sync::Arc;

use compio::buf::buf_try;
use compio::fs::File;
use compio::io::AsyncWriteExt;

use super::core::{CompfsBuffer, CompfsCore};
use opendal_core::raw::*;
use opendal_core::*;

/// Maximum number of slices submitted in one vectored write.
const MAX_IOV: usize = 1024;

#[derive(Debug)]
pub struct CompfsWriter {
    core: Arc<CompfsCore>,
    file: Option<Cursor<File>>,
}

impl CompfsWriter {
    pub(super) fn new(core: Arc<CompfsCore>, file: Cursor<File>) -> Self {
        Self {
            core,
            file: Some(file),
        }
    }
}

impl oio::Write for CompfsWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let Some(mut file) = self.file.clone() else {
            return Err(Error::new(ErrorKind::Unexpected, "file has closed"));
        };

        let compfs_buf = CompfsBuffer::from(bs);
        let pos = self
            .core
            .exec(move || async move {
                for batch in compfs_buf.0.chunks(MAX_IOV) {
                    buf_try!(@try file
                        .write_vectored_all(CompfsBuffer(batch.to_vec()))
                        .await);
                }
                Ok(file.position())
            })
            .await?;
        self.file.as_mut().unwrap().set_position(pos);

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let Some(f) = self.file.take() else {
            return Err(Error::new(ErrorKind::Unexpected, "file has closed"));
        };

        self.core
            .exec(move || async move {
                f.get_ref().sync_all().await?;
                f.into_inner().close().await
            })
            .await?;

        Ok(MetadataBuilder::unknown().build())
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "cannot abort completion-based operations",
        ))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use opendal_core::Buffer;
    use opendal_core::Operator;

    use crate::Compfs;

    #[tokio::test]
    async fn test_write_buffer_with_more_than_iov_max_chunks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let op = Operator::new(Compfs::default().root(temp_dir.path().to_str().unwrap())).unwrap();

        let chunks = (0..2048)
            .map(|i| Bytes::from(vec![(i % 251) as u8]))
            .collect::<Vec<_>>();
        let mut expected = chunks.iter().flatten().copied().collect::<Vec<_>>();

        let mut writer = op.writer("many-chunks").await.unwrap();
        writer.write(Buffer::from(chunks)).await.unwrap();
        writer.write(Bytes::from_static(b"tail")).await.unwrap();
        writer.close().await.unwrap();
        expected.extend_from_slice(b"tail");

        let actual = op.read("many-chunks").await.unwrap().to_bytes();
        assert_eq!(actual.as_ref(), expected.as_slice());
    }
}
