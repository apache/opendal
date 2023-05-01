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

use std::io::SeekFrom;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use async_compat::Compat;
use bb8::PooledConnection;
use futures::executor::block_on;
use openssh_sftp_client::file::TokioCompatFile;
use openssh_sftp_client::metadata::MetaData as SftpMeta;
use owning_ref::OwningHandle;

use super::backend::Manager;
use crate::raw::oio;
use crate::raw::oio::into_reader::FdReader;
use crate::raw::oio::ReadExt;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

pub struct SftpReader {
    // todo: add safety explanation
    file: OwningHandle<
        Box<PooledConnection<'static, Manager>>,
        Box<FdReader<Compat<TokioCompatFile<'static>>>>,
    >,
}

impl SftpReader {
    pub async fn new(
        conn: PooledConnection<'static, Manager>,
        path: PathBuf,
        start: u64,
        end: u64,
    ) -> Result<Self> {
        let mut file = OwningHandle::new_with_fn(Box::new(conn), |conn| unsafe {
            let file = block_on((*conn).sftp.open(path)).unwrap();
            let f = Compat::new(TokioCompatFile::from(file));
            Box::new(oio::into_reader::from_fd(f, start, end))
        });

        file.seek(SeekFrom::Start(0)).await?;

        Ok(SftpReader { file })
    }
}

impl oio::Read for SftpReader {
    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        Pin::new(&mut *self.file).poll_read(cx, buf)
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        Pin::new(&mut *self.file).poll_seek(cx, pos)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<bytes::Bytes>>> {
        Pin::new(&mut *self.file).poll_next(cx)
    }
}

impl From<SftpMeta> for Metadata {
    fn from(meta: SftpMeta) -> Self {
        let mode = meta
            .file_type()
            .map(|filetype| {
                if filetype.is_file() {
                    EntryMode::FILE
                } else if filetype.is_dir() {
                    EntryMode::DIR
                } else {
                    EntryMode::Unknown
                }
            })
            .unwrap_or(EntryMode::Unknown);

        let mut metadata = Metadata::new(mode);

        if let Some(size) = meta.len() {
            metadata.set_content_length(size);
        }

        if let Some(modified) = meta.modified() {
            metadata.set_last_modified(modified.as_system_time().into());
        }

        metadata
    }
}
