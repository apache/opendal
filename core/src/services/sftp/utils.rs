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

use std::io;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bb8::Pool;
use bb8::PooledConnection;
use futures::executor::block_on;
use futures::ready;
use openssh_sftp_client::file::File;
use openssh_sftp_client::file::TokioCompatFile;
use openssh_sftp_client::metadata::MetaData as SftpMeta;
use openssh_sftp_client::Sftp;
use tokio::io::AsyncBufRead;
use tokio::io::AsyncRead;
use tokio::io::AsyncSeek;

use super::backend::Manager;
use super::backend::SftpBackend;
use super::error::parse_io_error;
use crate::raw::oio;
use crate::EntryMode;
use crate::Error;
use crate::ErrorKind;
use crate::Metadata;
use crate::Result;

pub struct SftpReader {
    backend: SftpBackend,
    path: PathBuf,
    offset: u64,
    size: u64,
}

unsafe impl Sync for SftpReader {}

impl SftpReader {
    pub fn new(backend: SftpBackend, path: &str, offset: u64, size: u64) -> Self {
        SftpReader {
            backend,
            path: path.into(),
            offset,
            size,
        }
    }
}

impl oio::Read for SftpReader {
    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let client = block_on(self.backend.sftp_connect())?;
        let mut file = TokioCompatFile::from(block_on(client.open(self.path.clone()))?);

        if self.offset > 0 {
            Pin::new(&mut file)
                .start_seek(SeekFrom::Start(self.offset))
                .map_err(parse_io_error)?;
            ready!(Pin::new(&mut file).poll_complete(cx)).map_err(parse_io_error)?;
        }

        let mut buf = tokio::io::ReadBuf::new(buf);
        ready!(Pin::new(&mut file).poll_read(cx, &mut buf)).map_err(parse_io_error)?;
        Poll::Ready(Ok(buf.filled().len()))
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let (_, _) = (cx, pos);

        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support seeking",
        )))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<bytes::Bytes>>> {
        // let mut file = TokioCompatFile::from(self.backend.open(self.path).map_err(parse_io_error)?);

        // let mut buf = bytes::BytesMut::with_capacity(1024 * 1024);
        // let n = futures::ready!(Pin::new(&mut file).poll_read(cx, &mut buf))?;

        // if n == 0 {
        //     Poll::Ready(None)
        // } else {
        //     buf.truncate(n);
        //     Poll::Ready(Some(Ok(buf.freeze())))
        // }
        todo!()
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

        let metadata = Metadata::new(mode);

        // if let Some(modified) = meta.modified() {
        //     metadata.set_last_modified(modified.into());
        // }

        metadata
    }
}
