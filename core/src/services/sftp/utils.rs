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
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use async_compat::Compat;
use futures::AsyncBufRead;
use futures::AsyncRead;
use futures::AsyncSeek;
use openssh_sftp_client::file::File;
use openssh_sftp_client::file::TokioCompatFile;
use openssh_sftp_client::metadata::MetaData as SftpMeta;

use crate::raw::oio;
use crate::raw::oio::FromFileReader;
use crate::raw::oio::ReadExt;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

pub struct SftpReaderInner {
    file: Pin<Box<Compat<TokioCompatFile>>>,
}
pub type SftpReader = FromFileReader<SftpReaderInner>;

impl SftpReaderInner {
    pub async fn new(file: File) -> Self {
        let file = Compat::new(file.into());
        Self {
            file: Box::pin(file),
        }
    }
}

impl SftpReader {
    /// Create a new reader from a file, starting at the given offset and ending at the given offset.
    pub async fn new(file: File, start: u64, end: u64) -> Result<Self> {
        let file = SftpReaderInner::new(file).await;
        let mut r = oio::into_read_from_file(file, start, end);
        r.seek(SeekFrom::Start(0)).await?;
        Ok(r)
    }
}

impl AsyncRead for SftpReaderInner {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.file).poll_read(cx, buf)
    }
}

impl AsyncBufRead for SftpReaderInner {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context) -> Poll<std::io::Result<&[u8]>> {
        let this = self.get_mut();
        Pin::new(&mut this.file).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        Pin::new(&mut this.file).consume(amt)
    }
}

impl AsyncSeek for SftpReaderInner {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let this = self.get_mut();
        Pin::new(&mut this.file).poll_seek(cx, pos)
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
