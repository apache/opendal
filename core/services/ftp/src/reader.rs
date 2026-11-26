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
use super::core::FtpCore;
use super::core::Manager;
use super::core::format_ftp_error;
use super::lister::FtpLister;
use super::lister::parse_list_line;
use super::writer::FtpWriter;
use asyncband::pool::bounded;
use bytes::BytesMut;
use opendal_core::raw::*;
use opendal_core::*;
use std::sync::Arc;
use suppaftp::FtpError;
use suppaftp::Status;
use suppaftp::tokio::AsyncRustlsStream;
use suppaftp::tokio::TransferStream;
use suppaftp::types::Response;
use tokio::io::AsyncReadExt;

pub struct FtpReadStream {
    /// Declared before `_ftp_stream` so the data connection is always closed
    /// before the control connection returns to the pool.
    data_stream: Option<TransferStream<AsyncRustlsStream>>,

    /// Keep the connection alive while data stream is alive.
    _ftp_stream: bounded::Object<Manager>,
    /// Bytes left in the requested range, or `u64::MAX` for an open-ended read.
    remaining: u64,
    chunk: usize,
    buf: BytesMut,
}

/// # Safety
///
/// We only have `&mut self` for FtpReadStream.
unsafe impl Sync for FtpReadStream {}

impl FtpReadStream {
    pub async fn new(
        mut ftp_stream: bounded::Object<Manager>,
        path: String,
        range: BytesRange,
    ) -> Result<Self> {
        let (offset, size) = (range.offset(), range.size().unwrap_or(u64::MAX));
        if offset != 0 {
            ftp_stream
                .resume_transfer(offset as usize)
                .await
                .map_err(format_ftp_error)?;
        }
        let data_stream = ftp_stream
            .retr_as_stream(path)
            .await
            .map_err(format_ftp_error)?;

        Ok(Self {
            data_stream: Some(data_stream),

            _ftp_stream: ftp_stream,
            remaining: size,
            chunk: 1024 * 1024,
            buf: BytesMut::new(),
        })
    }
}

impl oio::ReadStream for FtpReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        // A zero-length range never reads from the server. Finish the
        // transfer without checking the reply: closing the data connection
        // early makes the server report an aborted transfer even though the
        // requested range is complete.
        if self.remaining == 0 {
            if let Some(data_stream) = self.data_stream.take() {
                let _ = data_stream.finish().await;
            }
            return Ok(Buffer::new());
        }

        let Some(data_stream) = self.data_stream.as_mut() else {
            return Ok(Buffer::new());
        };

        let limit = self.remaining.min(self.chunk as u64) as usize;
        self.buf.resize(limit, 0);
        let n = data_stream
            .read(&mut self.buf)
            .await
            .map_err(new_std_io_error)?;
        self.remaining -= n as u64;

        if n == 0 || self.remaining == 0 {
            let data_stream = self
                .data_stream
                .take()
                .expect("ftp data stream must be initialized");
            let result = data_stream.finish().await;
            // A range-limited read closes the data connection before the
            // server finishes sending, so its completion reply may report an
            // aborted transfer even though every requested byte arrived.
            if n == 0 {
                result.map_err(format_ftp_error)?;
            }
        }

        Ok(Buffer::from(self.buf.split_to(n).freeze()))
    }
}

/// Reader returned by this backend.
pub struct FtpReader {
    backend: FtpBackend,
    path: String,
}

impl FtpReader {
    pub(super) fn new(backend: FtpBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

pub struct FtpLazyWriter {
    core: Arc<FtpCore>,
    path: String,
    append: bool,
    inner: Option<FtpWriter>,
}

impl FtpLazyWriter {
    pub(super) fn new(core: Arc<FtpCore>, path: &str, op: OpWrite) -> Self {
        Self {
            core,
            path: path.to_string(),
            append: op.append(),
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut FtpWriter> {
        if self.inner.is_none() {
            let parent = get_parent(&self.path);
            let paths: Vec<&str> = parent.split('/').collect();

            // TODO: we can optimize this by checking dir existence first.
            let mut ftp_stream = self.core.ftp_connect(Operation::Write).await?;
            let mut curr_path = String::new();

            for path in paths {
                if path.is_empty() {
                    continue;
                }
                curr_path.push_str(path);
                curr_path.push('/');
                match ftp_stream.mkdir(&curr_path).await {
                    // Do nothing if status is FileUnavailable or OK(()) is return.
                    Err(FtpError::UnexpectedResponse(Response {
                        status: Status::FileUnavailable,
                        ..
                    }))
                    | Ok(()) => (),
                    Err(e) => {
                        return Err(format_ftp_error(e));
                    }
                }
            }

            let w = FtpWriter::new(ftp_stream, self.path.clone(), self.append);
            self.inner = Some(w);
        }

        Ok(self.inner.as_mut().expect("ftp writer must be initialized"))
    }
}

impl oio::Write for FtpLazyWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner().await?.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        // Create the inner writer even when nothing was written so that
        // closing the writer still creates or touches the remote file and
        // reports its final metadata.
        self.inner().await?.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        match &mut self.inner {
            Some(w) => w.abort().await,
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "FtpWriter doesn't support abort",
            )),
        }
    }
}

pub struct FtpLazyLister {
    core: Arc<FtpCore>,
    path: String,
    inner: Option<FtpLister>,
}

impl FtpLazyLister {
    pub(super) fn new(core: Arc<FtpCore>, path: &str) -> Self {
        Self {
            core,
            path: path.to_string(),
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut FtpLister> {
        if self.inner.is_none() {
            let mut ftp_stream = self.core.ftp_connect(Operation::List).await?;

            let pathname = if self.path == "/" {
                None
            } else {
                Some(self.path.as_str())
            };
            let files = ftp_stream.list(pathname).await.map_err(format_ftp_error)?;

            // Listing `dir/` returns `dir/` itself first, as other services
            // do. A non-empty listing proves the directory exists. Some
            // servers answer LIST on a missing path with an empty success,
            // and a parent listing can miss the directory (an unlistable
            // parent, or servers that hide dot directories), so an empty
            // listing is confirmed by changing into the directory.
            let mut dir_exists = false;
            if self.path != "/" && self.path.ends_with('/') {
                dir_exists = files
                    .iter()
                    .any(|line| matches!(parse_list_line(line), Ok(Some(_))));
                if !dir_exists {
                    dir_exists = self.core.ftp_dir_exists(ftp_stream, &self.path).await?;
                }
            }

            let mut lister = FtpLister::new(if self.path == "/" { "" } else { &self.path }, files);
            if dir_exists {
                lister = lister.with_dir_entry();
            }
            self.inner = Some(lister);
        }

        Ok(self.inner.as_mut().expect("ftp lister must be initialized"))
    }
}

impl oio::List for FtpLazyLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner().await?.next().await
    }
}

impl oio::StreamRead for FtpReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();

        let ftp_stream = backend.core.ftp_connect(Operation::Read).await?;
        let rp = RpRead::default();
        let stream = FtpReadStream::new(ftp_stream, path.to_string(), range).await?;

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}
