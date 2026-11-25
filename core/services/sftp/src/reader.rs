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
use super::core::SftpConnection;
use super::core::create_dir_all;
use super::core::is_not_found;
use super::core::is_sftp_failure;
use super::core::parse_sftp_error;
use super::lister::SftpLister;
use super::writer::SftpWriter;
use bytes::BytesMut;
use opendal_core::raw::*;
use opendal_core::*;
use openssh_sftp_client::file::File;
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

pub struct SftpReadStream {
    file: File,
    chunk: usize,
    size: Option<usize>,
    read: usize,
    buf: BytesMut,
    // Keep the connection checked out until after the file handle drops.
    _conn: SftpConnection,
}

impl SftpReadStream {
    pub fn new(conn: SftpConnection, file: File, size: Option<u64>) -> Self {
        Self {
            file,
            size: size.map(|v| v as usize),
            chunk: 2 * 1024 * 1024,
            read: 0,
            buf: BytesMut::new(),
            _conn: conn,
        }
    }
}

impl oio::ReadStream for SftpReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        if self.read >= self.size.unwrap_or(usize::MAX) {
            return Ok(Buffer::new());
        }

        let size = if let Some(size) = self.size {
            (size - self.read).min(self.chunk)
        } else {
            self.chunk
        };
        self.buf.reserve(size);

        let Some(bytes) = self
            .file
            .read(size as u32, self.buf.split_off(0))
            .await
            .map_err(parse_sftp_error)?
        else {
            return Ok(Buffer::new());
        };

        self.read += bytes.len();
        self.buf = bytes;
        let bs = self.buf.split();
        Ok(Buffer::from(bs.freeze()))
    }
}

/// Reader returned by this backend.
pub struct SftpReader {
    backend: SftpBackend,
    path: String,
}

impl SftpReader {
    pub(super) fn new(backend: SftpBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for SftpReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();

        let client = backend.core.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&backend.core.root);

        let path = fs.canonicalize(path).await.map_err(parse_sftp_error)?;

        let mut f = client
            .open(path.as_path())
            .await
            .map_err(parse_sftp_error)?;

        if range.offset() != 0 {
            f.seek(SeekFrom::Start(range.offset()))
                .await
                .map_err(new_std_io_error)?;
        }

        let rp = RpRead::default();
        let stream = SftpReadStream::new(client, f, range.size());

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

pub struct SftpLazyWriter {
    backend: SftpBackend,
    path: String,
    op: OpWrite,
    inner: Option<SftpWriter>,
}

impl SftpLazyWriter {
    pub(super) fn new(backend: SftpBackend, path: &str, op: OpWrite) -> Self {
        Self {
            backend,
            path: path.to_string(),
            op,
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut SftpWriter> {
        if self.inner.is_none() {
            let client = self.backend.core.connect().await?;

            if let Some((dir, _)) = self.path.rsplit_once('/') {
                create_dir_all(
                    &client,
                    &self.backend.core.root,
                    dir.trim_start_matches('/'),
                )
                .await?;
            }

            let file = {
                let mut fs = client.fs();
                fs.set_cwd(&self.backend.core.root);
                let path = fs
                    .canonicalize(&self.path)
                    .await
                    .map_err(parse_sftp_error)?;

                let mut option = client.options();
                if self.op.if_not_exists() {
                    option.create_new(true);
                } else {
                    option.create(true);
                }

                if self.op.append() {
                    option.append(true);
                } else {
                    option.write(true).truncate(true);
                }

                match option.open(&path).await {
                    Ok(f) => f,
                    Err(e) if self.op.if_not_exists() && is_sftp_failure(&e) => {
                        if fs.metadata(&path).await.is_ok() {
                            return Err(Error::new(
                                ErrorKind::ConditionNotMatch,
                                "file already exists, doesn't match the condition if_not_exists",
                            )
                            .set_source(e));
                        }
                        return Err(parse_sftp_error(e));
                    }
                    Err(e) => return Err(parse_sftp_error(e)),
                }
            };

            self.inner = Some(SftpWriter::new(file, client));
        }

        Ok(self.inner.as_mut().expect("writer must be initialized"))
    }
}

impl oio::Write for SftpLazyWriter {
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
                "SftpWriter doesn't support abort",
            )),
        }
    }
}

pub struct SftpLazyLister {
    backend: SftpBackend,
    path: String,
    inner: Option<Option<SftpLister>>,
}

impl SftpLazyLister {
    pub(super) fn new(backend: SftpBackend, path: &str) -> Self {
        Self {
            backend,
            path: path.to_string(),
            inner: None,
        }
    }
}

impl oio::List for SftpLazyLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.inner.is_none() {
            let client = self.backend.core.connect().await?;
            let dir = {
                let mut fs = client.fs();
                fs.set_cwd(&self.backend.core.root);

                let file_path = format!("./{}", self.path);
                fs.open_dir(&file_path).await
            };

            self.inner = Some(match dir {
                Ok(dir) => Some(SftpLister::new(dir.read_dir(), self.path.clone(), client)),
                Err(e) if is_not_found(&e) => None,
                Err(e) => return Err(parse_sftp_error(e)),
            });
        }

        match self.inner.as_mut().expect("lister must be initialized") {
            Some(lister) => lister.next().await,
            None => Ok(None),
        }
    }
}
