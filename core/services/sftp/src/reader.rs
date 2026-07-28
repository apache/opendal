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

use bytes::Bytes;
use russh_sftp::protocol::{FileAttributes, OpenFlags};

use super::backend::*;
use super::core::PIPELINE_DEPTH;
use super::core::SftpSessionRef;
use super::core::is_eof;
use super::core::is_not_found;
use super::core::is_sftp_failure;
use super::core::parse_sftp_error;
use super::lister::SftpLister;
use super::writer::SftpWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// Streams a remote file by issuing several SFTP reads concurrently.
///
/// SFTP is a request/response protocol, so a single outstanding read caps
/// throughput at one packet per round trip. Each call therefore fans out up to
/// [`PIPELINE_DEPTH`] reads and returns them as one multi-chunk [`Buffer`].
pub struct SftpReadStream {
    /// Keeps the session alive while data stream is alive.
    conn: SftpSessionRef,

    handle: String,
    offset: u64,
    /// Remaining bytes to read when the caller asked for a bounded range.
    remaining: Option<u64>,
    finished: bool,
}

impl SftpReadStream {
    pub fn new(conn: SftpSessionRef, handle: String, offset: u64, size: Option<u64>) -> Self {
        Self {
            conn,
            handle,
            offset,
            remaining: size,
            finished: false,
        }
    }
}

impl oio::ReadStream for SftpReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        if self.finished || self.remaining == Some(0) {
            return Ok(Buffer::new());
        }

        let chunk = self.conn.read_len as u64;

        // Plan a batch of reads covering contiguous offsets.
        let mut planned = 0u64;
        let mut wants = Vec::with_capacity(PIPELINE_DEPTH);
        let mut inflight = Vec::with_capacity(PIPELINE_DEPTH);
        for _ in 0..PIPELINE_DEPTH {
            let want = match self.remaining {
                Some(remaining) => remaining.saturating_sub(planned).min(chunk),
                None => chunk,
            };
            if want == 0 {
                break;
            }

            let offset = self.offset + planned;
            planned += want;
            wants.push(want);

            let session = self.conn.session.clone();
            let handle = self.handle.clone();
            inflight.push(async move { session.read(handle, offset, want as u32).await });
        }

        if inflight.is_empty() {
            return Ok(Buffer::new());
        }

        let results = futures::future::join_all(inflight).await;

        // Assemble the batch in order. A short read means the server gave us
        // less than we asked for, so everything planned after it is discarded
        // and re-requested from the corrected offset on the next call.
        let mut parts = Vec::with_capacity(results.len());
        let mut consumed = 0u64;
        for (result, want) in results.into_iter().zip(wants) {
            match result {
                Ok(data) => {
                    let len = data.data.len() as u64;
                    if len > 0 {
                        parts.push(Bytes::from(data.data));
                        consumed += len;
                    }

                    if len < want {
                        self.finished = len == 0;
                        break;
                    }
                }
                Err(e) if is_eof(&e) => {
                    self.finished = true;
                    break;
                }
                Err(e) => return Err(parse_sftp_error(e)),
            }
        }

        if consumed == 0 {
            self.finished = true;
            return Ok(Buffer::new());
        }

        self.offset += consumed;
        if let Some(remaining) = self.remaining.as_mut() {
            *remaining -= consumed;
        }

        Ok(Buffer::from(parts))
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
        let core = &self.backend.core;
        let path = core.abs_path(&self.path);

        let conn = core.connect().await?;
        let handle = conn
            .session
            .open(path, OpenFlags::READ, FileAttributes::default())
            .await
            .map_err(parse_sftp_error)?
            .handle;

        let rp = RpRead::default();
        let stream = SftpReadStream::new(conn.session_ref(), handle, range.offset(), range.size());

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

pub struct SftpLazyWriter {
    backend: SftpBackend,
    ctx: OperationContext,
    path: String,
    op: OpWrite,
    inner: Option<SftpWriter>,
}

impl SftpLazyWriter {
    pub(super) fn new(
        backend: SftpBackend,
        ctx: OperationContext,
        path: &str,
        op: OpWrite,
    ) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            op,
            inner: None,
        }
    }

    async fn inner(&mut self) -> Result<&mut SftpWriter> {
        if self.inner.is_none() {
            if let Some((dir, _)) = self.path.rsplit_once('/') {
                self.backend
                    .create_dir(&self.ctx, dir, OpCreateDir::default())
                    .await?;
            }

            let core = &self.backend.core;
            let path = core.abs_path(&self.path);
            let conn = core.connect().await?;

            let mut flags = OpenFlags::WRITE | OpenFlags::CREATE;
            if self.op.if_not_exists() {
                flags |= OpenFlags::EXCLUDE;
            }
            if self.op.append() {
                flags |= OpenFlags::APPEND;
            } else {
                flags |= OpenFlags::TRUNCATE;
            }

            let res = conn
                .session
                .open(path.as_str(), flags, FileAttributes::default())
                .await;

            let handle = match res {
                Ok(handle) => handle.handle,
                Err(e) if self.op.if_not_exists() && is_sftp_failure(&e) => {
                    if conn.session.stat(path.as_str()).await.is_ok() {
                        return Err(Error::new(
                            ErrorKind::ConditionNotMatch,
                            "file already exists, doesn't match the condition if_not_exists",
                        )
                        .set_source(e));
                    }
                    return Err(parse_sftp_error(e));
                }
                Err(e) => return Err(parse_sftp_error(e)),
            };

            // Appending starts at the current end of the file; the server
            // honours `APPEND`, but the local offset must match so that the
            // pipelined writes address the right region.
            let offset = if self.op.append() {
                conn.session
                    .stat(path.as_str())
                    .await
                    .map(|attrs| attrs.attrs.size.unwrap_or(0))
                    .unwrap_or(0)
            } else {
                0
            };

            self.inner = Some(SftpWriter::new(conn.session_ref(), handle, offset));
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
        self.inner().await?.abort().await
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
            let core = &self.backend.core;
            let conn = core.connect().await?;
            let dir_path = core.abs_path(&self.path);

            self.inner = Some(match conn.session.opendir(dir_path).await {
                Ok(handle) => Some(SftpLister::new(
                    conn.session_ref(),
                    handle.handle,
                    self.path.clone(),
                )),
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
