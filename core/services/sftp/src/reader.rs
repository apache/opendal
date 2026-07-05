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
use super::core::Manager;
use super::core::is_not_found;
use super::core::is_sftp_failure;
use super::core::parse_sftp_error;
use super::lister::SftpLister;
use super::writer::SftpWriter;
use bytes::BytesMut;
use fastpool::bounded;
use opendal_core::raw::oio::ReadStream as _;
use opendal_core::raw::*;
use opendal_core::*;
use openssh_sftp_client::Error as SftpClientError;
use openssh_sftp_client::error::SftpErrorKind;
use openssh_sftp_client::file::File;
use std::error::Error as StdError;
use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::AsyncSeekExt;

const SFTP_READ_CHUNK_SIZE: usize = 2 * 1024 * 1024;

pub struct SftpReadStream {
    /// Keep the connection alive while data stream is alive.
    _conn: bounded::Object<Manager>,

    file: File,
    size: Option<usize>,
    read: usize,
    buf: BytesMut,
}

impl SftpReadStream {
    pub fn new(conn: bounded::Object<Manager>, file: File, size: Option<u64>) -> Self {
        Self {
            _conn: conn,
            file,
            size: size.map(|v| v as usize),
            read: 0,
            buf: BytesMut::new(),
        }
    }
}

impl oio::ReadStream for SftpReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        if self.read >= self.size.unwrap_or(usize::MAX) {
            return Ok(Buffer::new());
        }

        let size = if let Some(size) = self.size {
            (size - self.read).min(SFTP_READ_CHUNK_SIZE)
        } else {
            SFTP_READ_CHUNK_SIZE
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
///
/// On the first read, the reader opens a file handle and tries a positioned
/// read (seek + read). If the server supports it, the handle is cached for the
/// reader's lifetime and all subsequent reads go through the positioned path.
/// If the server does not support positioned reads, the result is cached
/// service-wide and the reader falls back to streaming.
pub struct SftpReader {
    backend: SftpBackend,
    path: String,
    /// Lazily resolved read strategy for this reader.
    ///
    /// This intentionally uses std synchronization instead of an async once
    /// cell. A race can duplicate the first open/probe, but that does not
    /// affect correctness, and we prefer std tools when they are sufficient.
    positioned: Arc<Mutex<Option<PositionedReadStrategy>>>,
}

#[derive(Clone)]
enum PositionedReadStrategy {
    Supported(Arc<SftpReaderHandle>),
    Unsupported,
}

impl PositionedReadStrategy {
    fn handle(&self) -> Option<Arc<SftpReaderHandle>> {
        match self {
            Self::Supported(handle) => Some(handle.clone()),
            Self::Unsupported => None,
        }
    }
}

impl SftpReader {
    pub(super) fn new(backend: SftpBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
            positioned: Arc::new(Mutex::new(None)),
        }
    }

    async fn open_stream(
        &self,
        range: BytesRange,
    ) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (client, mut file) = self.open_file().await?;

        if range.offset() != 0 {
            file.seek(SeekFrom::Start(range.offset()))
                .await
                .map_err(new_std_io_error)?;
        }

        let stream = SftpReadStream::new(client, file, range.size());
        Ok((
            RpRead::default(),
            Box::new(stream) as Box<dyn oio::ReadStreamDyn>,
        ))
    }

    async fn open_file(&self) -> Result<(bounded::Object<Manager>, File)> {
        let backend = &self.backend;
        let path = self.path.as_str();

        let client = backend.core.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&backend.core.root);

        let path = fs.canonicalize(path).await.map_err(parse_sftp_error)?;

        let file = client
            .open(path.as_path())
            .await
            .map_err(parse_sftp_error)?;

        Ok((client, file))
    }

    /// Return a cached positioned-read handle for this reader, or `None`
    /// if the server does not support positioned reads.
    ///
    /// On the very first call across the entire service, this opens a file
    /// and attempts a positioned read to detect support. The file handle is
    /// reused for subsequent reads when supported. The detection result is
    /// cached in `SftpCore::positioned_read_support` so all future readers
    /// skip the probe entirely.
    async fn positioned_handle(&self) -> Result<Option<Arc<SftpReaderHandle>>> {
        // Fast path: a previous reader already determined positioned reads
        // are not supported, so no file needs to be opened.
        if self
            .backend
            .core
            .positioned_read_support
            .get()
            .is_some_and(|&supported| !supported)
        {
            return Ok(None);
        }

        let cached = { self.positioned.lock().expect("mutex poisoned").clone() };
        if let Some(strategy) = cached {
            return Ok(strategy.handle());
        }

        let (client, file) = self.open_file().await?;
        let handle = Arc::new(SftpReaderHandle::new(client, file));

        // If a previous reader already resolved support, respect that.
        let strategy = if let Some(&supported) = self.backend.core.positioned_read_support.get() {
            if supported {
                PositionedReadStrategy::Supported(handle)
            } else {
                PositionedReadStrategy::Unsupported
            }
        } else {
            // First reader across the service: probe by trying a real
            // positioned read on the file we already opened.
            match probe_positioned_read(&handle).await {
                Ok(()) => {
                    // Probe succeeded: positioned reads work. Cache the
                    // result service-wide so other readers skip the probe.
                    let _ = self.backend.core.positioned_read_support.set(true);
                    PositionedReadStrategy::Supported(handle)
                }
                Err(err) if is_positioned_read_unsupported(&err) => {
                    // Positioned reads are not supported. Cache the result
                    // service-wide so other readers skip the probe.
                    let _ = self.backend.core.positioned_read_support.set(false);
                    PositionedReadStrategy::Unsupported
                }
                Err(err) => return Err(err),
            }
        };

        *self.positioned.lock().expect("mutex poisoned") = Some(strategy.clone());
        Ok(strategy.handle())
    }
}

pub struct SftpReaderHandle {
    /// Keep the connection alive while the file handle is cached by `SftpReader`.
    _conn: bounded::Object<Manager>,
    file: File,
}

impl SftpReaderHandle {
    pub(super) fn new(conn: bounded::Object<Manager>, file: File) -> Self {
        Self { _conn: conn, file }
    }
}

async fn read_positioned(handle: &SftpReaderHandle, offset: u64, size: usize) -> Result<Buffer> {
    if size == 0 {
        return Ok(Buffer::new());
    }

    let mut file = handle.file.clone();
    file.seek(SeekFrom::Start(offset))
        .await
        .map_err(new_std_io_error)?;

    match file
        .read(size as u32, BytesMut::with_capacity(size))
        .await
        .map_err(parse_sftp_error)?
    {
        Some(bytes) => Ok(Buffer::from(bytes.freeze())),
        None => Ok(Buffer::new()),
    }
}

/// Probe whether positioned reads work by seeking to the end of the file and
/// attempting a small read. A server that supports positioned reads will
/// succeed (returning empty at EOF); a server that does not support seek
/// will return an error we classify as "unsupported".
async fn probe_positioned_read(handle: &SftpReaderHandle) -> Result<()> {
    let mut file = handle.file.clone();
    let len = file.metadata().await.map_err(parse_sftp_error)?.len();

    let probe_offset = len.unwrap_or(0);
    file.seek(SeekFrom::Start(probe_offset))
        .await
        .map_err(new_std_io_error)?;

    // A small read to confirm the data path works after seeking.
    let _ = file
        .read(1u32, BytesMut::with_capacity(1))
        .await
        .map_err(parse_sftp_error)?;

    Ok(())
}

fn is_positioned_read_unsupported(err: &Error) -> bool {
    if err.kind() == ErrorKind::Unsupported {
        return true;
    }

    err.source()
        .and_then(|source| source.downcast_ref::<SftpClientError>())
        .is_some_and(|source| {
            matches!(
                source,
                SftpClientError::UnsupportedSftpProtocol { .. }
                    | SftpClientError::SftpError(SftpErrorKind::OpUnsupported, _)
            )
        })
}

impl oio::Read for SftpReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        if let Some(handle) = self.positioned_handle().await? {
            let stream = SftpPositionedReadStream {
                handle,
                offset: range.offset(),
                remaining: range.size(),
            };
            Ok((
                RpRead::default(),
                Box::new(stream) as Box<dyn oio::ReadStreamDyn>,
            ))
        } else {
            self.open_stream(range).await
        }
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        if let Some(handle) = self.positioned_handle().await? {
            let size = range.size().ok_or_else(|| {
                Error::new(ErrorKind::Unsupported, "read requires a bounded range")
            })?;

            let mut offset = range.offset();
            let mut remaining = size;
            let mut bufs = Vec::new();

            while remaining > 0 {
                let read_size = remaining.min(SFTP_READ_CHUNK_SIZE as u64) as usize;
                let buf = read_positioned(&handle, offset, read_size).await?;
                if buf.len() > read_size {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "reader got unexpected data size",
                    )
                    .with_context("expect", read_size)
                    .with_context("actual", buf.len()));
                }
                if buf.is_empty() {
                    return Err(Error::new(
                        ErrorKind::RangeNotSatisfied,
                        "range exceeds content length",
                    )
                    .with_context("offset", offset)
                    .with_context("remaining", remaining));
                }

                let n = buf.len() as u64;
                offset += n;
                remaining -= n;
                bufs.push(buf);
            }

            Ok((RpRead::default(), bufs.into_iter().flatten().collect()))
        } else {
            let expected = range.size().ok_or_else(|| {
                Error::new(ErrorKind::Unsupported, "read requires a bounded range")
            })?;
            let (rp, mut stream) = self.open_stream(range).await?;
            let buffer = stream.read_all().await?;
            if buffer.len() as u64 != expected {
                return Err(
                    Error::new(ErrorKind::Unexpected, "reader got unexpected data size")
                        .with_context("expect", expected)
                        .with_context("actual", buffer.len() as u64),
                );
            }
            Ok((rp, buffer))
        }
    }
}

struct SftpPositionedReadStream {
    handle: Arc<SftpReaderHandle>,
    offset: u64,
    remaining: Option<u64>,
}

impl oio::ReadStream for SftpPositionedReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        if self.remaining == Some(0) {
            return Ok(Buffer::new());
        }

        let read_size = self
            .remaining
            .map(|remaining| remaining.min(SFTP_READ_CHUNK_SIZE as u64) as usize)
            .unwrap_or(SFTP_READ_CHUNK_SIZE);

        let buf = read_positioned(&self.handle, self.offset, read_size).await?;
        if buf.len() > read_size {
            return Err(
                Error::new(ErrorKind::Unexpected, "reader got unexpected data size")
                    .with_context("expect", read_size)
                    .with_context("actual", buf.len()),
            );
        }
        if buf.is_empty() {
            if let Some(remaining) = self.remaining {
                return Err(Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "range exceeds content length",
                )
                .with_context("offset", self.offset)
                .with_context("remaining", remaining));
            }
            return Ok(Buffer::new());
        }

        let n = buf.len() as u64;
        self.offset += n;
        if let Some(remaining) = &mut self.remaining {
            *remaining -= n;
        }

        Ok(buf)
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

            let client = self.backend.core.connect().await?;

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

            let res = option.open(&path).await;
            let file = match res {
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
            };

            self.inner = Some(SftpWriter::new(file));
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
            let client = self.backend.core.connect().await?;
            let mut fs = client.fs();
            fs.set_cwd(&self.backend.core.root);

            let file_path = format!("./{}", self.path);

            self.inner = Some(match fs.open_dir(&file_path).await {
                Ok(dir) => Some(SftpLister::new(dir.read_dir(), self.path.clone())),
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
