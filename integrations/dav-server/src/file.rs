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

use std::fmt::{Debug, Formatter};
use std::io::SeekFrom;

use bytes::{Buf, Bytes, BytesMut};
use dav_server::fs::{DavFile, OpenOptions};
use dav_server::fs::{DavMetaData, FsResult};
use dav_server::fs::{FsError, FsFuture};
use futures::FutureExt;
use futures::{AsyncReadExt, AsyncSeekExt, StreamExt};
use opendal_core::{ErrorKind, FuturesAsyncReader, Operator, Writer};

use super::metadata::OpendalMetaData;
use super::utils::*;

/// OpendalFile is a `DavFile` implementation for opendal.
pub struct OpendalFile {
    op: Operator,
    path: String,
    state: State,
    buf: BytesMut,
}

impl Debug for OpendalFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpendalFile")
            .field("path", &self.path)
            .field(
                "state",
                match &self.state {
                    State::Read(_) => &"read",
                    State::Writing(_) => &"writing",
                    State::Appending { .. } => &"appending",
                    State::Closed => &"closed",
                    State::Failed => &"failed",
                },
            )
            .finish()
    }
}

enum State {
    Read(Box<FuturesAsyncReader>),
    Writing(Writer),
    Appending {
        writer: Writer,
        offset: u64,
        written: u64,
    },
    Closed,
    Failed,
}

impl OpendalFile {
    /// Return whether `dir_path` (ending with `/`) exists as an explicit
    /// directory marker or an implicit prefix with children.
    async fn dir_exists(op: &Operator, dir_path: &str) -> FsResult<bool> {
        let capability = op.info().capability();
        if capability.stat {
            match op.stat(dir_path).await {
                Ok(meta) => {
                    if meta.is_dir() {
                        return Ok(true);
                    }
                }
                Err(err) if err.kind() == ErrorKind::NotADirectory => return Ok(false),
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(convert_error(err)),
            }
        }

        if capability.list {
            let mut lister = match op.lister_with(dir_path).limit(1).await {
                Ok(lister) => lister,
                Err(err)
                    if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) =>
                {
                    return Ok(false);
                }
                Err(err) => return Err(convert_error(err)),
            };
            match lister.next().await {
                Some(Ok(_)) => return Ok(true),
                Some(Err(err))
                    if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) =>
                {
                    return Ok(false);
                }
                Some(Err(err)) => return Err(convert_error(err)),
                None => return Ok(false),
            }
        }

        Ok(false)
    }

    /// Create a new opendal file.
    pub async fn open(op: Operator, path: String, options: OpenOptions) -> FsResult<Self> {
        if path.is_empty() || path.ends_with('/') {
            return Err(FsError::Forbidden);
        }

        let state = if options.read {
            if op.info().capability().stat {
                match op.stat(&path).await {
                    Ok(meta) if meta.is_dir() => return Err(FsError::Forbidden),
                    Ok(_) => {}
                    Err(err) if err.kind() == ErrorKind::IsADirectory => {
                        return Err(FsError::Forbidden);
                    }
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        let dir_path = format!("{path}/");
                        if Self::dir_exists(&op, &dir_path).await? {
                            return Err(FsError::Forbidden);
                        }
                        return Err(FsError::NotFound);
                    }
                    Err(err) => return Err(convert_error(err)),
                }
            } else {
                let dir_path = format!("{path}/");
                if Self::dir_exists(&op, &dir_path).await? {
                    return Err(FsError::Forbidden);
                }
            }

            let r = op
                .reader(&path)
                .await
                .map_err(convert_error)?
                .into_futures_async_read(..)
                .await
                .map_err(convert_error)?;
            State::Read(Box::new(r))
        } else if options.write {
            // Reject opening a path as a file when a collection already exists
            // at `{path}/`. Object stores and memory treat `dir` and `dir/` as
            // separate keys, so writing `dir` would otherwise shadow the
            // collection in `metadata` and `PROPFIND`.
            let dir_path = format!("{path}/");
            if Self::dir_exists(&op, &dir_path).await? {
                return Err(if options.create_new {
                    FsError::Exists
                } else {
                    FsError::Forbidden
                });
            }

            let mut w = op
                .writer_with(&path)
                .append(options.append)
                .if_not_exists(options.create_new)
                .await
                .map_err(convert_error)?;
            if options.append {
                let offset = if op.info().capability().stat {
                    match op.stat(&path).await {
                        Ok(meta) if meta.is_dir() => {
                            let _ = w.abort().await;
                            return Err(FsError::Forbidden);
                        }
                        Ok(meta) => meta.content_length(),
                        Err(err) if err.kind() == ErrorKind::NotFound => 0,
                        Err(err) => {
                            let _ = w.abort().await;
                            return Err(convert_error(err));
                        }
                    }
                } else {
                    0
                };
                State::Appending {
                    writer: w,
                    offset,
                    written: 0,
                }
            } else {
                State::Writing(w)
            }
        } else {
            return Err(FsError::NotImplemented);
        };

        Ok(Self {
            op,
            path,
            state,
            buf: BytesMut::new(),
        })
    }

    async fn write(&mut self, buf: impl Buf) -> FsResult<()> {
        let len = buf.remaining() as u64;
        let (writer, next_append) = match &mut self.state {
            State::Writing(writer) => (writer, None),
            State::Appending {
                writer,
                offset,
                written,
            } => {
                let next_offset = offset.checked_add(len).ok_or(FsError::GeneralFailure)?;
                let next_written = written.checked_add(len).ok_or(FsError::GeneralFailure)?;
                (writer, Some((next_offset, next_written)))
            }
            _ => return Err(FsError::GeneralFailure),
        };

        if let Err(err) = writer.write_from(buf).await {
            let _ = writer.abort().await;
            self.state = State::Failed;
            return Err(convert_error(err));
        }

        if let (
            State::Appending {
                offset, written, ..
            },
            Some((next_offset, next_written)),
        ) = (&mut self.state, next_append)
        {
            *offset = next_offset;
            *written = next_written;
        }

        Ok(())
    }
}

impl DavFile for OpendalFile {
    fn metadata(&mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        async move {
            self.op
                .stat(&self.path)
                .await
                .map(|opendal_metadata| {
                    Box::new(OpendalMetaData::new(opendal_metadata)) as Box<dyn DavMetaData>
                })
                .map_err(convert_error)
        }
        .boxed()
    }

    fn write_buf(&mut self, buf: Box<dyn Buf + Send>) -> FsFuture<'_, ()> {
        self.write(buf).boxed()
    }

    fn write_bytes(&mut self, buf: Bytes) -> FsFuture<'_, ()> {
        self.write(buf).boxed()
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<'_, Bytes> {
        async move {
            let State::Read(r) = &mut self.state else {
                return Err(FsError::GeneralFailure);
            };

            self.buf.resize(count, 0);
            let len = r
                .read(&mut self.buf)
                .await
                .map_err(|_| FsError::GeneralFailure)?;
            Ok(self.buf.split_to(len).freeze())
        }
        .boxed()
    }

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<'_, u64> {
        async move {
            match &mut self.state {
                State::Read(r) => r.seek(pos).await.map_err(|_| FsError::GeneralFailure),
                State::Appending {
                    offset, written, ..
                } => match pos {
                    // dav-server calls seek(Start(0)) before writing a SabreDAV append PATCH;
                    // allow it before any bytes are written while returning the append offset.
                    SeekFrom::Start(0) if *written == 0 => Ok(*offset),
                    SeekFrom::Start(target) if target == *offset => Ok(*offset),
                    SeekFrom::Current(0) | SeekFrom::End(0) => Ok(*offset),
                    _ => Err(FsError::NotImplemented),
                },
                State::Writing(_) => Err(FsError::NotImplemented),
                State::Closed | State::Failed => Err(FsError::GeneralFailure),
            }
        }
        .boxed()
    }

    fn flush(&mut self) -> FsFuture<'_, ()> {
        async move {
            let writer = match &mut self.state {
                State::Writing(writer) | State::Appending { writer, .. } => writer,
                _ => return Err(FsError::GeneralFailure),
            };

            if let Err(err) = writer.close().await {
                let _ = writer.abort().await;
                self.state = State::Failed;
                return Err(convert_error(err));
            }

            self.state = State::Closed;
            Ok(())
        }
        .boxed()
    }
}
