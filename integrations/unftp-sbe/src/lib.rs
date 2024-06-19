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

use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::task::Poll;

use bytes::{Buf, BufMut};
use libunftp::auth::UserDetail;
use libunftp::storage::{self, StorageBackend};
use opendal::{raw::oio::PooledBuf, Buffer, Operator};
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug)]
pub struct OpendalStorage {
    op: Operator,
    pool: PooledBuf,
}

impl OpendalStorage {
    pub fn new(op: Operator) -> Self {
        Self {
            op,
            pool: PooledBuf::new(16),
        }
    }
}

/// A wrapper around [`Buffer`] to implement [`tokio::io::AsyncRead`].
pub struct IoBuffer(Buffer);

impl AsyncRead for IoBuffer {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let len = std::io::copy(&mut self.as_mut().0.by_ref().reader(), &mut buf.writer())?;
        self.0.advance(len as usize);
        Poll::Ready(Ok(()))
    }
}

/// A wrapper around [`opendal::Metadata`] to implement [`libunftp::storage::Metadata`].
pub struct OpendalMetadata(pub opendal::Metadata);

impl storage::Metadata for OpendalMetadata {
    fn len(&self) -> u64 {
        self.0.content_length()
    }

    fn is_dir(&self) -> bool {
        self.0.is_dir()
    }

    fn is_file(&self) -> bool {
        self.0.is_file()
    }

    fn is_symlink(&self) -> bool {
        false
    }

    fn modified(&self) -> storage::Result<std::time::SystemTime> {
        self.0.last_modified().map(Into::into).ok_or_else(|| {
            storage::Error::new(storage::ErrorKind::LocalError, "no last modified time")
        })
    }

    fn gid(&self) -> u32 {
        0
    }

    fn uid(&self) -> u32 {
        0
    }
}

fn cvt_err(err: opendal::Error) -> storage::Error {
    let kind = match err.kind() {
        opendal::ErrorKind::NotFound => storage::ErrorKind::PermanentFileNotAvailable,
        opendal::ErrorKind::AlreadyExists => storage::ErrorKind::PermanentFileNotAvailable,
        opendal::ErrorKind::PermissionDenied => storage::ErrorKind::PermissionDenied,
        _ => storage::ErrorKind::LocalError,
    };
    storage::Error::new(kind, err)
}

fn cvt_path(path: &Path) -> storage::Result<&str> {
    path.to_str().ok_or_else(|| {
        storage::Error::new(
            storage::ErrorKind::LocalError,
            "Path is not a valid UTF-8 string",
        )
    })
}

#[async_trait::async_trait]
impl<User: UserDetail> StorageBackend<User> for OpendalStorage {
    type Metadata = OpendalMetadata;

    async fn metadata<P: AsRef<Path> + Send + Debug>(
        &self,
        _: &User,
        path: P,
    ) -> storage::Result<Self::Metadata> {
        let metadata = self
            .op
            .stat(cvt_path(path.as_ref())?)
            .await
            .map_err(cvt_err)?;
        Ok(OpendalMetadata(metadata))
    }

    async fn list<P: AsRef<Path> + Send + Debug>(
        &self,
        _: &User,
        path: P,
    ) -> storage::Result<Vec<storage::Fileinfo<PathBuf, Self::Metadata>>>
    where
        Self::Metadata: storage::Metadata,
    {
        let ret = self
            .op
            .list(cvt_path(path.as_ref())?)
            .await
            .map_err(cvt_err)?
            .into_iter()
            .map(|x| {
                let (path, metadata) = x.into_parts();
                storage::Fileinfo {
                    path: path.into(),
                    metadata: OpendalMetadata(metadata),
                }
            })
            .collect();
        Ok(ret)
    }

    async fn get<P: AsRef<Path> + Send + Debug>(
        &self,
        _: &User,
        path: P,
        start_pos: u64,
    ) -> storage::Result<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>> {
        let buf = self
            .op
            .read_with(cvt_path(path.as_ref())?)
            .range(start_pos..)
            .await
            .map_err(cvt_err)?;
        Ok(Box::new(IoBuffer(buf)))
    }

    async fn put<
        P: AsRef<Path> + Send + Debug,
        R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static,
    >(
        &self,
        _: &User,
        mut input: R,
        path: P,
        _: u64,
    ) -> storage::Result<u64> {
        let mut buf = self.pool.get();
        input.read_buf(&mut buf).await?;
        let bytes = buf.split().freeze();
        let len = bytes.len() as u64;
        self.pool.put(buf);
        self.op
            .write_with(cvt_path(path.as_ref())?, bytes)
            .await
            .map_err(cvt_err)?;
        Ok(len)
    }

    async fn del<P: AsRef<Path> + Send + Debug>(&self, _: &User, path: P) -> storage::Result<()> {
        self.op
            .delete(cvt_path(path.as_ref())?)
            .await
            .map_err(cvt_err)
    }

    async fn mkd<P: AsRef<Path> + Send + Debug>(&self, _: &User, path: P) -> storage::Result<()> {
        self.op
            .create_dir(cvt_path(path.as_ref())?)
            .await
            .map_err(cvt_err)
    }

    async fn rename<P: AsRef<Path> + Send + Debug>(
        &self,
        _: &User,
        from: P,
        to: P,
    ) -> storage::Result<()> {
        let (from, to) = (cvt_path(from.as_ref())?, cvt_path(to.as_ref())?);
        self.op.rename(from, to).await.map_err(cvt_err)
    }

    async fn rmd<P: AsRef<Path> + Send + Debug>(&self, _: &User, path: P) -> storage::Result<()> {
        self.op
            .remove_all(cvt_path(path.as_ref())?)
            .await
            .map_err(cvt_err)
    }

    async fn cwd<P: AsRef<Path> + Send + Debug>(&self, _: &User, path: P) -> storage::Result<()> {
        use opendal::ErrorKind::*;

        match self.op.stat(cvt_path(path.as_ref())?).await {
            Ok(_) => Ok(()),
            Err(e) if matches!(e.kind(), NotFound | NotADirectory) => Err(storage::Error::new(
                storage::ErrorKind::PermanentDirectoryNotAvailable,
                e,
            )),
            Err(e) => Err(cvt_err(e)),
        }
    }
}
