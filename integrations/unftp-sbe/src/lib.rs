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

//! `unftp-sbe-opendal` is an [unftp](https://crates.io/crates/unftp) `StorageBackend` implementation using opendal.
//!
//! This crate can help you to access ANY storage services with the same ftp API.
//!
//! # Example
//!
//! This example demonstrates how to use `unftp-sbe-opendal` with the `S3` service.
//!
//! ```no_run
//! use anyhow::Result;
//! use opendal::Operator;
//! use opendal::services;
//! use unftp_sbe_opendal::OpendalStorage;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create any service desired
//!     let op = opendal::Operator::from_iter::<services::S3>(
//!         [
//!             ("bucket".to_string(), "my_bucket".to_string()),
//!             ("access_key".to_string(), "my_access_key".to_string()),
//!             ("secret_key".to_string(), "my_secret_key".to_string()),
//!             ("endpoint".to_string(), "my_endpoint".to_string()),
//!             ("region".to_string(), "my_region".to_string()),
//!         ],
//!     )?;
//!
//!     // Wrap the operator with `OpendalStorage`
//!     let backend = OpendalStorage::new(op);
//!
//!     // Build the actual unftp server
//!     let server = libunftp::ServerBuilder::new(Box::new(move || backend.clone())).build()?;
//!
//!     // Start the server
//!     server.listen("0.0.0.0:0").await?;
//!
//!     Ok(())
//! }
//! ```

use std::fmt::Debug;
use std::path::{Path, PathBuf};

use opendal::Operator;
use unftp_core::auth::UserDetail;
use unftp_core::storage::{self, Error, StorageBackend};

use futures::StreamExt;
use tokio::io::AsyncReadExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;

#[derive(Debug, Clone)]
pub struct OpendalStorage {
    op: Operator,
}

impl OpendalStorage {
    pub fn new(op: Operator) -> Self {
        Self { op }
    }
}

/// A wrapper around [`opendal::Metadata`] to implement [`storage::Metadata`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OpendalMetadata(opendal::Metadata);

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
        match self.0.last_modified() {
            Some(ts) => Ok(ts.into()),
            None => Err(Error::new(
                storage::ErrorKind::LocalError,
                "no last modified time",
            )),
        }
    }

    fn gid(&self) -> u32 {
        0
    }

    fn uid(&self) -> u32 {
        0
    }
}

fn convert_err(err: opendal::Error) -> Error {
    let kind = match err.kind() {
        opendal::ErrorKind::NotFound => storage::ErrorKind::PermanentFileNotAvailable,
        opendal::ErrorKind::AlreadyExists => storage::ErrorKind::PermanentFileNotAvailable,
        opendal::ErrorKind::PermissionDenied => storage::ErrorKind::PermissionDenied,
        _ => storage::ErrorKind::LocalError,
    };
    Error::new(kind, err)
}

fn convert_path(path: &Path) -> storage::Result<&str> {
    path.to_str().ok_or_else(|| {
        Error::new(
            storage::ErrorKind::LocalError,
            "Path is not a valid UTF-8 string",
        )
    })
}

async fn copy_read_write_loop<R>(
    input: &mut R,
    output: &mut opendal::Writer,
) -> storage::Result<u64>
where
    R: tokio::io::AsyncRead + Unpin + ?Sized,
{
    const COPY_BUFFER_SIZE: usize = 256 * 1024;

    let mut copied = 0u64;
    let mut buf = vec![0u8; COPY_BUFFER_SIZE];
    let mut filled = 0;

    loop {
        let n = input.read(&mut buf[filled..]).await.map_err(|err| {
            Error::new(
                storage::ErrorKind::LocalError,
                format!("Failed to read data: {err}"),
            )
        })?;
        if n == 0 {
            if filled > 0 {
                output.write(buf[..filled].to_vec()).await.map_err(|err| {
                    Error::new(
                        storage::ErrorKind::LocalError,
                        format!("Failed to write data: {err}"),
                    )
                })?;
                copied += filled as u64;
            }
            return Ok(copied);
        }

        filled += n;
        if filled == buf.len() {
            output.write(buf[..filled].to_vec()).await.map_err(|err| {
                Error::new(
                    storage::ErrorKind::LocalError,
                    format!("Failed to write data: {err}"),
                )
            })?;
            copied += filled as u64;
            filled = 0;
        }
    }
}

#[async_trait::async_trait]
impl<User: UserDetail> StorageBackend<User> for OpendalStorage {
    type Metadata = OpendalMetadata;

    async fn metadata<P: AsRef<Path> + Send + Debug>(
        &self,
        _: &User,
        path: P,
    ) -> storage::Result<Self::Metadata> {
        use opendal::ErrorKind::*;

        let path = opendal::raw::normalize_path(convert_path(path.as_ref())?);
        let directory = format!("{}/", path.trim_end_matches('/'));

        // The FTP root always exists; some backends cannot stat it.
        if directory == "/" {
            return Ok(OpendalMetadata(opendal::MetadataBuilder::dir().build()));
        }

        // Try the path as a file first. Object stores only address directories
        // with a trailing `/`, so fall back to the directory form below.
        let mut stat_error = match self.op.stat(&path).await {
            Ok(metadata) => return Ok(OpendalMetadata(metadata)),
            Err(e) if matches!(e.kind(), NotFound | NotADirectory | IsADirectory) => e,
            Err(e) => return Err(convert_err(e)),
        };

        if !path.ends_with('/') {
            match self.op.stat(&directory).await {
                Ok(metadata) if metadata.is_dir() => return Ok(OpendalMetadata(metadata)),
                Ok(_) => {}
                Err(e) if matches!(e.kind(), NotFound | NotADirectory | IsADirectory) => {
                    stat_error = e;
                }
                Err(e) => return Err(convert_err(e)),
            }
        }

        // Object stores can serve implicit prefix directories that have no
        // directory entry of their own; probing needs the list capability.
        if !self.op.info().capability().list {
            return Err(convert_err(stat_error));
        }
        match self.op.lister(&directory).await {
            Ok(mut entries) => match entries.next().await {
                Some(Ok(_)) => Ok(OpendalMetadata(opendal::MetadataBuilder::dir().build())),
                Some(Err(e)) if matches!(e.kind(), NotFound | NotADirectory | Unsupported) => {
                    Err(convert_err(stat_error))
                }
                Some(Err(e)) => Err(convert_err(e)),
                None => Err(convert_err(stat_error)),
            },
            Err(e) if matches!(e.kind(), NotFound | NotADirectory | Unsupported) => {
                Err(convert_err(stat_error))
            }
            Err(e) => Err(convert_err(e)),
        }
    }

    async fn list<P: AsRef<Path> + Send + Debug>(
        &self,
        _: &User,
        path: P,
    ) -> storage::Result<Vec<storage::Fileinfo<PathBuf, Self::Metadata>>>
    where
        Self::Metadata: storage::Metadata,
    {
        let path = opendal::raw::normalize_path(convert_path(path.as_ref())?);
        let directory = format!("{}/", path.trim_end_matches('/'));
        let mut entries = self.op.list(&directory).await.map_err(convert_err)?;
        if entries.is_empty() && path != directory {
            // A listing can also name a single file.
            entries = self.op.list(&path).await.map_err(convert_err)?;
            entries.retain(|entry| entry.path() == path);
        } else {
            // OpenDAL can include the directory entry itself in a prefix listing.
            entries.retain(|entry| entry.path() != directory);
        }
        let ret = entries
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
        let reader = self
            .op
            .reader(convert_path(path.as_ref())?)
            .await
            .map_err(convert_err)?
            .into_futures_async_read(start_pos..)
            .await
            .map_err(convert_err)?
            .compat();
        Ok(Box::new(reader))
    }

    async fn put<
        P: AsRef<Path> + Send + Debug,
        R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static,
    >(
        &self,
        _: &User,
        mut input: R,
        path: P,
        start_pos: u64,
    ) -> storage::Result<u64> {
        let path = convert_path(path.as_ref())?;

        // libunftp sends nonzero `start_pos` for APPE, where it equals the
        // current file size. Silently overwriting there would corrupt data,
        // so writes at an offset require an append-capable backend and must
        // land exactly at the end of the existing file.
        let append = start_pos > 0;
        if append {
            if !self.op.info().capability().write_can_append {
                return Err(Error::new(
                    storage::ErrorKind::CommandNotImplemented,
                    "the storage service does not support appending to an existing file",
                ));
            }
            match self.op.stat(path).await {
                Ok(meta) if meta.content_length() == start_pos => {}
                Ok(_) => {
                    return Err(Error::new(
                        storage::ErrorKind::PermanentFileNotAvailable,
                        format!("start position {start_pos} does not match the size of {path}"),
                    ));
                }
                Err(err) => return Err(convert_err(err)),
            }
        }

        let writer = if append {
            self.op.writer_with(path).append(true).await
        } else {
            self.op.writer(path).await
        }
        .map_err(convert_err)?;

        let mut writer = writer;
        match copy_read_write_loop(&mut input, &mut writer).await {
            Ok(len) => match writer.close().await {
                Ok(_) => Ok(len),
                Err(close_err) => {
                    // A failed close can leave staged or multipart data behind.
                    // Abort it, but return the original close error.
                    let _ = writer.abort().await;
                    Err(convert_err(close_err))
                }
            },
            Err(copy_err) => {
                // The writer is still open, so abort it to discard buffered
                // and uploaded data. Some services do not support abort;
                // report that failure without hiding the original error.
                match writer.abort().await {
                    Ok(()) => Err(copy_err),
                    Err(abort_err) => Err(Error::new(
                        copy_err.kind(),
                        format!("{copy_err} AND failed to abort writer: {abort_err}"),
                    )),
                }
            }
        }
    }

    async fn del<P: AsRef<Path> + Send + Debug>(&self, _: &User, path: P) -> storage::Result<()> {
        self.op
            .delete(convert_path(path.as_ref())?)
            .await
            .map_err(convert_err)
    }

    async fn mkd<P: AsRef<Path> + Send + Debug>(&self, _: &User, path: P) -> storage::Result<()> {
        let mut path_str = convert_path(path.as_ref())?.to_string();
        if !path_str.ends_with('/') {
            path_str.push('/');
        }
        self.op.create_dir(&path_str).await.map_err(convert_err)
    }

    async fn rename<P: AsRef<Path> + Send + Debug>(
        &self,
        _: &User,
        from: P,
        to: P,
    ) -> storage::Result<()> {
        let (from, to) = (convert_path(from.as_ref())?, convert_path(to.as_ref())?);
        self.op.rename(from, to).await.map_err(convert_err)
    }

    async fn rmd<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &User,
        path: P,
    ) -> storage::Result<()> {
        let path = opendal::raw::normalize_path(convert_path(path.as_ref())?);
        if path == "/" {
            return Err(Error::new(
                storage::ErrorKind::PermissionDenied,
                "cannot remove the FTP root directory",
            ));
        }

        // Recursive deletion without a trailing slash is a prefix match on
        // services such as memory and can remove siblings like `dd/`.
        let directory = format!("{}/", path.trim_end_matches('/'));
        self.cwd(user, directory.as_str()).await?;
        self.op
            .delete_with(&directory)
            .recursive(true)
            .await
            .map_err(convert_err)
    }

    async fn cwd<P: AsRef<Path> + Send + Debug>(&self, _: &User, path: P) -> storage::Result<()> {
        use opendal::ErrorKind::*;

        // Object stores only address directories with a trailing `/`.
        // Normalizing to root yields "/", which `trim_end_matches('/')`
        // preserves.
        let path = opendal::raw::normalize_path(convert_path(path.as_ref())?);
        let directory = format!("{}/", path.trim_end_matches('/'));

        // The FTP root always exists; some backends cannot stat it.
        if directory == "/" {
            return Ok(());
        }

        match self.op.stat(&directory).await {
            Ok(meta) if meta.is_dir() => Ok(()),
            Ok(_) => Err(Error::new(
                storage::ErrorKind::PermanentDirectoryNotAvailable,
                format!("{directory} is not a directory"),
            )),
            Err(e) if matches!(e.kind(), NotFound | NotADirectory | Unsupported) => {
                // Object stores can serve implicit prefix directories that
                // have no directory entry of their own; probing needs the
                // list capability. The probe only reads the first entry.
                if !self.op.info().capability().list {
                    return Err(Error::new(
                        storage::ErrorKind::PermanentDirectoryNotAvailable,
                        e,
                    ));
                }
                match self.op.lister(&directory).await {
                    Ok(mut entries) => match entries.next().await {
                        Some(Ok(_)) => Ok(()),
                        Some(Err(e))
                            if matches!(e.kind(), NotFound | NotADirectory | Unsupported) =>
                        {
                            Err(Error::new(
                                storage::ErrorKind::PermanentDirectoryNotAvailable,
                                e,
                            ))
                        }
                        Some(Err(e)) => Err(convert_err(e)),
                        None => Err(Error::new(
                            storage::ErrorKind::PermanentDirectoryNotAvailable,
                            e,
                        )),
                    },
                    Err(e) if matches!(e.kind(), NotFound | NotADirectory | Unsupported) => Err(
                        Error::new(storage::ErrorKind::PermanentDirectoryNotAvailable, e),
                    ),
                    Err(e) => Err(convert_err(e)),
                }
            }
            Err(e) => Err(convert_err(e)),
        }
    }
}
