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

use dav_server::davpath::DavPath;
use dav_server::fs::DavMetaData;
use dav_server::fs::FsError;
use dav_server::fs::{DavDirEntry, FsFuture};
use dav_server::fs::{DavFile, FsStream};
use dav_server::fs::{DavFileSystem, ReadDirMeta};
use futures::FutureExt;
use futures::StreamExt;
use opendal_core::raw::normalize_path;
use opendal_core::{ErrorKind, MetadataBuilder, Operator};
use std::path::Path;

use super::dir::OpendalStream;
use super::file::OpendalFile;
use super::metadata::OpendalMetaData;
use super::utils::{convert_error, dir_exists, implicit_dir_exists};

/// OpendalFs is a `DavFileSystem` implementation for opendal.
///
/// `OpendalFs` omits weak ETags because dav-server emits only strong ETag headers
/// from filesystem metadata.
///
/// OpenDAL metadata has no symbolic-link flag, so
/// [`hide_symlinks(true)`](dav_server::DavConfig::hide_symlinks) cannot filter
/// symbolic links through this adapter.
///
/// ```
/// use anyhow::Result;
/// use dav_server_opendalfs::dav_server::davpath::DavPath;
/// use dav_server_opendalfs::dav_server::fs::DavFileSystem;
/// use dav_server_opendalfs::OpendalFs;
/// use opendal_core::services::Memory;
/// use opendal_core::Operator;
///
/// #[tokio::test]
/// async fn test() -> Result<()> {
///     let op = Operator::new(Memory::default())?;
///
///     let webdavfs = OpendalFs::new(op);
///
///     let metadata = webdavfs
///         .metadata(&DavPath::new("/").unwrap())
///         .await
///         .unwrap();
///     println!("{}", metadata.is_dir());
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct OpendalFs {
    pub op: Operator,
}

impl OpendalFs {
    /// Create a new `OpendalFs` instance.
    pub fn new(op: Operator) -> Box<OpendalFs> {
        Box::new(OpendalFs { op })
    }

    fn fs_path(&self, path: &DavPath) -> Result<String, FsError> {
        String::from_utf8(path.as_bytes().to_vec())
            .map(|path| normalize_path(path.as_str()))
            .map_err(|_| FsError::GeneralFailure)
    }

    fn fs_dir_path(&self, path: &DavPath) -> Result<String, FsError> {
        Ok(Self::into_dir_path(self.fs_path(path)?))
    }

    fn into_dir_path(mut path: String) -> String {
        if !Self::is_dir_path(&path) {
            path.push('/');
        }
        path
    }

    fn is_dir_path(path: &str) -> bool {
        path.is_empty() || path.ends_with('/')
    }

    /// Return whether `path` names an existing resource.
    ///
    /// A trailing slash may make some services report `NotADirectory` for a
    /// file, and a missing trailing slash may make them report `IsADirectory`
    /// for a directory. Both mean this exact path does not exist, so callers
    /// can check the file and directory forms separately.
    async fn path_exists(&self, path: &str) -> Result<bool, FsError> {
        match self.op.stat(path).await {
            Ok(_) => Ok(true),
            Err(e)
                if matches!(
                    e.kind(),
                    ErrorKind::NotFound | ErrorKind::NotADirectory | ErrorKind::IsADirectory
                ) =>
            {
                Ok(false)
            }
            Err(e) => Err(convert_error(e)),
        }
    }
}

impl DavFileSystem for OpendalFs {
    fn open<'a>(
        &'a self,
        path: &'a DavPath,
        options: dav_server::fs::OpenOptions,
    ) -> FsFuture<'a, Box<dyn DavFile>> {
        async move {
            let path = self.fs_path(path)?;
            let file = OpendalFile::open(self.op.clone(), path, options).await?;
            Ok(Box::new(file) as Box<dyn DavFile>)
        }
        .boxed()
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        async move {
            let path = self.fs_dir_path(path)?;
            self.op
                .lister(path.as_str())
                .await
                .map(|lister| OpendalStream::new(self.op.clone(), lister, path.as_str()).boxed())
                .map_err(convert_error)
        }
        .boxed()
    }

    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        async move {
            let path = self.fs_path(path)?;
            let metadata = match self.op.stat(path.as_str()).await {
                Ok(metadata) => metadata,
                Err(e) if path == "/" && e.kind() == ErrorKind::NotFound => {
                    MetadataBuilder::dir().build()
                }
                Err(e) if Self::is_dir_path(&path) && e.kind() == ErrorKind::NotFound => {
                    if implicit_dir_exists(&self.op, path.as_str()).await? {
                        MetadataBuilder::dir().build()
                    } else {
                        return Err(FsError::NotFound);
                    }
                }
                Err(e)
                    if !Self::is_dir_path(&path)
                        && matches!(e.kind(), ErrorKind::NotFound | ErrorKind::IsADirectory) =>
                {
                    let dir_path = Self::into_dir_path(path);
                    match self.op.stat(dir_path.as_str()).await {
                        Ok(metadata) => metadata,
                        Err(dir_err) if dir_err.kind() == ErrorKind::NotFound => {
                            if implicit_dir_exists(&self.op, dir_path.as_str()).await? {
                                MetadataBuilder::dir().build()
                            } else {
                                return Err(FsError::NotFound);
                            }
                        }
                        Err(dir_err) => return Err(convert_error(dir_err)),
                    }
                }
                Err(e) => return Err(convert_error(e)),
            };

            Ok(Box::new(OpendalMetaData::new(metadata)) as Box<dyn DavMetaData>)
        }
        .boxed()
    }

    fn symlink_metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        self.metadata(path)
    }

    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let dir_path = self.fs_dir_path(path)?;

            // The root collection always exists, so MKCOL on it must report
            // Exists instead of creating another root.
            if dir_path == "/" {
                return Err(FsError::Exists);
            }

            let file_path = dir_path.trim_end_matches('/');

            // Check that the parent collection exists. During MKCOL, the
            // server MUST make the request URI a member of its parent
            // collection, unless the request URI is "/". If no such ancestor
            // exists, the method MUST fail.
            // See https://datatracker.ietf.org/doc/html/rfc4918#section-9.3.1
            if let Some(parent) = Path::new(&dir_path).parent()
                && !parent.as_os_str().is_empty()
            {
                let parent_dir = format!("{}/", parent.display());
                if !dir_exists(&self.op, parent_dir.as_str()).await? {
                    return Err(FsError::NotFound);
                }
            }

            // Some backends keep files and directory markers as separate keys,
            // so "foo" and "foo/" can both exist. MKCOL must fail with Exists
            // when either form already names a resource (RFC 4918 9.3.1).
            if self.path_exists(file_path).await? || dir_exists(&self.op, &dir_path).await? {
                return Err(FsError::Exists);
            }

            self.op.create_dir(&dir_path).await.map_err(convert_error)
        }
        .boxed()
    }

    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let dir_path = self.fs_dir_path(path)?;

            // Removing the root collection is never allowed.
            if dir_path == "/" {
                return Err(FsError::Forbidden);
            }

            let file_path = dir_path.trim_end_matches('/');

            if self.op.info().capability().list {
                let mut lister = self
                    .op
                    .lister_with(dir_path.as_str())
                    .limit(2)
                    .await
                    .map_err(convert_error)?;
                let mut found = false;
                while let Some(entry) = lister.next().await {
                    let entry = entry.map_err(convert_error)?;
                    if entry.path() == dir_path {
                        found = true;
                    } else {
                        return Err(FsError::Exists);
                    }
                }
                if !found {
                    // Some services do not yield the directory itself when
                    // listing an empty directory, while an implicit
                    // object-store directory disappears as soon as its last
                    // child is removed during recursive DELETE. Refuse only
                    // when the path actually names a file, then fall through
                    // to the idempotent delete of `dir_path`.
                    if self.op.info().capability().stat
                        && !self.path_exists(&dir_path).await?
                        && self.path_exists(file_path).await?
                    {
                        return Err(FsError::Forbidden);
                    }
                }
            }

            self.op.delete(&dir_path).await.map_err(convert_error)
        }
        .boxed()
    }

    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let path = self.fs_path(path)?;
            self.op.delete(&path).await.map_err(convert_error)
        }
        .boxed()
    }

    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            if from.is_collection() {
                let from_path = self.fs_dir_path(from)?;
                let to_path = self.fs_dir_path(to)?;

                // Moving the root or onto the root is not a collection move.
                // Refuse it before it can reach the create_dir + delete below.
                if from_path == "/" || to_path == "/" {
                    return Err(FsError::Forbidden);
                }

                let to_file_path = to_path.trim_end_matches('/');
                if self.path_exists(to_file_path).await? || dir_exists(&self.op, &to_path).await? {
                    return Err(FsError::Exists);
                }

                // Services with native rename can move a non-empty collection
                // atomically. Fall back to the empty-collection move when the
                // service cannot rename this collection. `Operator::rename`
                // takes file paths, so drop the collection trailing slashes.
                if self.op.info().capability().rename {
                    match self
                        .op
                        .rename(from_path.trim_end_matches('/'), to_file_path)
                        .await
                    {
                        Ok(()) => return Ok(()),
                        Err(e) => match e.kind() {
                            // The service does not move collections; fall back
                            // to the empty-collection move below.
                            ErrorKind::Unsupported | ErrorKind::NotFound => {}
                            // Some services report IsADirectory when they
                            // cannot rename directories (smb) and also when
                            // the destination is an existing directory. If a
                            // destination collection appeared after the check
                            // above, refuse the move instead of clobbering it.
                            ErrorKind::IsADirectory => {
                                if self.path_exists(to_file_path).await?
                                    || dir_exists(&self.op, &to_path).await?
                                {
                                    return Err(FsError::Exists);
                                }
                            }
                            _ => return Err(convert_error(e)),
                        },
                    }
                }

                let mut lister = self
                    .op
                    .lister_with(from_path.as_str())
                    .limit(2)
                    .await
                    .map_err(convert_error)?;
                let mut found = false;
                while let Some(entry) = lister.next().await {
                    let entry = entry.map_err(convert_error)?;
                    if entry.path() == from_path {
                        found = true;
                    } else {
                        // Only empty directory moves are supported.
                        return Err(FsError::NotImplemented);
                    }
                }
                if !found {
                    return Err(FsError::NotFound);
                }

                self.op
                    .create_dir(to_path.as_str())
                    .await
                    .map_err(convert_error)?;
                return self
                    .op
                    .delete(from_path.as_str())
                    .await
                    .map_err(convert_error);
            }

            let from_path = self.fs_path(from)?;
            let to_path = self.fs_path(to)?;
            if from_path == "/" || to_path == "/" {
                return Err(FsError::Forbidden);
            }
            let to_path = to_path.trim_end_matches('/');
            self.op
                .rename(from_path.as_str(), to_path)
                .await
                .map_err(convert_error)
        }
        .boxed()
    }

    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let from_path = self.fs_path(from)?;
            let to_path = self.fs_path(to)?;
            if from_path == "/" || to_path == "/" {
                return Err(FsError::Forbidden);
            }
            let to_path = to_path.trim_end_matches('/');
            self.op
                .copy(from_path.as_str(), to_path)
                .await
                .map(|_| ())
                .map_err(convert_error)
        }
        .boxed()
    }
}
