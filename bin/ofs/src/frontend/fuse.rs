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

use std::ffi::OsStr;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;
use std::vec::IntoIter;

use fuse3::async_trait;
use fuse3::path::prelude::*;
use fuse3::Errno;
use fuse3::Result;
use futures_util::stream;
use futures_util::stream::Iter;
use futures_util::StreamExt;
use opendal::EntryMode;
use opendal::ErrorKind;
use opendal::Metadata;
use opendal::Operator;

const TTL: Duration = Duration::from_secs(1); // 1 second

pub(super) struct Ofs {
    op: Operator,
    gid: u32,
    uid: u32,
}

impl Ofs {
    pub fn new(op: Operator, uid: u32, gid: u32) -> Self {
        Self { op, uid, gid }
    }
}

#[async_trait]
impl PathFilesystem for Ofs {
    type DirEntryStream = Iter<IntoIter<Result<DirectoryEntry>>>;
    type DirEntryPlusStream = Iter<IntoIter<Result<DirectoryEntryPlus>>>;

    // Init a fuse filesystem
    async fn init(&self, _req: Request) -> Result<()> {
        Ok(())
    }

    // Callback when fs is being destroyed
    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<ReplyEntry> {
        log::debug!(
            "lookup(parent={}, name=\"{}\")",
            parent.to_string_lossy(),
            name.to_string_lossy()
        );

        let path = PathBuf::from(parent).join(name);
        let metadata = self
            .op
            .stat(&path.to_string_lossy())
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::Unsupported => Errno::from(libc::EOPNOTSUPP),
                _ => Errno::from(libc::ENOENT),
            })?;

        let now = SystemTime::now();
        let attr = metadata2file_attr(&metadata, now);

        Ok(ReplyEntry { ttl: TTL, attr })
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        log::debug!("getattr(path={:?})", path);

        let metadata = self
            .op
            .stat(&path.unwrap_or_default().to_string_lossy())
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::Unsupported => Errno::from(libc::EOPNOTSUPP),
                _ => Errno::from(libc::ENOENT),
            })?;

        let now = SystemTime::now();
        let mut attr = metadata2file_attr(&metadata, now);
        attr.uid = self.uid;
        attr.gid = self.gid;

        Ok(ReplyAttr { ttl: TTL, attr })
    }

    async fn setattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        log::debug!("setattr(path={:?})", path);
        Err(libc::EOPNOTSUPP.into())
    }

    async fn symlink(
        &self,
        req: Request,
        parent: &OsStr,
        name: &OsStr,
        link_path: &OsStr,
    ) -> Result<ReplyEntry> {
        log::debug!(
            "symlink(req={:?}, parent={:?}, name={:?}, link_path={:?})",
            req,
            parent,
            name,
            link_path
        );
        Err(libc::EOPNOTSUPP.into())
    }

    async fn mknod(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _rdev: u32,
    ) -> Result<ReplyEntry> {
        log::debug!(
            "mknod(parent={:?}, name={:?}, mode=0o{:o})",
            parent,
            name,
            mode
        );
        Err(libc::EOPNOTSUPP.into())
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        log::debug!(
            "mkdir(parent={:?}, name={:?}, mode=0o{:o})",
            parent,
            name,
            mode
        );

        let path = PathBuf::from(parent).join(name);
        self.op
            .create_dir(&path.to_string_lossy())
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::Unsupported => Errno::from(libc::EOPNOTSUPP),
                ErrorKind::AlreadyExists => Errno::from(libc::EEXIST),
                ErrorKind::PermissionDenied => Errno::from(libc::EACCES),
                _ => Errno::from(libc::ENOENT),
            })?;

        let metadata = Metadata::new(EntryMode::DIR);
        let now = SystemTime::now();
        let mut attr = metadata2file_attr(&metadata, now);
        attr.uid = self.uid;
        attr.gid = self.gid;

        Ok(ReplyEntry { ttl: TTL, attr })
    }

    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        log::debug!("unlink(parent={:?}, name={:?})", parent, name);
        Err(libc::EOPNOTSUPP.into())
    }

    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        log::debug!("rmdir(parent={:?}, name={:?})", parent, name);

        let path = PathBuf::from(parent).join(name);
        self.op
            .delete(&path.to_string_lossy())
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::Unsupported => Errno::from(libc::EOPNOTSUPP),
                ErrorKind::NotFound => Errno::from(libc::ENOENT),
                ErrorKind::PermissionDenied => Errno::from(libc::EACCES),
                _ => Errno::from(libc::ENOENT),
            })?;

        Ok(())
    }

    async fn rename(
        &self,
        _req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<()> {
        log::debug!(
            "rename(p={:?}, name={:?}, newp={:?}, newname={:?})",
            origin_parent,
            origin_name,
            parent,
            name
        );

        let origin_path = PathBuf::from(origin_parent).join(origin_name);
        let path = PathBuf::from(parent).join(name);

        self.op
            .rename(&origin_path.to_string_lossy(), &path.to_string_lossy())
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::Unsupported => Errno::from(libc::EOPNOTSUPP),
                ErrorKind::NotFound => Errno::from(libc::ENOENT),
                ErrorKind::PermissionDenied => Errno::from(libc::EACCES),
                ErrorKind::IsSameFile => Errno::from(libc::EINVAL),
                _ => Errno::from(libc::ENOENT),
            })?;

        Ok(())
    }

    async fn link(
        &self,
        _req: Request,
        path: &OsStr,
        new_parent: &OsStr,
        new_name: &OsStr,
    ) -> Result<ReplyEntry> {
        log::debug!(
            "link(path={:?}, new_parent={:?}, new_name={:?})",
            path,
            new_parent,
            new_name
        );
        Err(libc::EOPNOTSUPP.into())
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> Result<ReplyOpen> {
        // TODO
        log::debug!("open(path={:?}, flags=0x{:x})", path, flags);
        Err(libc::ENOSYS.into())
    }

    async fn read(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        // TODO
        log::debug!(
            "read(path={:?}, fh={}, offset={}, size={})",
            path,
            fh,
            offset,
            size
        );

        Err(libc::ENOSYS.into())
    }

    async fn write(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        data: &[u8],
        flags: u32,
    ) -> Result<ReplyWrite> {
        // TODO
        log::debug!(
            "write(path={:?}, fh={}, offset={}, len={}, flags=0x{:x})",
            path,
            fh,
            offset,
            data.len(),
            flags
        );

        Err(libc::ENOSYS.into())
    }

    async fn readdir(
        &self,
        req: Request,
        path: &OsStr,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream>> {
        log::debug!(
            "readdir(req={:?}, path={:?}, fh={}, offset={})",
            req,
            path,
            fh,
            offset
        );

        let current_dir = path.to_string_lossy();
        let entries = self
            .op
            .list(&current_dir)
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::NotFound => Errno::new_not_exist(),
                ErrorKind::NotADirectory => Errno::new_is_not_dir(),
                _ => Errno::from(libc::ENOENT),
            })?;

        let relative_paths = [
            Result::Ok(DirectoryEntry {
                kind: FileType::Directory,
                name: ".".into(),
                offset: 1,
            }),
            Result::Ok(DirectoryEntry {
                kind: FileType::Directory,
                name: "..".into(),
                offset: 2,
            }),
        ];

        let res = relative_paths
            .into_iter()
            .chain(entries.iter().enumerate().map(|(i, entry)| {
                Result::Ok(DirectoryEntry {
                    kind: entry_mode2file_type(entry.metadata().mode()),
                    name: entry.name().trim_matches('/').into(),
                    offset: (i + 3) as i64,
                })
            }))
            .skip(offset as usize)
            .collect::<Vec<_>>();
        log::debug!("readdir entries={:#?}", res);

        Ok(ReplyDirectory {
            entries: stream::iter(res),
        })
    }

    async fn access(&self, _req: Request, path: &OsStr, mask: u32) -> Result<()> {
        log::debug!("access(path={:?}, mask=0x{:x})", path, mask);
        self.op
            .stat(&path.to_string_lossy())
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::Unsupported => Errno::from(libc::EOPNOTSUPP),
                _ => Errno::from(libc::ENOENT),
            })?;

        Ok(())
    }

    async fn readdirplus(
        &self,
        req: Request,
        parent: &OsStr,
        fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream>> {
        log::debug!(
            "readdirplus(req={:?}, parent={:?}, fh={}, offset={})",
            req,
            parent,
            fh,
            offset
        );

        let current_dir = parent.to_string_lossy();
        let entries = self
            .op
            .list(&current_dir)
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::NotFound => Errno::new_not_exist(),
                ErrorKind::NotADirectory => Errno::new_is_not_dir(),
                _ => Errno::from(libc::ENOENT),
            })?;

        let now = SystemTime::now();
        let relative_path_metadata = Metadata::new(EntryMode::DIR);
        let relative_path_attr = metadata2file_attr(&relative_path_metadata, now);
        let relative_paths = stream::iter([
            Result::Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: ".".into(),
                offset: 1,
                attr: relative_path_attr,
                entry_ttl: TTL,
                attr_ttl: TTL,
            }),
            Result::Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: "..".into(),
                offset: 2,
                attr: relative_path_attr,
                entry_ttl: TTL,
                attr_ttl: TTL,
            }),
        ]);

        let children = stream::iter(entries)
            .enumerate()
            .then(|(i, entry)| async move {
                let metadata = self
                    .op
                    .stat(&entry.name())
                    .await
                    .unwrap_or_else(|_| entry.metadata().clone());
                let mut attr = metadata2file_attr(&metadata, now);
                attr.uid = self.uid;
                attr.gid = self.gid;
                Result::Ok(DirectoryEntryPlus {
                    kind: entry_mode2file_type(entry.metadata().mode()),
                    name: entry.name().trim_matches('/').into(),
                    offset: (i + 3) as i64,
                    attr,
                    entry_ttl: TTL,
                    attr_ttl: TTL,
                })
            });

        let res = relative_paths
            .chain(children)
            .skip(offset as usize)
            .collect::<Vec<_>>()
            .await;

        Ok(ReplyDirectoryPlus {
            entries: stream::iter(res),
        })
    }
}

const fn entry_mode2file_type(mode: EntryMode) -> FileType {
    match mode {
        EntryMode::DIR => FileType::Directory,
        _ => FileType::RegularFile,
    }
}

fn metadata2file_attr(metadata: &Metadata, atime: SystemTime) -> FileAttr {
    let last_modified = metadata.last_modified().map(|t| t.into()).unwrap_or(atime);
    let kind = entry_mode2file_type(metadata.mode());
    FileAttr {
        size: metadata.content_length(),
        blocks: 0,
        atime,
        mtime: last_modified,
        ctime: last_modified,
        kind,
        perm: fuse3::perm_from_mode_and_kind(kind, 0o775),
        nlink: 0,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        blksize: 4096,
    }
}
