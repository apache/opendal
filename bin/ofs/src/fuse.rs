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
use std::ffi::OsString;
use std::num::NonZeroU32;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Duration;
use std::time::SystemTime;

use bytes::Bytes;

use fuse3::path::prelude::*;
use fuse3::Errno;
use fuse3::Result;
use futures_util::stream;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;

use opendal::Entry;
use opendal::EntryMode;
use opendal::ErrorKind;
use opendal::Metadata;
use opendal::Operator;
use sharded_slab::Slab;

const TTL: Duration = Duration::from_secs(1); // 1 second

#[derive(Debug, Clone)]
struct OpenedFile {
    path: OsString,
    is_read: bool,
    is_write: bool,
    is_append: bool,
    offset: u64,
}

#[derive(Debug, Clone, Copy)]
struct FileKey(usize);

impl TryFrom<u64> for FileKey {
    type Error = Errno;

    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Err(Errno::from(libc::EBADF)),
            _ => Ok(FileKey(value as usize - 1)),
        }
    }
}

impl FileKey {
    fn to_fh(self) -> u64 {
        self.0 as u64 + 1 // ensure fh is not 0
    }
}

pub(super) struct Fuse {
    op: Operator,
    gid: u32,
    uid: u32,
    opened_files: Slab<RwLock<OpenedFile>>,
}

impl Fuse {
    pub fn new(op: Operator, uid: u32, gid: u32) -> Self {
        Self {
            op,
            uid,
            gid,
            opened_files: Slab::new(),
        }
    }

    fn check_flags(&self, flags: u32) -> Result<(bool, bool)> {
        let mode = flags & libc::O_ACCMODE as u32;
        let is_read = mode == libc::O_RDONLY as u32 || mode == libc::O_RDWR as u32;
        let is_write = mode == libc::O_WRONLY as u32 || mode == libc::O_RDWR as u32;
        if !is_read && !is_write {
            Err(Errno::from(libc::EINVAL))?;
        }

        let capability = self.op.info().full_capability();
        if is_read && !capability.read {
            Err(Errno::from(libc::EACCES))?;
        }
        if is_write && !capability.write {
            Err(Errno::from(libc::EACCES))?;
        }

        log::trace!("check_flags: is_read={}, is_write={}", is_read, is_write);
        Ok((is_read, is_write))
    }

    // Get opened file and check given path
    fn get_opened_file(&self, key: FileKey, path: Option<&OsStr>) -> Result<OpenedFile> {
        let file = match self
            .opened_files
            .get(key.0)
            .as_ref()
            .ok_or(Errno::from(libc::ENOENT))?
            .deref()
            .read()
        {
            Ok(file) => file.clone(),
            Err(_) => Err(Errno::from(libc::EBADF))?,
        };

        if matches!(path, Some(path) if path != file.path) {
            log::trace!(
                "get_opened_file: path not match: path={:?}, file={:?}",
                path,
                file.path
            );
            Err(Errno::from(libc::EBADF))?;
        }

        Ok(file)
    }

    // Set opened file offset
    fn set_opened_file_offset(
        &self,
        key: FileKey,
        path: Option<&OsStr>,
        offset: u64,
    ) -> Result<()> {
        let binding = self.opened_files.get(key.0);
        let mut file = match binding
            .as_ref()
            .ok_or(Errno::from(libc::ENOENT))?
            .deref()
            .write()
        {
            Ok(file) => file,
            Err(_) => Err(Errno::from(libc::EBADF))?,
        };
        if matches!(path, Some(path) if path != file.path) {
            log::trace!(
                "set_opened_file: path not match: path={:?}, file={:?}",
                path,
                file.path
            );
            Err(Errno::from(libc::EBADF))?;
        }

        file.offset = offset;

        Ok(())
    }
}

impl PathFilesystem for Fuse {
    type DirEntryStream<'a> = BoxStream<'a, Result<DirectoryEntry>>;
    type DirEntryPlusStream<'a> = BoxStream<'a, Result<DirectoryEntryPlus>>;

    // Init a fuse filesystem
    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(16 * 1024).unwrap(),
        })
    }

    // Callback when fs is being destroyed
    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<ReplyEntry> {
        log::debug!("lookup(parent={:?}, name={:?})", parent, name);

        let path = PathBuf::from(parent).join(name);
        let metadata = self
            .op
            .stat(&path.to_string_lossy())
            .await
            .map_err(opendal_error2errno)?;

        let now = SystemTime::now();
        let attr = metadata2file_attr(&metadata, now, self.uid, self.gid);

        Ok(ReplyEntry { ttl: TTL, attr })
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: Option<u64>,
        flags: u32,
    ) -> Result<ReplyAttr> {
        log::debug!("getattr(path={:?}, fh={:?}, flags={:?})", path, fh, flags);

        let fh_path = fh.and_then(|fh| {
            self.opened_files
                .get(FileKey::try_from(fh).ok()?.0)
                .as_ref()
                .and_then(|f| f.read().ok())
                .map(|f| f.path.clone())
        });

        let file_path = match (path.map(Into::into), fh_path) {
            (Some(a), Some(b)) => {
                if a != b {
                    Err(Errno::from(libc::EBADF))?;
                }
                Some(a)
            }
            (a, b) => a.or(b),
        };

        let metadata = self
            .op
            .stat(&file_path.unwrap_or_default().to_string_lossy())
            .await
            .map_err(opendal_error2errno)?;

        let now = SystemTime::now();
        let attr = metadata2file_attr(&metadata, now, self.uid, self.gid);

        Ok(ReplyAttr { ttl: TTL, attr })
    }

    async fn setattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        log::debug!(
            "setattr(path={:?}, fh={:?}, set_attr={:?})",
            path,
            fh,
            set_attr
        );

        self.getattr(_req, path, fh, 0).await
    }

    async fn symlink(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        link_path: &OsStr,
    ) -> Result<ReplyEntry> {
        log::debug!(
            "symlink(parent={:?}, name={:?}, link_path={:?})",
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

        let mut path = PathBuf::from(parent).join(name);
        path.push(""); // ref https://users.rust-lang.org/t/trailing-in-paths/43166
        self.op
            .create_dir(&path.to_string_lossy())
            .await
            .map_err(opendal_error2errno)?;

        let metadata = Metadata::new(EntryMode::DIR);
        let now = SystemTime::now();
        let attr = metadata2file_attr(&metadata, now, self.uid, self.gid);

        Ok(ReplyEntry { ttl: TTL, attr })
    }

    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        log::debug!("unlink(parent={:?}, name={:?})", parent, name);

        let path = PathBuf::from(parent).join(name);
        self.op
            .delete(&path.to_string_lossy())
            .await
            .map_err(opendal_error2errno)?;

        Ok(())
    }

    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        log::debug!("rmdir(parent={:?}, name={:?})", parent, name);

        let path = PathBuf::from(parent).join(name);
        self.op
            .delete(&path.to_string_lossy())
            .await
            .map_err(opendal_error2errno)?;

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

        if !self.op.info().full_capability().rename {
            return Err(Errno::from(libc::EACCES))?;
        }

        let origin_path = PathBuf::from(origin_parent).join(origin_name);
        let path = PathBuf::from(parent).join(name);

        self.op
            .rename(&origin_path.to_string_lossy(), &path.to_string_lossy())
            .await
            .map_err(opendal_error2errno)?;

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

    async fn create(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated> {
        log::debug!(
            "create(parent={:?}, name={:?}, mode=0o{:o}, flags=0x{:x})",
            parent,
            name,
            mode,
            flags
        );

        let (is_read, is_write) = self.check_flags(flags)?;

        let path = PathBuf::from(parent).join(name);
        self.op
            .write(&path.to_string_lossy(), Bytes::new())
            .await
            .map_err(opendal_error2errno)?;

        let metadata = Metadata::new(EntryMode::FILE);
        let attr = metadata2file_attr(&metadata, SystemTime::now(), self.uid, self.gid);

        let key = self
            .opened_files
            .insert(RwLock::new(OpenedFile {
                path: path.into(),
                is_read,
                is_write,
                is_append: flags & libc::O_APPEND as u32 != 0,
                offset: 0,
            }))
            .ok_or(Errno::from(libc::EBUSY))?;

        Ok(ReplyCreated {
            ttl: TTL,
            attr,
            generation: 0,
            fh: FileKey(key).to_fh(),
            flags,
        })
    }

    async fn release(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> Result<()> {
        log::debug!(
            "release(path={:?}, fh={}, flags=0x{:x}, lock_owner={}, flush={})",
            path,
            fh,
            flags,
            lock_owner,
            flush
        );

        let file = self
            .opened_files
            .take(FileKey::try_from(fh)?.0)
            .map(|f| f.read().unwrap().clone())
            .ok_or(Errno::from(libc::EBADF))?;
        if matches!(path, Some(ref p) if p != &file.path) {
            Err(Errno::from(libc::EBADF))?;
        }

        Ok(())
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> Result<ReplyOpen> {
        log::debug!("open(path={:?}, flags=0x{:x})", path, flags);

        let (is_read, is_write) = self.check_flags(flags)?;

        let key = self
            .opened_files
            .insert(RwLock::new(OpenedFile {
                path: path.into(),
                is_read,
                is_write,
                is_append: flags & libc::O_APPEND as u32 != 0,
                offset: 0,
            }))
            .ok_or(Errno::from(libc::EBUSY))?;

        Ok(ReplyOpen {
            fh: FileKey(key).to_fh(),
            flags,
        })
    }

    async fn read(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        log::debug!(
            "read(path={:?}, fh={}, offset={}, size={})",
            path,
            fh,
            offset,
            size
        );

        let file = self.get_opened_file(FileKey::try_from(fh)?, path)?;

        if !file.is_read {
            Err(Errno::from(libc::EACCES))?;
        }

        let data = self
            .op
            .read_with(&file.path.to_string_lossy())
            .range(offset..offset + size as u64)
            .await
            .map_err(opendal_error2errno)?;

        self.set_opened_file_offset(FileKey::try_from(fh)?, path, offset + data.len() as u64)?;

        Ok(ReplyData { data: data.into() })
    }

    async fn write(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        flags: u32,
    ) -> Result<ReplyWrite> {
        log::debug!(
            "write(path={:?}, fh={}, offset={}, data_len={}, flags=0x{:x})",
            path,
            fh,
            offset,
            data.len(),
            flags
        );

        if offset != 0 {
            Err(Errno::from(libc::EINVAL))?;
        }

        let file = self.get_opened_file(FileKey::try_from(fh)?, path)?;
        if !file.is_write {
            Err(Errno::from(libc::EACCES))?;
        }

        self.op
            .write_with(
                &file.path.clone().to_string_lossy(),
                Bytes::copy_from_slice(data),
            )
            .append(file.is_append)
            .await
            .map_err(opendal_error2errno)?;

        self.set_opened_file_offset(FileKey::try_from(fh)?, path, offset + data.len() as u64)?;

        Ok(ReplyWrite {
            written: data.len() as _,
        })
    }

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        path: &'a OsStr,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        log::debug!("readdir(path={:?}, fh={}, offset={})", path, fh, offset);

        let mut current_dir = PathBuf::from(path);
        current_dir.push(""); // ref https://users.rust-lang.org/t/trailing-in-paths/43166
        let children = self
            .op
            .lister(&current_dir.to_string_lossy())
            .await
            .map_err(opendal_error2errno)?
            .enumerate()
            .map(|(i, entry)| {
                entry
                    .map(|e| DirectoryEntry {
                        kind: entry_mode2file_type(e.metadata().mode()),
                        name: e.name().trim_matches('/').into(),
                        offset: (i + 3) as i64,
                    })
                    .map_err(opendal_error2errno)
            });

        let relative_paths = stream::iter([
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
        ]);

        Ok(ReplyDirectory {
            entries: relative_paths.chain(children).skip(offset as usize).boxed(),
        })
    }

    async fn access(&self, _req: Request, path: &OsStr, mask: u32) -> Result<()> {
        log::debug!("access(path={:?}, mask=0x{:x})", path, mask);

        self.check_flags(mask)?;
        self.op
            .stat(&path.to_string_lossy())
            .await
            .map_err(opendal_error2errno)?;

        Ok(())
    }

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        parent: &'a OsStr,
        fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        log::debug!(
            "readdirplus(parent={:?}, fh={}, offset={})",
            parent,
            fh,
            offset
        );

        let make_entry = |op: Operator, i: usize, entry: opendal::Result<Entry>, uid, gid, now| async move {
            let e = entry.map_err(opendal_error2errno)?;
            let metadata = op
                .stat(e.name())
                .await
                .unwrap_or_else(|_| e.metadata().clone());
            let attr = metadata2file_attr(&metadata, now, uid, gid);
            Result::Ok(DirectoryEntryPlus {
                kind: entry_mode2file_type(metadata.mode()),
                name: e.name().trim_matches('/').into(),
                offset: (i + 3) as i64,
                attr,
                entry_ttl: TTL,
                attr_ttl: TTL,
            })
        };

        let now = SystemTime::now();
        let mut current_dir = PathBuf::from(parent);
        current_dir.push(""); // ref https://users.rust-lang.org/t/trailing-in-paths/43166
        let op = self.op.clone();
        let uid = self.uid;
        let gid = self.gid;
        let children = self
            .op
            .lister(&current_dir.to_string_lossy())
            .await
            .map_err(opendal_error2errno)?
            .enumerate()
            .then(move |(i, entry)| make_entry(op.clone(), i, entry, uid, gid, now));

        let relative_path_metadata = Metadata::new(EntryMode::DIR);
        let relative_path_attr = metadata2file_attr(&relative_path_metadata, now, uid, gid);
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

        Ok(ReplyDirectoryPlus {
            entries: relative_paths.chain(children).skip(offset as usize).boxed(),
        })
    }
    async fn rename2(
        &self,
        req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
        _flags: u32,
    ) -> Result<()> {
        log::debug!(
            "rename2(origin_parent={:?}, origin_name={:?}, parent={:?}, name={:?})",
            origin_parent,
            origin_name,
            parent,
            name
        );
        self.rename(req, origin_parent, origin_name, parent, name)
            .await
    }

    async fn lseek(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        whence: u32,
    ) -> Result<ReplyLSeek> {
        log::debug!(
            "lseek(path={:?}, fh={}, offset={}, whence={})",
            path,
            fh,
            offset,
            whence
        );

        let whence = whence as i32;

        let file = self.get_opened_file(FileKey::try_from(fh)?, path)?;

        if !file.is_read && !file.is_write {
            Err(Errno::from(libc::EACCES))?;
        }

        let offset = if whence == libc::SEEK_SET {
            offset
        } else if whence == libc::SEEK_CUR {
            file.offset + offset
        } else if whence == libc::SEEK_END {
            let metadata = self
                .op
                .stat(&path.unwrap().to_string_lossy())
                .await
                .map_err(opendal_error2errno)?;
            let content_size = metadata.content_length();

            if content_size >= offset as _ {
                content_size as u64 - offset
            } else {
                0
            }
        } else {
            return Err(libc::ENOSYS.into());
        };

        Ok(ReplyLSeek { offset })
    }

    async fn copy_file_range(
        &self,
        req: Request,
        from_path: Option<&OsStr>,
        fh_in: u64,
        offset_in: u64,
        to_path: Option<&OsStr>,
        fh_out: u64,
        offset_out: u64,
        length: u64,
        flags: u64,
    ) -> Result<ReplyCopyFileRange> {
        log::debug!(
            "copy_file_range(from_path={:?}, fh_in={}, offset_in={}, to_path={:?}, fh_out={}, offset_out={}, length={}, flags={})",
            from_path,
            fh_in,
            offset_in,
            to_path,
            fh_out,
            offset_out,
            length,
            flags
        );
        let data = self
            .read(req, from_path, fh_in, offset_in, length as _)
            .await?;

        let ReplyWrite { written } = self
            .write(req, to_path, fh_out, offset_out, &data.data, 0, flags as _)
            .await?;

        Ok(ReplyCopyFileRange {
            copied: u64::from(written),
        })
    }
}

const fn entry_mode2file_type(mode: EntryMode) -> FileType {
    match mode {
        EntryMode::DIR => FileType::Directory,
        _ => FileType::RegularFile,
    }
}

fn metadata2file_attr(metadata: &Metadata, atime: SystemTime, uid: u32, gid: u32) -> FileAttr {
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
        uid,
        gid,
        rdev: 0,
        blksize: 4096,
    }
}

fn opendal_error2errno(err: opendal::Error) -> fuse3::Errno {
    log::trace!("opendal_error2errno: {:?}", err);
    match err.kind() {
        ErrorKind::Unsupported => Errno::from(libc::EOPNOTSUPP),
        ErrorKind::IsADirectory => Errno::from(libc::EISDIR),
        ErrorKind::NotFound => Errno::from(libc::ENOENT),
        ErrorKind::PermissionDenied => Errno::from(libc::EACCES),
        ErrorKind::AlreadyExists => Errno::from(libc::EEXIST),
        ErrorKind::NotADirectory => Errno::from(libc::ENOTDIR),
        ErrorKind::ContentTruncated => Errno::from(libc::EAGAIN),
        ErrorKind::ContentIncomplete => Errno::from(libc::EIO),
        _ => Errno::from(libc::ENOENT),
    }
}
