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
use std::vec::IntoIter;

use async_trait::async_trait;
use fuse3::path::prelude::*;
use fuse3::Result;
use futures_util::stream::Empty;
use futures_util::stream::Iter;
use opendal::Operator;

pub(super) struct Ofs {
    op: Operator,
}

impl Ofs {
    pub fn new(op: Operator) -> Self {
        Self { op }
    }
}

#[async_trait]
impl PathFilesystem for Ofs {
    type DirEntryStream = Empty<Result<DirectoryEntry>>;
    type DirEntryPlusStream = Iter<IntoIter<Result<DirectoryEntryPlus>>>;

    // Init a fuse filesystem
    async fn init(&self, _req: Request) -> Result<()> {
        Ok(())
    }

    // Callback when fs is being destroyed
    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, _parent: &OsStr, _name: &OsStr) -> Result<ReplyEntry> {
        // TODO
        Err(libc::ENOSYS.into())
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        // TODO
        log::debug!("getattr(path={:?})", path);

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

    async fn mkdir(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        // TODO
        log::debug!(
            "mkdir(parent={:?}, name={:?}, mode=0o{:o})",
            parent,
            name,
            mode
        );

        Err(libc::ENOSYS.into())
    }

    async fn readdir(
        &self,
        _req: Request,
        path: &OsStr,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream>> {
        // TODO
        log::debug!("readdir(path={:?}, fh={}, offset={})", path, fh, offset);

        Err(libc::ENOSYS.into())
    }

    async fn mknod(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _rdev: u32,
    ) -> Result<ReplyEntry> {
        // TODO
        log::debug!(
            "mknod(parent={:?}, name={:?}, mode=0o{:o})",
            parent,
            name,
            mode
        );

        Err(libc::ENOSYS.into())
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> Result<ReplyOpen> {
        // TODO
        log::debug!("open(path={:?}, flags=0x{:x})", path, flags);

        Err(libc::ENOSYS.into())
    }

    async fn setattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        // TODO
        log::debug!("setattr(path={:?})", path);

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

    async fn release(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        flags: u32,
        _lock_owner: u64,
        flush: bool,
    ) -> Result<()> {
        // TODO
        log::debug!(
            "release(path={:?}, fh={}, flags={}, flush={})",
            path,
            fh,
            flags,
            flush
        );

        Err(libc::ENOSYS.into())
    }

    async fn rename(
        &self,
        _req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<()> {
        // TODO
        log::debug!(
            "rename(p={:?}, name={:?}, newp={:?}, newname={:?})",
            origin_parent,
            origin_name,
            parent,
            name
        );

        Err(libc::ENOSYS.into())
    }

    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        // TODO
        log::debug!("unlink(parent={:?}, name={:?})", parent, name);

        Err(libc::ENOSYS.into())
    }
}
