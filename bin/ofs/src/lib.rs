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

use fuse3::path::prelude::*;
use fuse3::MountOptions;

use opendal::Operator;

pub struct Ofs {
    pub op: Operator,
}

impl PathFilesystem for Ofs {

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
        _path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        // TODO
        Err(libc::ENOSYS.into())
    }

    async fn read(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _offset: u64,
        _size: u32,
    ) -> Result<ReplyData> {
        // TODO
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
        Err(libc::ENOSYS.into())
    }

    async fn readdir(
        &self,
        _req: Request,
        _path: &OsStr,
        _fh: u64,
        _offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream>> {
        // TODO
        Err(libc::ENOSYS.into())
    }

    async fn mknod(
        &self,
        _req: Request,
        _parent: &OsStr,
        _name: &OsStr,
        _mode: u32,
        _rdev: u32,
    ) -> Result<ReplyEntry> {
        // TODO
        Err(libc::ENOSYS.into())
    }

    async fn open(&self, _req: Request, _path: &OsStr, _flags: u32) -> Result<ReplyOpen> {
        // TODO
        Err(libc::ENOSYS.into())
    }

    async fn setattr(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: Option<u64>,
        _set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        // TODO
        Err(libc::ENOSYS.into())
    }

    async fn write(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _offset: u64,
        _data: &[u8],
        _flags: u32,
    ) -> Result<ReplyWrite> {
        // TODO
        Err(libc::ENOSYS.into())
    }

    async fn release(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        // TODO
        Err(libc::ENOSYS.into())
    }

    async fn rename(
        &self,
        _req: Request,
        _origin_parent: &OsStr,
        _origin_name: &OsStr,
        _parent: &OsStr,
        _name: &OsStr,
    ) -> Result<()> {
        // TODO
        Err(libc::ENOSYS.into())
    }

    async fn unlink(&self, _req: Request, _parent: &OsStr, _name: &OsStr) -> Result<()> {
        Err(libc::ENOSYS.into())
    }
}
