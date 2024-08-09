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

use std::io::Write;
use std::mem::size_of;
use std::time::Duration;

use log::debug;
use opendal::ErrorKind;
use opendal::Operator;
use sharded_slab::Slab;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;
use vm_memory::ByteValued;

use crate::error::*;
use crate::filesystem_message::*;
use crate::virtiofs_util::{Reader, Writer};

/// Version number of this interface.
const KERNEL_VERSION: u32 = 7;
/// Minor version number of this interface.
const KERNEL_MINOR_VERSION: u32 = 38;
/// Minimum Minor version number supported.
const MIN_KERNEL_MINOR_VERSION: u32 = 27;
/// The length of the header part of the message.
const BUFFER_HEADER_SIZE: u32 = 4096;
/// The maximum length of the data part of the message, used for read/write data.
const MAX_BUFFER_SIZE: u32 = 1 << 20;
/// The default time to live of the attributes.
const DEFAULT_TTL: Duration = Duration::from_secs(1);
/// The default mode of the opened file.
const DEFAULT_OPENED_FILE_MODE: u32 = 0o755;

enum FileType {
    Dir,
    File,
}

struct OpenedFile {
    path: String,
    metadata: Attr,
}

impl OpenedFile {
    fn new(file_type: FileType, path: &str, uid: u32, gid: u32) -> OpenedFile {
        let mut attr: Attr = unsafe { std::mem::zeroed() };
        attr.uid = uid;
        attr.gid = gid;
        match file_type {
            FileType::Dir => {
                attr.nlink = 2;
                attr.mode = libc::S_IFDIR | DEFAULT_OPENED_FILE_MODE;
            }
            FileType::File => {
                attr.nlink = 1;
                attr.mode = libc::S_IFREG | DEFAULT_OPENED_FILE_MODE;
            }
        }
        OpenedFile {
            path: path.to_string(),
            metadata: attr,
        }
    }
}

fn opendal_error2error(error: opendal::Error) -> Error {
    match error.kind() {
        ErrorKind::Unsupported => Error::from(libc::EOPNOTSUPP),
        ErrorKind::IsADirectory => Error::from(libc::EISDIR),
        ErrorKind::NotFound => Error::from(libc::ENOENT),
        ErrorKind::PermissionDenied => Error::from(libc::EACCES),
        ErrorKind::AlreadyExists => Error::from(libc::EEXIST),
        ErrorKind::NotADirectory => Error::from(libc::ENOTDIR),
        ErrorKind::RangeNotSatisfied => Error::from(libc::EINVAL),
        ErrorKind::RateLimited => Error::from(libc::EBUSY),
        _ => Error::from(libc::ENOENT),
    }
}

fn opendal_metadata2opened_file(
    path: &str,
    metadata: &opendal::Metadata,
    uid: u32,
    gid: u32,
) -> OpenedFile {
    let file_type = match metadata.mode() {
        opendal::EntryMode::DIR => FileType::Dir,
        _ => FileType::File,
    };
    OpenedFile::new(file_type, path, uid, gid)
}

/// Filesystem is a filesystem implementation with opendal backend,
/// and will decode and process messages from VMs.
pub struct Filesystem {
    rt: Runtime,
    core: Operator,
    uid: u32,
    gid: u32,
    opened_files: Slab<OpenedFile>,
}

impl Filesystem {
    pub fn new(core: Operator) -> Filesystem {
        let rt = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        // Here we set the uid and gid to 1000, which is the default value.
        Filesystem {
            rt,
            core,
            uid: 1000,
            gid: 1000,
            opened_files: Slab::new(),
        }
    }

    pub fn handle_message(&self, mut r: Reader, w: Writer) -> Result<usize> {
        let in_header: InHeader = r.read_obj().map_err(|e| {
            new_vhost_user_fs_error("failed to decode protocol messages", Some(e.into()))
        })?;
        if in_header.len > (MAX_BUFFER_SIZE + BUFFER_HEADER_SIZE) {
            // The message is too long here.
            return Filesystem::reply_error(in_header.unique, w);
        }
        if let Ok(opcode) = Opcode::try_from(in_header.opcode) {
            match opcode {
                Opcode::Init => self.init(in_header, r, w),
                Opcode::Destroy => self.destroy(in_header, r, w),
                Opcode::Getattr => self.getattr(in_header, r, w),
                Opcode::Setattr => self.setattr(in_header, r, w),
            }
        } else {
            Filesystem::reply_error(in_header.unique, w)
        }
    }
}

impl Filesystem {
    fn reply_ok<T: ByteValued>(
        out: Option<T>,
        data: Option<&[u8]>,
        unique: u64,
        mut w: Writer,
    ) -> Result<usize> {
        let mut len = size_of::<OutHeader>();
        if out.is_some() {
            len += size_of::<T>();
        }
        if let Some(data) = data {
            len += data.len();
        }
        let header = OutHeader {
            unique,
            error: 0, // Return no error.
            len: len as u32,
        };
        w.write_all(header.as_slice()).map_err(|e| {
            new_vhost_user_fs_error("failed to encode protocol messages", Some(e.into()))
        })?;
        if let Some(out) = out {
            w.write_all(out.as_slice()).map_err(|e| {
                new_vhost_user_fs_error("failed to encode protocol messages", Some(e.into()))
            })?;
        }
        if let Some(data) = data {
            w.write_all(data).map_err(|e| {
                new_vhost_user_fs_error("failed to encode protocol messages", Some(e.into()))
            })?;
        }
        Ok(w.bytes_written())
    }

    fn reply_error(unique: u64, mut w: Writer) -> Result<usize> {
        let header = OutHeader {
            unique,
            error: libc::EIO, // Here we simply return I/O error.
            len: size_of::<OutHeader>() as u32,
        };
        w.write_all(header.as_slice()).map_err(|e| {
            new_vhost_user_fs_error("failed to encode protocol messages", Some(e.into()))
        })?;
        Ok(w.bytes_written())
    }
}

impl Filesystem {
    fn init(&self, in_header: InHeader, mut r: Reader, w: Writer) -> Result<usize> {
        let InitIn { major, minor, .. } = r.read_obj().map_err(|e| {
            new_vhost_user_fs_error("failed to decode protocol messages", Some(e.into()))
        })?;

        if major != KERNEL_VERSION || minor < MIN_KERNEL_MINOR_VERSION {
            return Filesystem::reply_error(in_header.unique, w);
        }

        // We will directly return ok and do nothing for now.
        let out = InitOut {
            major: KERNEL_VERSION,
            minor: KERNEL_MINOR_VERSION,
            max_write: MAX_BUFFER_SIZE,
            ..Default::default()
        };
        Filesystem::reply_ok(Some(out), None, in_header.unique, w)
    }

    fn destroy(&self, _in_header: InHeader, _r: Reader, _w: Writer) -> Result<usize> {
        // do nothing for destroy.
        Ok(0)
    }

    fn getattr(&self, in_header: InHeader, _r: Reader, w: Writer) -> Result<usize> {
        debug!("getattr: inode={}", in_header.nodeid);

        let path = match self
            .opened_files
            .get(in_header.nodeid as usize)
            .map(|f| f.path.clone())
        {
            Some(path) => path,
            None => return Filesystem::reply_error(in_header.unique, w),
        };

        let mut metadata = match self.rt.block_on(self.do_get_metadata(&path)) {
            Ok(metadata) => metadata,
            Err(_) => return Filesystem::reply_error(in_header.unique, w),
        };
        metadata.metadata.ino = in_header.nodeid;

        let out = AttrOut {
            attr_valid: DEFAULT_TTL.as_secs(),
            attr_valid_nsec: DEFAULT_TTL.subsec_nanos(),
            attr: metadata.metadata,
            ..Default::default()
        };
        Filesystem::reply_ok(Some(out), None, in_header.unique, w)
    }

    fn setattr(&self, in_header: InHeader, _r: Reader, w: Writer) -> Result<usize> {
        debug!("setattr: inode={}", in_header.nodeid);

        // do nothing for setattr.
        self.getattr(in_header, _r, w)
    }
}

impl Filesystem {
    async fn do_get_metadata(&self, path: &str) -> Result<OpenedFile> {
        let metadata = self.core.stat(path).await.map_err(opendal_error2error)?;
        let attr = opendal_metadata2opened_file(path, &metadata, self.uid, self.gid);

        Ok(attr)
    }
}
