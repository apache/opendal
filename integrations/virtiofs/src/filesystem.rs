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

use std::collections::HashMap;
use std::io::Write;
use std::mem::size_of;
use std::sync::RwLock;

use opendal::{Buffer, Operator};
use sharded_slab::Slab;
use tokio::runtime::{Builder, Runtime};
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
const BUFFER_HEADER_SIZE: u32 = 256;
/// The maximum length of the data part of the message, used for read/write data.
const MAX_BUFFER_SIZE: u32 = 1 << 20;

/// FileType represents the type of the opened file.
enum FileType {
    Dir,
    File,
    Unknown,
}

/// FileKey represents the key assigned to the opened file.
#[derive(Clone, Copy)]
struct FileKey(usize);

/// OpenedFile represents file that opened in memory.
#[allow(dead_code)]
#[derive(Clone)]
struct OpenedFile {
    path: String,
    stat: libc::stat64,
}

impl OpenedFile {
    fn new(file_type: FileType, path: &str) -> OpenedFile {
        let mut stat: libc::stat64 = unsafe { std::mem::zeroed() };
        stat.st_uid = 1000;
        stat.st_gid = 1000;
        match file_type {
            FileType::Dir => {
                stat.st_nlink = 2;
                stat.st_mode = libc::S_IFDIR | 0o755;
            }
            FileType::File => {
                stat.st_nlink = 1;
                stat.st_mode = libc::S_IFREG | 0o755;
            }
            FileType::Unknown => (),
        }
        OpenedFile {
            stat,
            path: path.to_string(),
        }
    }
}

fn opendal_error2error(error: opendal::Error) -> Error {
    match error.kind() {
        opendal::ErrorKind::Unsupported => {
            new_vhost_user_fs_error("unsupported error occurred in backend storage system", None)
        }
        opendal::ErrorKind::NotFound => {
            new_vhost_user_fs_error("notfound error occurred in backend storage system", None)
        }
        _ => new_vhost_user_fs_error("unexpected error occurred in backend storage system", None),
    }
}

fn opendal_metadata2opened_file(path: &str, metadata: &opendal::Metadata) -> OpenedFile {
    let file_type = match metadata.mode() {
        opendal::EntryMode::DIR => FileType::Dir,
        opendal::EntryMode::FILE => FileType::File,
        opendal::EntryMode::Unknown => FileType::Unknown,
    };
    OpenedFile::new(file_type, path)
}

/// Filesystem is a filesystem implementation with opendal backend,
/// and will decode and process messages from VMs.
#[allow(dead_code)]
pub struct Filesystem {
    rt: Runtime,
    core: Operator,
    opened_inodes: Slab<RwLock<OpenedFile>>, // opened key -> opened file
    opened_inodes_map: RwLock<HashMap<String, FileKey>>, // opened path -> opened key
}

#[allow(dead_code)]
impl Filesystem {
    pub fn new(core: Operator) -> Filesystem {
        let rt = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        Filesystem {
            rt,
            core,
            opened_inodes: Slab::default(),
            opened_inodes_map: RwLock::new(HashMap::default()),
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
            }
        } else {
            Filesystem::reply_error(in_header.unique, w)
        }
    }
}

#[allow(dead_code)]
impl Filesystem {
    fn get_opened(&self, path: &str) -> Option<FileKey> {
        let map = self.opened_inodes_map.read().unwrap();
        map.get(path).copied()
    }

    fn insert_opened(&self, path: &str, key: FileKey) {
        let mut map = self.opened_inodes_map.write().unwrap();
        map.insert(path.to_string(), key);
    }

    fn delete_opened(&self, path: &str) {
        let mut map = self.opened_inodes_map.write().unwrap();
        map.remove(path);
    }

    fn get_opened_inode(&self, key: FileKey) -> Result<OpenedFile> {
        if let Some(opened_inode) = self.opened_inodes.get(key.0) {
            let inode_data = opened_inode.read().unwrap().clone();
            Ok(inode_data)
        } else {
            Err(new_unexpected_error("invalid inode", None))
        }
    }

    fn insert_opened_inode(&self, value: OpenedFile) -> Result<FileKey> {
        if let Some(key) = self.opened_inodes.insert(RwLock::new(value)) {
            Ok(FileKey(key))
        } else {
            Err(new_unexpected_error("too many opened files", None))
        }
    }

    fn delete_opened_inode(&self, key: FileKey) -> Result<()> {
        if self.opened_inodes.remove(key.0) {
            Ok(())
        } else {
            Err(new_unexpected_error("invalid inode", None))
        }
    }
}

#[allow(dead_code)]
impl Filesystem {
    async fn do_get_stat(&self, path: &str) -> Result<OpenedFile> {
        let metadata = self.core.stat(path).await.map_err(opendal_error2error)?;

        let attr = opendal_metadata2opened_file(path, &metadata);

        Ok(attr)
    }

    async fn do_create_file(&self, path: &str) -> Result<()> {
        self.core
            .write(path, Buffer::new())
            .await
            .map_err(opendal_error2error)?;

        Ok(())
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
}
