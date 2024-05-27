use std::collections::HashMap;
use std::ffi::CStr;
use std::io;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Duration;

use opendal::services::S3;
use opendal::{Buffer, Operator};
use sharded_slab::Slab;
use tokio::runtime::{Builder, Runtime};

use super::consts::*;
use super::*;
use crate::filesystem::*;
use crate::fuse::*;

pub struct OVFSFileSystem {
    rt: Runtime,
    core: Operator,
    opened_inodes: Slab<RwLock<InodeData>>, // inode key -> inode data
    opened_inodes_map: RwLock<HashMap<String, InodeKey>>, // path -> inode key
}

impl OVFSFileSystem {
    pub fn new() -> OVFSFileSystem {
        let mut builder = S3::default();
        builder.bucket("test");
        builder.endpoint("http://127.0.0.1:9000");
        builder.access_key_id("minioadmin");
        builder.secret_access_key("minioadmin");
        builder.region("us-east-1");
        let operator = Operator::new(builder)
            .expect("failed to build operator")
            .finish();
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        OVFSFileSystem {
            core: operator,
            rt: runtime,
            opened_inodes: Slab::default(),
            opened_inodes_map: RwLock::new(HashMap::default()),
        }
    }

    fn get_opened(&self, path: &str) -> Option<InodeKey> {
        let map = self.opened_inodes_map.read().unwrap();
        map.get(path).copied()
    }

    fn insert_opened(&self, path: &str, key: InodeKey) {
        let mut map = self.opened_inodes_map.write().unwrap();
        map.insert(path.to_string(), key);
    }

    fn delete_opened(&self, path: &str) {
        let mut map = self.opened_inodes_map.write().unwrap();
        map.remove(path);
    }

    fn get_opened_inode(&self, key: InodeKey) -> io::Result<InodeData> {
        if let Some(opened_inode) = self.opened_inodes.get(key.0) {
            let inode_data = opened_inode.read().unwrap().clone();
            Ok(inode_data)
        } else {
            Err(io::Error::from_raw_os_error(libc::EBADF)) // Invalid inode
        }
    }

    fn insert_opened_inode(&self, value: InodeData) -> io::Result<InodeKey> {
        if let Some(key) = self.opened_inodes.insert(RwLock::new(value)) {
            Ok(InodeKey(key))
        } else {
            Err(io::Error::from_raw_os_error(libc::ENFILE)) // Too many opened fiels
        }
    }

    fn delete_opened_inode(&self, key: InodeKey) -> io::Result<()> {
        if self.opened_inodes.remove(key.0) {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(libc::EBADF)) // Invalid inode
        }
    }
}

impl OVFSFileSystem {
    async fn do_get_stat(&self, path: &str) -> io::Result<InodeData> {
        let metadata = self.core.stat(&path).await.map_err(opendal_error2error)?;

        let attr = opendal_metadata2stat64(path, &metadata);

        Ok(attr)
    }

    async fn do_create_file(&self, path: &str) -> io::Result<()> {
        self.core
            .write(&path, Buffer::new())
            .await
            .map_err(opendal_error2error)?;

        Ok(())
    }

    async fn do_delete_file(&self, path: &str) -> io::Result<()> {
        self.core.delete(&path).await.map_err(opendal_error2error)?;

        Ok(())
    }
}

impl FileSystem for OVFSFileSystem {
    type Inode = Inode;
    type Handle = Handle;

    fn init(&self, _capable: FsOptions) -> io::Result<FsOptions> {
        let data = InodeData::new(InodeType::DIR, "/");
        let inode = self.insert_opened_inode(data.clone())?;
        self.insert_opened("/", inode);
        Ok(DEFAULT_FS_OPTIONS)
    }

    fn destroy(&self) {}

    fn lookup(&self, parent: Inode, name: &CStr) -> io::Result<Entry> {
        let name = match name.to_str() {
            Ok(name) => name,
            Err(_) => Err(io::Error::from_raw_os_error(libc::EINVAL))?,
        };
        let file = self.get_opened_inode(InodeKey::from(parent))?;
        let path = PathBuf::from(file.path)
            .join(name)
            .to_string_lossy()
            .to_string();
        if let Some(inode_key) = self.get_opened(&path) {
            let inode_data = self.get_opened_inode(inode_key)?;
            return Ok(Entry {
                inode: inode_key.to_inode(),
                attr: inode_data.stat,
                generation: DEFAULT_GENERATION,
                attr_flags: DEFAULT_ATTR_FLAGS,
                attr_timeout: DEFAULT_ATTR_TIMEOUT,
                entry_timeout: DEFAULT_ENTRY_TIMEOUT,
            });
        }
        let metadata = self.rt.block_on(self.do_get_stat(&path))?;
        let inode_key = self.insert_opened_inode(metadata.clone())?;
        self.insert_opened(&path, inode_key);
        Ok(Entry {
            inode: inode_key.to_inode(),
            attr: metadata.stat,
            generation: DEFAULT_GENERATION,
            attr_flags: DEFAULT_ATTR_FLAGS,
            attr_timeout: DEFAULT_ATTR_TIMEOUT,
            entry_timeout: DEFAULT_ENTRY_TIMEOUT,
        })
    }

    fn getattr(
        &self,
        inode: Self::Inode,
        _handle: Option<Self::Handle>,
    ) -> io::Result<(libc::stat64, Duration)> {
        let file = self.get_opened_inode(InodeKey::from(inode))?;
        let mut stat = file.stat;
        stat.st_ino = inode;
        Ok((stat, DEFAULT_ATTR_TIMEOUT))
    }

    fn create(
        &self,
        parent: Self::Inode,
        name: &CStr,
        _mode: u32,
        _flags: u32,
        _umask: u32,
    ) -> io::Result<(Entry, Option<Self::Handle>, OpenOptions)> {
        let name = match name.to_str() {
            Ok(name) => name,
            Err(_) => Err(io::Error::from_raw_os_error(libc::EINVAL))?,
        };
        let file = self.get_opened_inode(InodeKey::from(parent))?;
        let path = PathBuf::from(file.path)
            .join(name)
            .to_string_lossy()
            .to_string();
        self.rt.block_on(self.do_create_file(&path))?;
        let inode_data = InodeData::new(InodeType::FILE, &path);
        let inode_key = self.insert_opened_inode(inode_data.clone())?;
        self.insert_opened(&path, inode_key);
        let mut stat = inode_data.stat;
        stat.st_ino = inode_key.to_inode();
        Ok((
            Entry {
                inode: inode_key.to_inode(),
                attr: inode_data.stat,
                generation: DEFAULT_GENERATION,
                attr_flags: DEFAULT_ATTR_FLAGS,
                attr_timeout: DEFAULT_ATTR_TIMEOUT,
                entry_timeout: DEFAULT_ENTRY_TIMEOUT,
            },
            DEFAULT_FILE_HANDLE,
            DEFAULT_OPEN_OPTIONS,
        ))
    }

    fn unlink(&self, parent: Self::Inode, name: &CStr) -> io::Result<()> {
        let name = match name.to_str() {
            Ok(name) => name,
            Err(_) => Err(io::Error::from_raw_os_error(libc::EINVAL))?,
        };
        let file = self.get_opened_inode(InodeKey::from(parent))?;
        let path = PathBuf::from(file.path)
            .join(name)
            .to_string_lossy()
            .to_string();
        if let Some(inode) = self.get_opened(&path) {
            self.delete_opened(&path);
            let _ = self.delete_opened_inode(inode);
        }
        self.rt.block_on(self.do_delete_file(&path))?;
        Ok(())
    }

    fn access(&self, _inode: Self::Inode, _mask: u32) -> io::Result<()> {
        Ok(())
    }
}
