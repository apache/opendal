use std::ffi::CStr;
use std::io;
use std::time::Duration;

use crate::fuse::*;

pub struct Entry {
    pub inode: u64,
    pub generation: u64,
    pub attr: libc::stat64,
    pub attr_flags: u32,
    pub attr_timeout: Duration,
    pub entry_timeout: Duration,
}

impl From<Entry> for EntryOut {
    fn from(entry: Entry) -> EntryOut {
        EntryOut {
            nodeid: entry.inode,
            entry_valid: entry.entry_timeout.as_secs(),
            attr_valid: entry.attr_timeout.as_secs(),
            entry_valid_nsec: entry.entry_timeout.subsec_nanos(),
            attr_valid_nsec: entry.attr_timeout.subsec_nanos(),
            attr: Attr::with_flags(entry.attr, entry.attr_flags),
        }
    }
}

#[allow(unused_variables)]
pub trait FileSystem {
    type Inode: From<u64> + Into<u64>;
    type Handle: From<u64> + Into<u64>;

    fn init(&self, capable: FsOptions) -> io::Result<FsOptions> {
        Ok(FsOptions::empty())
    }

    fn destroy(&self) {}

    fn lookup(&self, parent: Self::Inode, name: &CStr) -> io::Result<Entry> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    fn getattr(
        &self,
        inode: Self::Inode,
        handle: Option<Self::Handle>,
    ) -> io::Result<(libc::stat64, Duration)> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    fn create(
        &self,
        parent: Self::Inode,
        name: &CStr,
        mode: u32,
        flags: u32,
        umask: u32,
    ) -> io::Result<(Entry, Option<Self::Handle>, OpenOptions)> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    fn unlink(&self, parent: Self::Inode, name: &CStr) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    fn access(&self, inode: Self::Inode, mask: u32) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    fn forget(&self, inode: Self::Inode, count: u64) {}
}
