mod consts;

pub mod ovfs_filesystem;

use std::io;

use consts::*;

pub type Inode = u64;
pub type Handle = u64;

#[derive(Clone, Copy)]
struct InodeKey(usize);

impl From<Inode> for InodeKey {
    fn from(value: Inode) -> InodeKey {
        InodeKey(value as usize - 1)
    }
}

impl InodeKey {
    fn to_inode(&self) -> Inode {
        self.0 as Inode + 1
    }
}

#[derive(Clone, PartialEq)]
enum InodeType {
    DIR,
    FILE,
    Unknown,
}

#[derive(Clone)]
struct InodeData {
    path: String,
    stat: libc::stat64,
}

impl InodeData {
    fn new(inode_type: InodeType, path: &str) -> InodeData {
        let mut stat: libc::stat64 = unsafe { std::mem::zeroed() };
        stat.st_uid = DEFAULT_UID;
        stat.st_gid = DEFAULT_GID;
        match inode_type {
            InodeType::DIR => {
                stat.st_nlink = 2;
                stat.st_mode = libc::S_IFDIR | DEFAULT_DIR_MODE;
            }
            InodeType::FILE => {
                stat.st_nlink = 1;
                stat.st_mode = libc::S_IFREG | DEFAULT_FILE_MODE;
            }
            _ => (),
        }
        InodeData {
            stat,
            path: path.to_string(),
        }
    }
}

fn opendal_error2error(error: opendal::Error) -> io::Error {
    match error.kind() {
        opendal::ErrorKind::Unsupported => io::Error::from_raw_os_error(libc::EOPNOTSUPP),
        opendal::ErrorKind::NotFound => io::Error::from_raw_os_error(libc::ENOENT),
        _ => io::Error::from_raw_os_error(libc::ENOENT),
    }
}

fn opendal_metadata2stat64(path: &str, metadata: &opendal::Metadata) -> InodeData {
    let inode_type = match metadata.mode() {
        opendal::EntryMode::DIR => InodeType::DIR,
        opendal::EntryMode::FILE => InodeType::FILE,
        opendal::EntryMode::Unknown => InodeType::Unknown,
    };
    let inode_data = InodeData::new(inode_type, path);
    inode_data
}
