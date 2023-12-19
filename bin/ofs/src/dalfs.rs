use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request, TimeOrNow,
};

use opendal::EntryMode;
use opendal::Metadata;
use opendal::Operator;

use libc::EACCES;
use libc::EIO;
use libc::ENOENT;
use libc::ENOSYS;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::path::Path;

use chrono::DateTime;
use chrono::Utc;
use std::result::Result;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::inode;
use crate::runtime;

const TTL: Duration = Duration::from_secs(1); // 1 second

pub struct DalFs {
    pub op: Operator,
    pub inodes: inode::InodeStore,
}

fn get_basename(path: &Path) -> &OsStr {
    path.file_name().expect("missing filename")
}

// Derivated from OpenDAL util
pub fn parse_datetime_from_from_timestamp_millis(s: i64) -> DateTime<Utc> {
    let st = UNIX_EPOCH
        .checked_add(Duration::from_millis(s as u64))
        .unwrap();
    st.into()
}

pub type LibcError = libc::c_int;

impl DalFs {
    fn cache_readdir<'a>(
        &'a mut self,
        ino: u64,
    ) -> Box<dyn Iterator<Item = Result<(OsString, FileAttr), LibcError>> + 'a> {
        let iter = self
            .inodes
            .children(ino)
            .into_iter()
            .map(move |child| Ok((get_basename(&child.path).into(), child.attr)));
        Box::new(iter)
    }

    fn remove_inode(&mut self, parent: u64, name: &OsStr) -> Result<(), i32> {
        let ino_opt = self.inodes.child(parent, name).map(|inode| inode.attr.ino);
        let path_ref = self.inodes[parent].path.join(name);
        let path = path_ref.to_str().unwrap();
        match runtime().block_on(self.op.delete(path)) {
            Ok(_) => {
                if let Some(ino) = ino_opt {
                    self.inodes.remove(ino);
                }
                Ok(())
            }
            Err(err) => {
                log::debug!("Remove inode failed: {}", err);
                Err(EIO)
            }
        }
    }
}

impl Filesystem for DalFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_str().unwrap();
        log::debug!("lookup(parent={}, name=\"{}\")", parent, name_str);

        match self.inodes.child(parent, name).cloned() {
            Some(child_inode) => reply.entry(&TTL, &child_inode.attr, 0),
            None => {
                let parent_inode = self.inodes[parent].clone();
                let child_path = parent_inode.path.join(name).as_path().display().to_string();
                match runtime().block_on(self.op.stat(&child_path)) {
                    Ok(child_metadata) => {
                        let inode = self.inodes.insert_metadata(&child_path, &child_metadata);
                        reply.entry(&TTL, &inode.attr, 0)
                    }
                    Err(err) => {
                        log::debug!("{}", err);
                        reply.error(ENOENT)
                    }
                }
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        log::debug!("getattr(ino={})", ino);

        let create_time: SystemTime = SystemTime::now();
        // TODO: Allow to read more attr
        {
            match self.inodes.get(ino) {
                Some(inode) => {
                    reply.attr(
                        &TTL,
                        &FileAttr {
                            ino,
                            size: 0,
                            blocks: 0,
                            atime: create_time,
                            mtime: create_time,
                            ctime: create_time,
                            crtime: create_time,
                            kind: inode.attr.kind,
                            perm: 0o755,
                            nlink: 2,
                            uid: 1000,
                            gid: 1000,
                            rdev: 0,
                            flags: 0,
                            blksize: 4096,
                        },
                    );
                }
                None => reply.error(ENOENT),
            };
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        log::debug!(
            "read(ino={}, fh={}, offset={}, size={})",
            ino,
            _fh,
            offset,
            size
        );

        match self.inodes.get(ino) {
            Some(inode) => {
                let path = Path::new(&inode.path);
                let result = runtime().block_on(self.op.read(path.to_str().unwrap()));

                match result {
                    Ok(buffer) => {
                        // Return the read data
                        let end_offset = offset + size as i64;
                        match buffer.len() {
                            len if len as i64 > offset + size as i64 => {
                                reply.data(&buffer[(offset as usize)..(end_offset as usize)]);
                            }
                            len if len as i64 > offset => {
                                reply.data(&buffer[(offset as usize)..]);
                            }
                            len => {
                                log::debug!("attempted read beyond buffer for ino {} len={} offset={} size={}", ino, len, offset, size);
                                reply.error(ENOENT);
                            }
                        }
                    }
                    Err(err) => {
                        log::warn!("Reading failed due to {:?}", err);
                        reply.error(ENOENT);
                    }
                };
            }
            None => {
                // FS will firstly lookup and then read inode, so inode should be there
                reply.error(ENOENT);
            }
        };
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        log::debug!(
            "mkdir(parent={}, name={:?}, mode=0o{:o})",
            parent,
            name,
            _mode
        );

        let path_ref = self.inodes[parent].path.join(name);
        let path = path_ref.to_str().unwrap();
        match runtime().block_on(self.op.create_dir(&(path.to_string() + "/"))) {
            Ok(_) => {
                let meta = Metadata::new(EntryMode::DIR);
                let mut attr = self.inodes.insert_metadata(path, &meta).attr;
                attr.perm = _mode as u16;
                reply.entry(&TTL, &attr, 0);
            }
            Err(err) => {
                log::debug!("mkdir error - {}", err);
                reply.error(EACCES);
            }
        };
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        log::debug!("readdir(ino={}, fh={}, offset={})", ino, _fh, offset);

        let dir_visited = self.inodes.get(ino).map(|n| n.visited).unwrap_or(false);
        if dir_visited {
            let cached_dir = self.cache_readdir(ino);
            let count = cached_dir.enumerate().count();
            if offset > 0 && offset as usize > count {
                reply.ok();
                return;
            }
        }

        let parent_ino = match ino {
            1 => 1,
            _ => {
                self.inodes
                    .parent(ino)
                    .expect("inode has no parent")
                    .attr
                    .ino
            }
        };

        if offset < 2 {
            match offset {
                0 => {
                    let _ = reply.add(ino, 0, FileType::Directory, ".");
                    let _ = reply.add(parent_ino, 1, FileType::Directory, "..");
                }
                1 => {
                    let _ = reply.add(parent_ino, 1, FileType::Directory, "..");
                }
                _ => {}
            }
        }

        match dir_visited {
            // read directory from OpenDAL and save to cache
            false => {
                let parent_path = &self.inodes[ino].path.clone();

                let entries = match runtime().block_on(self.op.list(parent_path.to_str().unwrap()))
                {
                    Ok(entries) => entries,
                    Err(error) => {
                        log::warn!("readdir failed due to {:?}", error);
                        return reply.error(EACCES);
                    }
                };
                for (_, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                    let metadata = runtime().block_on(self.op.stat(entry.path())).unwrap();
                    let child_path = parent_path.join(entry.name());
                    let _inode = self.inodes.insert_metadata(&child_path, &metadata);

                    match metadata.mode() {
                        EntryMode::FILE => {
                            log::debug!("Handling file");
                            // reply.add(_inode, i + offset + 2, FileType::RegularFile, child_path);
                        }
                        EntryMode::DIR => {
                            log::debug!("Handling dir {} {}", entry.path(), entry.name());
                            // reply.add(_inode, i + offset + 2, FileType::Directory, child_path);
                        }
                        EntryMode::Unknown => continue,
                    };
                }
            }
            // already read directory into cache
            true => {}
        };

        // Read from cache for visited and non-visited to keep the order
        for (i, next) in self.cache_readdir(ino).enumerate().skip(offset as usize) {
            match next {
                Ok((filename, attr)) => {
                    let _ = reply.add(attr.ino, i as i64 + offset + 2, attr.kind, &filename);
                }
                Err(err) => {
                    return reply.error(err);
                }
            }
        }

        // Mark this node visited
        let inodes = &mut self.inodes;
        let dir_inode = inodes
            .get_mut(ino)
            .expect("inode missing for dir just listed");
        dir_inode.visited = true;

        reply.ok();
    }

    fn mknod(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        log::debug!(
            "mknod(parent={}, name={:?}, mode=0o{:o})",
            parent,
            name,
            _mode
        );

        // TODO: check if we have write access to this dir in OpenDAL
        let path = self.inodes[parent].path.join(name);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        let mut meta = Metadata::new(EntryMode::FILE);
        meta.set_last_modified(parse_datetime_from_from_timestamp_millis(
            now.as_secs() as i64 * 1000 + now.subsec_millis() as i64,
        ));
        meta.set_content_length(0);

        // FIXME: cloning because it's quick-and-dirty
        let attr = self.inodes.insert_metadata(Path::new(&path), &meta).attr;

        let path_str = path.to_str().unwrap();
        match runtime().block_on(self.op.write(path_str, vec![])) {
            Ok(_) => reply.entry(&TTL, &attr, 0),
            Err(err) => {
                log::warn!("Creating node failed due to {:?}", err);
                reply.error(ENOENT);
            }
        };
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        log::debug!("open(ino={}, flags=0x{:x})", ino, flags);

        match self.inodes.get(ino) {
            Some(_) => {
                // TODO: Create reader and/or writer
                reply.opened(0, flags as u32);
            }
            None => reply.error(ENOENT),
        };
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        _mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        log::debug!(
            "setattr(ino={}, mode={:?}, size={:?}, fh={:?}, flags={:?})",
            ino,
            _mode,
            size,
            _fh,
            flags
        );
        match self.inodes.get_mut(ino) {
            Some(inode) => {
                if let Some(new_size) = size {
                    inode.attr.size = new_size;
                }
                if let Some(new_uid) = uid {
                    inode.attr.uid = new_uid;
                }
                if let Some(new_gid) = gid {
                    inode.attr.gid = new_gid;
                }
                // TODO: is mode (u32) equivalent to attr.perm (u16)?
                reply.attr(&TTL, &inode.attr);
            }
            None => reply.error(ENOENT),
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        // TODO: check if in read-only mode: reply EROFS
        log::debug!(
            "write(ino={}, fh={}, offset={}, len={}, flags=0x{:x})",
            ino,
            fh,
            offset,
            data.len(),
            flags
        );

        let is_replace =
            (offset == 0) && (self.inodes.get(ino).unwrap().attr.size < data.len() as u64);

        // Open a reader and flush all data to writer if not replace
        if !is_replace {
            match self.inodes.get_mut(ino) {
                Some(inode) => {
                    // We assume to have reading perm with writing perm
                    let original_data =
                        match runtime().block_on(self.op.read(inode.path.to_str().unwrap())) {
                            Ok(d) => d, // TODO: Do not copy all data
                            Err(err) => {
                                log::warn!("Reading failed due to {:?}", err);
                                reply.error(ENOENT);
                                return;
                            }
                        };
                    let mut new_size = original_data.len() as u64;
                    // TODO: Validate the length

                    let mut writer =
                        match runtime().block_on(self.op.writer(inode.path.to_str().unwrap())) {
                            Ok(writer) => writer,
                            Err(err) => {
                                log::warn!("Writing failed due to {:?}", err);
                                reply.error(ENOENT);
                                return;
                            }
                        };

                    let _ = runtime().block_on(writer.write(original_data));
                    // Write new content
                    new_size += match runtime().block_on(writer.write(data.to_vec())) {
                        Ok(_) => {
                            reply.written(data.len() as u32);
                            data.len() as u64
                        }
                        Err(err) => {
                            log::warn!("Writing failed due to {:?}", err);
                            reply.error(ENOENT);
                            0
                        }
                    };

                    let _ = runtime().block_on(writer.close());
                    inode.attr.size = new_size;
                }
                None => {
                    log::debug!("reading failed");
                    reply.error(ENOENT);
                }
            }
        } else {
            // Replace the file
            let new_size = match self.inodes.get_mut(ino) {
                Some(inode) => {
                    match runtime()
                        .block_on(self.op.write(inode.path.to_str().unwrap(), data.to_vec()))
                    {
                        Ok(_) => {
                            reply.written(data.len() as u32);
                            data.len() as u64
                        }
                        Err(err) => {
                            log::warn!("Writing failed due to {:?}", err);
                            reply.error(ENOENT);
                            0
                        }
                    }
                }
                None => {
                    log::debug!("write failed to read file");
                    reply.error(ENOENT);
                    return;
                }
            };

            let inode = &mut self.inodes[ino];
            inode.attr.size = new_size;
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        log::debug!("flush(ino={}, fh={})", ino, fh);
        // TODO: find a way to flush reader and/or writer
        reply.error(ENOSYS);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: i32,
        _lock_owner: Option<u64>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        log::debug!(
            "release(ino={}, fh={}, flags={}, flush={})",
            ino,
            fh,
            flags,
            flush
        );
        // TODO: close writer and reader
        reply.ok();
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        log::debug!(
            "rename(p={}, name={:?}, newp={}, newname={:?})",
            parent,
            name,
            newparent,
            newname
        );
        let old_path_ref = self.inodes[parent].path.join(name);
        let old_path = old_path_ref.to_str().unwrap();
        match runtime().block_on(self.op.reader(old_path)) {
            Ok(reader) => {
                let mut buf_reader = tokio::io::BufReader::with_capacity(8 * 1024 * 1024, reader);

                let path_ref = self.inodes[newparent].path.join(newname);
                let path = path_ref.to_str().unwrap();
                match runtime().block_on(self.op.writer(path)) {
                    Ok(mut writer) => {
                        if runtime()
                            .block_on(tokio::io::copy_buf(&mut buf_reader, &mut writer))
                            .is_err()
                        {
                            log::warn!("Renaming failed");
                        }
                        let _ = runtime().block_on(writer.close());
                    }
                    Err(err) => {
                        log::warn!("Renaming failed due to {:?}", err);
                        return reply.error(ENOENT);
                    }
                };
            }
            Err(err) => {
                log::warn!("Renaming failed due to {:?}", err);
                return reply.error(ENOENT);
            }
        };
        // Update the node
        match runtime().block_on(self.op.delete(old_path)) {
            Ok(_) => {
                match self.remove_inode(parent, name) {
                    Ok(_) => {
                        // Mark unvisited
                        let inodes = &mut self.inodes;
                        let dir_inode = inodes
                            .get_mut(parent)
                            .expect("inode missing for dir just listed");
                        dir_inode.visited = false;

                        reply.ok()
                    }
                    Err(err) => {
                        log::warn!("Renaming failed due to {:?}", err);
                        reply.error(EIO);
                    }
                }
            }
            Err(err) => {
                log::warn!("Renaming failed due to {:?}", err);
                reply.error(EIO)
            }
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::debug!("unlink(parent={}, name={:?})", parent, name);

        match self.remove_inode(parent, name) {
            Ok(_) => reply.ok(),
            Err(err) => {
                log::warn!("Removing failed due to {:?}", err);
                reply.error(EIO);
            }
        }
    }
}
