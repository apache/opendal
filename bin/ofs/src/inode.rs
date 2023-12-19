/* Original file: https://github.com/anowell/netfuse/blob/master/src/inode.rs */
use fuser::{FileAttr, FileType};
use opendal::EntryMode;
use opendal::Metadata;
use sequence_trie::SequenceTrie;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::ops::{Index, IndexMut};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct Inode {
    pub path: PathBuf,
    pub attr: FileAttr,
    pub visited: bool,
}

impl Inode {
    pub fn new<P: AsRef<Path>>(path: P, attr: FileAttr) -> Inode {
        Inode {
            path: PathBuf::from(path.as_ref()),
            attr,
            visited: false,
        }
    }
}

#[derive(Debug)]
pub struct InodeStore {
    inode_map: HashMap<u64, Inode>,
    ino_trie: SequenceTrie<OsString, u64>,
    uid: u32,
    gid: u32,
    last_ino: u64,
}

impl InodeStore {
    pub fn new(perm: u16, uid: u32, gid: u32) -> InodeStore {
        let mut store = InodeStore {
            inode_map: HashMap::new(),
            ino_trie: SequenceTrie::new(),
            uid,
            gid,
            last_ino: 1, // 1 is reserved for root
        };

        let now = SystemTime::now();
        let fs_root = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm,
            nlink: 0,
            uid,
            gid,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        };

        store.insert(Inode::new("/", fs_root));
        store
    }

    pub fn get(&self, ino: u64) -> Option<&Inode> {
        self.inode_map.get(&ino)
    }

    pub fn get_by_path<P: AsRef<Path>>(&self, path: P) -> Option<&Inode> {
        let sequence = path_to_sequence(path.as_ref());
        self.ino_trie.get(&sequence).and_then(|ino| self.get(*ino))
    }

    pub fn insert_metadata<P: AsRef<Path>>(&mut self, path: P, metadata: &Metadata) -> &Inode {
        // Non-lexical borrows can't come soon enough
        let ino_opt = self.get_by_path(path.as_ref()).map(|inode| inode.attr.ino);
        let ino = ino_opt.unwrap_or_else(|| {
            self.last_ino += 1;
            self.last_ino
        });

        log::debug!("insert metadata: {} {}", ino, path.as_ref().display());

        let now = SystemTime::now();
        let mut ts = now;
        if let Some(last_modified_datetime) = metadata.last_modified() {
            ts = last_modified_datetime.into();
        }
        let attr = FileAttr {
            ino,
            size: metadata.content_length(),
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: match metadata.mode() {
                EntryMode::FILE => FileType::RegularFile,
                EntryMode::DIR => FileType::Directory,
                // TODO: We do not know how to handle it
                EntryMode::Unknown => FileType::RegularFile,
            },
            perm: 0o550,
            nlink: 0,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        };

        self.insert(Inode::new(path, attr));
        self.get(ino).unwrap()
    }

    pub fn child<S: AsRef<OsStr>>(&self, ino: u64, name: S) -> Option<&Inode> {
        self.get(ino).and_then(|inode| {
            let mut sequence = path_to_sequence(&inode.path);
            sequence.push(name.as_ref().to_owned());
            self.ino_trie.get(&sequence).and_then(|ino| self.get(*ino))
        })
    }

    pub fn children(&self, ino: u64) -> Vec<&Inode> {
        match self.get(ino) {
            Some(inode) => {
                let sequence = path_to_sequence(&inode.path);
                let node = self
                    .ino_trie
                    .get_node(&sequence)
                    .expect("inconsistent fs - failed to lookup by path after lookup by ino");
                node.children()
                    .iter()
                    .filter_map(|c| c.value())
                    .map(|ino| {
                        self.get(*ino)
                            .expect("inconsistent fs - found child without inode")
                    })
                    .collect()
            }
            None => vec![],
        }
    }

    // All inodes have a parent (root parent is root)
    // Return value of None means the ino wasn't found
    pub fn parent(&self, ino: u64) -> Option<&Inode> {
        // parent of root is root
        if ino == 1 {
            return self.get(1);
        }

        self.get(ino).and_then(|inode| {
            let sequence = path_to_sequence(&inode.path);
            match sequence.len() {
                1 => self.get(1),
                len => self
                    .ino_trie
                    .get(&sequence[0..(len - 1)])
                    .and_then(|p_ino| self.get(*p_ino)),
            }
        })
    }

    pub fn get_mut(&mut self, ino: u64) -> Option<&mut Inode> {
        self.inode_map.get_mut(&ino)
    }

    // pub fn get_mut_by_path<P: AsRef<Path>>(&mut self, path: P) -> Option<&mut Inode> {
    //     let sequence = path_to_sequence(path.as_ref());
    //     self.ino_trie.get(&sequence).cloned()
    //         .and_then(move |ino| self.get_mut(ino))
    // }

    pub fn insert(&mut self, inode: Inode) {
        let ino = inode.attr.ino;
        let path = inode.path.clone();
        let sequence = path_to_sequence(&inode.path);

        if let Some(old_inode) = self.inode_map.insert(ino, inode) {
            if old_inode.path != path {
                panic!(
                    "Corrupted inode store: reinserted conflicting ino {} (path={}, oldpath={})",
                    ino,
                    path.display(),
                    old_inode.path.display()
                );
            } else {
                log::debug!("Updating ino {} at path {}", ino, path.display());
            }
        }

        if self.ino_trie.insert(&sequence, ino).is_some() {
            let node = self.ino_trie.get_node_mut(&sequence).unwrap_or_else(|| {
                panic!(
                    "Corrupt inode store: couldn't insert or modify ino_trie at {:?}",
                    &sequence
                )
            });
            // TODO: figure out why this check triggers a false alarm panic on backspacing to dir and then tabbing
            // if node.value.is_some() {
            //     panic!("Corrupt inode store: reinserted ino {} into ino_trie, prev value: {}", ino, node.value.unwrap());
            // }
            if let Some(v) = node.value_mut() {
                *v = ino;
            }
        }
    }

    pub fn remove(&mut self, ino: u64) {
        let sequence = {
            let path = &self.inode_map[&ino].path;
            path_to_sequence(path)
        };

        self.inode_map.remove(&ino);
        self.ino_trie.remove(&sequence);

        assert!(self.inode_map.get(&ino).is_none());
        assert!(self.ino_trie.get(&sequence).is_none());
    }
}

impl Index<u64> for InodeStore {
    type Output = Inode;

    fn index(&self, index: u64) -> &Inode {
        self.get(index).unwrap()
    }
}

impl IndexMut<u64> for InodeStore {
    fn index_mut(&mut self, index: u64) -> &mut Inode {
        self.get_mut(index).unwrap()
    }
}

fn path_to_sequence(path: &Path) -> Vec<OsString> {
    path.iter().map(|s| s.to_owned()).collect()
}
