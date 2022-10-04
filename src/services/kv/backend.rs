// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::KeyValueAccessor;
use crate::ops::{OpCreate, OpRead, OpStat};
use crate::path::{build_rooted_abs_path, get_basename, get_parent};
use crate::services::kv::accessor::KeyValueStreamer;
use crate::services::kv::ScopedKey;
use crate::{
    Accessor, AccessorCapability, AccessorMetadata, BytesReader, ObjectMetadata, ObjectMode,
};
use anyhow::anyhow;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind, Result};
use time::OffsetDateTime;

/// Backend of kv service.
#[derive(Debug)]
pub struct Backend<S: KeyValueAccessor> {
    kv: S,
    root: String,
}

#[async_trait]
impl<S> Accessor for Backend<S>
where
    S: KeyValueAccessor,
{
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_capabilities(
            AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
        );
        am
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);
        let parent = get_parent(&p);
        let basename = get_basename(path);
        let inode = self.create_dir_parents(parent).await?;

        match args.mode() {
            ObjectMode::DIR => {
                self.create_dir(inode, basename).await?;
            }
            ObjectMode::FILE => {
                self.create_file(inode, basename).await?;
            }
            ObjectMode::Unknown => unimplemented!(),
        }

        Ok(())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        let p = build_rooted_abs_path(&self.root, path);
        let inode = self.lookup(&p).await?;
        let meta = self.get_inode(inode).await?;
        Ok(meta)
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        todo!()
    }
}

impl<S: KeyValueAccessor> Backend<S> {
    async fn get_meta(&self) -> Result<KeyValueMeta> {
        let meta = self.kv.get(&ScopedKey::meta().encode()).await?;
        match meta {
            None => Ok(KeyValueMeta::default()),
            Some(bs) => Ok(bincode::deserialize(&bs).map_err(format_bincode_error)?),
        }
    }

    async fn set_meta(&self, meta: KeyValueMeta) -> Result<()> {
        let bs = bincode::serialize(&meta).map_err(format_bincode_error)?;
        self.kv.set(&ScopedKey::meta().encode(), &bs).await?;

        Ok(())
    }

    async fn get_next_inode(&self) -> Result<u64> {
        let mut meta = self.get_meta().await?;
        let inode = meta.next_inode();
        self.set_meta(meta).await?;

        Ok(inode)
    }

    async fn get_inode(&self, ino: u64) -> Result<ObjectMetadata> {
        // Handle root inode.
        if ino == INODE_ROOT {
            return Ok(ObjectMetadata::new(ObjectMode::DIR));
        }

        let bs = self.kv.get(&ScopedKey::inode(ino).encode()).await?;
        match bs {
            None => Err(Error::new(
                ErrorKind::NotFound,
                anyhow!("inode {} not found", ino),
            )),
            Some(bs) => Ok(bincode::deserialize(&bs).map_err(format_bincode_error)?),
        }
    }

    async fn create_inode(&self, ino: u64, meta: ObjectMetadata) -> Result<()> {
        let key = ScopedKey::inode(ino);
        let value = bincode::serialize(&meta).map_err(format_bincode_error)?;
        self.kv.set(&key.encode(), &value).await
    }

    async fn create_entry(&self, parent: u64, name: &str, entry: KeyValueEntry) -> Result<()> {
        let key = ScopedKey::entry(parent, name);
        let value = bincode::serialize(&entry).map_err(format_bincode_error)?;
        self.kv.set(&key.encode(), &value).await
    }

    async fn get_entry(&self, parent: u64, name: &str) -> Result<KeyValueEntry> {
        let key = ScopedKey::entry(parent, name);
        let bs = self.kv.get(&key.encode()).await?;
        match bs {
            None => Err(Error::new(
                ErrorKind::NotFound,
                anyhow!("entry parent: {}, name: {} is not found", parent, name),
            )),
            Some(bs) => Ok(bincode::deserialize(&bs).map_err(format_bincode_error)?),
        }
    }

    async fn list_entries(&self, parent: u64) -> Result<KeyValueStreamer> {
        let key = ScopedKey::entry(parent, "");
        self.kv.scan(&key.encode()).await
    }

    async fn create_block(&self, ino: u64, block: u64, content: &[u8]) -> Result<()> {
        let key = ScopedKey::block(ino, block);
        self.kv.set(&key.encode(), content).await
    }

    async fn list_blocks(&self, ino: u64) -> Result<KeyValueStreamer> {
        let key = ScopedKey::block(ino, 0);
        self.kv.scan(&key.encode()).await
    }

    async fn create_dir(&self, parent: u64, name: &str) -> Result<u64> {
        let name = name.trim_matches('/');

        #[cfg(debug_assertions)]
        {
            let p = self.get_inode(parent).await?;
            assert!(p.mode().is_dir())
        }

        let inode = self.get_next_inode().await?;
        let meta = ObjectMetadata::new(ObjectMode::DIR);
        self.create_inode(inode, meta).await?;
        let entry = KeyValueEntry::new(inode, ObjectMode::DIR);
        self.create_entry(parent, name, entry).await?;

        Ok(inode)
    }

    async fn create_file(&self, parent: u64, name: &str) -> Result<u64> {
        let name = name.trim_matches('/');

        #[cfg(debug_assertions)]
        {
            let p = self.get_inode(parent).await?;
            assert!(p.mode().is_dir());
        }

        let inode = self.get_next_inode().await?;
        let meta = ObjectMetadata::new(ObjectMode::FILE)
            .with_last_modified(OffsetDateTime::now_utc())
            .with_content_length(0);
        self.create_inode(inode, meta).await?;
        let entry = KeyValueEntry::new(inode, ObjectMode::DIR);
        self.create_entry(parent, name, entry).await?;

        Ok(inode)
    }

    async fn lookup(&self, path: &str) -> Result<u64> {
        if path == "/" {
            return Ok(INODE_ROOT);
        }

        let mut inode = INODE_ROOT;
        for name in path.split('/') {
            let entry = self.get_entry(inode, name).await?;
            inode = entry.ino;
        }
        Ok(inode)
    }

    async fn create_dir_parents(&self, path: &str) -> Result<u64> {
        if path == "/" {
            return Ok(INODE_ROOT);
        }

        let mut inode = INODE_ROOT;
        for name in path.split('/') {
            let entry = self.get_entry(inode, name).await;
            inode = match entry {
                Ok(entry) => entry.ino,
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    self.create_dir(inode, name).await?
                }
                Err(err) => return Err(err),
            }
        }
        Ok(inode)
    }
}

/// OpenDAL will reserve all inode between 0~16.
const INODE_ROOT: u64 = 17;

#[derive(Serialize, Deserialize)]
struct KeyValueMeta {
    /// First valid inode will start from `17 + 1`.
    next_inode: u64,
}

impl Default for KeyValueMeta {
    fn default() -> Self {
        Self {
            next_inode: INODE_ROOT,
        }
    }
}

impl KeyValueMeta {
    fn next_inode(&mut self) -> u64 {
        let inode = self.next_inode;
        self.next_inode += 1;
        inode
    }
}

#[derive(Serialize, Deserialize)]
struct KeyValueEntry {
    ino: u64,
    mode: ObjectMode,
}

impl KeyValueEntry {
    fn new(ino: u64, mode: ObjectMode) -> Self {
        Self { ino, mode }
    }
}

fn format_bincode_error(err: bincode::Error) -> Error {
    Error::new(ErrorKind::Other, anyhow!("bincode: {:?}", err))
}
