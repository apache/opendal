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
use crate::services::kv::accessor::KeyValueStreamer;
use crate::services::kv::ScopedKey;
use crate::{Accessor, AccessorCapability, AccessorMetadata, ObjectMetadata, ObjectMode};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind, Result};

/// Backend of kv service.
#[derive(Debug)]
pub struct Backend<S: KeyValueAccessor> {
    kv: S,
}

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

    async fn get_inode(&self, ino: u64) -> Result<Option<ObjectMetadata>> {
        let bs = self.kv.get(&ScopedKey::inode(ino).encode()).await?;
        match bs {
            None => Ok(None),
            Some(bs) => Ok(Some(
                bincode::deserialize(&bs).map_err(format_bincode_error)?,
            )),
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

    async fn list_entries(&self, parent: u64) -> Result<KeyValueStreamer> {
        let key = ScopedKey::entry(parent, "");
        self.kv.scan(&key.encode()).await
    }

    async fn lookup_entry(&self, parent: u64, name: &str) -> Result<KeyValueEntry> {
        todo!()
    }

    async fn create_block(&self, ino: u64, block: u64, content: &[u8]) -> Result<()> {
        let key = ScopedKey::block(ino, block);
        self.kv.set(&key.encode(), &content).await
    }

    async fn list_blocks(&self, ino: u64) -> Result<KeyValueStreamer> {
        let key = ScopedKey::block(ino, 0);
        self.kv.scan(&key.encode()).await
    }

    async fn create_dir(&self, parent: u64, name: &str) -> Result<()> {
        let p = self.get_inode(parent).await?;
        if p.is_none() {
            return Err(Error::new(
                ErrorKind::Other,
                anyhow!("parent dir is not exist"),
            ));
        }

        let inode = self.get_next_inode().await?;
        let meta = ObjectMetadata::new(ObjectMode::DIR);
        self.create_inode(inode, meta).await?;
        let entry = KeyValueEntry::new(inode, ObjectMode::DIR);
        self.create_entry(parent, name, entry).await?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct KeyValueMeta {
    /// First valid inode will start from `1024 + 1`.
    next_inode: u64,
}

impl Default for KeyValueMeta {
    fn default() -> Self {
        Self { next_inode: 1025 }
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
