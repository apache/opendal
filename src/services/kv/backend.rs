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
use crate::ops::{OpCreate, OpList, OpRead, OpStat, OpWrite};
use crate::path::{build_rooted_abs_path, get_basename, get_parent};
use crate::services::kv::accessor::KeyValueStreamer;
use crate::services::kv::ScopedKey;
use crate::{
    Accessor, AccessorCapability, AccessorMetadata, BytesReader, ObjectMetadata, ObjectMode,
    ObjectStreamer,
};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::io;
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

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let p = build_rooted_abs_path(&self.root, path);
        let inode = self.lookup(&p).await?;

        todo!()
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        let p = build_rooted_abs_path(&self.root, path);
        let parent = get_parent(&p);
        let basename = get_basename(path);
        let parent_inode = self.create_dir_parents(parent).await?;

        // TODO: we need transaction here.
        let inode = self
            .create_file_with(
                parent_inode,
                basename,
                ObjectMetadata::new(ObjectMode::FILE)
                    .with_last_modified(OffsetDateTime::now_utc())
                    .with_content_length(args.size()),
            )
            .await?;
        self.write_blocks(inode, args.size(), r).await?;
        Ok(args.size())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        let p = build_rooted_abs_path(&self.root, path);
        let inode = self.lookup(&p).await?;
        let meta = self.get_inode(inode).await?;
        Ok(meta)
    }

    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        todo!()
    }
}

impl<S: KeyValueAccessor> Backend<S> {
    async fn get_meta(&self) -> Result<KeyValueMeta> {
        let meta = self.kv.get(&ScopedKey::meta().encode()).await?;
        match meta {
            None => Ok(KeyValueMeta::default()),
            Some(bs) => Ok(bincode::deserialize(&bs).map_err(new_bincode_error)?),
        }
    }

    async fn set_meta(&self, meta: KeyValueMeta) -> Result<()> {
        let bs = bincode::serialize(&meta).map_err(new_bincode_error)?;
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
            Some(bs) => Ok(bincode::deserialize(&bs).map_err(new_bincode_error)?),
        }
    }

    async fn create_inode(&self, ino: u64, meta: ObjectMetadata) -> Result<()> {
        let key = ScopedKey::inode(ino);
        let value = bincode::serialize(&meta).map_err(new_bincode_error)?;
        self.kv.set(&key.encode(), &value).await
    }

    async fn create_entry(&self, parent: u64, name: &str, entry: KeyValueEntry) -> Result<()> {
        let key = ScopedKey::entry(parent, name);
        let value = bincode::serialize(&entry).map_err(new_bincode_error)?;
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
            Some(bs) => Ok(bincode::deserialize(&bs).map_err(new_bincode_error)?),
        }
    }

    async fn list_entries(&self, parent: u64) -> Result<KeyValueStreamer> {
        let key = ScopedKey::entry(parent, "");
        self.kv.scan(&key.encode()).await
    }

    async fn create_block(&self, ino: u64, version: u64, block: u64, content: &[u8]) -> Result<()> {
        let key = ScopedKey::block(ino, version, block);
        self.kv.set(&key.encode(), content).await
    }

    async fn write_blocks(&self, ino: u64, size: u64, mut r: BytesReader) -> Result<()> {
        let next_version = self.get_version(ino).await? + 1;

        let blocks = div_ceil(size, BLOCK_SIZE as u64);
        let mut buf = vec![0; BLOCK_SIZE];

        for block in 0..blocks {
            let copied = io::copy(&mut r, &mut buf).await? as usize;
            self.create_block(ino, next_version, block, &buf[..copied])
                .await?;
        }

        self.set_version(ino, next_version).await?;
        Ok(())
    }

    async fn read_block(
        &self,
        ino: u64,
        version: u64,
        block: u64,
        offset: usize,
        size: usize,
    ) -> Result<Vec<u8>> {
        let key = ScopedKey::block(ino, version, block);
        let bs = self.kv.get(&key.encode()).await?;
        match bs {
            None => Err(Error::new(
                ErrorKind::NotFound,
                anyhow!(
                    "block ino: {}, version: {}, block: {} is not found",
                    ino,
                    version,
                    block
                ),
            )),
            Some(bs) => Ok(bs.into_iter().skip(offset).take(size).collect()),
        }
    }

    async fn get_version(&self, ino: u64) -> Result<u64> {
        let key = ScopedKey::version(ino);
        let ver = self.kv.get(&key.encode()).await?;
        match ver {
            None => Ok(0),
            Some(bs) => Ok(bincode::deserialize(&bs).map_err(new_bincode_error)?),
        }
    }

    async fn set_version(&self, ino: u64, ver: u64) -> Result<()> {
        let key = ScopedKey::version(ino);
        let bs = bincode::serialize(&ver).map_err(new_bincode_error)?;
        self.kv.set(&key.encode(), &bs).await
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
        let entry = KeyValueEntry::new(inode, ObjectMode::FILE);
        self.create_entry(parent, name, entry).await?;

        Ok(inode)
    }

    async fn create_file_with(&self, parent: u64, name: &str, meta: ObjectMetadata) -> Result<u64> {
        let name = name.trim_matches('/');

        #[cfg(debug_assertions)]
        {
            let p = self.get_inode(parent).await?;
            assert!(p.mode().is_dir());
        }

        let inode = self.get_next_inode().await?;
        self.create_inode(inode, meta).await?;
        let entry = KeyValueEntry::new(inode, ObjectMode::FILE);
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

/// Use 64 KiB as a block.
const BLOCK_SIZE: usize = 64 * 1024;

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

fn new_bincode_error(err: bincode::Error) -> Error {
    Error::new(ErrorKind::Other, anyhow!("bincode: {:?}", err))
}

fn div_ceil(lhs: u64, rhs: u64) -> u64 {
    let d = lhs / rhs;
    let r = lhs % rhs;
    if r > 0 && rhs > 0 {
        d + 1
    } else {
        d
    }
}

/// Returns (block_id, offset, size)
fn calculate_blocks(offset: u64, size: u64) -> Vec<(u64, usize, usize)> {
    let start_block = offset / BLOCK_SIZE as u64;

    let skipped_rem = offset % BLOCK_SIZE as u64;
    let read_blocks = (size + skipped_rem) / BLOCK_SIZE as u64;
    let read_rem = (size + skipped_rem) % BLOCK_SIZE as u64;
    // read_blocks == 0 means we are reading the part of first block.
    if read_blocks == 0 {
        return vec![(start_block, skipped_rem as usize, size as usize)];
    }

    let mut blocks = Vec::new();
    blocks.push((
        start_block,
        skipped_rem as usize,
        BLOCK_SIZE - skipped_rem as usize,
    ));
    for idx in 1..read_blocks {
        blocks.push((start_block + idx, 0, BLOCK_SIZE));
    }
    if read_rem != 0 {
        blocks.push((start_block + read_blocks, 0, read_rem as usize));
    }

    blocks
}

struct BlockReader<S: KeyValueAccessor> {
    backend: Backend<S>,
    ino: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_blocks() {
        let cases = vec![
            ("one block without offset", 0, 128, vec![(0, 0, 128)]),
            ("one block with offset", 128, 128, vec![(0, 128, 128)]),
            (
                "block with overlapped size",
                128,
                BLOCK_SIZE + 1,
                vec![(0, 128, BLOCK_SIZE - 128), (1, 0, 129)],
            ),
            (
                "block with 2 block size",
                BLOCK_SIZE,
                2 * BLOCK_SIZE,
                vec![(1, 0, BLOCK_SIZE), (2, 0, BLOCK_SIZE)],
            ),
            (
                "block with extra size",
                BLOCK_SIZE,
                2 * BLOCK_SIZE + 1,
                vec![(1, 0, BLOCK_SIZE), (2, 0, BLOCK_SIZE), (3, 0, 1)],
            ),
            (
                "block with offset",
                BLOCK_SIZE + 1,
                2 * BLOCK_SIZE - 1,
                vec![(1, 1, BLOCK_SIZE - 1), (2, 0, BLOCK_SIZE)],
            ),
            (
                "block remain the first bytes",
                BLOCK_SIZE - 1,
                2 * BLOCK_SIZE - 1,
                vec![
                    (0, BLOCK_SIZE - 1, 1),
                    (1, 0, BLOCK_SIZE),
                    (2, 0, BLOCK_SIZE - 2),
                ],
            ),
            (
                "block remain the first bytes v2",
                BLOCK_SIZE - 1,
                2 * BLOCK_SIZE,
                vec![
                    (0, BLOCK_SIZE - 1, 1),
                    (1, 0, BLOCK_SIZE),
                    (2, 0, BLOCK_SIZE - 1),
                ],
            ),
        ];
        for (name, offset, size, expected) in cases {
            let actual = calculate_blocks(offset as u64, size as u64);
            assert_eq!(expected, actual);
        }
    }
}
