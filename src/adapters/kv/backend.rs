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

use std::cmp::min;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::vec::IntoIter;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Buf;
use bytes::BytesMut;
use futures::future::BoxFuture;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::Future;
use futures::Stream;
use futures::TryStreamExt;
use pin_project::pin_project;
use time::OffsetDateTime;
use tokio::sync::OnceCell;

use super::Adapter;
use super::Key;
use super::KeyStreamer;
use super::BLOCK_SIZE;
use super::INODE_ROOT;
use crate::object::EmptyObjectStreamer;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::path::build_rooted_abs_path;
use crate::path::get_basename;
use crate::path::get_parent;
use crate::path::normalize_root;
use crate::Accessor;
use crate::AccessorCapability;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::ObjectEntry;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::ObjectStreamer;

/// Backend of kv service.
#[derive(Debug, Clone)]
pub struct Backend<S: Adapter> {
    kv: S,
    root: String,
    initiated: OnceCell<u64>,
}

impl<S> Backend<S>
where
    S: Adapter,
{
    /// Create a new kv backend.
    pub fn new(kv: S) -> Self {
        Self {
            kv,
            root: "/".to_string(),
            initiated: OnceCell::new(),
        }
    }

    /// Configure root within this backend.
    pub fn with_root(mut self, root: &str) -> Self {
        self.root = normalize_root(root);
        self
    }
}

#[async_trait]
impl<S> Accessor for Backend<S>
where
    S: Adapter,
{
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_root(&self.root);
        am.set_capabilities(
            AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
        );

        let kvam = self.kv.metadata();
        am.set_scheme(kvam.scheme());
        am.set_name(kvam.name());
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
        let meta = self.get_inode(inode).await?;
        let blocks = calculate_blocks(
            args.offset().unwrap_or_default(),
            args.size().unwrap_or_else(|| meta.content_length()),
        );
        let r = BlockReader::new(self.clone(), inode, blocks);
        Ok(Box::new(r))
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

    async fn delete(&self, path: &str, _: OpDelete) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);
        let parent = get_parent(&p);
        let basename = get_basename(&p);
        let parent_inode = match self.lookup(parent).await {
            Ok(inode) => inode,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };
        let inode = match self.get_entry(parent_inode, basename).await {
            Ok(inode) => inode,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };

        self.remove_entry(parent_inode, basename).await?;
        self.remove_inode(inode).await?;
        self.remove_blocks(inode).await?;
        Ok(())
    }

    async fn list(&self, path: &str, _: OpList) -> Result<ObjectStreamer> {
        let p = build_rooted_abs_path(&self.root, path);
        let inode = match self.lookup(&p).await {
            Ok(inode) => inode,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(Box::new(EmptyObjectStreamer))
            }
            Err(err) => return Err(err),
        };
        let s = self.list_entries(inode).await?;
        let os = ObjectStream::new(Arc::new(self.clone()), s, path.to_string());
        Ok(Box::new(os))
    }
}

impl<S: Adapter> Backend<S> {
    /// Get next inode.
    ///
    /// This function is used to fetch and update the next inode.
    async fn get_next_inode(&self) -> Result<u64> {
        self.kv.next_id().await
    }

    /// Get object metadata by inode.
    async fn get_inode(&self, ino: u64) -> Result<ObjectMetadata> {
        // Handle root inode.
        if ino == INODE_ROOT {
            return Ok(ObjectMetadata::new(ObjectMode::DIR));
        }

        let bs = self.kv.get(&Key::inode(ino).encode()).await?;
        match bs {
            None => Err(Error::new(
                ErrorKind::NotFound,
                anyhow!("inode {} not found", ino),
            )),
            Some(bs) => {
                let (meta, _) = bincode::serde::decode_from_slice(&bs, bincode::config::standard())
                    .map_err(new_bincode_decode_error)?;
                Ok(meta)
            }
        }
    }

    /// Create a new inode.
    async fn create_inode(&self, ino: u64, meta: ObjectMetadata) -> Result<()> {
        let key = Key::inode(ino);
        let value = bincode::serde::encode_to_vec(&meta, bincode::config::standard())
            .map_err(new_bincode_encode_error)?;
        self.kv.set(&key.encode(), &value).await
    }

    /// Remove inode.
    async fn remove_inode(&self, ino: u64) -> Result<()> {
        let key = Key::inode(ino);
        self.kv.delete(&key.encode()).await
    }

    /// Create a new entry.
    async fn create_entry(&self, parent: u64, name: &str, inode: u64) -> Result<()> {
        let key = Key::entry(parent, name);
        let value = bincode::encode_to_vec(&inode, bincode::config::standard())
            .map_err(new_bincode_encode_error)?;
        self.kv.set(&key.encode(), &value).await
    }

    /// Get the inode of an entry by parent, and it's name.
    async fn get_entry(&self, parent: u64, name: &str) -> Result<u64> {
        let key = Key::entry(parent, name);
        let bs = self.kv.get(&key.encode()).await?;
        match bs {
            None => Err(Error::new(
                ErrorKind::NotFound,
                anyhow!("entry parent: {}, name: {} is not found", parent, name),
            )),
            Some(bs) => {
                let (ino, _) = bincode::decode_from_slice(&bs, bincode::config::standard())
                    .map_err(new_bincode_decode_error)?;
                Ok(ino)
            }
        }
    }

    /// Remove entry.
    async fn remove_entry(&self, parent: u64, name: &str) -> Result<()> {
        let key = Key::entry(parent, name);
        self.kv.delete(&key.encode()).await
    }

    /// List all entries by parent's inode.
    async fn list_entries(&self, parent: u64) -> Result<KeyStreamer> {
        self.kv.scan(&Key::entry_prefix(parent)).await
    }

    /// Create a new block by inode, version and block id.
    async fn create_block(&self, ino: u64, block: u64, content: &[u8]) -> Result<()> {
        let key = Key::block(ino, block);
        self.kv.set(&key.encode(), content).await
    }

    /// Write blocks by inode, version and input data.
    async fn write_blocks(&self, ino: u64, size: u64, mut r: BytesReader) -> Result<()> {
        let blocks = size / BLOCK_SIZE as u64;
        let remain = (size % BLOCK_SIZE as u64) as usize;

        let mut buf = vec![0; BLOCK_SIZE];

        for block in 0..blocks {
            r.read_exact(&mut buf).await?;
            self.create_block(ino, block, &buf).await?;
        }
        if remain != 0 {
            r.read_exact(&mut buf[..remain]).await?;
            self.create_block(ino, blocks, &buf[..remain]).await?;
        }

        Ok(())
    }

    /// Read a block by its inode, version, block id along with offset and size.
    async fn read_block(
        &self,
        ino: u64,
        block: u64,
        offset: usize,
        size: usize,
    ) -> Result<Vec<u8>> {
        debug_assert!(
            offset + size <= BLOCK_SIZE,
            "given offset {} size {} must be lower then block size",
            offset,
            size
        );

        let key = Key::block(ino, block);
        let bs = self.kv.get(&key.encode()).await?;
        match bs {
            None => Err(Error::new(
                ErrorKind::NotFound,
                anyhow!("block ino: {},  block: {} is not found", ino, block),
            )),
            Some(bs) => {
                if offset == 0 && size == bs.len() {
                    Ok(bs)
                } else {
                    Ok(bs.into_iter().skip(offset).take(size).collect())
                }
            }
        }
    }

    /// Remove all blocks.
    async fn remove_blocks(&self, ino: u64) -> Result<()> {
        let mut keys = self.kv.scan(&Key::block_prefix(ino)).await?;
        while let Some(key) = keys.try_next().await? {
            self.kv.delete(&key).await?;
        }
        Ok(())
    }

    /// Create a dir.
    async fn create_dir(&self, parent: u64, name: &str) -> Result<u64> {
        #[cfg(debug_assertions)]
        {
            let p = self.get_inode(parent).await?;
            assert!(p.mode().is_dir())
        }

        let inode = self.get_next_inode().await?;
        let meta = ObjectMetadata::new(ObjectMode::DIR);
        self.create_entry(parent, name, inode).await?;
        self.create_inode(inode, meta).await?;

        Ok(inode)
    }

    /// Create an empty file.
    async fn create_file(&self, parent: u64, name: &str) -> Result<u64> {
        #[cfg(debug_assertions)]
        {
            let p = self.get_inode(parent).await?;
            assert!(p.mode().is_dir());
        }

        let inode = self.get_next_inode().await?;
        let meta = ObjectMetadata::new(ObjectMode::FILE)
            .with_last_modified(OffsetDateTime::now_utc())
            .with_content_length(0);
        self.create_entry(parent, name, inode).await?;
        self.create_inode(inode, meta).await?;

        Ok(inode)
    }

    /// Create a file with specified object metadata.
    async fn create_file_with(&self, parent: u64, name: &str, meta: ObjectMetadata) -> Result<u64> {
        #[cfg(debug_assertions)]
        {
            let p = self.get_inode(parent).await?;
            assert!(p.mode().is_dir());
        }

        let inode = self.get_next_inode().await?;
        self.create_entry(parent, name, inode).await?;
        self.create_inode(inode, meta).await?;

        Ok(inode)
    }

    /// Get the inode of given path.
    async fn lookup(&self, path: &str) -> Result<u64> {
        if path == "/" {
            return Ok(INODE_ROOT);
        }

        let _ = self
            .initiated
            .get_or_try_init(|| async { self.create_dir_parents(&self.root).await })
            .await?;

        let mut inode = INODE_ROOT;
        for name in path.split('/').filter(|v| !v.is_empty()) {
            inode = self.get_entry(inode, name).await?;
        }
        Ok(inode)
    }

    /// Create a dir with all its parents.
    async fn create_dir_parents(&self, path: &str) -> Result<u64> {
        if path == "/" {
            return Ok(INODE_ROOT);
        }

        let mut inode = INODE_ROOT;
        for name in path.split('/').filter(|v| !v.is_empty()) {
            let entry = self.get_entry(inode, name).await;
            inode = match entry {
                Ok(entry) => entry,
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    self.create_dir(inode, name).await?
                }
                Err(err) => return Err(err),
            }
        }
        Ok(inode)
    }
}

fn new_bincode_encode_error(err: bincode::error::EncodeError) -> Error {
    Error::new(ErrorKind::Other, anyhow!("bincode encode: {:?}", err))
}

fn new_bincode_decode_error(err: bincode::error::DecodeError) -> Error {
    Error::new(ErrorKind::Other, anyhow!("bincode decode: {:?}", err))
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

#[pin_project]
struct BlockReader<S: Adapter> {
    backend: Backend<S>,
    ino: u64,
    blocks: IntoIter<(u64, usize, usize)>,
    buf: BytesMut,
    fut: Option<BoxFuture<'static, Result<Vec<u8>>>>,
}

impl<S: Adapter> BlockReader<S> {
    pub fn new(backend: Backend<S>, ino: u64, blocks: Vec<(u64, usize, usize)>) -> Self {
        Self {
            backend,
            ino,
            blocks: blocks.into_iter(),
            buf: BytesMut::with_capacity(BLOCK_SIZE),
            fut: None,
        }
    }
}

impl<S> AsyncRead for BlockReader<S>
where
    S: Adapter,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let mut this = self.project();

        loop {
            if this.buf.has_remaining() {
                let size = min(buf.len(), this.buf.len());
                buf[..size].copy_from_slice(&this.buf[..size]);
                this.buf.advance(size);
                return Poll::Ready(Ok(size));
            }

            match &mut this.fut {
                None => match this.blocks.next() {
                    None => return Poll::Ready(Ok(0)),
                    Some((block, offset, size)) => {
                        let backend = this.backend.clone();
                        let ino = *this.ino;
                        let fut = async move { backend.read_block(ino, block, offset, size).await };
                        *this.fut = Some(Box::pin(fut));
                        continue;
                    }
                },
                Some(fut) => {
                    let bs = ready!(Pin::new(fut).poll(cx)?);
                    *this.fut = None;

                    // Clear the buf to reuse the same space.
                    this.buf.clear();
                    // Extend from given slice.
                    this.buf.extend_from_slice(&bs);
                    continue;
                }
            }
        }
    }
}

#[pin_project]
struct ObjectStream<S: Adapter> {
    backend: Arc<Backend<S>>,
    stream: KeyStreamer,
    path: String,
    current: String,
    fut: Option<BoxFuture<'static, Result<ObjectMetadata>>>,
}

impl<S> ObjectStream<S>
where
    S: Adapter + 'static,
{
    pub fn new(backend: Arc<Backend<S>>, stream: KeyStreamer, path: String) -> Self {
        Self {
            backend,
            stream,
            path: {
                if path == "/" {
                    "".to_string()
                } else {
                    path
                }
            },
            current: "".to_string(),
            fut: None,
        }
    }
}

impl<S> Stream for ObjectStream<S>
where
    S: Adapter + 'static,
{
    type Item = Result<ObjectEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match &mut this.fut {
                None => {
                    let key = ready!(Pin::new(&mut this.stream).poll_next(cx));
                    match key {
                        None => return Poll::Ready(None),
                        Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                        Some(Ok(bs)) => {
                            let backend = this.backend.clone();
                            let key = Key::decode(&bs)?;
                            let (parent, name) = key.into_entry();
                            *this.current = name.clone();
                            let fut = async move {
                                let inode = backend.get_entry(parent, &name).await?;
                                backend.get_inode(inode).await
                            };
                            *this.fut = Some(Box::pin(fut));
                            continue;
                        }
                    }
                }
                Some(fut) => {
                    let om = ready!(Pin::new(fut).poll(cx))?;
                    *this.fut = None;

                    let path = if om.mode().is_dir() {
                        this.path.clone() + this.current + "/"
                    } else {
                        this.path.clone() + this.current
                    };
                    return Poll::Ready(Some(Ok(ObjectEntry::new(
                        this.backend.clone(),
                        &path,
                        om,
                    ))));
                }
            }
        }
    }
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
            assert_eq!(expected, actual, "{}", name);
        }
    }
}
