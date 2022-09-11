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

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::vec::IntoIter;

use async_trait::async_trait;

use crate::accessor::AccessorCapability;
use crate::ops::OpAbortMultipart;
use crate::ops::OpCompleteMultipart;
use crate::ops::OpCreate;
use crate::ops::OpCreateMultipart;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpPresign;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::OpWriteMultipart;
use crate::ops::PresignedRequest;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BlockingBytesReader;
use crate::BytesReader;
use crate::DirEntry;
use crate::DirIterator;
use crate::DirStreamer;
use crate::Layer;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::ObjectPart;

/// ImmutableIndexLayer is used to add an immutable in-memory index for
/// underlying storage services.
///
/// Especially useful for services without list capability like HTTP.
///
/// # Examples
///
/// ```rust, no_run
/// use opendal::layers::ImmutableIndexLayer;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let mut iil = ImmutableIndexLayer::default();
///
/// for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
///     iil.insert(i.to_string())
/// }
///
/// let op = Operator::from_env(Scheme::Http).unwrap().layer(iil);
/// ```
#[derive(Default, Debug, Clone)]
pub struct ImmutableIndexLayer {
    set: BTreeSet<String>,
}

impl ImmutableIndexLayer {
    /// Insert a key into index.
    pub fn insert(&mut self, key: String) {
        self.set.insert(key);
    }

    /// Insert keys from iter.
    pub fn extend_iter<T, I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = String>,
    {
        self.set.extend(iter);
    }
}

impl Layer for ImmutableIndexLayer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(ImmutableIndexAccessor {
            set: self.set.clone(),
            inner,
        })
    }
}

#[derive(Debug, Clone)]
struct ImmutableIndexAccessor {
    inner: Arc<dyn Accessor>,
    /// TODO: we can introduce trie here to lower the memory footprint.
    set: BTreeSet<String>,
}

impl ImmutableIndexAccessor {
    fn children(&self, path: &str) -> Vec<String> {
        let mut res = Vec::new();

        for i in self.set.iter() {
            // `/xyz` should not belong to `/abc`
            if !i.starts_with(path) {
                continue;
            }

            // remove `/abc` if self
            if i == path {
                continue;
            }

            match i[path.len()..].find('/') {
                // File `/abc/def.csv` must belong to `/abc`
                None => res.push(i.to_string()),
                Some(idx) => {
                    // The index of first `/` after `/abc`.
                    let dir_idx = idx + 1 + path.len();

                    if dir_idx == i.len() {
                        // Dir `/abc/def/` belongs to `/abc/`
                        res.push(i.to_string())
                    } else {
                        // File/Dir `/abc/def/xyz` deoesn't belongs to `/abc`.
                        // But we need to list `/abc/def` out so that we can walk down.
                        res.push(i[..dir_idx].to_string())
                    }
                }
            }
        }

        res
    }
}

#[async_trait]
impl Accessor for ImmutableIndexAccessor {
    /// Add list capabilities for underlying storage services.
    fn metadata(&self) -> AccessorMetadata {
        let mut meta = self.inner.metadata();
        meta.set_capabilities(meta.capabilities() | AccessorCapability::List);

        meta
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        self.inner.create(args).await
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        self.inner.read(args).await
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        self.inner.write(args, r).await
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        self.inner.stat(args).await
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        self.inner.delete(args).await
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let mut path = args.path();
        if path == "/" {
            path = ""
        }

        Ok(Box::new(ImmutableDir::new(
            Arc::new(self.clone()),
            self.children(path),
        )))
    }

    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        self.inner.presign(args)
    }

    async fn create_multipart(&self, args: &OpCreateMultipart) -> Result<String> {
        self.inner.create_multipart(args).await
    }

    async fn write_multipart(&self, args: &OpWriteMultipart, r: BytesReader) -> Result<ObjectPart> {
        self.inner.write_multipart(args, r).await
    }

    async fn complete_multipart(&self, args: &OpCompleteMultipart) -> Result<()> {
        self.inner.complete_multipart(args).await
    }

    async fn abort_multipart(&self, args: &OpAbortMultipart) -> Result<()> {
        self.inner.abort_multipart(args).await
    }

    fn blocking_create(&self, args: &OpCreate) -> Result<()> {
        self.inner.blocking_create(args)
    }

    fn blocking_read(&self, args: &OpRead) -> Result<BlockingBytesReader> {
        self.inner.blocking_read(args)
    }

    fn blocking_write(&self, args: &OpWrite, r: BlockingBytesReader) -> Result<u64> {
        self.inner.blocking_write(args, r)
    }

    fn blocking_stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        self.inner.blocking_stat(args)
    }

    fn blocking_delete(&self, args: &OpDelete) -> Result<()> {
        self.inner.blocking_delete(args)
    }

    fn blocking_list(&self, args: &OpList) -> Result<DirIterator> {
        let mut path = args.path();
        if path == "/" {
            path = ""
        }

        Ok(Box::new(ImmutableDir::new(
            Arc::new(self.clone()),
            self.children(path),
        )))
    }
}

struct ImmutableDir {
    backend: Arc<ImmutableIndexAccessor>,
    idx: IntoIter<String>,
}

impl ImmutableDir {
    fn new(backend: Arc<ImmutableIndexAccessor>, idx: Vec<String>) -> Self {
        Self {
            backend,
            idx: idx.into_iter(),
        }
    }
}

impl Iterator for ImmutableDir {
    type Item = Result<DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.idx.next() {
            None => None,
            Some(path) => {
                let mode = if path.ends_with('/') {
                    ObjectMode::DIR
                } else {
                    ObjectMode::FILE
                };

                Some(Ok(DirEntry::new(self.backend.clone(), mode, &path)))
            }
        }
    }
}

impl futures::Stream for ImmutableDir {
    type Item = Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.idx.next() {
            None => Poll::Ready(None),
            Some(path) => {
                let mode = if path.ends_with('/') {
                    ObjectMode::DIR
                } else {
                    ObjectMode::FILE
                };

                Poll::Ready(Some(Ok(DirEntry::new(self.backend.clone(), mode, &path))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Result;
    use futures::TryStreamExt;

    use crate::layers::immutable_index::ImmutableIndexLayer;
    use crate::layers::LoggingLayer;
    use crate::ObjectMode;
    use crate::Operator;
    use crate::Scheme;

    #[test]
    fn test_blocking_list() -> Result<()> {
        let _ = env_logger::try_init();

        let mut iil = ImmutableIndexLayer::default();
        for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
            iil.insert(i.to_string())
        }

        let op = Operator::from_iter(
            Scheme::Http,
            vec![("endpoint".to_string(), "https://xuanwo.io".to_string())].into_iter(),
        )?
        .layer(LoggingLayer)
        .layer(iil);

        let mut map = HashMap::new();
        for entry in op.object("").blocking_list()? {
            let entry = entry?;
            map.insert(entry.path().to_string(), entry.mode());
        }

        assert_eq!(map["file"], ObjectMode::FILE);
        assert_eq!(map["dir/"], ObjectMode::DIR);
        assert_eq!(map["dir_without_prefix/"], ObjectMode::DIR);
        Ok(())
    }

    #[tokio::test]
    async fn test_list() -> Result<()> {
        let _ = env_logger::try_init();

        let mut iil = ImmutableIndexLayer::default();
        for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
            iil.insert(i.to_string())
        }

        let op = Operator::from_iter(
            Scheme::Http,
            vec![("endpoint".to_string(), "https://xuanwo.io".to_string())].into_iter(),
        )?
        .layer(LoggingLayer)
        .layer(iil);

        let mut map = HashMap::new();
        let mut ds = op.object("").list().await?;
        while let Some(entry) = ds.try_next().await? {
            map.insert(entry.path().to_string(), entry.mode());
        }

        assert_eq!(map["file"], ObjectMode::FILE);
        assert_eq!(map["dir/"], ObjectMode::DIR);
        assert_eq!(map["dir_without_prefix/"], ObjectMode::DIR);
        Ok(())
    }
}
