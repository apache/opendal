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

use std::fmt::Debug;
use std::io::ErrorKind;
use std::io::Result;
use std::sync::Arc;

use async_trait::async_trait;
use futures::io;
use futures::io::Cursor;

use crate::error::new_other_object_error;
use crate::multipart::ObjectPart;
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
use crate::ops::Operation;
use crate::ops::PresignedRequest;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BlockingBytesReader;
use crate::BytesReader;
use crate::DirIterator;
use crate::DirStreamer;
use crate::Layer;
use crate::ObjectMetadata;

/// MetadataCacheLayer will add metadata cache support for OpenDAL.
///
/// # Notes
///
/// This layer only maintains its own states. Users should care about the cache
/// consistency by themselves. For example, in the following situations, users
/// could get out-dated metadata cache:
///
/// - Users have operations on underlying operator directly.
/// - Other nodes have operations on underlying storage directly.
/// - Concurrent read/write/delete on the same path.
///
/// To make sure metadata cache consistent across the cluster, please make sure
/// all nodes in the cluster use the same cache services like redis or tikv.
///
/// # Examples
///
/// ```
/// use anyhow::Result;
/// use opendal::layers::MetadataCacheLayer;
/// use opendal::Operator;
/// use opendal::Scheme;
/// use opendal::services::memory;
///
/// let _ = Operator::from_env(Scheme::Fs)
///     .expect("must init")
///     .layer(MetadataCacheLayer::new(memory::Builder::default().build().expect("must init")));
/// ```
#[derive(Debug, Clone)]
pub struct MetadataCacheLayer {
    cache: Arc<dyn Accessor>,
}

impl MetadataCacheLayer {
    /// Create a new metadata cache layer.
    pub fn new(acc: impl Accessor + 'static) -> Self {
        Self {
            cache: Arc::new(acc),
        }
    }
}

impl Layer for MetadataCacheLayer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(MetadataCacheAccessor {
            cache: self.cache.clone(),
            inner,
        })
    }
}

#[derive(Debug)]
struct MetadataCacheAccessor {
    cache: Arc<dyn Accessor>,
    inner: Arc<dyn Accessor>,
}

#[async_trait]
impl Accessor for MetadataCacheAccessor {
    fn metadata(&self) -> AccessorMetadata {
        self.inner.metadata()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.cache.delete(path, OpDelete::new()).await?;
        self.inner.create(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        self.cache.delete(path, OpDelete::new()).await?;
        self.inner.write(path, args, r).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        match self.cache.read(path, OpRead::new(..)).await {
            Ok(r) => {
                let buffer = Vec::with_capacity(1024);
                let mut bs = Cursor::new(buffer);
                io::copy(r, &mut bs).await?;

                let meta = bincode::deserialize(&bs.into_inner())
                    .map_err(|err| new_other_object_error(Operation::Stat, path, err))?;
                Ok(meta)
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let meta = self.inner.stat(path, args).await?;
                let bs = bincode::serialize(&meta)
                    .map_err(|err| new_other_object_error(Operation::Stat, path, err))?;
                self.cache
                    .write(
                        path,
                        OpWrite::new(bs.len() as u64),
                        Box::new(Cursor::new(bs)),
                    )
                    .await?;
                Ok(meta)
            }
            Err(err) => Err(err),
        }
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<()> {
        self.cache.delete(path, OpDelete::new()).await?;
        self.inner.delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<DirStreamer> {
        self.inner.list(path, args).await
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<PresignedRequest> {
        self.inner.presign(path, args)
    }

    async fn create_multipart(&self, path: &str, args: OpCreateMultipart) -> Result<String> {
        self.inner.create_multipart(path, args).await
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: BytesReader,
    ) -> Result<ObjectPart> {
        self.inner.write_multipart(path, args, r).await
    }

    async fn complete_multipart(&self, path: &str, args: OpCompleteMultipart) -> Result<()> {
        self.cache.delete(path, OpDelete::new()).await?;
        self.inner.complete_multipart(path, args).await
    }

    async fn abort_multipart(&self, path: &str, args: OpAbortMultipart) -> Result<()> {
        self.inner.abort_multipart(path, args).await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.cache.blocking_delete(path, OpDelete::new())?;
        self.inner.blocking_create(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        self.inner.blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite, r: BlockingBytesReader) -> Result<u64> {
        self.cache.blocking_delete(path, OpDelete::new())?;
        self.inner.blocking_write(path, args, r)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        match self.cache.blocking_read(path, OpRead::new(..)) {
            Ok(r) => {
                let meta = bincode::deserialize_from(r)
                    .map_err(|err| new_other_object_error(Operation::BlockingStat, path, err))?;
                Ok(meta)
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let meta = self.inner.blocking_stat(path, args)?;
                let bs = bincode::serialize(&meta)
                    .map_err(|err| new_other_object_error(Operation::BlockingStat, path, err))?;
                self.cache.blocking_write(
                    path,
                    OpWrite::new(bs.len() as u64),
                    Box::new(std::io::Cursor::new(bs)),
                )?;
                Ok(meta)
            }
            Err(err) => Err(err),
        }
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<()> {
        self.cache.blocking_delete(path, OpDelete::new())?;
        self.inner.blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<DirIterator> {
        self.inner.blocking_list(path, args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::memory;
    use crate::Operator;

    #[tokio::test]
    async fn test_metadata_cache() -> anyhow::Result<()> {
        let op = Operator::new(memory::Builder::default().build()?);

        let cache_layer = MetadataCacheLayer::new(memory::Builder::default().build()?);
        let cached_op = op.clone().layer(cache_layer);

        // Write a new object into op.
        op.object("test_exist")
            .write("Hello, World!".as_bytes())
            .await?;
        // Stat from cached op.
        let meta = cached_op.object("test_exist").metadata().await?;
        assert_eq!(meta.content_length(), 13);

        // Write into cache op.
        cached_op
            .object("test_exist")
            .write("Hello, Xuanwo!".as_bytes())
            .await?;
        // op and cached op should have same data.
        let meta = op.object("test_exist").metadata().await?;
        assert_eq!(meta.content_length(), 14);
        let meta = cached_op.object("test_exist").metadata().await?;
        assert_eq!(meta.content_length(), 14);

        // Stat not exist object.
        let meta = cached_op.object("test_not_exist").metadata().await;
        assert_eq!(meta.unwrap_err().kind(), ErrorKind::NotFound);

        Ok(())
    }
}
