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
use std::sync::Arc;

use super::*;
use crate::raw::*;
use crate::*;
use async_trait::async_trait;
use futures::io::Cursor;
use futures::AsyncReadExt;

#[derive(Debug, Clone)]
pub struct CacheAccessor {
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,

    strategy: CacheStrategy,
    policy: Arc<dyn CachePolicy>,
}

impl CacheAccessor {
    pub fn new(
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        strategy: CacheStrategy,
        policy: Arc<dyn CachePolicy>,
    ) -> Self {
        CacheAccessor {
            inner,
            cache,
            strategy,
            policy,
        }
    }
}

#[async_trait]
impl Accessor for CacheAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.cache
            .delete(&format_meta_cache_path(path), OpDelete::new())
            .await?;
        self.inner.create(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        let (cache_read, cache_fill_method) = self.policy.on_read(path, &args);

        match self.strategy {
            CacheStrategy::Whole => {
                new_whole_cache_reader(
                    self.inner.clone(),
                    self.cache.clone(),
                    path,
                    args,
                    cache_read,
                    cache_fill_method,
                )
                .await
            }
            CacheStrategy::Fixed(step) => {
                new_fixed_cache_reader(
                    self.inner.clone(),
                    self.cache.clone(),
                    path,
                    args,
                    step,
                    cache_read,
                    cache_fill_method,
                )
                .await
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
        self.cache
            .delete(&format_meta_cache_path(path), OpDelete::new())
            .await?;
        self.inner.write(path, args, r).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        match self
            .cache
            .read(&format_meta_cache_path(path), OpRead::new())
            .await
        {
            Ok((_, mut r)) => {
                let mut bs = Vec::with_capacity(1024);
                r.read_to_end(&mut bs).await.map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "read object metadata from cache")
                        .set_source(err)
                })?;

                let meta = self.decode_metadata(&bs)?;
                Ok(RpStat::new(meta))
            }
            Err(err) if err.kind() == ErrorKind::ObjectNotFound => {
                let meta = self.inner.stat(path, args).await?.into_metadata();
                let bs = self.encode_metadata(&meta)?;
                self.cache
                    .write(
                        path,
                        OpWrite::new(bs.len() as u64),
                        Box::new(Cursor::new(bs)),
                    )
                    .await?;
                Ok(RpStat::new(meta))
            }
            // We will ignore any other errors happened in cache.
            Err(_) => self.inner.stat(path, args).await,
        }
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.cache
            .delete(&format_meta_cache_path(path), OpDelete::new())
            .await?;
        self.inner.delete(path, args).await
    }
}

impl CacheAccessor {
    fn encode_metadata(&self, meta: &ObjectMetadata) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(meta, bincode::config::standard()).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "encode object metadata into cache")
                .with_operation("CacheLayer::encode_metadata")
                .set_source(err)
        })
    }

    fn decode_metadata(&self, bs: &[u8]) -> Result<ObjectMetadata> {
        let (meta, _) = bincode::serde::decode_from_slice(bs, bincode::config::standard())
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "decode object metadata from cache")
                    .with_operation("CacheLayer::decode_metadata")
                    .set_source(err)
            })?;
        Ok(meta)
    }
}

/// Build the path for OpenDAL Metadata Cache.
fn format_meta_cache_path(path: &str) -> String {
    format!("{path}.omc")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::memory;
    use crate::Operator;

    #[tokio::test]
    async fn test_whole_content_cache() -> anyhow::Result<()> {
        let op = Operator::new(memory::Builder::default().build()?);

        let cache_layer = CacheLayer::new(Arc::new(memory::Builder::default().build()?).into())
            .with_strategy(CacheStrategy::Whole);
        let cached_op = op.clone().layer(cache_layer);

        // Write a new object into op.
        op.object("test_exist")
            .write("Hello, World!".as_bytes())
            .await?;

        // Read from cached op.
        let data = cached_op.object("test_exist").read().await?;
        assert_eq!(data.len(), 13);

        // Wait for https://github.com/datafuselabs/opendal/issues/957
        // // Write into cache op.
        // cached_op
        //     .object("test_exist")
        //     .write("Hello, Xuanwo!".as_bytes())
        //     .await?;
        // // op and cached op should have same data.
        // let data = op.object("test_exist").read().await?;
        // assert_eq!(data.len(), 14);
        // let data = cached_op.object("test_exist").read().await?;
        // assert_eq!(data.len(), 14);

        // Read not exist object.
        let data = cached_op.object("test_not_exist").read().await;
        assert_eq!(data.unwrap_err().kind(), ErrorKind::ObjectNotFound);

        Ok(())
    }

    #[tokio::test]
    async fn test_fixed_content_cache() -> anyhow::Result<()> {
        let op = Operator::new(memory::Builder::default().build()?);

        let cache_layer = CacheLayer::new(Arc::new(memory::Builder::default().build()?).into())
            .with_strategy(CacheStrategy::Fixed(5));
        let cached_op = op.clone().layer(cache_layer);

        // Write a new object into op.
        op.object("test_exist")
            .write("Hello, World!".as_bytes())
            .await?;

        // Read from cached op.
        let data = cached_op.object("test_exist").read().await?;
        assert_eq!(data.len(), 13);

        // Wait for https://github.com/datafuselabs/opendal/issues/957
        // Write into cache op.
        // cached_op
        //     .object("test_exist")
        //     .write("Hello, Xuanwo!".as_bytes())
        //     .await?;
        // // op and cached op should have same data.
        // let data = op.object("test_exist").read().await?;
        // assert_eq!(data.len(), 14);
        // let data = cached_op.object("test_exist").read().await?;
        // assert_eq!(data.len(), 14);

        // Read part of data
        let data = cached_op.object("test_exist").range_read(5..).await?;
        assert_eq!(data.len(), 8);
        assert_eq!(data, ", World!".as_bytes());

        // Write a new object into op.
        op.object("test_new")
            .write("Hello, OpenDAL!".as_bytes())
            .await?;

        // Read part of data
        let data = cached_op.object("test_new").range_read(6..).await?;
        assert_eq!(data.len(), 9);
        assert_eq!(data, " OpenDAL!".as_bytes());

        // Read not exist object.
        let data = cached_op.object("test_not_exist").read().await;
        assert_eq!(data.unwrap_err().kind(), ErrorKind::ObjectNotFound);

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_cache() -> anyhow::Result<()> {
        let op = Operator::new(memory::Builder::default().build()?);

        let cache_layer = CacheLayer::new(Arc::new(memory::Builder::default().build()?).into())
            .with_strategy(CacheStrategy::Fixed(5));
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
        assert_eq!(meta.unwrap_err().kind(), ErrorKind::ObjectNotFound);

        Ok(())
    }
}
