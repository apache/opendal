// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use async_trait::async_trait;
use bytes;
use bytes::Bytes;
use futures::future::poll_fn;

use crate::raw::oio::ReadExt;
use crate::raw::*;
use crate::*;

/// Add blocking API support for non-blocking services.
///
/// # Notes
///
/// - Please only enable this layer when the underlying service does not support blocking.
///
/// # Examples
///
/// ## In async context
///
/// BlockingLayer will use current async context's runtime to handle the async calls.
///
/// ```rust
/// # use anyhow::Result;
/// use opendal::layers::BlockingLayer;
/// use opendal::services::S3;
/// use opendal::BlockingOperator;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Create fs backend builder.
///     let mut builder = S3::default();
///     builder.bucket("test");
///     builder.region("us-east-1");
///
///     // Build an `BlockingOperator` with blocking layer to start operating the storage.
///     let _: BlockingOperator = Operator::new(builder)?
///         .layer(BlockingLayer::create()?)
///         .finish()
///         .blocking();
///
///     Ok(())
/// }
/// ```
///
/// ## In async context with blocking functions
///
/// If `BlockingLayer` is called in blocking function, please fetch a [`tokio::runtime::EnterGuard`]
/// first. You can use [`Handle::try_current`] first to get the handle and than call [`Handle::enter`].
/// This often happens in the case that async function calls blocking function.
///
/// ```rust
/// use opendal::layers::BlockingLayer;
/// use opendal::services::S3;
/// use opendal::BlockingOperator;
/// use opendal::Operator;
/// use opendal::Result;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let _ = blocking_fn()?;
///     Ok(())
/// }
///
/// fn blocking_fn() -> Result<BlockingOperator> {
///     // Create fs backend builder.
///     let mut builder = S3::default();
///     builder.bucket("test");
///     builder.region("us-east-1");
///
///     let handle = tokio::runtime::Handle::try_current().unwrap();
///     let _guard = handle.enter();
///     // Build an `BlockingOperator` with blocking layer to start operating the storage.
///     let op: BlockingOperator = Operator::new(builder)?
///         .layer(BlockingLayer::create()?)
///         .finish()
///         .blocking();
///     Ok(op)
/// }
/// ```
///
/// ## In blocking context
///
/// In a pure blocking context, we can create a runtime and use it to create the `BlockingLayer`.
///
/// > The following code uses a global statically created runtime as an example, please manage the
/// runtime on demand.
///
/// ```rust
/// use once_cell::sync::Lazy;
/// use opendal::layers::BlockingLayer;
/// use opendal::services::S3;
/// use opendal::BlockingOperator;
/// use opendal::Operator;
/// use opendal::Result;
///
/// static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
///     tokio::runtime::Builder::new_multi_thread()
///         .enable_all()
///         .build()
///         .unwrap()
/// });
/// ///
///
/// fn main() -> Result<()> {
///     // Create fs backend builder.
///     let mut builder = S3::default();
///     builder.bucket("test");
///     builder.region("us-east-1");
///
///     // Fetch the `EnterGuard` from global runtime.
///     let _guard = RUNTIME.enter();
///     // Build an `BlockingOperator` with blocking layer to start operating the storage.
///     let _: BlockingOperator = Operator::new(builder)?
///         .layer(BlockingLayer::create()?)
///         .finish()
///         .blocking();
///
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct BlockingLayer;

impl BlockingLayer {
    /// Create a new `BlockingLayer` with the current runtime's handle
    pub fn create() -> Result<Self> {
        Ok(BlockingLayer)
    }
}

impl<A: Accessor> Layer<A> for BlockingLayer {
    type LayeredAccessor = BlockingAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        BlockingAccessor { inner }
    }
}

#[derive(Clone, Debug)]
pub struct BlockingAccessor<A: Accessor> {
    inner: A,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Accessor> LayeredAccessor for BlockingAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = BlockingWrapper<A::Reader>;
    type Writer = A::Writer;
    type BlockingWriter = BlockingWrapper<A::Writer>;
    type Lister = A::Lister;
    type BlockingLister = BlockingWrapper<A::Lister>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorInfo {
        let mut meta = self.inner.info();
        meta.full_capability_mut().blocking = true;
        meta
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner.copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner.rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner.delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner.batch(args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        pollster::block_on(self.inner.create_dir(path, args))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        pollster::block_on(async {
            let (rp, reader) = self.inner.read(path, args).await?;
            let blocking_reader = Self::BlockingReader::new(reader);

            Ok((rp, blocking_reader))
        })
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        pollster::block_on(async {
            let (rp, writer) = self.inner.write(path, args).await?;
            let blocking_writer = Self::BlockingWriter::new(writer);
            Ok((rp, blocking_writer))
        })
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        pollster::block_on(self.inner.copy(from, to, args))
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        pollster::block_on(self.inner.rename(from, to, args))
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        pollster::block_on(self.inner.stat(path, args))
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        pollster::block_on(self.inner.delete(path, args))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        pollster::block_on(async {
            let (rp, lister) = self.inner.list(path, args).await?;
            let blocking_lister = Self::BlockingLister::new(lister);
            Ok((rp, blocking_lister))
        })
    }
}

pub struct BlockingWrapper<I> {
    inner: I,
}

impl<I> BlockingWrapper<I> {
    fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<I: oio::Read + 'static> oio::BlockingRead for BlockingWrapper<I> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        pollster::block_on(self.inner.read(buf))
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        pollster::block_on(self.inner.seek(pos))
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        pollster::block_on(self.inner.next())
    }
}

impl<I: oio::Write + 'static> oio::BlockingWrite for BlockingWrapper<I> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        pollster::block_on(poll_fn(|cx| self.inner.poll_write(cx, bs)))
    }

    fn close(&mut self) -> Result<()> {
        pollster::block_on(poll_fn(|cx| self.inner.poll_close(cx)))
    }
}

impl<I: oio::List> oio::BlockingList for BlockingWrapper<I> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        pollster::block_on(poll_fn(|cx| self.inner.poll_next(cx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blocking_layer_in_blocking_context() {
        let layer = BlockingLayer::create();
        assert!(layer.is_ok())
    }

    #[test]
    fn test_blocking_layer_in_async_context() {
        let layer = BlockingLayer::create();
        assert!(layer.is_ok());
    }
}
