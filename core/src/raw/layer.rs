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

use std::fmt::Debug;

use async_trait::async_trait;

use crate::raw::*;
use crate::*;

/// Layer is used to intercept the operations on the underlying storage.
///
/// Struct that implement this trait must accept input `Arc<dyn Accessor>` as inner,
/// and returns a new `Arc<dyn Accessor>` as output.
///
/// All functions in `Accessor` requires `&self`, so it's implementer's responsibility
/// to maintain the internal mutability. Please also keep in mind that `Accessor`
/// requires `Send` and `Sync`.
///
/// # Notes
///
/// ## Inner
///
/// It's required to implement `fn inner() -> Option<Arc<dyn Accessor>>` for layer's accessors.
///
/// By implement this method, all API calls will be forwarded to inner accessor instead.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use async_trait::async_trait;
/// use opendal::raw::*;
/// use opendal::*;
///
/// /// Implement the real accessor logic here.
/// #[derive(Debug)]
/// struct TraceAccessor<A: Accessor> {
///     inner: A,
/// }
///
/// #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
/// #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
/// impl<A: Accessor> LayeredAccessor for TraceAccessor<A> {
///     type Inner = A;
///     type Reader = A::Reader;
///     type BlockingReader = A::BlockingReader;
///     type Writer = A::Writer;
///     type BlockingWriter = A::BlockingWriter;
///     type Lister = A::Lister;
///     type BlockingLister = A::BlockingLister;
///
///     fn inner(&self) -> &Self::Inner {
///         &self.inner
///     }
///
///     async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
///         self.inner.read(path, args).await
///     }
///
///     fn blocking_read(
///         &self,
///         path: &str,
///         args: OpRead,
///     ) -> Result<(RpRead, Self::BlockingReader)> {
///         self.inner.blocking_read(path, args)
///     }
///
///     async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
///         self.inner.write(path, args).await
///     }
///
///     fn blocking_write(
///         &self,
///         path: &str,
///         args: OpWrite,
///     ) -> Result<(RpWrite, Self::BlockingWriter)> {
///         self.inner.blocking_write(path, args)
///     }
///
///     async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
///         self.inner.list(path, args).await
///     }
///
///     fn blocking_list(
///         &self,
///         path: &str,
///         args: OpList,
///     ) -> Result<(RpList, Self::BlockingLister)> {
///         self.inner.blocking_list(path, args)
///     }
/// }
///
/// /// The public struct that exposed to users.
/// ///
/// /// Will be used like `op.layer(TraceLayer)`
/// struct TraceLayer;
///
/// impl<A: Accessor> Layer<A> for TraceLayer {
///     type LayeredAccessor = TraceAccessor<A>;
///
///     fn layer(&self, inner: A) -> Self::LayeredAccessor {
///         TraceAccessor { inner }
///     }
/// }
/// ```
pub trait Layer<A: Accessor> {
    /// The layered accessor that returned by this layer.
    type LayeredAccessor: Accessor;

    /// Intercept the operations on the underlying storage.
    fn layer(&self, inner: A) -> Self::LayeredAccessor;
}

/// LayeredAccessor is layered accessor that forward all not implemented
/// method to inner.
#[allow(missing_docs)]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait LayeredAccessor: Send + Sync + Debug + Unpin + 'static {
    type Inner: Accessor;
    type Reader: oio::Read;
    type BlockingReader: oio::BlockingRead;
    type Writer: oio::Write;
    type BlockingWriter: oio::BlockingWrite;
    type Lister: oio::List;
    type BlockingLister: oio::BlockingList;

    fn inner(&self) -> &Self::Inner;

    fn metadata(&self) -> AccessorInfo {
        self.inner().info()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner().create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)>;

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)>;

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner().copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner().rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner().stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner().delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)>;

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner().batch(args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner().presign(path, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner().blocking_create_dir(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)>;

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)>;

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner().blocking_copy(from, to, args)
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner().blocking_rename(from, to, args)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner().blocking_stat(path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner().blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<L: LayeredAccessor> Accessor for L {
    type Reader = L::Reader;
    type BlockingReader = L::BlockingReader;
    type Writer = L::Writer;
    type BlockingWriter = L::BlockingWriter;
    type Lister = L::Lister;
    type BlockingLister = L::BlockingLister;

    fn info(&self) -> AccessorInfo {
        (self as &L).metadata()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        (self as &L).create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        (self as &L).read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        (self as &L).write(path, args).await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        (self as &L).copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        (self as &L).rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        (self as &L).stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        (self as &L).delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        (self as &L).list(path, args).await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        (self as &L).batch(args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        (self as &L).presign(path, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        (self as &L).blocking_create_dir(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        (self as &L).blocking_read(path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        (self as &L).blocking_write(path, args)
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        (self as &L).blocking_copy(from, to, args)
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        (self as &L).blocking_rename(from, to, args)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        (self as &L).blocking_stat(path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        (self as &L).blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        (self as &L).blocking_list(path, args)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::lock::Mutex;

    use super::*;
    use crate::services::Memory;

    #[derive(Debug)]
    struct Test<A: Accessor> {
        #[allow(dead_code)]
        inner: Option<A>,
        deleted: Arc<Mutex<bool>>,
    }

    impl<A: Accessor> Layer<A> for &Test<A> {
        type LayeredAccessor = Test<A>;

        fn layer(&self, inner: A) -> Self::LayeredAccessor {
            Test {
                inner: Some(inner),
                deleted: self.deleted.clone(),
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl<A: Accessor> Accessor for Test<A> {
        type Reader = ();
        type BlockingReader = ();
        type Writer = ();
        type BlockingWriter = ();
        type Lister = ();
        type BlockingLister = ();

        fn info(&self) -> AccessorInfo {
            let mut am = AccessorInfo::default();
            am.set_scheme(Scheme::Custom("test"));
            am
        }

        async fn delete(&self, _: &str, _: OpDelete) -> Result<RpDelete> {
            let mut x = self.deleted.lock().await;
            *x = true;

            assert!(self.inner.is_some());

            // We will not call anything here to test the layer.
            Ok(RpDelete::default())
        }
    }

    #[tokio::test]
    async fn test_layer() {
        let test = Test {
            inner: None,
            deleted: Arc::new(Mutex::new(false)),
        };

        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(&test)
            .finish();

        op.delete("xxxxx").await.unwrap();

        assert!(*test.deleted.clone().lock().await);
    }
}
