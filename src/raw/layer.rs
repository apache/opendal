// Copyright 2022 Datafuse Labs
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

use async_trait::async_trait;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Layer is used to intercept the operations on the underlying storage.
///
/// Struct that implement this trait must accept input `Arc<dyn Accessor>` as inner,
/// and returns a new `Arc<dyn Accessor>` as output.
///
/// All functions in `Accessor` requires `&self`, so it's implementor's responsibility
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
/// use opendal::ops::*;
/// use opendal::raw::*;
/// use opendal::*;
///
/// /// Implement the real accessor logic here.
/// #[derive(Debug)]
/// struct TraceAccessor<A: Accessor> {
///     inner: A,
/// }
///
/// #[async_trait]
/// impl<A: Accessor> LayeredAccessor for TraceAccessor<A> {
///     type Inner = A;
///     type Reader = A::Reader;
///     type BlockingReader = A::BlockingReader;
///     type Pager = A::Pager;
///     type BlockingPager = A::BlockingPager;
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
///     async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
///         self.inner.list(path, args).await
///     }
///
///     fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
///         self.inner.blocking_list(path, args)
///     }
///
///     async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
///         self.inner.scan(path, args).await
///     }
///
///     fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
///         self.inner.blocking_scan(path, args)
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
    /// The layered accessor that return by this layer.
    type LayeredAccessor: Accessor;

    /// Intercept the operations on the underlying storage.
    fn layer(&self, inner: A) -> Self::LayeredAccessor;
}

/// LayeredAccessor is layered accessor that forward all not implemented
/// method to inner.
#[allow(missing_docs)]
#[async_trait]
pub trait LayeredAccessor: Send + Sync + Debug + Unpin + 'static {
    type Inner: Accessor;
    type Reader: output::Read;
    type BlockingReader: output::BlockingRead;
    type Pager: output::Page;
    type BlockingPager: output::BlockingPage;

    fn inner(&self) -> &Self::Inner;

    fn metadata(&self) -> AccessorMetadata {
        self.inner().metadata()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner().create(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)>;

    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        self.inner().write(path, args, r).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner().stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner().delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)>;

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)>;

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        self.inner().batch(args).await
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner().presign(path, args)
    }

    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        self.inner().create_multipart(path, args).await
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: input::Reader,
    ) -> Result<RpWriteMultipart> {
        self.inner().write_multipart(path, args, r).await
    }

    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        self.inner().complete_multipart(path, args).await
    }

    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        self.inner().abort_multipart(path, args).await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        self.inner().blocking_create(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)>;

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        r: input::BlockingReader,
    ) -> Result<RpWrite> {
        self.inner().blocking_write(path, args, r)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner().blocking_stat(path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        self.inner().blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)>;

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)>;
}

#[async_trait]
impl<L: LayeredAccessor> Accessor for L {
    type Reader = L::Reader;
    type BlockingReader = L::BlockingReader;
    type Pager = L::Pager;
    type BlockingPager = L::BlockingPager;

    fn metadata(&self) -> AccessorMetadata {
        (self as &L).metadata()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        (self as &L).create(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        (self as &L).read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        (self as &L).write(path, args, r).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        (self as &L).stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        (self as &L).delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        (self as &L).list(path, args).await
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        (self as &L).scan(path, args).await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        (self as &L).batch(args).await
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        (self as &L).presign(path, args)
    }

    async fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> Result<RpCreateMultipart> {
        (self as &L).create_multipart(path, args).await
    }

    async fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: input::Reader,
    ) -> Result<RpWriteMultipart> {
        (self as &L).write_multipart(path, args, r).await
    }

    async fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> Result<RpCompleteMultipart> {
        (self as &L).complete_multipart(path, args).await
    }

    async fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> Result<RpAbortMultipart> {
        (self as &L).abort_multipart(path, args).await
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        (self as &L).blocking_create(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        (self as &L).blocking_read(path, args)
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        r: input::BlockingReader,
    ) -> Result<RpWrite> {
        (self as &L).blocking_write(path, args, r)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        (self as &L).blocking_stat(path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        (self as &L).blocking_delete(path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        (self as &L).blocking_list(path, args)
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        (self as &L).blocking_scan(path, args)
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

    #[async_trait::async_trait]
    impl<A: Accessor> Accessor for Test<A> {
        type Reader = ();
        type BlockingReader = ();
        type Pager = ();
        type BlockingPager = ();

        fn metadata(&self) -> AccessorMetadata {
            let mut am = AccessorMetadata::default();
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

        let op = Operator::create(Memory::default())
            .unwrap()
            .layer(&test)
            .finish();

        op.object("xxxxx").delete().await.unwrap();

        assert!(*test.deleted.clone().lock().await);
    }
}
