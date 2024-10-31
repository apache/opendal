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
use std::sync::Arc;

use futures::Future;

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
/// use opendal::raw::*;
/// use opendal::*;
///
/// /// Implement the real accessor logic here.
/// #[derive(Debug)]
/// struct TraceAccessor<A: Access> {
///     inner: A,
/// }
///
/// impl<A: Access> LayeredAccess for TraceAccessor<A> {
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
/// impl<A: Access> Layer<A> for TraceLayer {
///     type LayeredAccess = TraceAccessor<A>;
///
///     fn layer(&self, inner: A) -> Self::LayeredAccess {
///         TraceAccessor { inner }
///     }
/// }
/// ```
pub trait Layer<A: Access> {
    /// The layered accessor that returned by this layer.
    type LayeredAccess: Access;

    /// Intercept the operations on the underlying storage.
    fn layer(&self, inner: A) -> Self::LayeredAccess;
}

/// LayeredAccess is layered accessor that forward all not implemented
/// method to inner.
#[allow(missing_docs)]
pub trait LayeredAccess: Send + Sync + Debug + Unpin + 'static {
    type Inner: Access;
    type Reader: oio::Read;
    type BlockingReader: oio::BlockingRead;
    type Writer: oio::Write;
    type BlockingWriter: oio::BlockingWrite;
    type Lister: oio::List;
    type BlockingLister: oio::BlockingList;

    fn inner(&self) -> &Self::Inner;

    fn metadata(&self) -> Arc<AccessorInfo> {
        self.inner().info()
    }

    fn create_dir(
        &self,
        path: &str,
        args: OpCreateDir,
    ) -> impl Future<Output = Result<RpCreateDir>> + MaybeSend {
        self.inner().create_dir(path, args)
    }

    fn read(
        &self,
        path: &str,
        args: OpRead,
    ) -> impl Future<Output = Result<(RpRead, Self::Reader)>> + MaybeSend;

    fn write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> impl Future<Output = Result<(RpWrite, Self::Writer)>> + MaybeSend;

    fn copy(
        &self,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> impl Future<Output = Result<RpCopy>> + MaybeSend {
        self.inner().copy(from, to, args)
    }

    fn rename(
        &self,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> impl Future<Output = Result<RpRename>> + MaybeSend {
        self.inner().rename(from, to, args)
    }

    fn stat(&self, path: &str, args: OpStat) -> impl Future<Output = Result<RpStat>> + MaybeSend {
        self.inner().stat(path, args)
    }

    fn delete(
        &self,
        path: &str,
        args: OpDelete,
    ) -> impl Future<Output = Result<RpDelete>> + MaybeSend {
        self.inner().delete(path, args)
    }

    fn list(
        &self,
        path: &str,
        args: OpList,
    ) -> impl Future<Output = Result<(RpList, Self::Lister)>> + MaybeSend;

    fn batch(&self, args: OpBatch) -> impl Future<Output = Result<RpBatch>> + MaybeSend {
        self.inner().batch(args)
    }

    fn presign(
        &self,
        path: &str,
        args: OpPresign,
    ) -> impl Future<Output = Result<RpPresign>> + MaybeSend {
        self.inner().presign(path, args)
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

impl<L: LayeredAccess> Access for L {
    type Reader = L::Reader;
    type Writer = L::Writer;
    type Lister = L::Lister;
    type BlockingReader = L::BlockingReader;
    type BlockingWriter = L::BlockingWriter;
    type BlockingLister = L::BlockingLister;

    fn info(&self) -> Arc<AccessorInfo> {
        LayeredAccess::metadata(self)
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        LayeredAccess::create_dir(self, path, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        LayeredAccess::stat(self, path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        LayeredAccess::read(self, path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        LayeredAccess::write(self, path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        LayeredAccess::delete(self, path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        LayeredAccess::list(self, path, args).await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        LayeredAccess::copy(self, from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        LayeredAccess::rename(self, from, to, args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        LayeredAccess::presign(self, path, args).await
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        LayeredAccess::batch(self, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        LayeredAccess::blocking_create_dir(self, path, args)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        LayeredAccess::blocking_stat(self, path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        LayeredAccess::blocking_read(self, path, args)
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        LayeredAccess::blocking_write(self, path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        LayeredAccess::blocking_delete(self, path, args)
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        LayeredAccess::blocking_list(self, path, args)
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        LayeredAccess::blocking_copy(self, from, to, args)
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        LayeredAccess::blocking_rename(self, from, to, args)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::lock::Mutex;

    use super::*;
    use crate::services::Memory;

    #[derive(Debug)]
    struct Test<A: Access> {
        #[allow(dead_code)]
        inner: Option<A>,
        deleted: Arc<Mutex<bool>>,
    }

    impl<A: Access> Layer<A> for &Test<A> {
        type LayeredAccess = Test<A>;

        fn layer(&self, inner: A) -> Self::LayeredAccess {
            Test {
                inner: Some(inner),
                deleted: self.deleted.clone(),
            }
        }
    }

    impl<A: Access> Access for Test<A> {
        type Reader = ();
        type Writer = ();
        type Lister = ();
        type BlockingReader = ();
        type BlockingWriter = ();
        type BlockingLister = ();

        fn info(&self) -> Arc<AccessorInfo> {
            Arc::new(AccessorInfo::default().with_scheme(Scheme::Custom("test")))
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
