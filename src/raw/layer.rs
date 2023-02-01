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

use crate::raw::*;
use crate::*;
use std::fmt::Debug;

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
/// use opendal::raw::Accessor;
/// use opendal::Layer;
///
/// /// Implement the real accessor logic here.
/// #[derive(Debug)]
/// struct TraceAccessor {
///     inner: Arc<dyn Accessor>,
/// }
///
/// impl Accessor for TraceAccessor {}
///
/// /// The public struct that exposed to users.
/// ///
/// /// Will be used like `op.layer(TraceLayer)`
/// struct TraceLayer;
///
/// impl Layer for TraceLayer {
///     fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
///         Arc::new(TraceAccessor { inner })
///     }
/// }
/// ```
pub trait Layer<A: Accessor> {
    type LayeredAccessor: Accessor;

    /// Intercept the operations on the underlying storage.
    fn layer(&self, inner: A) -> Self::LayeredAccessor;
}

pub trait LayeredAccessor: Send + Sync + Debug + Unpin + 'static {
    type Inner: Accessor;
    type Reader: output::Read;
    type BlockingReader: output::BlockingRead;

    fn inner(&self) -> &Self::Inner;

    fn metadata(&self) -> AccessorMetadata {
        self.inner().metadata()
    }

    fn create(&self, path: &str, args: OpCreate) -> FutureResult<RpCreate> {
        self.inner().create(path, args)
    }

    fn read(&self, path: &str, args: OpRead) -> FutureResult<(RpRead, Self::Reader)>;

    fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> FutureResult<RpWrite> {
        self.inner().write(path, args, r)
    }

    fn stat(&self, path: &str, args: OpStat) -> FutureResult<RpStat> {
        self.inner().stat(path, args)
    }

    fn delete(&self, path: &str, args: OpDelete) -> FutureResult<RpDelete> {
        self.inner().delete(path, args)
    }

    fn list(&self, path: &str, args: OpList) -> FutureResult<(RpList, ObjectPager)> {
        self.inner().list(path, args)
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner().presign(path, args)
    }

    fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> FutureResult<RpCreateMultipart> {
        self.inner().create_multipart(path, args)
    }

    fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: input::Reader,
    ) -> FutureResult<RpWriteMultipart> {
        self.inner().write_multipart(path, args, r)
    }

    fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> FutureResult<RpCompleteMultipart> {
        self.inner().complete_multipart(path, args)
    }

    fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> FutureResult<RpAbortMultipart> {
        self.inner().abort_multipart(path, args)
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

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, BlockingObjectPager)> {
        self.inner().blocking_list(path, args)
    }
}

impl<L: LayeredAccessor> Accessor for L {
    type Reader = L::Reader;
    type BlockingReader = L::BlockingReader;

    fn metadata(&self) -> AccessorMetadata {
        (self as &L).metadata()
    }

    fn create(&self, path: &str, args: OpCreate) -> FutureResult<RpCreate> {
        (self as &L).create(path, args)
    }

    fn read(&self, path: &str, args: OpRead) -> FutureResult<(RpRead, Self::Reader)> {
        (self as &L).read(path, args)
    }

    fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> FutureResult<RpWrite> {
        (self as &L).write(path, args, r)
    }

    fn stat(&self, path: &str, args: OpStat) -> FutureResult<RpStat> {
        (self as &L).stat(path, args)
    }

    fn delete(&self, path: &str, args: OpDelete) -> FutureResult<RpDelete> {
        (self as &L).delete(path, args)
    }

    fn list(&self, path: &str, args: OpList) -> FutureResult<(RpList, ObjectPager)> {
        (self as &L).list(path, args)
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        (self as &L).presign(path, args)
    }

    fn create_multipart(
        &self,
        path: &str,
        args: OpCreateMultipart,
    ) -> FutureResult<RpCreateMultipart> {
        (self as &L).create_multipart(path, args)
    }

    fn write_multipart(
        &self,
        path: &str,
        args: OpWriteMultipart,
        r: input::Reader,
    ) -> FutureResult<RpWriteMultipart> {
        (self as &L).write_multipart(path, args, r)
    }

    fn complete_multipart(
        &self,
        path: &str,
        args: OpCompleteMultipart,
    ) -> FutureResult<RpCompleteMultipart> {
        (self as &L).complete_multipart(path, args)
    }

    fn abort_multipart(
        &self,
        path: &str,
        args: OpAbortMultipart,
    ) -> FutureResult<RpAbortMultipart> {
        (self as &L).abort_multipart(path, args)
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

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, BlockingObjectPager)> {
        (self as &L).blocking_list(path, args)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::services::fs;
    use futures::lock::Mutex;

    use super::*;
    use crate::*;

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

        fn delete(&self, _: &str, _: OpDelete) -> FutureResult<RpDelete> {
            let fut = async {
                let mut x = self.deleted.lock().await;
                *x = true;

                assert!(self.inner.is_some());

                // We will not call anything here to test the layer.
                Ok(RpDelete::default())
            };

            Box::pin(fut)
        }
    }

    #[tokio::test]
    async fn test_layer() {
        let test = Test {
            inner: None,
            deleted: Arc::new(Mutex::new(false)),
        };

        let op = Operator::new(fs::Builder::default().build().unwrap())
            .layer(&test)
            .finish();

        op.object("xxxxx").delete().await.unwrap();

        assert!(*test.deleted.clone().lock().await);
    }
}
