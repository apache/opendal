use std::future::Future;
use std::sync::Arc;

use futures::FutureExt;
use object_store::ObjectStore;
use opendal::raw::*;
use opendal::*;

use super::error::parse_error;

pub struct ObjectStoreDeleter {
    store: Arc<dyn ObjectStore + 'static>,
    path: object_store::path::Path,
}

impl ObjectStoreDeleter {
    pub(crate) fn new(store: Arc<dyn ObjectStore + 'static>, path: &str) -> Self {
        Self {
            store,
            path: object_store::path::Path::from(path),
        }
    }
}

impl oio::Delete for ObjectStoreDeleter {
    fn delete(&mut self, path: &str, _: OpDelete) -> Result<()> {
        self.path = object_store::path::Path::from(path);
        Ok(())
    }

    fn flush(&mut self) -> impl Future<Output = Result<usize>> + MaybeSend {
        async move {
            self.store.delete(&self.path).await.map_err(parse_error)?;
            Ok(0)
        }
        .boxed()
    }
}
