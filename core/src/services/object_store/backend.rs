use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::raw::*;
use crate::Error;
use crate::ErrorKind;
use crate::*;
use object_store::ObjectStore;

/// ObjectStore backend builder
#[derive(Default)]
pub struct ObjectStoreBuilder {
    store: Option<Arc<dyn ObjectStore + 'static>>,
}

impl Debug for ObjectStoreBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("ObjectStoreBuilder");
        d.finish_non_exhaustive()
    }
}

impl ObjectStoreBuilder {
    /// Set the object store instance
    pub fn store(mut self, store: Arc<dyn ObjectStore + 'static>) -> Self {
        self.store = Some(store);
        self
    }
}

impl Builder for ObjectStoreBuilder {
    type Config = ();
    const SCHEME: Scheme = Scheme::ObjectStore;

    fn build(self) -> Result<impl Access> {
        let store = self.store.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "object store is required")
                .with_context("service", Scheme::ObjectStore)
        })?;

        Ok(ObjectStoreBackend { store })
    }
}

/// ObjectStore backend
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore + 'static>,
}

impl Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("ObjectStoreBackend");
        d.finish_non_exhaustive()
    }
}

impl Access for ObjectStoreBackend {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type Deleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut info = AccessorInfo::default();
        info.set_scheme(Scheme::ObjectStore);
        info.set_capability(Capability {
            read: true,
            write: true,
            delete: true,
            list: true,
            ..Default::default()
        });
        Arc::new(info)
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        todo!()
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        todo!()
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        todo!()
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        todo!()
    }
}
