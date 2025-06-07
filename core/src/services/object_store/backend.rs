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
    type Accessor = ObjectStoreBackend;

    fn build(self) -> Result<Self::Accessor> {
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
    type Reader = oio::StreamingReader;
    type Writer = oio::AppendWriter;
    type Lister = oio::PageLister;
    type Deleter = oio::BatchDeleter<oio::BlockingDeleter>;

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
        let store = self.store.clone();
        let path = path.to_string();

        let reader = oio::StreamingReader::new(Box::new(move || {
            let store = store.clone();
            let path = path.clone();
            Box::pin(async move {
                let bytes = store.get(&path).await.map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "read failed")
                        .with_context("path", &path)
                        .set_source(e)
                })?;
                Ok(bytes.bytes().await.map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "read failed")
                        .with_context("path", &path)
                        .set_source(e)
                })?)
            })
        }));

        Ok((RpRead::default(), reader))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let store = self.store.clone();
        let path = path.to_string();

        let writer = oio::AppendWriter::new(Box::new(move |bs| {
            let store = store.clone();
            let path = path.clone();
            Box::pin(async move {
                store.put(&path, bs).await.map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "write failed")
                        .with_context("path", &path)
                        .set_source(e)
                })?;
                Ok(())
            })
        }));

        Ok((RpWrite::default(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let store = self.store.clone();
        let deleter = oio::BlockingDeleter::new(Box::new(move |path| {
            let store = store.clone();
            let path = path.to_string();
            Box::pin(async move {
                store.delete(&path).await.map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "delete failed")
                        .with_context("path", &path)
                        .set_source(e)
                })?;
                Ok(())
            })
        }));

        Ok((RpDelete::default(), oio::BatchDeleter::new(deleter)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let store = self.store.clone();
        let path = path.to_string();

        let lister = oio::PageLister::new(Box::new(move |token| {
            let store = store.clone();
            let path = path.clone();
            Box::pin(async move {
                let list = store.list(Some(&path)).await.map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "list failed")
                        .with_context("path", &path)
                        .set_source(e)
                })?;

                let entries = list
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "list failed")
                            .with_context("path", &path)
                            .set_source(e)
                    })?
                    .into_iter()
                    .map(|meta| {
                        let path = meta.location.to_string();
                        let size = meta.size as u64;
                        let last_modified = meta.last_modified;
                        let is_dir = meta.location.ends_with('/');

                        let mut entry = oio::Entry::new(&path);
                        entry.set_size(size);
                        entry.set_last_modified(last_modified);
                        entry.set_is_dir(is_dir);
                        entry
                    })
                    .collect();

                Ok((entries, None))
            })
        }));

        Ok((RpList::default(), lister))
    }
}
