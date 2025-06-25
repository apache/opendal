use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use object_store::ObjectStore;
use opendal::raw::*;
use opendal::Error;
use opendal::ErrorKind;
use opendal::*;

mod deleter;
mod error;
mod lister;
mod reader;
mod writer;


use deleter::ObjectStoreDeleter;
use error::parse_error;
use lister::ObjectStoreLister;
use reader::ObjectStoreReader;
use writer::ObjectStoreWriter;

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
    const SCHEME: Scheme = Scheme::Custom("object_store");

    fn build(self) -> Result<impl Access> {
        let store = self.store.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "object store is required")
                .with_context("service", Scheme::Custom("object_store"))
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
    type Reader = ObjectStoreReader;
    type Writer = ObjectStoreWriter;
    type Lister = ObjectStoreLister;
    type Deleter = ObjectStoreDeleter;

    fn info(&self) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Custom("object_store"))
            .set_root("/")
            .set_name("object_store")
            .set_native_capability(Capability {
                stat: true,
                stat_has_content_length: true,
                stat_has_last_modified: true,

                read: true,
                write: true,
                delete: true,

                list: true,
                list_has_content_length: true,
                list_has_last_modified: true,
                ..Default::default()
            });
        Arc::new(info)
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let path = object_store::path::Path::from(path);
        let meta = self.store.head(&path).await.map_err(parse_error)?;

        let mut metadata = Metadata::new(EntryMode::FILE);
        metadata.set_content_length(meta.size);
        metadata.set_last_modified(meta.last_modified);
        if let Some(etag) = meta.e_tag {
            metadata.set_etag(&etag);
        }
        if let Some(version) = meta.version {
            metadata.set_version(&version);
        }
        Ok(RpStat::new(metadata))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let reader = ObjectStoreReader::new(self.store.clone(), path, args).await?;
        Ok((reader.rp(), reader))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = ObjectStoreWriter::new(self.store.clone(), path, args);
        Ok((RpWrite::default(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let deleter = ObjectStoreDeleter::new(self.store.clone());
        Ok((RpDelete::default(), deleter))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let lister = ObjectStoreLister::new(self.store.clone(), path, args).await?;
        Ok((RpList::default(), lister))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_object_store_backend_builder() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let builder = ObjectStoreBuilder::default().store(store);
        
        let backend = builder.build().expect("build should succeed");
        assert!(backend.info().scheme() == Scheme::Custom("object_store"));
    }

    #[tokio::test]
    async fn test_object_store_backend_info() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::default()
            .store(store)
            .build()
            .expect("build should succeed");
        
        let info = backend.info();
        assert_eq!(info.scheme(), Scheme::Custom("object_store"));
        assert_eq!(info.name(), "object_store");
        assert_eq!(info.root(), "/");
        
        let cap = info.native_capability();
        assert!(cap.stat);
        assert!(cap.read);
        assert!(cap.write);
        assert!(cap.delete);
        assert!(cap.list);
    }

    #[tokio::test]
    async fn test_object_store_backend_basic_operations() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::default()
            .store(store.clone())
            .build()
            .expect("build should succeed");
        
        let path = "test_file.txt";
        let content = b"Hello, world!";
        
        // Test write
        let (_, mut writer) = backend
            .write(path, OpWrite::default())
            .await
            .expect("write should succeed");
        
        writer.write(content.into()).await.expect("write content should succeed");
        writer.close().await.expect("close should succeed");
        
        // Test stat
        let stat_result = backend
            .stat(path, OpStat::default())
            .await
            .expect("stat should succeed");
        
        assert_eq!(stat_result.metadata().content_length(), content.len() as u64);
        
        // Test read
        let (_, mut reader) = backend
            .read(path, OpRead::default())
            .await
            .expect("read should succeed");
        
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read should succeed");
        assert_eq!(buf, content);
    }

    #[tokio::test]
    async fn test_object_store_backend_with_operator() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let op = Operator::new(ObjectStoreBuilder::default().store(store))
            .expect("operator creation should succeed")
            .finish();
        
        let path = "test_operator.txt";
        let content = "Hello from operator!";
        
        // Write using operator
        op.write(path, content)
            .await
            .expect("write should succeed");
        
        // Read using operator
        let result = op.read(path).await.expect("read should succeed");
        assert_eq!(result, content.as_bytes());
        
        // Check metadata
        let meta = op.stat(path).await.expect("stat should succeed");
        assert_eq!(meta.content_length(), content.len() as u64);
        assert!(meta.is_file());
    }
}
