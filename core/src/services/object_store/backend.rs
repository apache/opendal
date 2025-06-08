use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::sync::Arc;

use crate::raw::*;
use crate::services::object_store::error::parse_error;
use crate::Error;
use crate::ErrorKind;
use crate::*;
use object_store::GetRange;
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
        let info = AccessorInfo::default();
        info.set_scheme(Scheme::ObjectStore)
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

struct ObjectStoreReader {
    store: Arc<dyn ObjectStore + 'static>,
    path: object_store::path::Path,
    args: OpRead,
}

impl ObjectStoreReader {
    pub fn new(
        store: Arc<dyn ObjectStore + 'static>,
        path: object_store::path::Path,
        args: OpRead,
    ) -> Self {
        Self { store, path, args }
    }

    fn parse_args(&self, args: &OpRead) -> Result<object_store::GetOptions> {
        let mut options = object_store::GetOptions::default();

        if let Some(version) = args.version() {
            options.version = Some(version.to_string());
        }

        if let Some(if_match) = args.if_match() {
            options.if_match = Some(if_match.to_string());
        }

        if let Some(if_none_match) = args.if_none_match() {
            options.if_none_match = Some(if_none_match.to_string());
        }

        if let Some(if_modified_since) = args.if_modified_since() {
            options.if_modified_since = Some(if_modified_since);
        }

        if let Some(if_unmodified_since) = args.if_unmodified_since() {
            options.if_unmodified_since = Some(if_unmodified_since);
        }

        if !args.range().is_full() {
            let range = args.range();
            match range.size() {
                Some(size) => {
                    options.range = Some(GetRange::Bounded(range.offset()..range.offset() + size));
                }
                None => {
                    options.range = Some(GetRange::Offset(range.offset()));
                }
            }
        }

        Ok(options)
    }
}

impl oio::Read for ObjectStoreReader {
    fn read(&mut self) -> impl Future<Output = Result<Buffer>> + MaybeSend {
        async {
            let opts = self.parse_args(&self.args)?;
            let result = self
                .store
                .get_opts(&self.path, opts)
                .await
                .map_err(parse_error)?;
            let bytes = result.bytes().await.map_err(parse_error)?;
            let buf = Buffer::from(bytes);
            Ok(buf)
        }
    }
}
