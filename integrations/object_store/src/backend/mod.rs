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
