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

use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use opendal::raw::oio::BatchDeleter;
use opendal::raw::oio::MultipartWriter;
use opendal::raw::*;
use opendal::Error;
use opendal::ErrorKind;
use opendal::*;

mod core;
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

use crate::service::core::format_metadata as parse_metadata;
use crate::service::core::parse_op_stat;

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
    pub fn new(store: Arc<dyn ObjectStore + 'static>) -> Self {
        Self { store: Some(store) }
    }
}

impl Builder for ObjectStoreBuilder {
    type Config = ();

    fn build(self) -> Result<impl Access> {
        let store = self.store.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "object store is required")
                .with_context("service", Scheme::Custom("object_store"))
        })?;

        Ok(ObjectStoreService { store })
    }
}

/// ObjectStore backend
pub struct ObjectStoreService {
    store: Arc<dyn ObjectStore + 'static>,
}

impl Debug for ObjectStoreService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("ObjectStoreBackend");
        d.finish_non_exhaustive()
    }
}

impl Access for ObjectStoreService {
    type Reader = ObjectStoreReader;
    type Writer = MultipartWriter<ObjectStoreWriter>;
    type Lister = ObjectStoreLister;
    type Deleter = BatchDeleter<ObjectStoreDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        info.set_scheme("object_store")
            .set_root("/")
            .set_name("object_store")
            .set_native_capability(Capability {
                stat: true,
                stat_with_if_match: true,
                stat_with_if_unmodified_since: true,
                read: true,
                write: true,
                delete: true,
                list: true,
                list_with_limit: true,
                list_with_start_after: true,
                delete_with_version: false,
                ..Default::default()
            });
        Arc::new(info)
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let path = ObjectStorePath::from(path);
        let opts = parse_op_stat(&args)?;
        let result = self
            .store
            .get_opts(&path, opts)
            .await
            .map_err(parse_error)?;
        let metadata = parse_metadata(&result.meta);
        Ok(RpStat::new(metadata))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let reader = ObjectStoreReader::new(self.store.clone(), path, args).await?;
        Ok((reader.rp(), reader))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = ObjectStoreWriter::new(self.store.clone(), path, args);
        Ok((
            RpWrite::default(),
            MultipartWriter::new(self.info(), writer, 10),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let deleter = BatchDeleter::new(ObjectStoreDeleter::new(self.store.clone()));
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
    use opendal::raw::oio::{Delete, List, Read, Write};
    use opendal::Buffer;

    #[tokio::test]
    async fn test_object_store_backend_builder() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let builder = ObjectStoreBuilder::new(store);

        let backend = builder.build().expect("build should succeed");
        assert!(backend.info().scheme() == "object_store");
    }

    #[tokio::test]
    async fn test_object_store_backend_info() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::new(store)
            .build()
            .expect("build should succeed");

        let info = backend.info();
        assert_eq!(info.scheme(), "object_store");
        assert_eq!(info.name(), "object_store".into());
        assert_eq!(info.root(), "/".into());

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
        let backend = ObjectStoreBuilder::new(store.clone())
            .build()
            .expect("build should succeed");

        let path = "test_file.txt";
        let content = b"Hello, world!";

        // Test write
        let (_, mut writer) = backend
            .write(path, OpWrite::default())
            .await
            .expect("write should succeed");

        writer
            .write(Buffer::from(&content[..]))
            .await
            .expect("write content should succeed");
        writer.close().await.expect("close should succeed");

        // Test stat
        let stat_result = backend
            .stat(path, OpStat::default())
            .await
            .expect("stat should succeed");

        assert_eq!(
            stat_result.into_metadata().content_length(),
            content.len() as u64
        );

        // Test read
        let (_, mut reader) = backend
            .read(path, OpRead::default())
            .await
            .expect("read should succeed");

        let buf = reader.read().await.expect("read should succeed");
        assert_eq!(buf.to_vec(), content);
    }

    #[tokio::test]
    async fn test_object_store_backend_multipart_upload() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::new(store.clone())
            .build()
            .expect("build should succeed");

        let path = "test_file.txt";
        let content =
            b"Hello, multipart upload! This is a test content for multipart upload functionality.";
        let content_len = content.len();

        // Test multipart upload with multiple chunks
        let (_, mut writer) = backend
            .write(path, OpWrite::default())
            .await
            .expect("write should succeed");

        // Write content in chunks to simulate multipart upload
        let chunk_size = 20;
        for chunk in content.chunks(chunk_size) {
            writer
                .write(Buffer::from(chunk))
                .await
                .expect("write chunk should succeed");
        }

        writer.close().await.expect("close should succeed");

        // Verify the uploaded file
        let stat_result = backend
            .stat(path, OpStat::default())
            .await
            .expect("stat should succeed");

        assert_eq!(
            stat_result.into_metadata().content_length(),
            content_len as u64
        );

        // Read back and verify content
        let (_, mut reader) = backend
            .read(path, OpRead::default())
            .await
            .expect("read should succeed");

        let buf = reader.read().await.expect("read should succeed");
        assert_eq!(buf.to_vec(), content);
    }

    #[tokio::test]
    async fn test_object_store_backend_list() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::new(store.clone())
            .build()
            .expect("build should succeed");

        // Create multiple files
        let files = vec![
            ("dir1/file1.txt", b"content1"),
            ("dir1/file2.txt", b"content2"),
            ("dir2/file3.txt", b"content3"),
        ];

        for (path, content) in &files {
            let (_, mut writer) = backend
                .write(path, OpWrite::default())
                .await
                .expect("write should succeed");
            writer
                .write(Buffer::from(&content[..]))
                .await
                .expect("write content should succeed");
            writer.close().await.expect("close should succeed");
        }

        // List directory
        let (_, mut lister) = backend
            .list("dir1/", OpList::default())
            .await
            .expect("list should succeed");

        let mut entries = Vec::new();
        while let Some(entry) = lister.next().await.expect("next should succeed") {
            entries.push(entry);
        }

        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|e| e.path() == "dir1/file1.txt"));
        assert!(entries.iter().any(|e| e.path() == "dir1/file2.txt"));
    }

    #[tokio::test]
    async fn test_object_store_backend_delete() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::new(store)
            .build()
            .expect("build should succeed");

        let path = "test_delete.txt";
        let content = b"To be deleted";

        // Write file
        let (_, mut writer) = backend
            .write(path, OpWrite::default())
            .await
            .expect("write should succeed");
        writer
            .write(Buffer::from(&content[..]))
            .await
            .expect("write content should succeed");
        writer.close().await.expect("close should succeed");

        // Verify file exists
        backend
            .stat(path, OpStat::default())
            .await
            .expect("file should exist");

        // Delete file
        let (_, mut deleter) = backend.delete().await.expect("delete should succeed");
        deleter
            .delete(path, OpDelete::default())
            .expect("delete should succeed");
        deleter.flush().await.expect("flush should succeed");

        // Verify file is deleted
        let result = backend.stat(path, OpStat::default()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_object_store_backend_error_handling() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::new(store)
            .build()
            .expect("build should succeed");

        // Test stat on non-existent file
        let result = backend.stat("non_existent.txt", OpStat::default()).await;
        assert!(result.is_err());

        // Test read on non-existent file
        let result = backend.read("non_existent.txt", OpRead::default()).await;
        assert!(result.is_err());

        // Test list on non-existent directory
        let result = backend.list("non_existent_dir/", OpList::default()).await;
        // This should succeed but return empty results
        if let Ok((_, mut lister)) = result {
            let entry = lister.next().await.expect("next should succeed");
            assert!(entry.is_none());
        }
    }
}
