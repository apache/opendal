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

use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use opendal::Error;
use opendal::ErrorKind;
use opendal::raw::oio::BatchDeleter;
use opendal::raw::oio::MultipartWriter;
use opendal::raw::*;
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

pub const OBJECT_STORE_SCHEME: &str = "object_store";

/// ObjectStore backend builder
#[derive(Default)]
pub struct ObjectStoreBuilder {
    store: Option<Arc<dyn ObjectStore + 'static>>,
}

impl Debug for ObjectStoreBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

    fn build(self) -> Result<impl Service> {
        let store = self.store.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "object store is required")
                .with_context("service", OBJECT_STORE_SCHEME)
        })?;

        Ok(ObjectStoreService {
            store,
            info: ServiceInfo::new(OBJECT_STORE_SCHEME, "/", "object_store"),
            capability: Capability {
                stat: true,
                stat_with_if_match: true,
                stat_with_if_unmodified_since: true,
                read: true,
                read_with_suffix: true,
                write: true,
                delete: true,
                list: true,
                list_with_start_after: true,
                list_with_recursive: true,
                delete_with_version: false,
                ..Default::default()
            },
        })
    }
}

/// ObjectStore backend
pub struct ObjectStoreService {
    store: Arc<dyn ObjectStore + 'static>,
    info: ServiceInfo,
    capability: Capability,
}

impl Debug for ObjectStoreService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("ObjectStoreBackend");
        d.finish_non_exhaustive()
    }
}

impl Service for ObjectStoreService {
    type Reader = oio::StreamReader<ObjectStoreReader>;
    type Writer = MultipartWriter<ObjectStoreWriter>;
    type Lister = ObjectStoreLister;
    type Deleter = BatchDeleter<ObjectStoreDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
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

    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        Ok(oio::StreamReader::new(ObjectStoreReader::new(
            self.store.clone(),
            path,
            args,
        )))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let writer = ObjectStoreWriter::new(self.store.clone(), path, args);
        Ok(MultipartWriter::new(ctx.executor().clone(), writer, 10))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let deleter = BatchDeleter::new(ObjectStoreDeleter::new(self.store.clone()), Some(1000));
        Ok(deleter)
    }

    fn list(&self, _ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let lister = ObjectStoreLister::new(self.store.clone(), path, args)?;
        Ok(lister)
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use opendal::Buffer;
    use opendal::raw::oio::{Delete, List, Read, ReadStream, Write};

    fn test_ctx() -> OperationContext {
        OperationContext::new()
    }

    #[tokio::test]
    async fn test_object_store_backend_builder() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let builder = ObjectStoreBuilder::new(store);

        let backend = builder.build().expect("build should succeed");
        assert_eq!(backend.info().scheme(), OBJECT_STORE_SCHEME);
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

        let cap = backend.capability();
        assert!(cap.stat);
        assert!(cap.read);
        assert!(cap.write);
        assert!(cap.delete);
        assert!(cap.list);
        assert!(cap.list_with_recursive);
        assert!(cap.list_with_start_after);
        assert!(!cap.list_with_limit);
    }

    #[tokio::test]
    async fn test_object_store_backend_basic_operations() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::new(store.clone())
            .build()
            .expect("build should succeed");
        let ctx = test_ctx();

        let path = "test_file.txt";
        let content = b"Hello, world!";

        // Test write
        let mut writer = backend
            .write(&ctx, path, OpWrite::default())
            .expect("write should succeed");

        writer
            .write(Buffer::from(&content[..]))
            .await
            .expect("write content should succeed");
        writer.close().await.expect("close should succeed");

        // Test stat
        let stat_result = backend
            .stat(&ctx, path, OpStat::default())
            .await
            .expect("stat should succeed");

        assert_eq!(
            stat_result.into_metadata().content_length(),
            content.len() as u64
        );

        // Test read
        let reader = backend
            .read(&ctx, path, OpRead::default())
            .expect("read should succeed");

        let (_, mut stream) = reader
            .open(BytesRange::default())
            .await
            .expect("open should succeed");
        let buf = stream.read().await.expect("read should succeed");
        assert_eq!(buf.to_vec(), content);
    }

    #[tokio::test]
    async fn test_object_store_backend_multipart_upload() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::new(store.clone())
            .build()
            .expect("build should succeed");
        let ctx = test_ctx();

        let path = "test_file.txt";
        let content =
            b"Hello, multipart upload! This is a test content for multipart upload functionality.";
        let content_len = content.len();

        // Test multipart upload with multiple chunks
        let mut writer = backend
            .write(&ctx, path, OpWrite::default())
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
            .stat(&ctx, path, OpStat::default())
            .await
            .expect("stat should succeed");

        assert_eq!(
            stat_result.into_metadata().content_length(),
            content_len as u64
        );

        // Read back and verify content
        let reader = backend
            .read(&ctx, path, OpRead::default())
            .expect("read should succeed");

        let (_, mut stream) = reader
            .open(BytesRange::default())
            .await
            .expect("open should succeed");
        let buf = stream.read_all().await.expect("read should succeed");
        assert_eq!(buf.to_vec(), content);
    }

    #[tokio::test]
    async fn test_object_store_backend_list() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::new(store.clone())
            .build()
            .expect("build should succeed");
        let ctx = test_ctx();

        // Create multiple files
        let files = vec![
            ("dir1/file1.txt", b"content1"),
            ("dir1/file2.txt", b"content2"),
            ("dir2/file3.txt", b"content3"),
        ];

        for (path, content) in &files {
            let mut writer = backend
                .write(&ctx, path, OpWrite::default())
                .expect("write should succeed");
            writer
                .write(Buffer::from(&content[..]))
                .await
                .expect("write content should succeed");
            writer.close().await.expect("close should succeed");
        }

        // List directory
        let mut lister = backend
            .list(&ctx, "dir1/", OpList::default())
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
        let ctx = test_ctx();

        let path = "test_delete.txt";
        let content = b"To be deleted";

        // Write file
        let mut writer = backend
            .write(&ctx, path, OpWrite::default())
            .expect("write should succeed");
        writer
            .write(Buffer::from(&content[..]))
            .await
            .expect("write content should succeed");
        writer.close().await.expect("close should succeed");

        // Verify file exists
        backend
            .stat(&ctx, path, OpStat::default())
            .await
            .expect("file should exist");

        // Delete file
        let mut deleter = backend.delete(&ctx).expect("delete should succeed");
        deleter
            .delete(path, OpDelete::default())
            .await
            .expect("delete should succeed");
        deleter.close().await.expect("close should succeed");

        // Verify file is deleted
        let result = backend.stat(&ctx, path, OpStat::default()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_object_store_backend_error_handling() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::new(store)
            .build()
            .expect("build should succeed");
        let ctx = test_ctx();

        // Test stat on non-existent file
        let result = backend
            .stat(&ctx, "non_existent.txt", OpStat::default())
            .await;
        assert!(result.is_err());

        // Test read on non-existent file
        let reader = backend
            .read(&ctx, "non_existent.txt", OpRead::default())
            .expect("read should create reader");
        let result = reader.read(BytesRange::from(0..1)).await;
        assert!(result.is_err());

        // Test list on non-existent directory
        let result = backend.list(&ctx, "non_existent_dir/", OpList::default());
        // This should succeed but return empty results
        if let Ok(mut lister) = result {
            let entry = lister.next().await.expect("next should succeed");
            assert!(entry.is_none());
        }
    }

    fn new_object_store_operator() -> Operator {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Operator::new(ObjectStoreBuilder::new(store)).expect("build should succeed")
    }

    fn sorted_entry_paths(entries: &[Entry]) -> Vec<String> {
        let mut paths = entries
            .iter()
            .map(|e| e.path().to_string())
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }

    #[tokio::test]
    async fn test_object_store_list_limit_is_page_hint() -> anyhow::Result<()> {
        let op = new_object_store_operator();
        for path in ["a/file-1", "a/file-2", "a/file-3"] {
            op.write(path, "content").await?;
        }

        let entries = op.list_with("a/").limit(1).await?;

        assert_eq!(
            sorted_entry_paths(&entries),
            vec![
                "a/file-1".to_string(),
                "a/file-2".to_string(),
                "a/file-3".to_string(),
            ],
            "limit is a page size hint and must not truncate the listing"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_object_store_list_non_recursive() -> anyhow::Result<()> {
        let op = new_object_store_operator();
        op.write("a/file.txt", "file").await?;
        op.write("a/sub/nested.txt", "nested").await?;
        op.write("a/sub2/other.txt", "other").await?;

        let entries = op.list("a/").await?;
        assert_eq!(
            sorted_entry_paths(&entries),
            vec![
                "a/file.txt".to_string(),
                "a/sub/".to_string(),
                "a/sub2/".to_string(),
            ]
        );

        let file = entries
            .iter()
            .find(|entry| entry.path() == "a/file.txt")
            .expect("direct file should be listed");
        assert_eq!(file.metadata().mode(), EntryMode::FILE);
        assert_eq!(file.metadata().content_length(), "file".len() as u64);
        assert!(file.metadata().last_modified().is_some());
        assert!(file.metadata().etag().is_some());

        for path in ["a/sub/", "a/sub2/"] {
            let dir = entries
                .iter()
                .find(|entry| entry.path() == path)
                .expect("direct child directory should be listed");
            assert_eq!(dir.metadata().mode(), EntryMode::DIR);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_object_store_list_recursive() -> anyhow::Result<()> {
        let op = new_object_store_operator();
        op.write("a/file.txt", "file").await?;
        op.write("a/sub/nested.txt", "nested").await?;
        op.write("a/sub2/other.txt", "other").await?;

        let entries = op.list_with("a/").recursive(true).await?;

        assert_eq!(
            sorted_entry_paths(&entries),
            vec![
                "a/file.txt".to_string(),
                "a/sub/nested.txt".to_string(),
                "a/sub2/other.txt".to_string(),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_object_store_list_start_after() -> anyhow::Result<()> {
        let op = new_object_store_operator();
        for path in ["a/file-1", "a/file-2", "a/file-3", "a/sub/nested"] {
            op.write(path, "content").await?;
        }

        let entries = op.list_with("a/").start_after("a/file-2").await?;
        assert_eq!(
            sorted_entry_paths(&entries),
            vec!["a/file-3".to_string(), "a/sub/".to_string()]
        );

        // The marker is inside `a/sub/`, so the folded directory sorts before
        // the marker and must not be returned.
        let entries = op.list_with("a/").start_after("a/sub/nested").await?;
        assert!(entries.is_empty());

        let entries = op
            .list_with("a/")
            .recursive(true)
            .start_after("a/file-2")
            .await?;
        assert_eq!(
            sorted_entry_paths(&entries),
            vec!["a/file-3".to_string(), "a/sub/nested".to_string()]
        );

        let entries = op
            .list_with("a/")
            .recursive(true)
            .start_after("a/sub/")
            .await?;
        assert_eq!(
            sorted_entry_paths(&entries),
            vec!["a/sub/nested".to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_object_store_list_root_and_missing_prefix() -> anyhow::Result<()> {
        let op = new_object_store_operator();
        op.write("root.txt", "root").await?;
        op.write("a/file.txt", "file").await?;
        op.write("a/sub/nested.txt", "nested").await?;

        for path in ["/", ""] {
            let entries = op.list(path).await?;
            assert_eq!(
                sorted_entry_paths(&entries),
                vec!["a/".to_string(), "root.txt".to_string()],
                "root listing for {path:?} should contain direct children only"
            );
        }

        for path in ["missing/", "missing", "missing/nested/"] {
            let entries = op.list(path).await?;
            assert!(
                entries.is_empty(),
                "missing prefix {path:?} should return no entries"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_object_store_list_prefix_semantics() -> anyhow::Result<()> {
        let op = new_object_store_operator();
        op.write("parent/y", "y").await?;
        op.write("parent/yy", "yy").await?;
        op.write("parent/z", "z").await?;

        // Listing a path without a trailing slash treats it as a prefix.
        let entries = op.list("parent/y").await?;
        assert_eq!(
            sorted_entry_paths(&entries),
            vec!["parent/y".to_string(), "parent/yy".to_string()]
        );

        let entries = op.list_with("parent/y").recursive(true).await?;
        assert_eq!(
            sorted_entry_paths(&entries),
            vec!["parent/y".to_string(), "parent/yy".to_string()]
        );

        // The listed path itself is returned as a directory entry.
        let entries = op.list("parent").await?;
        assert_eq!(sorted_entry_paths(&entries), vec!["parent/".to_string()]);
        assert_eq!(entries[0].metadata().mode(), EntryMode::DIR);
        Ok(())
    }

    #[tokio::test]
    async fn test_object_store_list_special_characters() -> anyhow::Result<()> {
        let op = new_object_store_operator();
        op.write("a#/child", "child").await?;

        let entries = op.list("a#").await?;
        assert_eq!(sorted_entry_paths(&entries), vec!["a#/".to_string()]);
        assert_eq!(entries[0].metadata().mode(), EntryMode::DIR);

        let entries = op.list("a#/").await?;
        assert_eq!(sorted_entry_paths(&entries), vec!["a#/child".to_string()]);
        assert_eq!(entries[0].metadata().mode(), EntryMode::FILE);

        let entries = op.list_with("a#").recursive(true).await?;
        assert_eq!(sorted_entry_paths(&entries), vec!["a#/child".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn test_object_store_list_start_after_special_characters() -> anyhow::Result<()> {
        let op = new_object_store_operator();
        for path in ["a/[", "a/\\", "a/~", "a/z"] {
            op.write(path, "content").await?;
        }

        let expected = vec!["a/\\".to_string(), "a/z".to_string(), "a/~".to_string()];

        let entries = op.list_with("a/").start_after("a/[").await?;
        assert_eq!(sorted_entry_paths(&entries), expected);

        let entries = op
            .list_with("a/")
            .recursive(true)
            .start_after("a/[")
            .await?;
        assert_eq!(sorted_entry_paths(&entries), expected);

        // Percent encoding reverses the order of `~` and `z`. The filter must
        // use decoded OpenDAL paths, not object_store's encoded locations.
        let entries = op
            .list_with("a/")
            .recursive(true)
            .start_after("a/z")
            .await?;
        assert_eq!(sorted_entry_paths(&entries), vec!["a/~".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn test_object_store_read_out_of_bounds_range_returns_range_not_satisfied()
    -> anyhow::Result<()> {
        let op = new_object_store_operator();
        op.write("file.txt", "hello").await?;
        op.write("empty.txt", "").await?;

        for (path, range) in [
            ("file.txt", BytesRange::from(10..20)),
            ("file.txt", BytesRange::from(10..)),
            ("file.txt", BytesRange::from(5..)),
            ("empty.txt", BytesRange::from(0..1)),
        ] {
            let err = op
                .read_with(path)
                .range(range)
                .await
                .expect_err("out-of-bounds range read must fail");
            assert_eq!(
                err.kind(),
                ErrorKind::RangeNotSatisfied,
                "path={path}, range={range}"
            );
        }

        let err = op
            .read_with("missing.txt")
            .range(10..20)
            .await
            .expect_err("missing object read must fail");
        assert_eq!(err.kind(), ErrorKind::NotFound);

        Ok(())
    }

    #[tokio::test]
    async fn test_object_store_in_bounds_generic_error_stays_unexpected() -> anyhow::Result<()> {
        #[derive(Debug)]
        struct FailingRangeStore {
            inner: InMemory,
        }

        impl std::fmt::Display for FailingRangeStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "FailingRangeStore")
            }
        }

        #[async_trait::async_trait]
        impl ObjectStore for FailingRangeStore {
            async fn put_opts(
                &self,
                location: &ObjectStorePath,
                payload: object_store::PutPayload,
                opts: object_store::PutOptions,
            ) -> object_store::Result<object_store::PutResult> {
                self.inner.put_opts(location, payload, opts).await
            }

            async fn put_multipart_opts(
                &self,
                location: &ObjectStorePath,
                opts: object_store::PutMultipartOptions,
            ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
                self.inner.put_multipart_opts(location, opts).await
            }

            async fn get_opts(
                &self,
                location: &ObjectStorePath,
                options: object_store::GetOptions,
            ) -> object_store::Result<object_store::GetResult> {
                if !options.head && options.range.is_some() {
                    return Err(object_store::Error::Generic {
                        store: "FailingRangeStore",
                        source: "transient backend error".into(),
                    });
                }
                self.inner.get_opts(location, options).await
            }

            fn delete_stream(
                &self,
                locations: futures::stream::BoxStream<
                    'static,
                    object_store::Result<ObjectStorePath>,
                >,
            ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectStorePath>>
            {
                self.inner.delete_stream(locations)
            }

            fn list(
                &self,
                prefix: Option<&ObjectStorePath>,
            ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
            {
                self.inner.list(prefix)
            }

            async fn list_with_delimiter(
                &self,
                prefix: Option<&ObjectStorePath>,
            ) -> object_store::Result<object_store::ListResult> {
                self.inner.list_with_delimiter(prefix).await
            }

            async fn copy_opts(
                &self,
                from: &ObjectStorePath,
                to: &ObjectStorePath,
                options: object_store::CopyOptions,
            ) -> object_store::Result<()> {
                self.inner.copy_opts(from, to, options).await
            }
        }

        let store: Arc<dyn ObjectStore> = Arc::new(FailingRangeStore {
            inner: InMemory::new(),
        });
        let op = Operator::new(ObjectStoreBuilder::new(store))?;
        op.write("file.txt", "hello").await?;

        let err = op
            .read_with("file.txt")
            .range(0..3)
            .await
            .expect_err("injected generic error must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        Ok(())
    }

    #[tokio::test]
    async fn test_object_store_not_modified_returns_condition_not_match() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend = ObjectStoreBuilder::new(store).build()?;
        let ctx = test_ctx();

        let mut writer = backend.write(&ctx, "file.txt", OpWrite::default())?;
        writer.write(Buffer::from("hello")).await?;
        writer.close().await?;

        let meta = backend
            .stat(&ctx, "file.txt", OpStat::default())
            .await?
            .into_metadata();
        let etag = meta.etag().expect("InMemory should provide etag");
        let last_modified = meta
            .last_modified()
            .expect("InMemory should provide last_modified");

        let err = backend
            .stat(
                &ctx,
                "file.txt",
                options::StatOptions {
                    if_none_match: Some(etag.to_string()),
                    ..Default::default()
                }
                .into(),
            )
            .await
            .expect_err("matching if_none_match on stat must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        let err = backend
            .stat(
                &ctx,
                "file.txt",
                options::StatOptions {
                    if_modified_since: Some(last_modified),
                    ..Default::default()
                }
                .into(),
            )
            .await
            .expect_err("unchanged if_modified_since on stat must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        let (_, op_read, _) = options::ReadOptions {
            if_none_match: Some(etag.to_string()),
            ..Default::default()
        }
        .into();
        let reader = backend.read(&ctx, "file.txt", op_read)?;
        let err = reader
            .open(BytesRange::default())
            .await
            .err()
            .expect("matching if_none_match on read must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        Ok(())
    }
}
