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

use std::sync::Arc;

use futures::stream::{self, StreamExt};
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectStorePath;
use opendal::raw::oio::BatchDeleteResult;
use opendal::raw::*;
use opendal::*;

use super::error::parse_error;

pub struct ObjectStoreDeleter {
    store: Arc<dyn ObjectStore + 'static>,
}

impl ObjectStoreDeleter {
    pub(crate) fn new(store: Arc<dyn ObjectStore + 'static>) -> Self {
        Self { store }
    }
}

/// Object store keys cannot end with `/`, and `object_store::path::Path::from`
/// drops a trailing slash. A directory path therefore has no marker object to
/// delete, and converting it would target the same-named regular file.
fn is_directory_path(path: &str) -> bool {
    path.ends_with('/')
}

impl oio::BatchDelete for ObjectStoreDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        if is_directory_path(&path) {
            return Ok(());
        }

        let object_path = ObjectStorePath::from(path);
        match self.store.delete(&object_path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(parse_error(err)),
        }
    }

    async fn delete_batch(&self, paths: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let mut files = Vec::new();
        let mut result_batch = BatchDeleteResult::default();
        for (path, args) in paths {
            if is_directory_path(&path) {
                result_batch.succeeded.push((path, args));
            } else {
                files.push((path, args));
            }
        }

        if files.is_empty() {
            return Ok(result_batch);
        }

        let locations: Vec<_> = files
            .iter()
            .map(|(path, _)| Ok(ObjectStorePath::from(path.as_str())))
            .collect();
        let mut results = self.store.delete_stream(stream::iter(locations).boxed());
        for (path, args) in files {
            let Some(result) = results.next().await else {
                break;
            };
            match result {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {
                    result_batch.succeeded.push((path, args));
                }
                Err(
                    err @ (object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. }
                    | object_store::Error::NotModified { .. }
                    | object_store::Error::PermissionDenied { .. }
                    | object_store::Error::Unauthenticated { .. }),
                ) => {
                    result_batch.failed.push((path, args, parse_error(err)));
                }
                // An unattributed bulk error can stand for an entire chunk.
                Err(err) => return Err(parse_error(err)),
            }
        }

        Ok(result_batch)
    }
}

#[cfg(test)]
mod tests {
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;

    use super::*;
    use crate::ObjectStoreBuilder;

    #[tokio::test]
    async fn test_delete_directory_path_keeps_same_named_file() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&ObjectStorePath::from("foo"), "file".into())
            .await
            .unwrap();
        store
            .put(&ObjectStorePath::from("foo/child"), "child".into())
            .await
            .unwrap();
        let op = Operator::new(ObjectStoreBuilder::new(store)).unwrap();

        op.delete("foo/").await.unwrap();
        assert!(op.stat("foo").await.unwrap().is_file());
        assert!(op.stat("foo/child").await.unwrap().is_file());

        op.delete_iter(["foo/", "missing"]).await.unwrap();
        assert!(op.stat("foo").await.unwrap().is_file());
        assert!(op.stat("foo/child").await.unwrap().is_file());

        op.delete_iter(["foo/", "bar/"]).await.unwrap();
        assert!(op.stat("foo").await.unwrap().is_file());
        assert!(op.stat("foo/child").await.unwrap().is_file());
    }

    #[tokio::test]
    async fn test_delete_directory_and_root_paths_on_local_filesystem() {
        let root = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(root.path()).unwrap());
        store
            .put(&ObjectStorePath::from("foo"), "file".into())
            .await
            .unwrap();
        let op = Operator::new(ObjectStoreBuilder::new(store)).unwrap();

        op.delete("/").await.unwrap();
        op.delete("foo/").await.unwrap();
        op.delete_iter(["/", "foo/"]).await.unwrap();
        assert!(op.stat("foo").await.unwrap().is_file());
    }

    #[tokio::test]
    async fn test_local_filesystem_missing_deletes_are_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        store
            .put(&ObjectStorePath::from("present.txt"), "content".into())
            .await
            .unwrap();
        store
            .put(&ObjectStorePath::from("keep"), "keep contents".into())
            .await
            .unwrap();
        let op = Operator::new(ObjectStoreBuilder::new(Arc::new(store))).unwrap();

        op.delete("missing.txt").await.unwrap();
        op.delete_iter([
            "/",
            "keep/",
            "present.txt",
            "missing.txt",
            "also-missing.txt",
        ])
        .await
        .unwrap();
        assert_eq!(op.read("keep").await.unwrap().to_bytes(), "keep contents");
        assert_eq!(
            op.stat("present.txt").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }
}
