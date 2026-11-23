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

use std::collections::VecDeque;
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

fn take_pending(
    remaining: &mut VecDeque<(ObjectStorePath, String, OpDelete)>,
    object_path: &ObjectStorePath,
) -> Option<(String, OpDelete)> {
    let index = remaining
        .iter()
        .position(|(path, _, _)| path == object_path)?;
    remaining.remove(index).map(|(_, path, args)| (path, args))
}

fn take_pending_by_error_path(
    remaining: &mut VecDeque<(ObjectStorePath, String, OpDelete)>,
    error_path: Option<&str>,
) -> Option<(String, OpDelete)> {
    let error_path = error_path?;
    if let Ok(path) = ObjectStorePath::parse(error_path)
        && let Some(pending) = take_pending(remaining, &path)
    {
        return Some(pending);
    }

    take_pending(remaining, &ObjectStorePath::from(error_path))
}

fn fail_remaining(
    remaining: &mut VecDeque<(ObjectStorePath, String, OpDelete)>,
    result: &mut BatchDeleteResult,
    error: Error,
) {
    let kind = error.kind();
    let temporary = error.is_temporary();
    let message = error.to_string();
    let mut first_error = Some(error);

    for (_, path, args) in remaining.drain(..) {
        let error = first_error
            .take()
            .unwrap_or_else(|| Error::new(kind, message.clone()).with_temporary(temporary));
        result.failed.push((path, args, error));
    }
}

fn error_path(error: &object_store::Error) -> Option<&str> {
    match error {
        object_store::Error::NotFound { path, .. }
        | object_store::Error::AlreadyExists { path, .. }
        | object_store::Error::Precondition { path, .. }
        | object_store::Error::NotModified { path, .. }
        | object_store::Error::PermissionDenied { path, .. }
        | object_store::Error::Unauthenticated { path, .. } => Some(path),
        _ => None,
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
        let mut remaining = VecDeque::new();
        let mut result_batch = BatchDeleteResult::default();
        for (path, args) in paths {
            if is_directory_path(&path) {
                result_batch.succeeded.push((path, args));
                continue;
            }
            remaining.push_back((ObjectStorePath::from(path.as_str()), path, args));
        }

        let locations = remaining
            .iter()
            .map(|(path, _, _)| Ok::<_, object_store::Error>(path.clone()))
            .collect::<Vec<_>>();
        let stream = stream::iter(locations).boxed();
        let results = self.store.delete_stream(stream).collect::<Vec<_>>().await;

        let mut unattributed_error = None;
        let mut unmatched_not_found = 0;
        let mut unmatched_not_found_error = None;
        for result in results {
            match result {
                Ok(deleted_path) => {
                    if let Some((path, args)) = take_pending(&mut remaining, &deleted_path) {
                        result_batch.succeeded.push((path, args));
                    }
                }
                Err(err @ object_store::Error::NotFound { .. }) => {
                    if let Some((path, args)) =
                        take_pending_by_error_path(&mut remaining, error_path(&err))
                    {
                        result_batch.succeeded.push((path, args));
                    } else {
                        unmatched_not_found += 1;
                        if unmatched_not_found_error.is_none() {
                            unmatched_not_found_error = Some(parse_error(err));
                        }
                    }
                }
                Err(err) => {
                    if let Some((path, args)) =
                        take_pending_by_error_path(&mut remaining, error_path(&err))
                    {
                        result_batch.failed.push((path, args, parse_error(err)));
                    } else if unattributed_error.is_none() {
                        // A chunked store can return one error for an entire request. Keep
                        // consuming the stream so later per-object results are not discarded.
                        unattributed_error = Some(parse_error(err));
                    }
                }
            }
        }

        if unattributed_error.is_none() && unmatched_not_found == remaining.len() {
            // LocalFileSystem reports absolute filesystem paths in NotFound errors. Only
            // accept those unmatched errors when they account for every remaining input.
            for (_, path, args) in remaining.drain(..) {
                result_batch.succeeded.push((path, args));
            }
        } else if !remaining.is_empty() {
            let error = unattributed_error
                .or(unmatched_not_found_error)
                .unwrap_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "object store delete stream ended without a result for every input path",
                    )
                });
            fail_remaining(&mut remaining, &mut result_batch, error);
        }

        Ok(result_batch)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::io;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::prefix::PrefixStore;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use opendal::raw::oio::BatchDelete;

    use super::*;
    use crate::ObjectStoreBuilder;

    #[derive(Debug)]
    struct ScriptedDeleteStore {
        results: Mutex<Vec<object_store::Result<ObjectStorePath>>>,
    }

    impl ScriptedDeleteStore {
        fn new(results: Vec<object_store::Result<ObjectStorePath>>) -> Self {
            Self {
                results: Mutex::new(results),
            }
        }
    }

    impl fmt::Display for ScriptedDeleteStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("ScriptedDeleteStore")
        }
    }

    #[async_trait]
    impl ObjectStore for ScriptedDeleteStore {
        async fn put_opts(
            &self,
            _: &ObjectStorePath,
            _: PutPayload,
            _: PutOptions,
        ) -> object_store::Result<PutResult> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _: &ObjectStorePath,
            _: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }

        async fn get_opts(
            &self,
            _: &ObjectStorePath,
            _: GetOptions,
        ) -> object_store::Result<GetResult> {
            unimplemented!()
        }

        fn delete_stream(
            &self,
            _: BoxStream<'static, object_store::Result<ObjectStorePath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectStorePath>> {
            stream::iter(std::mem::take(&mut *self.results.lock().unwrap())).boxed()
        }

        fn list(
            &self,
            _: Option<&ObjectStorePath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            unimplemented!()
        }

        async fn list_with_delimiter(
            &self,
            _: Option<&ObjectStorePath>,
        ) -> object_store::Result<ListResult> {
            unimplemented!()
        }

        async fn copy_opts(
            &self,
            _: &ObjectStorePath,
            _: &ObjectStorePath,
            _: CopyOptions,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    fn generic_error() -> object_store::Error {
        object_store::Error::Generic {
            store: "scripted",
            source: io::Error::other("chunk delete failed").into(),
        }
    }

    fn not_found(path: &str) -> object_store::Error {
        object_store::Error::NotFound {
            path: path.to_string(),
            source: io::Error::new(io::ErrorKind::NotFound, "missing object").into(),
        }
    }

    fn permission_denied(path: &str) -> object_store::Error {
        object_store::Error::PermissionDenied {
            path: path.to_string(),
            source: io::Error::new(io::ErrorKind::PermissionDenied, "access denied").into(),
        }
    }

    async fn delete_batch(store: Arc<dyn ObjectStore>, paths: &[&str]) -> BatchDeleteResult {
        ObjectStoreDeleter::new(store)
            .delete_batch(
                paths
                    .iter()
                    .map(|path| ((*path).to_string(), OpDelete::default()))
                    .collect(),
            )
            .await
            .unwrap()
    }

    fn assert_result_paths(
        result: &BatchDeleteResult,
        mut succeeded: Vec<&str>,
        mut failed: Vec<&str>,
    ) {
        let mut actual_succeeded = result
            .succeeded
            .iter()
            .map(|(path, _)| path.as_str())
            .collect::<Vec<_>>();
        let mut actual_failed = result
            .failed
            .iter()
            .map(|(path, _, _)| path.as_str())
            .collect::<Vec<_>>();
        actual_succeeded.sort_unstable();
        actual_failed.sort_unstable();
        succeeded.sort_unstable();
        failed.sort_unstable();
        assert_eq!(actual_succeeded, succeeded);
        assert_eq!(actual_failed, failed);
    }

    #[tokio::test]
    async fn test_delete_batch_uses_returned_paths() {
        let store =
            ScriptedDeleteStore::new(vec![Ok(ObjectStorePath::from("b")), Err(generic_error())]);

        let result = delete_batch(Arc::new(store), &["a", "b"]).await;

        assert_result_paths(&result, vec!["b"], vec!["a"]);
        assert_eq!(result.failed[0].2.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_delete_batch_chunk_error_accounts_for_every_path() {
        let store = ScriptedDeleteStore::new(vec![Err(generic_error())]);

        let result = delete_batch(Arc::new(store), &["a", "b", "c"]).await;

        assert_result_paths(&result, vec![], vec!["a", "b", "c"]);
        assert!(
            result
                .failed
                .iter()
                .all(|(_, _, err)| err.kind() == ErrorKind::Unexpected)
        );
    }

    #[tokio::test]
    async fn test_delete_batch_partial_chunk_error_preserves_successes() {
        let store =
            ScriptedDeleteStore::new(vec![Ok(ObjectStorePath::from("b")), Err(generic_error())]);

        let result = delete_batch(Arc::new(store), &["a", "b", "c"]).await;

        assert_result_paths(&result, vec!["b"], vec!["a", "c"]);
    }

    #[tokio::test]
    async fn test_delete_batch_ignores_extra_results() {
        let store = ScriptedDeleteStore::new(vec![
            Ok(ObjectStorePath::from("a")),
            Ok(ObjectStorePath::from("b")),
            Ok(ObjectStorePath::from("extra")),
        ]);

        let result = delete_batch(Arc::new(store), &["a", "b"]).await;

        assert_result_paths(&result, vec!["a", "b"], vec![]);
    }

    #[tokio::test]
    async fn test_delete_batch_missing_results_are_failed() {
        let store = ScriptedDeleteStore::new(vec![Ok(ObjectStorePath::from("b"))]);

        let result = delete_batch(Arc::new(store), &["a", "b", "c"]).await;

        assert_result_paths(&result, vec!["b"], vec!["a", "c"]);
        assert!(
            result
                .failed
                .iter()
                .all(|(_, _, err)| err.kind() == ErrorKind::Unexpected)
        );
    }

    #[tokio::test]
    async fn test_delete_batch_duplicate_paths_are_accounted_individually() {
        let store =
            ScriptedDeleteStore::new(vec![Ok(ObjectStorePath::from("a")), Err(not_found("a"))]);

        let result = delete_batch(Arc::new(store), &["a", "a"]).await;

        assert_result_paths(&result, vec!["a", "a"], vec![]);
    }

    #[tokio::test]
    async fn test_delete_batch_matches_prefixed_success_paths() {
        let store = PrefixStore::new(
            ScriptedDeleteStore::new(vec![
                Ok(ObjectStorePath::from("prefix/c")),
                Err(generic_error()),
            ]),
            "prefix",
        );

        let result = delete_batch(Arc::new(store), &["a", "b", "c"]).await;

        assert_result_paths(&result, vec!["c"], vec!["a", "b"]);
    }

    #[tokio::test]
    async fn test_delete_batch_chunk_error_does_not_hide_later_success() {
        let store =
            ScriptedDeleteStore::new(vec![Err(generic_error()), Ok(ObjectStorePath::from("b"))]);

        let result = delete_batch(Arc::new(store), &["a", "b"]).await;

        assert_result_paths(&result, vec!["b"], vec!["a"]);
        assert_eq!(result.failed[0].2.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_delete_batch_defers_unmatched_not_found() {
        let store = ScriptedDeleteStore::new(vec![
            Err(not_found("/absolute/b")),
            Ok(ObjectStorePath::from("a")),
        ]);

        let result = delete_batch(Arc::new(store), &["a", "b"]).await;

        assert_result_paths(&result, vec!["a", "b"], vec![]);
    }

    #[tokio::test]
    async fn test_delete_batch_unconfirmed_not_found_does_not_invent_success() {
        let mixed_failures = ScriptedDeleteStore::new(vec![
            Err(generic_error()),
            Err(not_found("/absolute/missing")),
        ]);
        let result = delete_batch(Arc::new(mixed_failures), &["a", "missing"]).await;
        assert_result_paths(&result, vec![], vec!["a", "missing"]);
        assert!(
            result
                .failed
                .iter()
                .all(|(_, _, err)| err.kind() == ErrorKind::Unexpected)
        );

        let chunk_not_found = ScriptedDeleteStore::new(vec![Err(not_found("a,b,c"))]);
        let result = delete_batch(Arc::new(chunk_not_found), &["a", "b", "c"]).await;
        assert_result_paths(&result, vec![], vec!["a", "b", "c"]);
        assert!(
            result
                .failed
                .iter()
                .all(|(_, _, err)| err.kind() == ErrorKind::NotFound)
        );
    }

    #[tokio::test]
    async fn test_delete_batch_matches_encoded_not_found_path() {
        let store = ScriptedDeleteStore::new(vec![
            Err(not_found("a%23b.txt")),
            Ok(ObjectStorePath::from("other")),
        ]);

        let result = delete_batch(Arc::new(store), &["other", "a#b.txt"]).await;

        assert_result_paths(&result, vec!["a#b.txt", "other"], vec![]);
    }

    #[tokio::test]
    async fn test_delete_batch_matches_encoded_error_path() {
        let store = ScriptedDeleteStore::new(vec![
            Err(permission_denied("a%23b.txt")),
            Ok(ObjectStorePath::from("other")),
        ]);

        let result = delete_batch(Arc::new(store), &["a#b.txt", "other"]).await;

        assert_result_paths(&result, vec!["other"], vec!["a#b.txt"]);
        assert_eq!(result.failed[0].2.kind(), ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn test_delete_batch_prefixed_errors_do_not_invent_success() {
        let store = PrefixStore::new(
            ScriptedDeleteStore::new(vec![
                Err(not_found("prefix/b")),
                Err(permission_denied("prefix/a")),
            ]),
            "prefix",
        );

        let result = delete_batch(Arc::new(store), &["a", "b"]).await;

        assert_result_paths(&result, vec![], vec!["a", "b"]);
        assert!(
            result
                .failed
                .iter()
                .all(|(_, _, err)| err.kind() == ErrorKind::PermissionDenied)
        );
    }

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
        let op = Operator::new(ObjectStoreBuilder::new(Arc::new(store))).unwrap();

        op.delete("missing.txt").await.unwrap();
        op.delete_iter(["present.txt", "missing.txt", "also-missing.txt"])
            .await
            .unwrap();
        assert_eq!(
            op.stat("present.txt").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }
}
