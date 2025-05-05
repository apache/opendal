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

use futures::Future;
use moka::sync::Cache;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;

use crate::raw::*;
use crate::*;

/// The trait required for path cacher.
pub trait PathQuery {
    /// Fetch the id for the root of the service.
    fn root(&self) -> impl Future<Output = Result<String>> + MaybeSend;
    /// Query the id by parent_id and name.
    fn query(
        &self,
        parent_id: &str,
        name: &str,
    ) -> impl Future<Output = Result<Option<String>>> + MaybeSend;
    /// Create a dir by parent_id and name.
    fn create_dir(
        &self,
        parent_id: &str,
        name: &str,
    ) -> impl Future<Output = Result<String>> + MaybeSend;
}

/// PathCacher is a cache for path query.
///
/// OpenDAL is designed for path based storage systems, such as S3, HDFS, etc. But there are many
/// services that are not path based, such as OneDrive, Google Drive, etc. For these services, we
/// look up files based on id. The lookup of id is very expensive, so we cache the path to id mapping
/// in PathCacher.
///
/// # Behavior
///
/// The `path` in the cache is always an absolute one. For example, if the service root is `/root/`,
/// then the path of file `a/b` in cache will be `/root/a/b`.
pub struct PathCacher<Q: PathQuery> {
    query: Q,
    cache: Cache<String, String>,

    /// This optional lock here is used to prevent concurrent insertions of the same path.
    ///
    /// Some services like gdrive allows the same name to exist in the same directory. We need to introduce
    /// a global lock to prevent concurrent insertions of the same path.
    lock: Option<Mutex<()>>,
}

impl<Q: PathQuery> PathCacher<Q> {
    /// Create a new path cacher.
    pub fn new(query: Q) -> Self {
        Self {
            query,
            cache: Cache::new(64 * 1024),
            lock: None,
        }
    }

    /// Enable the lock for the path cacher.
    pub fn with_lock(mut self) -> Self {
        self.lock = Some(Mutex::default());
        self
    }

    async fn lock(&self) -> Option<MutexGuard<()>> {
        if let Some(l) = &self.lock {
            Some(l.lock().await)
        } else {
            None
        }
    }

    /// Insert a new cache entry.
    pub async fn insert(&self, path: &str, id: &str) {
        let _guard = self.lock().await;

        // This should never happen, but let's ignore the insert if happened.
        if self.cache.contains_key(path) {
            debug_assert!(
                self.cache.get(path) == Some(id.to_string()),
                "path {path} exists but it's value is inconsistent"
            );
            return;
        }

        self.cache.insert(path.to_string(), id.to_string());
    }

    /// Remove a cache entry.
    pub async fn remove(&self, path: &str) {
        let _guard = self.lock().await;

        self.cache.invalidate(path)
    }

    /// Get the id for the given path.
    pub async fn get(&self, path: &str) -> Result<Option<String>> {
        let _guard = self.lock().await;

        if let Some(id) = self.cache.get(path) {
            return Ok(Some(id));
        }

        let mut paths = VecDeque::new();
        let mut current_path = path;

        while current_path != "/" && !current_path.is_empty() {
            paths.push_front(current_path.to_string());
            current_path = get_parent(current_path);
            if let Some(id) = self.cache.get(current_path) {
                return self.query_down(&id, paths).await;
            }
        }

        let root_id = self.query.root().await?;
        self.cache.insert("/".to_string(), root_id.clone());
        self.query_down(&root_id, paths).await
    }

    /// `start_id` is the `file_id` to the start dir to query down.
    /// `paths` is in the order like `["/a/", "/a/b/", "/a/b/c/"]`.
    ///
    /// We should fetch the next `file_id` by sending `query`.
    async fn query_down(&self, start_id: &str, paths: VecDeque<String>) -> Result<Option<String>> {
        let mut current_id = start_id.to_string();
        for path in paths.into_iter() {
            let name = get_basename(&path);
            current_id = match self.query.query(&current_id, name).await? {
                Some(id) => {
                    self.cache.insert(path, id.clone());
                    id
                }
                None => return Ok(None),
            };
        }
        Ok(Some(current_id))
    }

    /// Ensure input dir exists.
    pub async fn ensure_dir(&self, path: &str) -> Result<String> {
        let _guard = self.lock().await;

        let mut tmp = "".to_string();
        // All parents that need to check.
        let mut parents = vec![];
        for component in path.split('/') {
            if component.is_empty() {
                continue;
            }

            tmp.push_str(component);
            tmp.push('/');
            parents.push(tmp.to_string());
        }

        let mut parent_id = match self.cache.get("/") {
            Some(v) => v,
            None => self.query.root().await?,
        };
        for parent in parents {
            parent_id = match self.cache.get(&parent) {
                Some(value) => value,
                None => {
                    let value = match self.query.query(&parent_id, get_basename(&parent)).await? {
                        Some(value) => value,
                        None => {
                            self.query
                                .create_dir(&parent_id, get_basename(&parent))
                                .await?
                        }
                    };
                    self.cache.insert(parent, value.clone());
                    value
                }
            }
        }

        Ok(parent_id)
    }
}

#[cfg(test)]
mod tests {

    use crate::raw::PathCacher;
    use crate::raw::PathQuery;
    use crate::*;

    struct TestQuery {}

    impl PathQuery for TestQuery {
        async fn root(&self) -> Result<String> {
            Ok("root/".to_string())
        }

        async fn query(&self, parent_id: &str, name: &str) -> Result<Option<String>> {
            if name.starts_with("not_exist") {
                return Ok(None);
            }
            Ok(Some(format!("{parent_id}{name}")))
        }

        async fn create_dir(&self, parent_id: &str, name: &str) -> Result<String> {
            Ok(format!("{parent_id}{name}"))
        }
    }

    #[tokio::test]
    async fn test_path_cacher_get() {
        let cases = vec![
            ("root", "/", Some("root/")),
            ("normal path", "/a", Some("root/a")),
            ("not exist normal dir", "/not_exist/a", None),
            ("not exist normal file", "/a/b/not_exist", None),
            ("nest path", "/a/b/c/d", Some("root/a/b/c/d")),
        ];

        for (name, input, expect) in cases {
            let cache = PathCacher::new(TestQuery {});

            let actual = cache.get(input).await.unwrap();
            assert_eq!(actual.as_deref(), expect, "{}", name)
        }
    }
}
