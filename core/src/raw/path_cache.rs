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

use crate::raw::{get_basename, get_parent};
use crate::*;
use async_trait::async_trait;
use moka::sync::Cache;

/// The trait required for path cacher.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PathQuery {
    /// Fetch the id for the root of the service.
    async fn root(&self) -> Result<String>;
    /// Query the id by parent_id and name.
    async fn query(&self, parent_id: &str, name: &str) -> Result<Option<String>>;
}

/// PathCacher is a cache for path query.
///
/// OpenDAL is designed for path based storage systemds, such as S3, HDFS, etc. But there are many
/// services that are not path based, such as OneDrive, Google Drive, etc. For these services, we
/// lookup files based on id. The lookup of id is very expensive, so we cache the path to id mapping
/// in PathCacher.
///
/// # Behavior
///
/// The `path` in cache is always an absolute. For example, if service root is `/root/`, then the
/// path of file `a/b` in cache will be `/root/a/b`.
pub struct PathCacher<Q: PathQuery> {
    query: Q,
    cache: Cache<String, String>,
}

impl<Q: PathQuery> PathCacher<Q> {
    /// Create a new path cacher.
    pub fn new(query: Q) -> Self {
        Self {
            query,
            cache: Cache::new(64 * 1024),
        }
    }

    /// Insert a new cache entry.
    pub fn insert(&self, path: &str, id: &str) {
        self.cache.insert(path.to_string(), id.to_string());
    }

    /// Remove a cache entry.
    pub fn remove(&self, path: &str) {
        self.cache.invalidate(path)
    }

    /// Get the id for the given path.
    pub async fn get(&self, path: &str) -> Result<Option<String>> {
        if let Some(id) = self.cache.get(path) {
            return Ok(Some(id));
        }

        let mut paths = vec![];
        let mut current_path = path;

        while current_path != "/" && !current_path.is_empty() {
            paths.push(current_path.to_string());
            current_path = get_parent(current_path);
            if let Some(id) = self.cache.get(current_path) {
                return self.query_down(&id, &paths).await;
            }
        }

        let root_id = self.query.root().await?;
        self.query_down(&root_id, &paths).await
    }

    async fn query_down(&self, start_id: &str, paths: &[String]) -> Result<Option<String>> {
        let mut current_id = start_id.to_string();
        for path in paths.iter().rev() {
            let name = get_basename(path);
            current_id = match self.query.query(&current_id, name).await? {
                Some(id) => {
                    self.cache.insert(path.clone(), id.clone());
                    id
                }
                None => return Ok(None),
            };
        }
        Ok(Some(current_id))
    }
}

#[cfg(test)]
mod tests {
    use crate::raw::{PathCacher, PathQuery};
    use crate::*;
    use async_trait::async_trait;

    struct TestQuery {}

    #[async_trait]
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
