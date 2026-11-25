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
use std::vec::IntoIter;

use opendal_core::MetadataBuilder;
use opendal_core::Result;
use opendal_core::raw::oio::{Entry, List};
use opendal_core::raw::{build_absolute_path, normalize_path};

use super::core::MysqlCore;

pub struct MysqlLister {
    root_prefix: String,
    entries: IntoIter<String>,
}

impl MysqlLister {
    pub async fn new(core: Arc<MysqlCore>, root: String, path: String) -> Result<Self> {
        let prefix = build_absolute_path(&root, &path);
        let entries = core.list(&prefix).await?;
        Ok(Self::from_entries(&root, entries))
    }

    fn from_entries(root: &str, entries: Vec<String>) -> Self {
        Self {
            root_prefix: build_absolute_path(root, "/"),
            entries: entries.into_iter(),
        }
    }
}

impl List for MysqlLister {
    async fn next(&mut self) -> Result<Option<Entry>> {
        for key in self.entries.by_ref() {
            let Some(rel) = key.strip_prefix(&self.root_prefix) else {
                continue;
            };
            // Ignore external keys that OpenDAL cannot address by their exact path.
            if !rel.is_empty() && (rel == "/" || normalize_path(rel) != rel) {
                continue;
            }
            let path = if rel.is_empty() { "/" } else { rel };
            let meta = if path.ends_with('/') {
                MetadataBuilder::dir()
            } else {
                MetadataBuilder::unknown()
            };
            return Ok(Some(Entry::new(path, meta.build())));
        }
        Ok(None)
    }
}

pub struct MysqlLazyLister {
    core: Arc<MysqlCore>,
    root: String,
    path: String,
    inner: Option<MysqlLister>,
}

impl MysqlLazyLister {
    pub fn new(core: Arc<MysqlCore>, root: String, path: String) -> Self {
        Self {
            core,
            root,
            path,
            inner: None,
        }
    }
}

impl List for MysqlLazyLister {
    async fn next(&mut self) -> Result<Option<Entry>> {
        if self.inner.is_none() {
            self.inner = Some(
                MysqlLister::new(self.core.clone(), self.root.clone(), self.path.clone()).await?,
            );
        }

        self.inner
            .as_mut()
            .expect("lister must be initialized")
            .next()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::EntryMode;

    async fn collect(mut lister: MysqlLister) -> Vec<(String, EntryMode)> {
        let mut out = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            out.push((entry.path().to_string(), entry.metadata().mode()));
        }
        out
    }

    #[tokio::test]
    async fn lister_skips_cross_root_and_shorter_non_matching_keys() {
        let lister = MysqlLister::from_entries(
            "/App/",
            vec![
                "app/secret.txt".to_string(),
                "App/".to_string(),
                "App/public.txt".to_string(),
                "App/dir/".to_string(),
            ],
        );

        assert_eq!(
            collect(lister).await,
            vec![
                ("/".to_string(), EntryMode::DIR),
                ("public.txt".to_string(), EntryMode::Unknown),
                ("dir/".to_string(), EntryMode::DIR),
            ]
        );
    }

    #[tokio::test]
    async fn lister_does_not_panic_on_byte_length_mismatched_keys() {
        // Under accent-insensitive collations, `"é/"` (3 bytes) can match a
        // 4-byte decomposed prefix `"e\u{0301}/"`, and `"e/"` (2 bytes) can
        // match a 3-byte prefix `"é/"`.
        let decomposed = MysqlLister::from_entries(
            "/e\u{0301}/",
            vec!["é/".to_string(), "e\u{0301}/ok.txt".to_string()],
        );
        assert_eq!(
            collect(decomposed).await,
            vec![("ok.txt".to_string(), EntryMode::Unknown)]
        );

        let accented =
            MysqlLister::from_entries("/é/", vec!["e/".to_string(), "é/ok.txt".to_string()]);
        assert_eq!(
            collect(accented).await,
            vec![("ok.txt".to_string(), EntryMode::Unknown)]
        );
    }

    #[tokio::test]
    async fn lister_skips_non_normalized_keys() {
        let lister =
            MysqlLister::from_entries("/", vec!["Dir/a".to_string(), "Dir//bad".to_string()]);
        assert_eq!(
            collect(lister).await,
            vec![("Dir/a".to_string(), EntryMode::Unknown)]
        );

        let root_lister = MysqlLister::from_entries(
            "/",
            vec![
                String::new(),
                "/external".to_string(),
                "file.txt".to_string(),
            ],
        );
        assert_eq!(
            collect(root_lister).await,
            vec![
                ("/".to_string(), EntryMode::DIR),
                ("file.txt".to_string(), EntryMode::Unknown),
            ]
        );
    }
}
