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

use std::collections::HashSet;

use crate::raw::*;
use crate::*;

/// ToHierarchyLister will convert a flat list to hierarchy by filter
/// not needed entries.
///
/// # Notes
///
/// ToHierarchyLister filter entries after fetch entries. So it's possible
/// to return an empty vec. It doesn't mean the all pages have been
/// returned.
///
/// Please keep calling next until we returned `Ok(None)`
pub struct HierarchyLister<P> {
    lister: P,
    path: String,
    visited: HashSet<String>,
    recursive: bool,
}

impl<P> HierarchyLister<P> {
    /// Create a new hierarchy lister
    pub fn new(lister: P, path: &str, recursive: bool) -> HierarchyLister<P> {
        let path = if path == "/" {
            "".to_string()
        } else {
            path.to_string()
        };

        HierarchyLister {
            lister,
            path,
            visited: HashSet::default(),
            recursive,
        }
    }

    /// ## NOTES
    ///
    /// We take `&mut Entry` here because we need to perform modification on entry in the case like
    /// listing "a/" with existing key `a/b/c`.
    ///
    /// In this case, we got a key `a/b/c`, but we should return `a/b/` instead to keep the hierarchy.
    fn keep_entry(&mut self, e: &mut oio::Entry) -> bool {
        // If path is not started with prefix, drop it.
        //
        // Ideally, it should never happen. But we just tolerate
        // this state.
        if !e.path().starts_with(&self.path) {
            return false;
        }

        // Don't return already visited path.
        if self.visited.contains(e.path()) {
            return false;
        }

        let prefix_len = self.path.len();

        let idx = if let Some(idx) = e.path()[prefix_len..].find('/') {
            idx + prefix_len + 1
        } else {
            // If there is no `/` in path, it's a normal file, we
            // can return it directly.
            return true;
        };

        // idx == path.len() means it's contain only one `/` at the
        // end of path.
        if idx == e.path().len() {
            if !self.visited.contains(e.path()) {
                self.visited.insert(e.path().to_string());
            }
            return true;
        }

        // If idx < path.len() means that are more levels to come.
        // We should check the first dir path.
        let has = {
            let path = &e.path()[..idx];
            self.visited.contains(path)
        };
        if !has {
            let path = {
                let path = &e.path()[..idx];
                path.to_string()
            };

            // The parent directory is synthesized from a descendant. Its metadata
            // describes that descendant, not this directory, so start from fresh
            // directory metadata instead of carrying file fields over.
            *e = oio::Entry::with(path.clone(), MetadataBuilder::dir().build());
            self.visited.insert(path);

            return true;
        }

        false
    }
}

impl<P: oio::List> oio::List for HierarchyLister<P> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            let mut entry = match self.lister.next().await? {
                Some(entry) => entry,
                None => return Ok(None),
            };

            if self.recursive {
                return Ok(Some(entry));
            }
            if self.keep_entry(&mut entry) {
                return Ok(Some(entry));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::oio::List as _;

    struct VecLister {
        entries: std::vec::IntoIter<oio::Entry>,
    }

    impl oio::List for VecLister {
        async fn next(&mut self) -> Result<Option<oio::Entry>> {
            Ok(self.entries.next())
        }
    }

    fn file_metadata() -> Metadata {
        let mut builder = MetadataBuilder::file(1024);
        builder
            .cache_control("max-age=3600")
            .content_disposition("attachment")
            .content_md5("content-md5")
            .content_type("text/plain")
            .content_encoding("gzip")
            .content_language("en")
            .etag("etag")
            .version("v1")
            .last_modified(Timestamp::new(1_700_000_000, 123_456_789).unwrap())
            .is_current(Some(true))
            .is_deleted(true)
            .user_metadata([("owner".to_string(), "opendal".to_string())]);
        builder.build()
    }

    async fn drain(mut lister: HierarchyLister<VecLister>) -> Vec<oio::Entry> {
        let mut entries = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            entries.push(entry);
        }
        entries
    }

    #[tokio::test]
    async fn nested_file_creates_directory_without_file_metadata() {
        let lister = HierarchyLister::new(
            VecLister {
                entries: vec![oio::Entry::new("a/b/c", file_metadata())].into_iter(),
            },
            "a/",
            false,
        );

        let entries = drain(lister).await;
        assert_eq!(entries.len(), 1);

        let entry = &entries[0];
        assert_eq!(entry.path(), "a/b/");

        let metadata = entry.metadata();
        assert_eq!(metadata.mode(), EntryMode::DIR);
        assert_eq!(metadata.content_length(), 0);
        assert_eq!(metadata.cache_control(), None);
        assert_eq!(metadata.content_disposition(), None);
        assert_eq!(metadata.content_md5(), None);
        assert_eq!(metadata.content_type(), None);
        assert_eq!(metadata.content_encoding(), None);
        assert_eq!(metadata.content_language(), None);
        assert_eq!(metadata.etag(), None);
        assert_eq!(metadata.version(), None);
        assert_eq!(metadata.last_modified(), None);
        assert_eq!(metadata.is_current(), None);
        assert!(!metadata.is_deleted());
        assert!(metadata.user_metadata().is_none());
    }

    #[tokio::test]
    async fn direct_directory_preserves_its_metadata() {
        let modified = Timestamp::new(1_700_000_000, 1).unwrap();
        let mut builder = MetadataBuilder::dir();
        builder
            .etag("dir-etag")
            .version("dir-v1")
            .content_type("application/x-directory")
            .last_modified(modified)
            .is_current(Some(false))
            .user_metadata([("owner".to_string(), "opendal".to_string())]);

        let lister = HierarchyLister::new(
            VecLister {
                entries: vec![oio::Entry::new("a/b/", builder.build())].into_iter(),
            },
            "a/",
            false,
        );

        let entries = drain(lister).await;
        assert_eq!(entries.len(), 1);

        let entry = &entries[0];
        assert_eq!(entry.path(), "a/b/");

        let metadata = entry.metadata();
        assert_eq!(metadata.mode(), EntryMode::DIR);
        assert_eq!(metadata.etag(), Some("dir-etag"));
        assert_eq!(metadata.version(), Some("dir-v1"));
        assert_eq!(metadata.content_type(), Some("application/x-directory"));
        assert_eq!(metadata.last_modified(), Some(modified));
        assert_eq!(metadata.is_current(), Some(false));
        assert_eq!(
            metadata.user_metadata().unwrap().get("owner"),
            Some("opendal")
        );
    }

    #[tokio::test]
    async fn direct_file_preserves_its_metadata() {
        let lister = HierarchyLister::new(
            VecLister {
                entries: vec![oio::Entry::new("a/file", file_metadata())].into_iter(),
            },
            "a/",
            false,
        );

        let entries = drain(lister).await;
        assert_eq!(entries.len(), 1);

        let entry = &entries[0];
        assert_eq!(entry.path(), "a/file");

        let metadata = entry.metadata();
        assert_eq!(metadata.mode(), EntryMode::FILE);
        assert_eq!(metadata.content_length(), 1024);
        assert_eq!(metadata.etag(), Some("etag"));
        assert_eq!(metadata.content_type(), Some("text/plain"));
        assert!(metadata.is_deleted());
    }
}
