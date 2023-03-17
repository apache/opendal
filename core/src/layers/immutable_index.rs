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
use std::fmt::Debug;
use std::mem;

use async_trait::async_trait;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Add an immutable in-memory index for underlying storage services.
///
/// Especially useful for services without list capability like HTTP.
///
/// # Examples
///
/// ```rust, no_run
/// use opendal::layers::ImmutableIndexLayer;
/// use opendal::services;
/// use opendal::Operator;
///
/// let mut iil = ImmutableIndexLayer::default();
///
/// for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
///     iil.insert(i.to_string())
/// }
///
/// let op = Operator::from_env::<services::Http>()
///     .unwrap()
///     .layer(iil)
///     .finish();
/// ```
#[derive(Default, Debug, Clone)]
pub struct ImmutableIndexLayer {
    vec: Vec<String>,
}

impl ImmutableIndexLayer {
    /// Insert a key into index.
    pub fn insert(&mut self, key: String) {
        self.vec.push(key);
    }

    /// Insert keys from iter.
    pub fn extend_iter<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = String>,
    {
        self.vec.extend(iter);
    }
}

impl<A: Accessor> Layer<A> for ImmutableIndexLayer {
    type LayeredAccessor = ImmutableIndexAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        ImmutableIndexAccessor {
            vec: self.vec.clone(),
            inner,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ImmutableIndexAccessor<A: Accessor> {
    inner: A,
    vec: Vec<String>,
}

impl<A: Accessor> ImmutableIndexAccessor<A> {
    fn children_flat(&self, path: &str) -> Vec<String> {
        self.vec
            .iter()
            .filter(|v| v.starts_with(path) && v.as_str() != path)
            .cloned()
            .collect()
    }

    fn children_hierarchy(&self, path: &str) -> Vec<String> {
        let mut res = HashSet::new();

        for i in self.vec.iter() {
            // `/xyz` should not belong to `/abc`
            if !i.starts_with(path) {
                continue;
            }

            // remove `/abc` if self
            if i == path {
                continue;
            }

            match i[path.len()..].find('/') {
                // File `/abc/def.csv` must belong to `/abc`
                None => {
                    res.insert(i.to_string());
                }
                Some(idx) => {
                    // The index of first `/` after `/abc`.
                    let dir_idx = idx + 1 + path.len();

                    if dir_idx == i.len() {
                        // Dir `/abc/def/` belongs to `/abc/`
                        res.insert(i.to_string());
                    } else {
                        // File/Dir `/abc/def/xyz` doesn't belong to `/abc`.
                        // But we need to list `/abc/def` out so that we can walk down.
                        res.insert(i[..dir_idx].to_string());
                    }
                }
            }
        }

        res.into_iter().collect()
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for ImmutableIndexAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Pager = ImmutableDir;
    type BlockingPager = ImmutableDir;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    /// Add list capabilities for underlying storage services.
    fn metadata(&self) -> AccessorInfo {
        let mut meta = self.inner.info();
        meta.set_capabilities(
            meta.capabilities() | AccessorCapability::List | AccessorCapability::Scan,
        );
        meta.set_hints(meta.hints());

        meta
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Pager)> {
        let mut path = path;
        if path == "/" {
            path = ""
        }

        Ok((
            RpList::default(),
            ImmutableDir::new(self.children_hierarchy(path)),
        ))
    }

    async fn scan(&self, path: &str, _: OpScan) -> Result<(RpScan, Self::Pager)> {
        let mut path = path;
        if path == "/" {
            path = ""
        }

        Ok((
            RpScan::default(),
            ImmutableDir::new(self.children_flat(path)),
        ))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner.blocking_read(path, args)
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    fn blocking_list(&self, path: &str, _: OpList) -> Result<(RpList, Self::BlockingPager)> {
        let mut path = path;
        if path == "/" {
            path = ""
        }

        Ok((
            RpList::default(),
            ImmutableDir::new(self.children_hierarchy(path)),
        ))
    }

    fn blocking_scan(&self, path: &str, _: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        let mut path = path;
        if path == "/" {
            path = ""
        }

        Ok((
            RpScan::default(),
            ImmutableDir::new(self.children_flat(path)),
        ))
    }
}

pub struct ImmutableDir {
    idx: Vec<String>,
}

impl ImmutableDir {
    fn new(idx: Vec<String>) -> Self {
        Self { idx }
    }

    fn inner_next_page(&mut self) -> Option<Vec<oio::Entry>> {
        if self.idx.is_empty() {
            return None;
        }

        let vs = mem::take(&mut self.idx);

        Some(
            vs.into_iter()
                .map(|v| {
                    let mode = if v.ends_with('/') {
                        EntryMode::DIR
                    } else {
                        EntryMode::FILE
                    };
                    let meta = Metadata::new(mode);
                    oio::Entry::with(v, meta)
                })
                .collect(),
        )
    }
}

#[async_trait]
impl oio::Page for ImmutableDir {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        Ok(self.inner_next_page())
    }
}

impl oio::BlockingPage for ImmutableDir {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        Ok(self.inner_next_page())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use anyhow::Result;
    use futures::TryStreamExt;
    use log::debug;

    use super::*;
    use crate::layers::LoggingLayer;
    use crate::services::Http;
    use crate::EntryMode;
    use crate::Operator;

    #[tokio::test]
    async fn test_list() -> Result<()> {
        let _ = env_logger::try_init();

        let mut iil = ImmutableIndexLayer::default();
        for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
            iil.insert(i.to_string())
        }

        let op = Operator::new(Http::from_iter(
            vec![("endpoint".to_string(), "https://xuanwo.io".to_string())].into_iter(),
        ))?
        .layer(LoggingLayer::default())
        .layer(iil)
        .finish();

        let mut map = HashMap::new();
        let mut set = HashSet::new();
        let mut ds = op.list("").await?;
        while let Some(entry) = ds.try_next().await? {
            debug!("got entry: {}", entry.path());
            assert!(
                set.insert(entry.path().to_string()),
                "duplicated value: {}",
                entry.path()
            );
            map.insert(
                entry.path().to_string(),
                op.metadata(&entry, Metakey::Mode).await?.mode(),
            );
        }

        assert_eq!(map["file"], EntryMode::FILE);
        assert_eq!(map["dir/"], EntryMode::DIR);
        assert_eq!(map["dir_without_prefix/"], EntryMode::DIR);
        Ok(())
    }

    #[tokio::test]
    async fn test_scan() -> Result<()> {
        let _ = env_logger::try_init();

        let mut iil = ImmutableIndexLayer::default();
        for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
            iil.insert(i.to_string())
        }

        let op = Operator::new(Http::from_iter(
            vec![("endpoint".to_string(), "https://xuanwo.io".to_string())].into_iter(),
        ))?
        .layer(LoggingLayer::default())
        .layer(iil)
        .finish();

        let mut ds = op.scan("/").await?;
        let mut set = HashSet::new();
        let mut map = HashMap::new();
        while let Some(entry) = ds.try_next().await? {
            debug!("got entry: {}", entry.path());
            assert!(
                set.insert(entry.path().to_string()),
                "duplicated value: {}",
                entry.path()
            );
            map.insert(
                entry.path().to_string(),
                op.metadata(&entry, Metakey::Mode).await?.mode(),
            );
        }

        debug!("current files: {:?}", map);

        assert_eq!(map["file"], EntryMode::FILE);
        assert_eq!(map["dir/"], EntryMode::DIR);
        assert_eq!(map["dir_without_prefix/file"], EntryMode::FILE);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_dir() -> Result<()> {
        let _ = env_logger::try_init();

        let mut iil = ImmutableIndexLayer::default();
        for i in [
            "dataset/stateful/ontime_2007_200.csv",
            "dataset/stateful/ontime_2008_200.csv",
            "dataset/stateful/ontime_2009_200.csv",
        ] {
            iil.insert(i.to_string())
        }

        let op = Operator::new(Http::from_iter(
            vec![("endpoint".to_string(), "https://xuanwo.io".to_string())].into_iter(),
        ))?
        .layer(LoggingLayer::default())
        .layer(iil)
        .finish();

        //  List /
        let mut map = HashMap::new();
        let mut set = HashSet::new();
        let mut ds = op.list("/").await?;
        while let Some(entry) = ds.try_next().await? {
            assert!(
                set.insert(entry.path().to_string()),
                "duplicated value: {}",
                entry.path()
            );
            map.insert(
                entry.path().to_string(),
                op.metadata(&entry, Metakey::Mode).await?.mode(),
            );
        }

        assert_eq!(map.len(), 1);
        assert_eq!(map["dataset/"], EntryMode::DIR);

        //  List dataset/stateful/
        let mut map = HashMap::new();
        let mut set = HashSet::new();
        let mut ds = op.list("dataset/stateful/").await?;
        while let Some(entry) = ds.try_next().await? {
            assert!(
                set.insert(entry.path().to_string()),
                "duplicated value: {}",
                entry.path()
            );
            map.insert(
                entry.path().to_string(),
                op.metadata(&entry, Metakey::Mode).await?.mode(),
            );
        }

        assert_eq!(map["dataset/stateful/ontime_2007_200.csv"], EntryMode::FILE);
        assert_eq!(map["dataset/stateful/ontime_2008_200.csv"], EntryMode::FILE);
        assert_eq!(map["dataset/stateful/ontime_2009_200.csv"], EntryMode::FILE);
        Ok(())
    }

    #[tokio::test]
    async fn test_walk_top_down_dir() -> Result<()> {
        let _ = env_logger::try_init();

        let mut iil = ImmutableIndexLayer::default();
        for i in [
            "dataset/stateful/ontime_2007_200.csv",
            "dataset/stateful/ontime_2008_200.csv",
            "dataset/stateful/ontime_2009_200.csv",
        ] {
            iil.insert(i.to_string())
        }

        let op = Operator::new(Http::from_iter(
            vec![("endpoint".to_string(), "https://xuanwo.io".to_string())].into_iter(),
        ))?
        .layer(LoggingLayer::default())
        .layer(iil)
        .finish();

        let mut ds = op.scan("/").await?;

        let mut map = HashMap::new();
        let mut set = HashSet::new();
        while let Some(entry) = ds.try_next().await? {
            assert!(
                set.insert(entry.path().to_string()),
                "duplicated value: {}",
                entry.path()
            );
            map.insert(
                entry.path().to_string(),
                op.metadata(&entry, Metakey::Mode).await?.mode(),
            );
        }

        debug!("current files: {:?}", map);

        assert_eq!(map["dataset/stateful/ontime_2007_200.csv"], EntryMode::FILE);
        assert_eq!(map["dataset/stateful/ontime_2008_200.csv"], EntryMode::FILE);
        assert_eq!(map["dataset/stateful/ontime_2009_200.csv"], EntryMode::FILE);
        Ok(())
    }
}
