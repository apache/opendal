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

//! Immutable index layer implementation for Apache OpenDAL.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::collections::HashSet;
use std::sync::Arc;
use std::vec::IntoIter;

use opendal_core::raw::*;
use opendal_core::*;

/// Add an immutable in-memory index for underlying storage services.
///
/// Especially useful for services without list capability like HTTP.
///
/// # Examples
///
/// ```no_run
/// # use std::collections::HashMap;
/// #
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_immutable_index::ImmutableIndexLayer;
/// #
/// # fn main() -> Result<()> {
/// let mut iil = ImmutableIndexLayer::new();
///
/// for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
///     iil.insert(i.to_string())
/// }
///
/// let op = Operator::from_iter::<services::Memory>(HashMap::<_, _>::default())?
///     .layer(iil);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct ImmutableIndexLayer {
    vec: Vec<String>,
}

impl ImmutableIndexLayer {
    /// Create a new [`ImmutableIndexLayer`].
    pub fn new() -> Self {
        Self::default()
    }
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

impl Layer for ImmutableIndexLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(self.layer(inner))
    }
}

impl ImmutableIndexLayer {
    fn layer(&self, inner: Servicer) -> ImmutableIndexService {
        ImmutableIndexService {
            inner,
            vec: self.vec.clone(),
        }
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct ImmutableIndexService {
    inner: Servicer,
    vec: Vec<String>,
}

impl ImmutableIndexService {
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

impl Service for ImmutableIndexService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = ImmutableDir;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        let mut capability = self.inner.capability();
        capability.list = true;
        capability.list_with_recursive = true;
        capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    async fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(ctx, path, args).await
    }

    async fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(ctx, path, args).await
    }

    async fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        self.inner.copy(ctx, from, to, args, opts).await
    }

    async fn list(
        &self,
        _: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::Lister)> {
        let mut path = path;
        if path == "/" {
            path = ""
        }

        let idx = if args.recursive() {
            self.children_flat(path)
        } else {
            self.children_hierarchy(path)
        };

        Ok((RpList::default(), ImmutableDir::new(idx)))
    }

    async fn delete(&self, ctx: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete(ctx).await
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

#[doc(hidden)]
pub struct ImmutableDir {
    idx: IntoIter<String>,
}

impl ImmutableDir {
    fn new(idx: Vec<String>) -> Self {
        Self {
            idx: idx.into_iter(),
        }
    }

    fn inner_next(&mut self) -> Option<oio::Entry> {
        self.idx.next().map(|v| {
            let mode = if v.ends_with('/') {
                EntryMode::DIR
            } else {
                EntryMode::FILE
            };
            let meta = Metadata::new(mode);
            oio::Entry::with(v, meta)
        })
    }
}

impl oio::List for ImmutableDir {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        Ok(self.inner_next())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use futures::TryStreamExt;
    use log::debug;
    use logforth::append::Testing;
    use logforth::filter::env_filter::EnvFilterBuilder;
    use logforth::layout::TextLayout;

    #[derive(Debug)]
    struct MockService;

    impl Service for MockService {
        type Reader = ();
        type Writer = ();
        type Lister = ();
        type Deleter = ();
        type Copier = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("mock")
        }

        fn capability(&self) -> Capability {
            Capability {
                list: true,
                list_with_recursive: true,
                ..Default::default()
            }
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

        async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn read(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpRead,
        ) -> Result<(RpRead, Self::Reader)> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn write(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpWrite,
        ) -> Result<(RpWrite, Self::Writer)> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn delete(&self, _: &OperationContext) -> Result<(RpDelete, Self::Deleter)> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn list(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpList,
        ) -> Result<(RpList, Self::Lister)> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn copy(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpCopy,
            _: OpCopier,
        ) -> Result<(RpCopy, Self::Copier)> {
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

    fn build_operator(layer: ImmutableIndexLayer) -> Operator {
        Operator::from_inner(Arc::new(MockService)).layer(layer)
    }

    fn setup() {
        let _ = logforth::starter_log::builder()
            .dispatch(|d| {
                d.filter(EnvFilterBuilder::from_default_env().build())
                    .append(Testing::default().with_layout(TextLayout::default()))
            })
            .try_apply();
    }

    #[tokio::test]
    async fn test_list() -> Result<()> {
        setup();

        let mut iil = ImmutableIndexLayer::default();
        for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
            iil.insert(i.to_string())
        }

        let op = build_operator(iil);

        let mut map = HashMap::new();
        let mut set = HashSet::new();
        let mut ds = op.lister("").await?;
        while let Some(entry) = ds.try_next().await? {
            debug!("got entry: {}", entry.path());
            assert!(
                set.insert(entry.path().to_string()),
                "duplicated value: {}",
                entry.path()
            );
            map.insert(entry.path().to_string(), entry.metadata().mode());
        }

        assert_eq!(map["file"], EntryMode::FILE);
        assert_eq!(map["dir/"], EntryMode::DIR);
        assert_eq!(map["dir_without_prefix/"], EntryMode::DIR);
        Ok(())
    }

    #[tokio::test]
    async fn test_scan() -> Result<()> {
        setup();

        let mut iil = ImmutableIndexLayer::default();
        for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
            iil.insert(i.to_string())
        }

        let op = build_operator(iil);

        let mut ds = op.lister_with("/").recursive(true).await?;
        let mut set = HashSet::new();
        let mut map = HashMap::new();
        while let Some(entry) = ds.try_next().await? {
            debug!("got entry: {}", entry.path());
            assert!(
                set.insert(entry.path().to_string()),
                "duplicated value: {}",
                entry.path()
            );
            map.insert(entry.path().to_string(), entry.metadata().mode());
        }

        debug!("current files: {map:?}");

        assert_eq!(map["file"], EntryMode::FILE);
        assert_eq!(map["dir/"], EntryMode::DIR);
        assert_eq!(map["dir_without_prefix/file"], EntryMode::FILE);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_dir() -> Result<()> {
        setup();

        let mut iil = ImmutableIndexLayer::default();
        for i in [
            "dataset/stateful/ontime_2007_200.csv",
            "dataset/stateful/ontime_2008_200.csv",
            "dataset/stateful/ontime_2009_200.csv",
        ] {
            iil.insert(i.to_string())
        }

        let op = build_operator(iil);

        //  List /
        let mut map = HashMap::new();
        let mut set = HashSet::new();
        let mut ds = op.lister("/").await?;
        while let Some(entry) = ds.try_next().await? {
            assert!(
                set.insert(entry.path().to_string()),
                "duplicated value: {}",
                entry.path()
            );
            map.insert(entry.path().to_string(), entry.metadata().mode());
        }

        assert_eq!(map.len(), 1);
        assert_eq!(map["dataset/"], EntryMode::DIR);

        //  List dataset/stateful/
        let mut map = HashMap::new();
        let mut set = HashSet::new();
        let mut ds = op.lister("dataset/stateful/").await?;
        while let Some(entry) = ds.try_next().await? {
            assert!(
                set.insert(entry.path().to_string()),
                "duplicated value: {}",
                entry.path()
            );
            map.insert(entry.path().to_string(), entry.metadata().mode());
        }

        assert_eq!(map["dataset/stateful/ontime_2007_200.csv"], EntryMode::FILE);
        assert_eq!(map["dataset/stateful/ontime_2008_200.csv"], EntryMode::FILE);
        assert_eq!(map["dataset/stateful/ontime_2009_200.csv"], EntryMode::FILE);
        Ok(())
    }

    #[tokio::test]
    async fn test_walk_top_down_dir() -> Result<()> {
        setup();

        let mut iil = ImmutableIndexLayer::default();
        for i in [
            "dataset/stateful/ontime_2007_200.csv",
            "dataset/stateful/ontime_2008_200.csv",
            "dataset/stateful/ontime_2009_200.csv",
        ] {
            iil.insert(i.to_string())
        }

        let op = build_operator(iil);

        let mut ds = op.lister_with("/").recursive(true).await?;

        let mut map = HashMap::new();
        let mut set = HashSet::new();
        while let Some(entry) = ds.try_next().await? {
            assert!(
                set.insert(entry.path().to_string()),
                "duplicated value: {}",
                entry.path()
            );
            map.insert(entry.path().to_string(), entry.metadata().mode());
        }

        debug!("current files: {map:?}");

        assert_eq!(map["dataset/stateful/ontime_2007_200.csv"], EntryMode::FILE);
        assert_eq!(map["dataset/stateful/ontime_2008_200.csv"], EntryMode::FILE);
        assert_eq!(map["dataset/stateful/ontime_2009_200.csv"], EntryMode::FILE);
        Ok(())
    }
}
