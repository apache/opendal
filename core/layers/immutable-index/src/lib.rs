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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]
use std::collections::HashSet;
use std::sync::Arc;
use std::vec::IntoIter;

use opendal_core::raw::*;
use opendal_core::*;

/// `ImmutableIndexLayer` adds an immutable in-memory index to a storage service.
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
        Arc::new(ImmutableIndexService {
            inner,
            vec: self.vec.clone(),
        })
    }
}

#[derive(Debug)]
struct ImmutableIndexService {
    inner: Servicer,
    vec: Vec<String>,
}

impl ImmutableIndexService {
    fn children_flat(&self, path: &str) -> Vec<String> {
        self.vec
            .iter()
            .filter(|v| !v.is_empty() && v.starts_with(path))
            .cloned()
            .collect()
    }

    fn children_hierarchy(&self, path: &str) -> Vec<String> {
        let mut res = HashSet::new();

        for i in self.vec.iter() {
            // The root is represented by an empty path, so an empty key must
            // not be emitted as an entry.
            if i.is_empty() {
                continue;
            }

            // `/xyz` should not belong to `/abc`
            if !i.starts_with(path) {
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
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        let mut capability = self.inner.capability();

        // List operations are answered by the index, so all `list_with_*`
        // options must reflect what the index actually implements instead of
        // inheriting the inner service's capabilities.
        capability.list = true;
        capability.list_with_recursive = true;
        capability.list_with_start_after = true;
        // `limit` is a per-request size hint; the index materializes the whole
        // listing, so it is ignored rather than advertised.
        capability.list_with_limit = false;
        // The index only stores paths, so it cannot return versions or
        // deleted entries.
        capability.list_with_versions = false;
        capability.list_with_deleted = false;
        // `GlobLayer` sits below this layer and advertises `list_with_glob` on
        // its wrapped stack, but this layer answers list calls directly. The
        // index cannot honor glob patterns.
        capability.list_with_glob = false;

        capability
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
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

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner.write(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }

    fn list(&self, _ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let mut path = path;
        if path == "/" {
            path = ""
        }

        let mut idx = if args.recursive() {
            self.children_flat(path)
        } else {
            self.children_hierarchy(path)
        };

        // Entries must be emitted in lexicographical order without duplicates
        // so `start_after` marks a resumable position and recursive listings
        // match non-recursive deduplication.
        idx.sort_unstable();
        idx.dedup();

        if let Some(start_after) = args.start_after() {
            idx.retain(|v| v.as_str() > start_after);
        }

        Ok(ImmutableDir::new(idx))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
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

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.inner.restore(ctx, path, args).await
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

struct ImmutableDir {
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
            let metadata = if v.ends_with('/') {
                MetadataBuilder::dir()
            } else {
                MetadataBuilder::unknown()
            };
            oio::Entry::with(v, metadata.build())
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
    use logforth::filter::rustlog::RustLogFilterBuilder;
    use logforth::layout::TextLayout;

    #[derive(Debug)]
    struct MockService;

    impl Service for MockService {
        type Reader = ();
        type Writer = ();
        type Lister = ();
        type Deleter = ();
        type Copier = ();
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("mock")
        }

        fn capability(&self) -> Capability {
            Capability {
                list: true,
                list_with_limit: true,
                list_with_start_after: true,
                list_with_recursive: true,
                list_with_versions: true,
                list_with_deleted: true,
                list_with_glob: true,
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

        fn read(&self, _ctx: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn write(&self, _ctx: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn list(&self, _ctx: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
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

    fn build_operator(layer: ImmutableIndexLayer) -> Operator {
        Operator::from_parts(OperationContext::default(), Arc::new(MockService)).layer(layer)
    }

    fn setup() {
        let _ = logforth::starter_log::builder()
            .dispatch(|d| {
                d.filter(RustLogFilterBuilder::from_default_env().build())
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

        assert_eq!(map["file"], EntryMode::Unknown);
        assert_eq!(map["dir/"], EntryMode::DIR);
        assert_eq!(map["dir_without_prefix/"], EntryMode::DIR);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_exact_file() -> Result<()> {
        setup();

        let mut iil = ImmutableIndexLayer::default();
        for i in ["file", "file-1"] {
            iil.insert(i.to_string())
        }

        let op = build_operator(iil);

        let entries = op.list("file").await?;
        let paths = entries.iter().map(|e| e.path()).collect::<Vec<_>>();
        assert_eq!(paths, vec!["file", "file-1"]);

        let entries = op.list_with("file").recursive(true).await?;
        let paths = entries.iter().map(|e| e.path()).collect::<Vec<_>>();
        assert_eq!(paths, vec!["file", "file-1"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_exact_dir() -> Result<()> {
        setup();

        let mut iil = ImmutableIndexLayer::default();
        for i in ["dir/", "dir/file", "dir/sub/", "dir/sub/file"] {
            iil.insert(i.to_string())
        }

        let op = build_operator(iil);

        let entries = op.list("dir/").await?;
        let paths = entries.iter().map(|e| e.path()).collect::<Vec<_>>();
        assert_eq!(paths, vec!["dir/", "dir/file", "dir/sub/"]);

        let entries = op.list_with("dir/").recursive(true).await?;
        let paths = entries.iter().map(|e| e.path()).collect::<Vec<_>>();
        assert_eq!(paths, vec!["dir/", "dir/file", "dir/sub/", "dir/sub/file"]);

        let entries = op.list("dir").await?;
        let paths = entries.iter().map(|e| e.path()).collect::<Vec<_>>();
        assert_eq!(paths, vec!["dir/"]);
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

        assert_eq!(map["file"], EntryMode::Unknown);
        assert_eq!(map["dir/"], EntryMode::DIR);
        assert_eq!(map["dir_without_prefix/file"], EntryMode::Unknown);
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

        assert_eq!(
            map["dataset/stateful/ontime_2007_200.csv"],
            EntryMode::Unknown
        );
        assert_eq!(
            map["dataset/stateful/ontime_2008_200.csv"],
            EntryMode::Unknown
        );
        assert_eq!(
            map["dataset/stateful/ontime_2009_200.csv"],
            EntryMode::Unknown
        );
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

        assert_eq!(
            map["dataset/stateful/ontime_2007_200.csv"],
            EntryMode::Unknown
        );
        assert_eq!(
            map["dataset/stateful/ontime_2008_200.csv"],
            EntryMode::Unknown
        );
        assert_eq!(
            map["dataset/stateful/ontime_2009_200.csv"],
            EntryMode::Unknown
        );
        Ok(())
    }

    /// The index owns all list options: `start_after` and `recursive` are
    /// implemented; `limit`, `versions`, `deleted`, and `glob` must not be
    /// inherited from the inner service, or the layer would advertise options
    /// it cannot honor.
    #[tokio::test]
    async fn test_capability_does_not_inherit_inner_list_options() -> Result<()> {
        let mut iil = ImmutableIndexLayer::default();
        iil.insert("file".to_string());

        let op = build_operator(iil);
        let capability = op.info().capability();

        assert!(capability.list);
        assert!(capability.list_with_recursive);
        assert!(capability.list_with_start_after);
        assert!(!capability.list_with_limit);
        assert!(!capability.list_with_versions);
        assert!(!capability.list_with_deleted);
        assert!(!capability.list_with_glob);

        // `versions`, `deleted`, and `glob` must fail fast instead of
        // returning the current entries as if they were versioned, deleted,
        // or filtered data.
        let err = op
            .list_with("/")
            .versions(true)
            .await
            .expect_err("versions must not be silently ignored");
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        let err = op
            .list_with("/")
            .deleted(true)
            .await
            .expect_err("deleted must not be silently ignored");
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        let err = op
            .list_with("/")
            .glob("*.txt")
            .await
            .expect_err("glob must not be silently ignored");
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        Ok(())
    }

    /// `start_after` resumes after the given key in lexicographical order, on
    /// the entries emitted by the list operation.
    #[tokio::test]
    async fn test_list_start_after() -> Result<()> {
        // (name, path, recursive, start_after, expected)
        let cases: &[(&str, &str, bool, &str, &[&str])] = &[
            ("before prefix", "a/d/", true, "0", &["a/d/x", "a/d/y"]),
            ("at key", "a/", true, "a/2", &["a/d/x", "a/d/y", "a/z"]),
            ("between keys", "a/d/", true, "a/d/w", &["a/d/x", "a/d/y"]),
            ("key in folder", "a/", true, "a/d/x", &["a/d/y", "a/z"]),
            ("after prefix", "a/", true, "a0", &[]),
            ("folds folders", "a/", false, "a/1", &["a/2", "a/d/", "a/z"]),
            ("before folder", "a/", false, "a/c", &["a/d/", "a/z"]),
            ("at folder skips it", "a/", false, "a/d/", &["a/z"]),
            ("in folder skips it", "a/", false, "a/d/x", &["a/z"]),
            ("prefix without slash", "a/d", false, "a/d/x", &[]),
            ("list root", "/", false, "a/2", &["a0", "b/"]),
            ("list root recursively", "/", true, "a0", &["b/1"]),
        ];

        let mut iil = ImmutableIndexLayer::default();
        for i in ["a/1", "a/2", "a/d/x", "a/d/y", "a/z", "a0", "b/1"] {
            iil.insert(i.to_string())
        }

        let op = build_operator(iil);

        for &(name, path, recursive, start_after, expected) in cases {
            let actual: Vec<String> = op
                .lister_with(path)
                .recursive(recursive)
                .start_after(start_after)
                .await?
                .map_ok(|entry| entry.path().to_string())
                .try_collect()
                .await?;
            assert_eq!(actual, expected, "{name}");
        }
        Ok(())
    }

    /// Listing emits entries in lexicographical order so `start_after` marks
    /// a resumable position.
    #[tokio::test]
    async fn test_list_emits_sorted_order() -> Result<()> {
        let mut iil = ImmutableIndexLayer::default();
        for i in ["b/2", "a/c", "b/1", "a/b", "a/d/1", "f.txt"] {
            iil.insert(i.to_string())
        }

        let op = build_operator(iil);

        let recursive: Vec<String> = op
            .lister_with("/")
            .recursive(true)
            .await?
            .map_ok(|entry| entry.path().to_string())
            .try_collect()
            .await?;
        assert_eq!(recursive, ["a/b", "a/c", "a/d/1", "b/1", "b/2", "f.txt"]);

        let hierarchy: Vec<String> = op
            .lister("/")
            .await?
            .map_ok(|entry| entry.path().to_string())
            .try_collect()
            .await?;
        assert_eq!(hierarchy, ["a/", "b/", "f.txt"]);
        Ok(())
    }

    /// Wrapping a backend that advertises `list_with_start_after` must honor
    /// the option instead of silently returning earlier entries.
    #[tokio::test]
    async fn test_start_after_with_memory_backend() -> Result<()> {
        let mut iil = ImmutableIndexLayer::default();
        for i in ["a", "b"] {
            iil.insert(i.to_string())
        }

        let op = Operator::from_iter::<services::Memory>(HashMap::<_, _>::default())?.layer(iil);

        let actual: Vec<String> = op
            .lister_with("/")
            .start_after("a")
            .await?
            .map_ok(|entry| entry.path().to_string())
            .try_collect()
            .await?;
        assert_eq!(actual, ["b"]);
        Ok(())
    }

    /// `start_after` on an empty index or a prefix that has no entries returns
    /// nothing instead of leaking entries outside the prefix.
    #[tokio::test]
    async fn test_list_start_after_empty() -> Result<()> {
        let op = build_operator(ImmutableIndexLayer::default());

        for recursive in [false, true] {
            let actual: Vec<String> = op
                .lister_with("/")
                .recursive(recursive)
                .start_after("a")
                .await?
                .map_ok(|entry| entry.path().to_string())
                .try_collect()
                .await?;
            assert!(actual.is_empty(), "recursive={recursive}");
        }

        let mut iil = ImmutableIndexLayer::default();
        for i in ["a/1", "a/2"] {
            iil.insert(i.to_string())
        }

        let op = build_operator(iil);

        // `start_after` beyond every key under the prefix.
        for recursive in [false, true] {
            let actual: Vec<String> = op
                .lister_with("a/")
                .recursive(recursive)
                .start_after("a/9")
                .await?
                .map_ok(|entry| entry.path().to_string())
                .try_collect()
                .await?;
            assert!(actual.is_empty(), "recursive={recursive}");
        }

        // A prefix with no entries must not leak entries from other prefixes,
        // even when `start_after` would admit them.
        let actual: Vec<String> = op
            .lister_with("missing/")
            .start_after("0")
            .await?
            .map_ok(|entry| entry.path().to_string())
            .try_collect()
            .await?;
        assert!(actual.is_empty());
        Ok(())
    }

    /// Direct `Service` callers validate against `ImmutableIndexService::capability()`
    /// via `OpList::check`, matching the `raw::Service` capability contract.
    #[test]
    fn test_list_capability_rejects_unsupported_options_at_service_level() {
        let mut iil = ImmutableIndexLayer::default();
        iil.insert("file".to_string());

        let service = iil.apply_service(Arc::new(MockService));
        let cap = service.capability();

        for args in [
            OpList::from(options::ListOptions {
                versions: true,
                ..Default::default()
            }),
            OpList::from(options::ListOptions {
                deleted: true,
                ..Default::default()
            }),
            OpList::new().with_glob("*.txt"),
        ] {
            let err = args
                .check(&cap)
                .expect_err("unsupported options must be rejected by service capability");
            assert_eq!(err.kind(), ErrorKind::Unsupported);
        }
    }

    /// Duplicate keys inserted into `ImmutableIndexLayer` must be emitted
    /// once in both non-recursive and recursive listings, including when
    /// validated by `SanityCheckLayer`.
    #[tokio::test]
    async fn test_list_deduplicates_duplicate_keys() -> Result<()> {
        let mut iil = ImmutableIndexLayer::default();
        iil.insert("dir/a".to_string());
        iil.insert("dir/a".to_string());
        iil.insert("dir/".to_string());
        iil.extend_iter([
            "dir/".to_string(),
            "file".to_string(),
            "file".to_string(),
            "dir/b".to_string(),
            "dir/b".to_string(),
        ]);

        let op = build_operator(iil).layer(opendal_layer_sanity_check::SanityCheckLayer);

        let hierarchy: Vec<(String, EntryMode)> = op
            .lister("/")
            .await?
            .map_ok(|entry| (entry.path().to_string(), entry.metadata().mode()))
            .try_collect()
            .await?;
        assert_eq!(
            hierarchy,
            [
                ("dir/".to_string(), EntryMode::DIR),
                ("file".to_string(), EntryMode::Unknown),
            ]
        );

        let recursive: Vec<(String, EntryMode)> = op
            .lister_with("/")
            .recursive(true)
            .await?
            .map_ok(|entry| (entry.path().to_string(), entry.metadata().mode()))
            .try_collect()
            .await?;
        assert_eq!(
            recursive,
            [
                ("dir/".to_string(), EntryMode::DIR),
                ("dir/a".to_string(), EntryMode::Unknown),
                ("dir/b".to_string(), EntryMode::Unknown),
                ("file".to_string(), EntryMode::Unknown),
            ]
        );

        let sub_recursive: Vec<String> = op
            .lister_with("dir/")
            .recursive(true)
            .await?
            .map_ok(|entry| entry.path().to_string())
            .try_collect()
            .await?;
        assert_eq!(sub_recursive, ["dir/", "dir/a", "dir/b"]);

        Ok(())
    }

    /// `start_after("/")` must filter lexicographically like every other
    /// cursor value, excluding `"/"` and keys sorting before `'/'` (ASCII 47),
    /// matching `services::Memory`'s contract.
    #[tokio::test]
    async fn test_list_start_after_root_and_pre_slash_keys() -> Result<()> {
        let keys = [
            "-dir/x",
            "-dir/y",
            "-first.txt",
            ".env",
            "/",
            "0.txt",
            "a.txt",
            "a/1",
        ];

        let mut iil = ImmutableIndexLayer::default();
        for key in keys {
            iil.insert(key.to_string());
        }

        let op = Operator::from_iter::<services::Memory>(HashMap::<_, _>::default())?.layer(iil);

        // (name, recursive, start_after, expected)
        let cases: &[(&str, bool, &str, &[&str])] = &[
            (
                "empty start_after non-recursive",
                false,
                "",
                &["-dir/", "-first.txt", ".env", "/", "0.txt", "a.txt", "a/"],
            ),
            (
                "empty start_after recursive",
                true,
                "",
                &[
                    "-dir/x",
                    "-dir/y",
                    "-first.txt",
                    ".env",
                    "/",
                    "0.txt",
                    "a.txt",
                    "a/1",
                ],
            ),
            (
                "slash start_after non-recursive excludes pre-slash keys and root",
                false,
                "/",
                &["0.txt", "a.txt", "a/"],
            ),
            (
                "slash start_after recursive excludes pre-slash keys and root",
                true,
                "/",
                &["0.txt", "a.txt", "a/1"],
            ),
            (
                "pre-slash start_after non-recursive",
                false,
                "-dir/",
                &["-first.txt", ".env", "/", "0.txt", "a.txt", "a/"],
            ),
            (
                "pre-slash start_after recursive",
                true,
                "-dir/x",
                &["-dir/y", "-first.txt", ".env", "/", "0.txt", "a.txt", "a/1"],
            ),
        ];

        for &(name, recursive, start_after, expected) in cases {
            let actual: Vec<String> = op
                .lister_with("/")
                .recursive(recursive)
                .start_after(start_after)
                .await?
                .map_ok(|entry| entry.path().to_string())
                .try_collect()
                .await?;
            assert_eq!(actual, expected, "{name}");
        }

        Ok(())
    }
}
