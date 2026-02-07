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

//! Route layer implementation for Apache OpenDAL.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use globset::Glob;
use globset::GlobSet;
use globset::GlobSetBuilder;
use opendal_core::raw::*;
use opendal_core::*;

/// Route operations to different operators by matching paths with glob patterns.
#[derive(Clone, Debug)]
pub struct RouteLayer {
    router: Arc<RouteRouter>,
}

impl RouteLayer {
    /// Create a builder for `RouteLayer`.
    pub fn builder() -> RouteLayerBuilder {
        RouteLayerBuilder::default()
    }
}

/// Builder for `RouteLayer`.
#[derive(Default)]
pub struct RouteLayerBuilder {
    routes: Vec<RouteEntry>,
}

impl RouteLayerBuilder {
    /// Add a route with a glob pattern.
    pub fn route(mut self, pattern: impl AsRef<str>, op: Operator) -> Self {
        self.routes.push(RouteEntry {
            pattern: pattern.as_ref().to_string(),
            accessor: op.into_inner(),
        });
        self
    }

    /// Build the `RouteLayer`.
    ///
    /// Returns an error if any glob pattern fails to compile.
    pub fn build(self) -> Result<RouteLayer> {
        let mut builder = GlobSetBuilder::new();
        let mut targets = Vec::with_capacity(self.routes.len());

        for entry in self.routes {
            let glob = Glob::new(&entry.pattern).map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "invalid route glob pattern")
                    .with_context("pattern", entry.pattern.clone())
                    .with_context("source", err.to_string())
            })?;
            builder.add(glob);
            targets.push(entry.accessor);
        }

        let glob = builder.build().map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "failed to build route glob set")
                .with_context("source", err.to_string())
        })?;

        Ok(RouteLayer {
            router: Arc::new(RouteRouter { glob, targets }),
        })
    }
}

struct RouteEntry {
    pattern: String,
    accessor: Accessor,
}

#[derive(Debug)]
struct RouteRouter {
    glob: GlobSet,
    targets: Vec<Accessor>,
}

impl RouteRouter {
    fn match_index(&self, path: &str) -> Option<usize> {
        self.glob.matches(path).into_iter().min()
    }

    fn select(&self, path: &str, default: &Accessor) -> Accessor {
        self.match_index(path)
            .and_then(|idx| self.targets.get(idx).cloned())
            .unwrap_or_else(|| default.clone())
    }

    fn target(&self, idx: usize, default: &Accessor) -> Accessor {
        self.targets
            .get(idx)
            .cloned()
            .unwrap_or_else(|| default.clone())
    }
}

impl Layer<Accessor> for RouteLayer {
    type LayeredAccess = RouteAccessor;

    fn layer(&self, inner: Accessor) -> Self::LayeredAccess {
        RouteAccessor {
            inner,
            router: self.router.clone(),
        }
    }
}

/// Accessor that routes operations to different targets based on path.
pub struct RouteAccessor {
    inner: Accessor,
    router: Arc<RouteRouter>,
}

impl Debug for RouteAccessor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl RouteAccessor {
    fn select(&self, path: &str) -> Accessor {
        self.router.select(path, &self.inner)
    }
}

impl LayeredAccess for RouteAccessor {
    type Inner = Accessor;
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = RouteDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.select(path).create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.select(path).read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.select(path).write(path, args).await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.select(from).copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.select(from).rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.select(path).stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            RouteDeleter::new(self.inner.clone(), self.router.clone()),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.select(path).list(path, args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.select(path).presign(path, args).await
    }
}

/// Deleter that batches deletions per routed accessor.
pub struct RouteDeleter {
    default: Accessor,
    router: Arc<RouteRouter>,
    default_deleter: Option<oio::Deleter>,
    target_deleters: Vec<Option<oio::Deleter>>,
}

impl RouteDeleter {
    fn new(default: Accessor, router: Arc<RouteRouter>) -> Self {
        let mut target_deleters = Vec::with_capacity(router.targets.len());
        target_deleters.resize_with(router.targets.len(), || None);
        Self {
            default,
            router,
            default_deleter: None,
            target_deleters,
        }
    }

    async fn get_deleter(&mut self, key: RouteKey) -> Result<&mut oio::Deleter> {
        match key {
            RouteKey::Default => {
                if self.default_deleter.is_none() {
                    let (_, deleter) = self.default.delete().await?;
                    self.default_deleter = Some(deleter);
                }
                Ok(self
                    .default_deleter
                    .as_mut()
                    .expect("default deleter must exist"))
            }
            RouteKey::Target(idx) => {
                if idx >= self.target_deleters.len() {
                    if self.default_deleter.is_none() {
                        let (_, deleter) = self.default.delete().await?;
                        self.default_deleter = Some(deleter);
                    }
                    return Ok(self
                        .default_deleter
                        .as_mut()
                        .expect("default deleter must exist"));
                }
                if self.target_deleters[idx].is_none() {
                    let accessor = self.router.target(idx, &self.default);
                    let (_, deleter) = accessor.delete().await?;
                    self.target_deleters[idx] = Some(deleter);
                }
                Ok(self.target_deleters[idx]
                    .as_mut()
                    .expect("target deleter must exist"))
            }
        }
    }
}

impl oio::Delete for RouteDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let key = match self.router.match_index(path) {
            Some(idx) => RouteKey::Target(idx),
            None => RouteKey::Default,
        };
        let deleter = self.get_deleter(key).await?;
        deleter.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        let mut first_err = None;
        if let Some(deleter) = self.default_deleter.as_mut() {
            if let Err(err) = deleter.close().await {
                first_err = Some(err);
            }
        }
        for deleter in self.target_deleters.iter_mut().flatten() {
            if let Err(err) = deleter.close().await {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }

        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
enum RouteKey {
    Default,
    Target(usize),
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use opendal_service_fs::Fs;

    use super::*;

    fn build_memory_operator() -> Result<Operator> {
        Ok(Operator::new(services::Memory::default())?.finish())
    }

    async fn build_fs_operator(label: &str) -> Result<(Operator, PathBuf)> {
        let root = fs_root(label);
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(new_std_io_error)?;
        let op = Operator::new(Fs::default().root(&root.to_string_lossy()))?.finish();
        Ok((op, root))
    }

    fn fs_root(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX_EPOCH")
            .as_nanos();
        let mut root = std::env::temp_dir();
        root.push(format!(
            "opendal-route-{label}-{nanos}-{}",
            std::process::id()
        ));
        root
    }

    async fn cleanup_fs_root(root: &Path) {
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    async fn assert_missing(op: &Operator, path: &str) -> Result<()> {
        let err = op.stat(path).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        Ok(())
    }

    #[tokio::test]
    async fn test_first_match_wins() -> Result<()> {
        let default_op = build_memory_operator()?;
        let (fast_op, fast_root) = build_fs_operator("first-match-fast").await?;
        let slow_op = build_memory_operator()?;

        let routed = default_op.clone().layer(
            RouteLayer::builder()
                .route("data/*.txt", fast_op.clone())
                .route("data/**", slow_op.clone())
                .build()?,
        );

        routed.write("data/file.txt", "v").await?;

        assert!(fast_op.stat("data/file.txt").await.is_ok());
        assert_missing(&slow_op, "data/file.txt").await?;
        assert_missing(&default_op, "data/file.txt").await?;

        cleanup_fs_root(&fast_root).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_fallback_to_default() -> Result<()> {
        let default_op = build_memory_operator()?;
        let (hot_op, hot_root) = build_fs_operator("fallback-hot").await?;

        let routed = default_op.clone().layer(
            RouteLayer::builder()
                .route("hot/**", hot_op.clone())
                .build()?,
        );

        routed.write("cold/file.txt", "v").await?;

        assert!(default_op.stat("cold/file.txt").await.is_ok());
        assert_missing(&hot_op, "cold/file.txt").await?;

        cleanup_fs_root(&hot_root).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_and_rename_route_by_from() -> Result<()> {
        let default_op = build_memory_operator()?;
        let (hot_op, hot_root) = build_fs_operator("copy-rename-hot").await?;

        let routed = default_op.clone().layer(
            RouteLayer::builder()
                .route("hot/**", hot_op.clone())
                .build()?,
        );

        routed.write("hot/src.txt", "v").await?;
        routed.copy("hot/src.txt", "cold/copied.txt").await?;

        assert!(hot_op.stat("hot/src.txt").await.is_ok());
        assert!(hot_op.stat("cold/copied.txt").await.is_ok());
        assert_missing(&default_op, "cold/copied.txt").await?;

        routed.write("hot/src2.txt", "v").await?;
        routed.rename("hot/src2.txt", "cold/renamed.txt").await?;

        assert_missing(&hot_op, "hot/src2.txt").await?;
        assert!(hot_op.stat("cold/renamed.txt").await.is_ok());
        assert_missing(&default_op, "cold/renamed.txt").await?;

        cleanup_fs_root(&hot_root).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_iter_routes_per_path() -> Result<()> {
        let default_op = build_memory_operator()?;
        let (hot_op, hot_root) = build_fs_operator("delete-hot").await?;

        let routed = default_op.clone().layer(
            RouteLayer::builder()
                .route("hot/**", hot_op.clone())
                .build()?,
        );

        routed.write("hot/a.txt", "v").await?;
        routed.write("cold/b.txt", "v").await?;

        routed.delete_iter(["hot/a.txt", "cold/b.txt"]).await?;

        assert_missing(&hot_op, "hot/a.txt").await?;
        assert_missing(&default_op, "cold/b.txt").await?;

        cleanup_fs_root(&hot_root).await;
        Ok(())
    }
}
