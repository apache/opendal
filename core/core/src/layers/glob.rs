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

use crate::raw::oio::{self, List};
use crate::raw::*;
use crate::*;

/// Provide client-side glob support for services that support listing.
///
/// OpenDAL installs this layer by default. Services that advertise native glob
/// support receive list requests unchanged. Otherwise the layer uses the pattern
/// to skip unrelated directories. With a cursor, versions, or delete markers,
/// it filters an underlying listing to preserve the service's cursor and entries.
/// For a custom stack, install it before layers that resolve symbolic links
/// when completing entry metadata.
///
/// See [`ListOptions::glob`](options::ListOptions::glob) for the pattern syntax.
#[derive(Debug, Clone, Copy, Default)]
pub struct GlobLayer;

impl Layer for GlobLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        let capability = inner.capability();
        if !capability.list || capability.list_with_glob {
            return inner;
        }
        Arc::new(GlobService { inner })
    }
}

#[derive(Debug)]
struct GlobService {
    inner: Servicer,
}

impl Service for GlobService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        let mut capability = self.inner.capability();
        capability.list_with_glob = true;
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

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner.write(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
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

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
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

    fn list(&self, ctx: &OperationContext, path: &str, mut args: OpList) -> Result<Self::Lister> {
        let Some(glob) = args.glob() else {
            return self.inner.list(ctx, path, args);
        };
        let pattern = GlobPattern::new(glob)?;
        args.clear_glob();
        let base = if path.ends_with('/') {
            path
        } else {
            get_parent(path)
        };

        if args.start_after().is_some() || args.versions() || args.deleted() {
            // A recursive service listing can return versions of directory markers
            // that a delimiter listing would fold into a common prefix.
            args.set_recursive(pattern.spans_directories());
            return Ok(Box::new(FilteredLister {
                inner: self.inner.list(ctx, path, args)?,
                pattern,
                base: base.to_owned(),
            }));
        }

        args.set_recursive(false);
        let mut directory = if base == "/" {
            String::new()
        } else {
            base.to_owned()
        };
        let mut position = 0;
        for component in pattern.literal_directories() {
            directory.push_str(component);
            directory.push('/');
            position += 1;
        }
        if directory.is_empty() {
            directory.push('/');
        }
        let prefix = if path == "/" { "" } else { path };
        let directories = if position == 0 || directory.starts_with(prefix) {
            vec![Directory {
                path: directory,
                positions: vec![position],
                lister: None,
                discovered: false,
            }]
        } else {
            Vec::new()
        };
        Ok(Box::new(GuidedLister {
            ctx: ctx.clone(),
            inner: self.inner.clone(),
            pattern,
            args,
            prefix: prefix.to_owned(),
            directories,
        }))
    }
}

struct Directory {
    path: String,
    positions: Vec<usize>,
    lister: Option<oio::Lister>,
    /// Found while listing a parent rather than requested by the caller.
    discovered: bool,
}

impl Directory {
    async fn next(
        &mut self,
        ctx: &OperationContext,
        inner: &Servicer,
        args: &OpList,
    ) -> Result<Option<oio::Entry>> {
        if self.lister.is_none() {
            self.lister = Some(inner.list(ctx, &self.path, args.clone())?);
        }
        self.lister
            .as_mut()
            .expect("directory listing is open")
            .next()
            .await
    }
}

struct GuidedLister {
    ctx: OperationContext,
    inner: Servicer,
    pattern: GlobPattern,
    args: OpList,
    prefix: String,
    directories: Vec<Directory>,
}

impl List for GuidedLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        while let Some(directory) = self.directories.last_mut() {
            let entry = match directory.next(&self.ctx, &self.inner, &self.args).await {
                Ok(Some(entry)) => entry,
                Ok(None) => {
                    self.directories.pop();
                    continue;
                }
                Err(err)
                    if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) =>
                {
                    self.directories.pop();
                    continue;
                }
                // Like recursive listings, skip discovered directories that cannot
                // be read. The starting directory still reports the error.
                Err(err) if err.kind() == ErrorKind::PermissionDenied && directory.discovered => {
                    log::warn!("glob listing skipped directory {}: {err}", directory.path);
                    self.directories.pop();
                    continue;
                }
                Err(err) => return Err(err),
            };
            if !entry.path().starts_with(&self.prefix) {
                continue;
            }
            let Some(name) = relative_child(entry.path(), &directory.path) else {
                continue;
            };
            let name = name.trim_end_matches('/');
            if name.contains('/') {
                continue;
            }
            let positions = self.pattern.advance(&directory.positions, name);
            let is_dir = entry.mode().is_dir();
            let matches = self.pattern.accepts(&positions, is_dir);
            // All viable positions travel with the same child. Overlapping
            // recursive matches therefore visit and emit each entry only once.
            if is_dir && self.pattern.can_descend(&positions) {
                self.directories.push(Directory {
                    path: entry.path().to_owned(),
                    positions,
                    lister: None,
                    discovered: true,
                });
            }
            if matches {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
}

struct FilteredLister {
    inner: oio::Lister,
    pattern: GlobPattern,
    base: String,
}

impl List for FilteredLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        while let Some(entry) = self.inner.next().await? {
            if let Some(path) = relative_child(entry.path(), &self.base)
                && self.pattern.matches(path, entry.mode().is_dir())
            {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
}

fn relative_child<'a>(path: &'a str, directory: &str) -> Option<&'a str> {
    let relative = if directory == "/" {
        path.trim_start_matches('/')
    } else {
        path.strip_prefix(directory)?
    };
    (!relative.is_empty()).then_some(relative)
}

#[cfg(test)]
mod tests;
