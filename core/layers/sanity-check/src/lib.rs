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

use opendal_core::raw::oio::List;
use opendal_core::raw::*;
use opendal_core::{Capability, Error, ErrorKind, OperationContext, Result};

/// Detects duplicate entries during a listing.
///
/// A normal listing checks for repeated paths. When the caller requests versions
/// or deleted entries, the layer checks for repeated path and version pairs,
/// treating a missing version ID as a distinct value.
///
/// On the first duplicate, the lister returns [`ErrorKind::Unexpected`]. The
/// layer retains the identity of each entry for the lifetime of the listing,
/// so memory usage grows with the number of entries and the lengths of their
/// paths and version IDs.
#[derive(Clone, Debug, Default)]
pub struct SanityCheckLayer;

impl Layer for SanityCheckLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(SanityCheckService { inner })
    }
}

#[derive(Debug)]
struct SanityCheckService {
    inner: Servicer,
}

impl Service for SanityCheckService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = SanityCheckLister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
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

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let compare_versions = args.versions() || args.deleted();
        let inner = self.inner.list(ctx, path, args)?;
        Ok(SanityCheckLister {
            inner,
            compare_versions,
            seen: HashSet::new(),
        })
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
}

struct SanityCheckLister {
    inner: oio::Lister,
    compare_versions: bool,
    seen: HashSet<(String, Option<String>)>,
}

impl List for SanityCheckLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let Some(entry) = self.inner.next().await? else {
            return Ok(None);
        };
        let path = entry.path();
        let version = if self.compare_versions {
            entry.metadata().version()
        } else {
            None
        };
        if !self
            .seen
            .insert((path.to_owned(), version.map(str::to_owned)))
        {
            let mut error = Error::new(ErrorKind::Unexpected, "duplicate entry in list response")
                .with_operation(Operation::List)
                .with_context("path", path);
            if let Some(version) = version {
                error = error.with_context("version", version);
            }
            return Err(error);
        }
        Ok(Some(entry))
    }
}
