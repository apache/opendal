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

#![deny(missing_docs)]

//! Verify stored full-object checksums during reads.

mod reader;

use std::sync::Arc;

use opendal_core::raw::*;
use opendal_core::{Capability, OperationContext, Result};

use crate::reader::ChecksumReader;

/// Verify a stored checksum when a single read response covers the whole object.
///
/// Verification is enabled by default. Whole-object reads without a supported
/// checksum succeed unless [`require_read`](Self::require_read) is enabled. A
/// mismatch returns a permanent [`ChecksumMismatch`](opendal_core::ErrorKind::ChecksumMismatch).
/// When the response reports the object length, the layer checks the digest
/// before delivering the final buffer. If the length is unknown, it checks at
/// EOF. Earlier buffers may already have reached the caller.
///
/// On services that advertise read checksum support, verification requests the
/// checksum on unbounded and nonempty bounded forward reads starting at offset zero.
/// A prefix read or the first chunk of a chunked read can therefore request a
/// checksum. The layer keeps the original range and only verifies a bounded
/// read when its response metadata shows that it covers the whole object.
/// Partial reads are exempt even when a checksum is required.
///
/// Place this layer outside the retry layer to verify the entire stream across
/// a resumed read. With mixed routing capabilities, install this layer on the
/// individual operators so each route can opt in according to its own support.
/// Outside a cache layer, verification uses the checksum on the response the
/// cache returns. Inside a cache layer, verification applies to origin reads.
#[derive(Clone, Debug)]
pub struct ChecksumLayer {
    verify_read: bool,
    require_read: bool,
}

impl Default for ChecksumLayer {
    fn default() -> Self {
        Self {
            verify_read: true,
            require_read: false,
        }
    }
}

impl ChecksumLayer {
    /// Create a layer with best-effort read verification enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable read verification.
    ///
    /// Disabling verification also disables automatic checksum requests and
    /// the requirement configured by [`Self::require_read`].
    pub fn verify_read(mut self, enabled: bool) -> Self {
        self.verify_read = enabled;
        self
    }

    /// Require a supported checksum for recognizable whole-object reads.
    ///
    /// If verification is enabled and the response has no supported checksum,
    /// the read returns [`Unsupported`](opendal_core::ErrorKind::Unsupported).
    pub fn require_read(mut self, required: bool) -> Self {
        self.require_read = required;
        self
    }
}

impl Layer for ChecksumLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        if !self.verify_read {
            return inner;
        }
        Arc::new(ChecksumService {
            inner,
            require_read: self.require_read,
        })
    }
}

#[derive(Debug)]
struct ChecksumService {
    inner: Servicer,
    require_read: bool,
}

impl Service for ChecksumService {
    type Reader = ChecksumReader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
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
        let args = if self.inner.capability().read_with_checksum {
            args.with_checksum_for_zero_offset()
        } else {
            args
        };
        let inner = self.inner.read(ctx, path, args)?;
        Ok(ChecksumReader::new(inner, self.require_read))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner.write(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
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
