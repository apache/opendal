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

use crate::auth::{BaiduNetdiskSigner, TokenRefreshCallback};
use crate::{
    BAIDU_NETDISK_SCHEME, config::BaiduNetdiskConfig, core::BaiduNetdiskCore,
    deleter::BaiduNetdiskDeleter, lister::BaiduNetdiskLister, reader::BaiduNetdiskReader,
    writer::BaiduNetdiskWriter,
};
use opendal_core::{raw::*, *};
use std::sync::Arc;

/// Builds an operator for the Baidu Netdisk Open Platform.
///
/// The root must be inside the application's `/apps/<application name>/` folder.
/// Writes require the complete file before uploading so all block hashes are known.
#[derive(Default)]
pub struct BaiduNetdiskBuilder {
    pub(super) config: BaiduNetdiskConfig,
    pub(super) on_refresh: Option<TokenRefreshCallback>,
}

impl std::fmt::Debug for BaiduNetdiskBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaiduNetdiskBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl BaiduNetdiskBuilder {
    /// Set the application directory used for every operation.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = Some(root.into());
        self
    }
    /// Set an initial access token. Without refresh credentials, the caller manages expiry.
    pub fn access_token(mut self, token: &str) -> Self {
        self.config.access_token = Some(token.into());
        self
    }
    /// Set the application AppKey used for OAuth refresh.
    pub fn client_id(mut self, value: &str) -> Self {
        self.config.client_id = Some(value.into());
        self
    }
    /// Set the application SecretKey used for OAuth refresh.
    pub fn client_secret(mut self, value: &str) -> Self {
        self.config.client_secret = Some(value.into());
        self
    }
    /// Set the latest single-use OAuth refresh token.
    pub fn refresh_token(mut self, value: &str) -> Self {
        self.config.refresh_token = Some(value.into());
        self
    }
    /// Persist rotated tokens after each successful OAuth refresh.
    ///
    /// The callback receives the new access token and refresh token. It runs under
    /// the refresh lock and must not call this operator. If it fails, subsequent
    /// requests retry the callback without consuming another refresh token.
    /// Without a callback, rotated tokens are retained only in memory.
    pub fn on_token_refresh(
        mut self,
        callback: impl Fn(&str, &str) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.on_refresh = Some(Arc::new(callback));
        self
    }
}

impl Builder for BaiduNetdiskBuilder {
    type Config = BaiduNetdiskConfig;
    fn build(self) -> Result<impl Service> {
        let root = normalize_root(self.config.root.as_deref().unwrap_or_default());
        if !root.starts_with("/apps/") || root.trim_matches('/').split('/').count() < 2 {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "root must be inside /apps/<application name>/",
            ));
        }
        let signer = BaiduNetdiskSigner::new(&self.config, self.on_refresh)?;
        Ok(BaiduNetdiskBackend {
            core: Arc::new(BaiduNetdiskCore {
                info: ServiceInfo::new(BAIDU_NETDISK_SCHEME, &root, ""),
                root,
                signer,
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct BaiduNetdiskBackend {
    pub core: Arc<BaiduNetdiskCore>,
}

impl Service for BaiduNetdiskBackend {
    type Reader = oio::StreamReader<BaiduNetdiskReader>;
    type Writer = oio::OneShotWriter<BaiduNetdiskWriter>;
    type Lister = oio::PageLister<BaiduNetdiskLister>;
    type Deleter = oio::OneShotDeleter<BaiduNetdiskDeleter>;
    type Copier = oio::OneShotCopier;
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }
    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            read_with_suffix: true,
            write: true,
            write_can_empty: true,
            write_total_max_size: Some((4u64 * 1024 * 1024 * 1024).min(usize::MAX as u64) as usize),
            create_dir: true,
            list: true,
            list_with_limit: true,
            delete: true,
            copy: true,
            rename: true,
            shared: true,
            ..Default::default()
        }
    }
    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "Baidu Netdisk does not support presigning",
        ))
    }
    async fn stat(&self, ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        Ok(RpStat::new(self.core.stat(ctx, path).await?.metadata()?))
    }
    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.core.create_dir(ctx, path).await?;
        Ok(RpCreateDir::default())
    }
    fn read(&self, ctx: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(oio::StreamReader::new(BaiduNetdiskReader {
            core: self.core.clone(),
            ctx: ctx.clone(),
            path: path.into(),
        }))
    }
    fn write(&self, ctx: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        Ok(oio::OneShotWriter::new(BaiduNetdiskWriter {
            core: self.core.clone(),
            ctx: ctx.clone(),
            path: path.into(),
        }))
    }
    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        Ok(oio::PageLister::new(BaiduNetdiskLister {
            core: self.core.clone(),
            ctx: ctx.clone(),
            path: path.into(),
            limit: args.limit().unwrap_or(1000).clamp(1, 1000),
        }))
    }
    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        Ok(oio::OneShotDeleter::new(BaiduNetdiskDeleter {
            core: self.core.clone(),
            ctx: ctx.clone(),
        }))
    }
    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _: OpCopy,
    ) -> Result<Self::Copier> {
        let core = self.core.clone();
        let ctx = ctx.clone();
        let from = from.to_string();
        let to = to.to_string();
        // Rebuild the request so RetryLayer can retry filemanager's busy errors.
        Ok(oio::OneShotCopier::new_with(move || {
            let core = core.clone();
            let ctx = ctx.clone();
            let from = from.clone();
            let to = to.clone();
            async move {
                let metadata = core.stat(&ctx, &from).await?.metadata()?;
                if from != to {
                    core.transfer(&ctx, "copy", &from, &to).await?;
                }
                Ok(metadata)
            }
        }))
    }
    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        if from != to {
            // The move API rejects existing destinations even with ondup=overwrite.
            // Use copy then delete consistently to provide overwrite semantics.
            self.core.transfer(ctx, "copy", from, to).await?;
            self.core
                .manage(
                    ctx,
                    "delete",
                    serde_json::json!([self.core.absolute_path(from)]),
                )
                .await?;
        }
        Ok(RpRename::default())
    }
}
