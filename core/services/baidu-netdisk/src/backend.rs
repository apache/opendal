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

use crate::{
    BAIDU_NETDISK_SCHEME,
    config::BaiduNetdiskConfig,
    core::BaiduNetdiskCore,
    deleter::BaiduNetdiskDeleter,
    lister::BaiduNetdiskLister,
    reader::BaiduNetdiskReader,
    writer::{BaiduNetdiskWriter, MAX_WRITE_SIZE},
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
    /// Set the access token. The caller manages authorization and renewal.
    pub fn access_token(mut self, token: &str) -> Self {
        self.config.access_token = Some(token.into());
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
        let access_token = self
            .config
            .access_token
            .filter(|v| !v.is_empty())
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "access_token is required"))?;
        Ok(BaiduNetdiskBackend {
            core: Arc::new(BaiduNetdiskCore {
                info: ServiceInfo::new(BAIDU_NETDISK_SCHEME, &root, ""),
                root,
                access_token,
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
    type Writer = BaiduNetdiskWriter;
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
            write_total_max_size: Some(MAX_WRITE_SIZE),
            create_dir: true,
            list: true,
            list_with_limit: true,
            delete: true,
            delete_with_recursive: true,
            copy: true,
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
        Ok(BaiduNetdiskWriter {
            buffer: None,
            core: self.core.clone(),
            ctx: ctx.clone(),
            path: path.into(),
        })
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
                let source = core.stat(&ctx, &from).await?;
                if source.isdir == 1 {
                    return Err(Error::new(
                        ErrorKind::IsADirectory,
                        "copy source is a directory",
                    ));
                }
                if from == to {
                    return source.metadata();
                }
                let destination_id = core.copy(&ctx, &from, &to).await?;
                let destination = core.stat(&ctx, &to).await?;
                if destination.fs_id != destination_id {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "copy destination was replaced",
                    ));
                }
                destination.metadata()
            }
        }))
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
            "Baidu Netdisk does not support rename with overwrite",
        ))
    }
}
