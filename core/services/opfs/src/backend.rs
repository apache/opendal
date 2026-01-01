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

use std::fmt::Debug;
use std::sync::Arc;

use super::OPFS_SCHEME;
use super::config::OpfsConfig;
use super::core::OpfsCore;
use super::delete::OpfsDeleter;

use opendal_core::raw::*;
use opendal_core::*;

#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct OpfsBuilder {
    pub(super) config: OpfsConfig,
}

impl OpfsBuilder {
    /// Set root for backend.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
}

impl Builder for OpfsBuilder {
    type Config = OpfsConfig;

    fn build(self) -> Result<impl Access> {
        let root = self.config.root.ok_or(
            Error::new(ErrorKind::ConfigInvalid, "root is not specified")
                .with_operation("Builder::build"),
        )?;

        let info = AccessorInfo::default();
        info.set_scheme(OPFS_SCHEME)
            .set_root(&root)
            .set_native_capability(Capability {
                stat: true,
                create_dir: true,
                delete: true,
                delete_with_recursive: true,
                ..Default::default()
            });

        let core = Arc::new(OpfsCore::new(Arc::new(info), root));

        Ok(OpfsBackend::new(core))
    }
}

/// OPFS Service backend
#[derive(Default, Debug, Clone)]
pub struct OpfsBackend {
    core: Arc<OpfsCore>,
}

impl OpfsBackend {
    pub(crate) fn new(core: Arc<OpfsCore>) -> Self {
        Self { core }
    }
}

impl Access for OpfsBackend {
    type Reader = ();

    type Writer = ();

    type Lister = ();

    type Deleter = oio::OneShotDeleter<OpfsDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let metadata = self.core.opfs_stat(path).await?;
        Ok(RpStat::new(metadata))
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.opfs_create_dir(path).await?;

        Ok(RpCreateDir::default())
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let deleter = oio::OneShotDeleter::new(OpfsDeleter::new(self.core.clone()));
        Ok((RpDelete::default(), deleter))
    }
}
