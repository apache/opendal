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

use web_sys::FileSystemGetDirectoryOptions;

use super::utils::*;
use crate::raw::*;
use crate::services::opfs::core::OpfsCore;
use crate::services::opfs::delete::OpfsDeleter;
use crate::*;

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
        let info = AccessorInfo::default();
        info.set_native_capability(Capability {
            stat: true,

            read: true,

            write: true,
            write_can_empty: true,
            write_can_append: true,
            write_can_multi: true,

            create_dir: true,
            delete: true,

            list: true,

            ..Default::default()
        });
        Arc::new(info)
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
