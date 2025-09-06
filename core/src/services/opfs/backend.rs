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

use super::core::OpfsCore;
use super::reader::OpfsReader;
use super::writer::OpfsWriter;
use crate::raw::oio::OneShotDeleter;
use crate::services::opfs::delete::OpfsDeleter;
use crate::services::opfs::lister::OpfsLister;
use crate::Result;
use crate::{raw::*, Capability};

/// OPFS Service backend
#[derive(Debug, Clone)]
pub struct OpfsBackend {
    core: Arc<OpfsCore>,
}

impl OpfsBackend {
    pub(crate) fn new(core: Arc<OpfsCore>) -> Self {
        Self { core }
    }
}

impl Access for OpfsBackend {
    type Reader = OpfsReader;

    type Writer = OpfsWriter;

    type Lister = OpfsLister;

    type Deleter = OneShotDeleter<OpfsDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        info.set_native_capability(Capability {
            stat: true,

            read: true,

            write: true,
            write_can_empty: true,
            write_can_append: true,
            write_can_multi: true,
            // write_with_if_not_exists: true,
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

    async fn write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = OpfsWriter::new(self.core.clone(), path, op).await?;
        Ok((RpWrite::default(), writer))
    }

    async fn read(&self, path: &str, op: OpRead) -> Result<(RpRead, Self::Reader)> {
        let reader = OpfsReader::new(self.core.clone(), path, &op).await?;
        Ok((RpRead::default(), reader))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let deleter = OneShotDeleter::new(OpfsDeleter::new(self.core.clone()));
        Ok((RpDelete::default(), deleter))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        // TODO: list all entries recursively
        let lister = OpfsLister::new(self.core.clone(), path).await?;
        Ok((RpList::default(), lister))
    }
}
