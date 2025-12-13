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

use opendal_core::raw::*;
use opendal_core::*;

use super::config::WasiFsConfig;
use super::core::WasiFsCore;
use super::deleter::WasiFsDeleter;
use super::lister::WasiFsLister;
use super::reader::WasiFsReader;
use super::writer::WasiFsWriter;
use super::WASI_FS_SCHEME;

/// WASI Filesystem service builder.
#[derive(Debug, Default)]
pub struct WasiFsBuilder {
    pub(super) config: WasiFsConfig,
}

impl WasiFsBuilder {
    /// Set the root path for the WASI filesystem.
    ///
    /// The root must be within a preopened directory.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = if path.is_empty() {
            None
        } else {
            Some(path.to_string())
        };
        self
    }
}

impl Builder for WasiFsBuilder {
    type Config = WasiFsConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(&self.config.root.unwrap_or_default());

        let core = WasiFsCore::new(&root)?;

        let mut info = AccessorInfo::default();
        info.set_scheme(WASI_FS_SCHEME)
            .set_root(&root)
            .set_native_capability(Capability {
                stat: true,
                read: true,
                write: true,
                write_can_empty: true,
                create_dir: true,
                delete: true,
                list: true,
                copy: true,
                rename: true,
                shared: false,
                ..Default::default()
            });

        Ok(WasiFsBackend {
            core: Arc::new(core),
            info: Arc::new(info),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WasiFsBackend {
    core: Arc<WasiFsCore>,
    info: Arc<AccessorInfo>,
}

impl Access for WasiFsBackend {
    type Reader = WasiFsReader;
    type Writer = WasiFsWriter;
    type Lister = WasiFsLister;
    type Deleter = oio::OneShotDeleter<WasiFsDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.create_dir(path)?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let metadata = self.core.stat(path)?;
        Ok(RpStat::new(metadata))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let reader = WasiFsReader::new(self.core.clone(), path, args.range())?;
        Ok((RpRead::new(), reader))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = WasiFsWriter::new(self.core.clone(), path, args)?;
        Ok((RpWrite::default(), writer))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let deleter = WasiFsDeleter::new(self.core.clone());
        Ok((RpDelete::default(), oio::OneShotDeleter::new(deleter)))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let lister = WasiFsLister::new(self.core.clone(), path)?;
        Ok((RpList::default(), lister))
    }

    async fn copy(&self, from: &str, to: &str, _: OpCopy) -> Result<RpCopy> {
        self.core.copy(from, to)?;
        Ok(RpCopy::default())
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        self.core.rename(from, to)?;
        Ok(RpRename::default())
    }
}
