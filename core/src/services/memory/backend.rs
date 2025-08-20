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

use super::core::*;
use super::delete::MemoryDeleter;
use super::lister::MemoryLister;
use super::writer::MemoryWriter;
use super::DEFAULT_SCHEME;
use crate::raw::oio;
use crate::raw::*;
use crate::services::MemoryConfig;
use crate::*;
impl Configurator for MemoryConfig {
    type Builder = MemoryBuilder;
    fn into_builder(self) -> Self::Builder {
        MemoryBuilder { config: self }
    }
}

/// In memory service support. (BTreeMap Based)
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct MemoryBuilder {
    config: MemoryConfig,
}

impl MemoryBuilder {
    /// Set the root for BTreeMap.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = Some(path.into());
        self
    }
}

impl Builder for MemoryBuilder {
    type Config = MemoryConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(self.config.root.as_deref().unwrap_or("/"));

        let core = MemoryCore::new();
        Ok(MemoryAccessor::new(core).with_normalized_root(root))
    }
}

/// MemoryAccessor implements Access trait directly
#[derive(Debug, Clone)]
pub struct MemoryAccessor {
    core: Arc<MemoryCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl MemoryAccessor {
    fn new(core: MemoryCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(DEFAULT_SCHEME);
        info.set_name(&format!("{:p}", Arc::as_ptr(&core.data)));
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            write: true,
            write_can_empty: true,
            write_with_cache_control: true,
            write_with_content_type: true,
            write_with_content_disposition: true,
            write_with_content_encoding: true,
            delete: true,
            stat: true,
            list: true,
            list_with_recursive: true,
            shared: false,
            ..Default::default()
        });

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info: Arc::new(info),
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info.set_root(&root);
        self.root = root;
        self
    }
}

impl Access for MemoryAccessor {
    type Reader = Buffer;
    type Writer = MemoryWriter;
    type Lister = oio::HierarchyLister<MemoryLister>;
    type Deleter = oio::OneShotDeleter<MemoryDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            match self.core.get(&p)? {
                Some(value) => Ok(RpStat::new(value.metadata)),
                None => Err(Error::new(
                    ErrorKind::NotFound,
                    "memory doesn't have this path",
                )),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        let value = match self.core.get(&p)? {
            Some(value) => value,
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "memory doesn't have this path",
                ))
            }
        };

        Ok((
            RpRead::new(),
            value.content.slice(args.range().to_range_as_usize()),
        ))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((
            RpWrite::new(),
            MemoryWriter::new(self.core.clone(), p, args),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(MemoryDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let p = build_abs_path(&self.root, path);
        let keys = self.core.scan(&p)?;
        let lister = MemoryLister::new(&self.root, keys);
        let lister = oio::HierarchyLister::new(lister, path, args.recursive());

        Ok((RpList::default(), lister))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accessor_metadata_name() {
        let b1 = MemoryBuilder::default().build().unwrap();
        assert_eq!(b1.info().name(), b1.info().name());

        let b2 = MemoryBuilder::default().build().unwrap();
        assert_ne!(b1.info().name(), b2.info().name())
    }
}
