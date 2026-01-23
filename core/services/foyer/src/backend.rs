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

use foyer::HybridCache;
use log::debug;

use super::FOYER_SCHEME;
use super::FoyerKey;
use super::FoyerValue;
use super::config::FoyerConfig;
use super::core::FoyerCore;
use super::deleter::FoyerDeleter;
use super::writer::FoyerWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// [foyer](https://github.com/foyer-rs/foyer) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct FoyerBuilder {
    pub(super) config: FoyerConfig,
    pub(super) cache: Option<HybridCache<FoyerKey, FoyerValue>>,
}

impl Debug for FoyerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl FoyerBuilder {
    /// Create a [`FoyerBuilder`] with the given [`foyer::HybridCache`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use opendal_service_foyer::Foyer;
    /// use opendal_service_foyer::FoyerKey;
    /// use opendal_service_foyer::FoyerValue;
    /// use foyer::{HybridCacheBuilder, Engine};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cache = HybridCacheBuilder::new()
    ///     .memory(64 * 1024 * 1024)
    ///     .storage(Engine::Large(Default::default()))
    ///     .build()
    ///     .await?;
    ///
    /// let builder = Foyer::new(cache);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(cache: HybridCache<FoyerKey, FoyerValue>) -> Self {
        Self {
            cache: Some(cache),
            ..Default::default()
        }
    }

    /// Set the name of this cache instance.
    pub fn name(mut self, name: &str) -> Self {
        if !name.is_empty() {
            self.config.name = Some(name.to_owned());
        }
        self
    }

    /// Set the root path of this backend.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = if path.is_empty() {
            None
        } else {
            Some(path.to_string())
        };
        self
    }
}

impl Builder for FoyerBuilder {
    type Config = FoyerConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let cache = self.cache.ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "foyer cache is required, use Foyer::new(cache) to create builder",
            )
        })?;

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        debug!("backend build finished: {:?}", self.config);

        let core = FoyerCore::new(cache, self.config.name.clone());

        Ok(FoyerBackend::new(core).with_normalized_root(root))
    }
}

/// FoyerBackend implements Access trait.
#[derive(Debug, Clone)]
pub struct FoyerBackend {
    core: Arc<FoyerCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl FoyerBackend {
    fn new(core: FoyerCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(FOYER_SCHEME);
        info.set_name(core.name().unwrap_or("foyer"));
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            write: true,
            write_can_empty: true,
            delete: true,
            stat: true,
            shared: true,
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

impl Access for FoyerBackend {
    type Reader = Buffer;
    type Writer = FoyerWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<FoyerDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            match self.core.get(&p).await? {
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(ErrorKind::NotFound, "key not found in foyer")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);

        let buffer = match self.core.get(&p).await? {
            Some(bs) => bs,
            None => return Err(Error::new(ErrorKind::NotFound, "key not found in foyer")),
        };

        let buffer = if args.range().is_full() {
            buffer
        } else {
            let range = args.range();
            let start = range.offset() as usize;
            let end = match range.size() {
                Some(size) => (range.offset() + size) as usize,
                None => buffer.len(),
            };
            buffer.slice(start..end.min(buffer.len()))
        };

        Ok((RpRead::new(), buffer))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), FoyerWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(FoyerDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }
}
