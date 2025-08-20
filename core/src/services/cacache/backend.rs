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

use chrono::DateTime;

use crate::raw::*;
use crate::services::CacacheConfig;
use crate::*;

use super::core::CacacheCore;
use super::delete::CacacheDeleter;
use super::writer::CacacheWriter;
use super::DEFAULT_SCHEME;
impl Configurator for CacacheConfig {
    type Builder = CacacheBuilder;
    fn into_builder(self) -> Self::Builder {
        CacacheBuilder { config: self }
    }
}

/// cacache service support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct CacacheBuilder {
    config: CacacheConfig,
}

impl CacacheBuilder {
    /// Set the path to the cacache data directory. Will create if not exists.
    pub fn datadir(mut self, path: &str) -> Self {
        self.config.datadir = Some(path.into());
        self
    }
}

impl Builder for CacacheBuilder {
    type Config = CacacheConfig;

    fn build(self) -> Result<impl Access> {
        let datadir_path = self.config.datadir.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                .with_context("service", Scheme::Cacache)
        })?;

        let core = CacacheCore {
            path: datadir_path.clone(),
        };

        let info = AccessorInfo::default();
        info.set_scheme(DEFAULT_SCHEME);
        info.set_name(&datadir_path);
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            write: true,
            delete: true,
            stat: true,
            rename: false,
            list: false,
            shared: false,
            ..Default::default()
        });

        Ok(CacacheAccessor {
            core: Arc::new(core),
            info: Arc::new(info),
        })
    }
}

/// Backend for cacache services.
#[derive(Debug, Clone)]
pub struct CacacheAccessor {
    core: Arc<CacacheCore>,
    info: Arc<AccessorInfo>,
}

impl Access for CacacheAccessor {
    type Reader = Buffer;
    type Writer = CacacheWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<CacacheDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let metadata = self.core.metadata(path).await?;

        match metadata {
            Some(meta) => {
                let mut md = Metadata::new(EntryMode::FILE);
                md.set_content_length(meta.size as u64);
                // Convert u128 milliseconds to DateTime<Utc>
                let millis = meta.time;
                let secs = (millis / 1000) as i64;
                let nanos = ((millis % 1000) * 1_000_000) as u32;
                if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
                    md.set_last_modified(dt);
                }
                Ok(RpStat::new(md))
            }
            None => Err(Error::new(ErrorKind::NotFound, "entry not found")),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let data = self.core.get(path).await?;

        match data {
            Some(bytes) => {
                let range = args.range();
                let buffer = if range.is_full() {
                    Buffer::from(bytes)
                } else {
                    let start = range.offset() as usize;
                    let end = match range.size() {
                        Some(size) => (range.offset() + size) as usize,
                        None => bytes.len(),
                    };
                    Buffer::from(bytes.slice(start..end.min(bytes.len())))
                };
                Ok((RpRead::new(), buffer))
            }
            None => Err(Error::new(ErrorKind::NotFound, "entry not found")),
        }
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::new(),
            CacacheWriter::new(self.core.clone(), path.to_string()),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(CacacheDeleter::new(self.core.clone())),
        ))
    }
}
