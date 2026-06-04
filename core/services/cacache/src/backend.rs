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

use super::CACACHE_SCHEME;
use super::config::CacacheConfig;
use super::core::CacacheCore;
use super::deleter::CacacheDeleter;
use super::writer::CacacheWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// cacache service support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct CacacheBuilder {
    pub(super) config: CacacheConfig,
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
                .with_context("service", CACACHE_SCHEME)
        })?;

        let core = CacacheCore {
            path: datadir_path.clone(),
        };

        let info = AccessorInfo::default();
        info.set_scheme(CACACHE_SCHEME);
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

        Ok(CacacheBackend {
            core: Arc::new(core),
            info: Arc::new(info),
        })
    }
}

/// Backend for cacache services.
#[derive(Debug, Clone)]
pub struct CacacheBackend {
    core: Arc<CacacheCore>,
    info: Arc<AccessorInfo>,
}

/// Reader returned by this backend.
pub struct CacacheReader {
    backend: CacacheBackend,
    path: String,
}

impl CacacheReader {
    fn new(backend: CacacheBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for CacacheReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let result: Result<(RpRead, Buffer)> = async {
            let data = backend.core.get(path).await?;

            match data {
                Some(bytes) => {
                    let content_length = bytes.len() as u64;
                    let buffer = Buffer::from(bytes.slice(range.to_content_range(bytes.len())?));
                    let metadata =
                        Metadata::new(EntryMode::FILE).with_content_length(content_length);
                    Ok((RpRead::new(metadata), buffer))
                }
                None => Err(Error::new(ErrorKind::NotFound, "entry not found")),
            }
        }
        .await;
        result.map(|(rp, stream)| (rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

impl Access for CacacheBackend {
    type Reader = oio::StreamReader<CacacheReader>;
    type Writer = CacacheWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<CacacheDeleter>;
    type Copier = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let metadata = self.core.metadata(path).await?;

        match metadata {
            Some(meta) => {
                let mut md = Metadata::new(EntryMode::FILE);
                md.set_content_length(meta.size as u64);
                // Convert u128 milliseconds to Timestamp
                let millis = meta.time as i64;
                if let Ok(dt) = Timestamp::from_millisecond(millis) {
                    md.set_last_modified(dt);
                }
                Ok(RpStat::new(md))
            }
            None => Err(Error::new(ErrorKind::NotFound, "entry not found")),
        }
    }
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::default(),
            oio::StreamReader::new(CacacheReader::new(self.clone(), path, args)),
        ))
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
