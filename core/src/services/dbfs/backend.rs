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
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use http::StatusCode;
use log::debug;
use serde::Deserialize;

use super::core::DbfsCore;
use super::delete::DbfsDeleter;
use super::error::parse_error;
use super::lister::DbfsLister;
use super::writer::DbfsWriter;
use crate::raw::*;
use crate::services::DbfsConfig;
use crate::*;

impl Configurator for DbfsConfig {
    type Builder = DbfsBuilder;
    fn into_builder(self) -> Self::Builder {
        DbfsBuilder { config: self }
    }
}

/// [Dbfs](https://docs.databricks.com/api/azure/workspace/dbfs)'s REST API support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct DbfsBuilder {
    config: DbfsConfig,
}

impl Debug for DbfsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("DbfsBuilder");

        ds.field("config", &self.config);

        ds.finish()
    }
}

impl DbfsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - Azure: `https://adb-1234567890123456.78.azuredatabricks.net`
    /// - Aws: `https://dbc-123a5678-90bc.cloud.databricks.com`
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.trim_end_matches('/').to_string())
        };
        self
    }

    /// Set the token of this backend.
    pub fn token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.token = Some(token.to_string());
        }
        self
    }
}

impl Builder for DbfsBuilder {
    const SCHEME: Scheme = Scheme::Dbfs;
    type Config = DbfsConfig;

    /// Build a DbfsBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Dbfs)),
        }?;
        debug!("backend use endpoint: {}", &endpoint);

        let token = match self.config.token {
            Some(token) => token,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "missing token for Dbfs",
                ));
            }
        };

        let client = HttpClient::new()?;
        Ok(DbfsBackend {
            core: Arc::new(DbfsCore {
                root,
                endpoint: endpoint.to_string(),
                token,
                client,
            }),
        })
    }
}

/// Backend for DBFS service
#[derive(Debug, Clone)]
pub struct DbfsBackend {
    core: Arc<DbfsCore>,
}

impl Access for DbfsBackend {
    type Reader = ();
    type Writer = oio::OneShotWriter<DbfsWriter>;
    type Lister = oio::PageLister<DbfsLister>;
    type Deleter = oio::OneShotDeleter<DbfsDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let am = AccessorInfo::default();
        am.set_scheme(Scheme::Dbfs)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,
                stat_has_cache_control: true,
                stat_has_content_length: true,
                stat_has_content_type: true,
                stat_has_content_encoding: true,
                stat_has_content_range: true,
                stat_has_etag: true,
                stat_has_content_md5: true,
                stat_has_last_modified: true,
                stat_has_content_disposition: true,

                write: true,
                create_dir: true,
                delete: true,
                rename: true,

                list: true,
                list_has_last_modified: true,
                list_has_content_length: true,

                shared: true,

                ..Default::default()
            });
        am.into()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let resp = self.core.dbfs_create_dir(path).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.core.dbfs_get_status(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let mut meta = parse_into_metadata(path, resp.headers())?;
                let bs = resp.into_body();
                let decoded_response: DbfsStatus =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                meta.set_last_modified(parse_datetime_from_from_timestamp_millis(
                    decoded_response.modification_time,
                )?);
                match decoded_response.is_dir {
                    true => meta.set_mode(EntryMode::DIR),
                    false => {
                        meta.set_mode(EntryMode::FILE);
                        meta.set_content_length(decoded_response.file_size as u64)
                    }
                };
                Ok(RpStat::new(meta))
            }
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(DbfsWriter::new(self.core.clone(), args, path.to_string())),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(DbfsDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = DbfsLister::new(self.core.clone(), path.to_string());

        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        self.core.dbfs_ensure_parent_path(to).await?;

        let resp = self.core.dbfs_rename(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpRename::default()),
            _ => Err(parse_error(resp)),
        }
    }
}

#[derive(Deserialize)]
struct DbfsStatus {
    // Not used fields.
    // path: String,
    is_dir: bool,
    file_size: i64,
    modification_time: i64,
}
