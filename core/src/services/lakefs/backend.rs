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
use chrono::TimeZone;
use chrono::Utc;
use http::Response;
use http::StatusCode;
use log::debug;

use super::core::LakefsCore;
use super::core::LakefsStatus;
use super::delete::LakefsDeleter;
use super::error::parse_error;
use super::lister::LakefsLister;
use super::writer::LakefsWriter;
use crate::raw::*;
use crate::services::LakefsConfig;
use crate::*;

impl Configurator for LakefsConfig {
    type Builder = LakefsBuilder;
    fn into_builder(self) -> Self::Builder {
        LakefsBuilder { config: self }
    }
}

/// [Lakefs](https://docs.lakefs.io/reference/api.html#/)'s API support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct LakefsBuilder {
    config: LakefsConfig,
}

impl Debug for LakefsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("config", &self.config);
        ds.finish()
    }
}

impl LakefsBuilder {
    /// Set the endpoint of this backend.
    ///
    /// endpoint must be full uri.
    ///
    /// This is required.
    /// - `http://127.0.0.1:8000` (lakefs daemon in local)
    /// - `https://my-lakefs.example.com` (lakefs server)
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.to_string());
        }
        self
    }

    /// Set username of this backend. This is required.
    pub fn username(mut self, username: &str) -> Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_string());
        }
        self
    }

    /// Set password of this backend. This is required.
    pub fn password(mut self, password: &str) -> Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_string());
        }
        self
    }

    /// Set branch of this backend or a commit ID. Default is main.
    ///
    /// Branch can be a branch name.
    ///
    /// For example, branch can be:
    /// - main
    /// - 1d0c4eb
    pub fn branch(mut self, branch: &str) -> Self {
        if !branch.is_empty() {
            self.config.branch = Some(branch.to_string());
        }
        self
    }

    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        if !root.is_empty() {
            self.config.root = Some(root.to_string());
        }
        self
    }

    /// Set the repository of this backend.
    ///
    /// This is required.
    pub fn repository(mut self, repository: &str) -> Self {
        if !repository.is_empty() {
            self.config.repository = Some(repository.to_string());
        }
        self
    }
}

impl Builder for LakefsBuilder {
    const SCHEME: Scheme = Scheme::Lakefs;
    type Config = LakefsConfig;

    /// Build a LakefsBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let endpoint = match self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Lakefs)),
        }?;
        debug!("backend use endpoint: {:?}", &endpoint);

        let repository = match &self.config.repository {
            Some(repository) => Ok(repository.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "repository is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Lakefs)),
        }?;
        debug!("backend use repository: {}", &repository);

        let branch = match &self.config.branch {
            Some(branch) => branch.clone(),
            None => "main".to_string(),
        };
        debug!("backend use branch: {}", &branch);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root: {}", &root);

        let username = match &self.config.username {
            Some(username) => Ok(username.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "username is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Lakefs)),
        }?;

        let password = match &self.config.password {
            Some(password) => Ok(password.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "password is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Lakefs)),
        }?;

        Ok(LakefsBackend {
            core: Arc::new(LakefsCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Lakefs)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_content_length: true,
                            stat_has_content_disposition: true,
                            stat_has_last_modified: true,

                            list: true,
                            list_has_content_length: true,
                            list_has_last_modified: true,

                            read: true,
                            write: true,
                            delete: true,
                            copy: true,
                            shared: true,
                            ..Default::default()
                        });
                    am.into()
                },
                endpoint,
                repository,
                branch,
                root,
                username,
                password,
            }),
        })
    }
}

/// Backend for Lakefs service
#[derive(Debug, Clone)]
pub struct LakefsBackend {
    core: Arc<LakefsCore>,
}

impl Access for LakefsBackend {
    type Reader = HttpBody;
    type Writer = oio::OneShotWriter<LakefsWriter>;
    type Lister = oio::PageLister<LakefsLister>;
    type Deleter = oio::OneShotDeleter<LakefsDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.core.get_object_metadata(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let mut meta = parse_into_metadata(path, resp.headers())?;
                let bs = resp.clone().into_body();

                let decoded_response: LakefsStatus =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                if let Some(size_bytes) = decoded_response.size_bytes {
                    meta.set_content_length(size_bytes);
                }
                meta.set_mode(EntryMode::FILE);
                if let Some(v) = parse_content_disposition(resp.headers())? {
                    meta.set_content_disposition(v);
                }

                meta.set_last_modified(Utc.timestamp_opt(decoded_response.mtime, 0).unwrap());

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self
            .core
            .get_object_content(path, args.range(), &args)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), resp.into_body()))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = LakefsLister::new(
            self.core.clone(),
            path.to_string(),
            args.limit(),
            args.start_after(),
            args.recursive(),
        );

        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(LakefsWriter::new(self.core.clone(), path.to_string(), args)),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(LakefsDeleter::new(self.core.clone())),
        ))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self.core.copy_object(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }
}
