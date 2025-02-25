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
use http::Response;
use http::StatusCode;
use log::debug;

use super::core::parse_info;
use super::core::ChainsafeCore;
use super::core::ObjectInfoResponse;
use super::delete::ChainsafeDeleter;
use super::error::parse_error;
use super::lister::ChainsafeLister;
use super::writer::ChainsafeWriter;
use super::writer::ChainsafeWriters;
use crate::raw::*;
use crate::services::ChainsafeConfig;
use crate::*;

impl Configurator for ChainsafeConfig {
    type Builder = ChainsafeBuilder;
    fn into_builder(self) -> Self::Builder {
        ChainsafeBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [chainsafe](https://storage.chainsafe.io/) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct ChainsafeBuilder {
    config: ChainsafeConfig,

    http_client: Option<HttpClient>,
}

impl Debug for ChainsafeBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("ChainsafeBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl ChainsafeBuilder {
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

    /// api_key of this backend.
    ///
    /// required.
    pub fn api_key(mut self, api_key: &str) -> Self {
        self.config.api_key = if api_key.is_empty() {
            None
        } else {
            Some(api_key.to_string())
        };

        self
    }

    /// Set bucket_id name of this backend.
    pub fn bucket_id(mut self, bucket_id: &str) -> Self {
        self.config.bucket_id = bucket_id.to_string();

        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for ChainsafeBuilder {
    const SCHEME: Scheme = Scheme::Chainsafe;
    type Config = ChainsafeConfig;

    /// Builds the backend and returns the result of ChainsafeBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle bucket_id.
        if self.config.bucket_id.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "bucket_id is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Chainsafe));
        }

        debug!("backend use bucket_id {}", &self.config.bucket_id);

        let api_key = match &self.config.api_key {
            Some(api_key) => Ok(api_key.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "api_key is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Chainsafe)),
        }?;

        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Chainsafe)
            })?
        };

        Ok(ChainsafeBackend {
            core: Arc::new(ChainsafeCore {
                root,
                api_key,
                bucket_id: self.config.bucket_id.clone(),
                client,
            }),
        })
    }
}

/// Backend for Chainsafe services.
#[derive(Debug, Clone)]
pub struct ChainsafeBackend {
    core: Arc<ChainsafeCore>,
}

impl Access for ChainsafeBackend {
    type Reader = HttpBody;
    type Writer = ChainsafeWriters;
    type Lister = oio::PageLister<ChainsafeLister>;
    type Deleter = oio::OneShotDeleter<ChainsafeDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let am = AccessorInfo::default();
        am.set_scheme(Scheme::Chainsafe)
            .set_root(&self.core.root)
            .set_native_capability(Capability {
                stat: true,
                stat_has_content_length: true,
                stat_has_content_type: true,

                read: true,

                create_dir: true,
                write: true,
                write_can_empty: true,

                delete: true,

                list: true,
                list_has_content_length: true,
                list_has_content_type: true,

                shared: true,

                ..Default::default()
            });

        am.into()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let resp = self.core.create_dir(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCreateDir::default()),
            // Allow 409 when creating a existing dir
            StatusCode::CONFLICT => Ok(RpCreateDir::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let resp = self.core.object_info(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let output: ObjectInfoResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                Ok(RpStat::new(parse_info(output.content)))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.download_object(path, args.range()).await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok((RpRead::new(), resp.into_body())),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let writer = ChainsafeWriter::new(self.core.clone(), args, path.to_string());

        let w = oio::OneShotWriter::new(writer);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(ChainsafeDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = ChainsafeLister::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
